"""Bounded, registered, single-use containers with streamed diagnostic output."""
from collections import defaultdict
import gzip
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time
import uuid

from runner import DockerRunner, IsolationError, ENV
from .inputs import ROOT
from .storage import canonical, digest, file_digest, write_json


class DockerUnavailable(RuntimeError):
    pass


class UnsafeIsolation(RuntimeError):
    pass


class TraceRunner(DockerRunner):
    def __init__(self, cfg, store, stop):
        execution = {**cfg["execution"], "workers": cfg["pipeline"]["collect_workers"],
                     "wall_seconds": cfg["trace"]["wall_seconds"],
                     "startup_seconds": cfg["trace"]["startup_seconds"]}
        super().__init__(execution, store.directory / "docker")
        self.cfg, self.store, self.stop = cfg, store, stop
        self.starts = threading.BoundedSemaphore(cfg["pipeline"]["start_workers"])
        self.registry_lock = threading.RLock()
        self.health_lock = threading.Lock()
        self.key_lock = threading.Lock()
        self.key_locks = {}
        self.last_health = 0
        self.scope = digest(str(ROOT))[:16]
        self.files = {"adapter.py": (ROOT / "adapters/driver.py").read_bytes(),
                      "values.py": (ROOT / "tracer/values.py").read_bytes(),
                      "driver.py": (ROOT / "tracer/collect.py").read_bytes(),
                      "probe.py": (ROOT / "adapters/probe.py").read_bytes()}
        self.identity = None

    def docker(self, args, timeout=30):
        try:
            return super().docker(args, timeout)
        except IsolationError:
            raise DockerUnavailable("Docker operation unavailable: " + args[0]) from None

    def clean(self):
        """Confirm absence using successful inventory; never infer from rm failure."""
        with self.registry_lock:
            names = set(self.docker(["ps", "-a", "--filter", "label=light.tracer=" + self.scope,
                                     "--format", "{{.Names}}"]).decode().splitlines())
            registered = dict(self.store.rows("container"))
            unknown = names - set(registered)
            if unknown:
                raise DockerUnavailable("Unreconciled tracer containers from another output directory; use its cleanup command")
            for name, row in registered.items():
                if row["status"] == "active":
                    continue
                if name in names:
                    try:
                        self.docker(["rm", "--force", name], timeout=15)
                    except DockerUnavailable:
                        self.store.put("container", name, {**row, "status": "cleanup_pending"})
                        continue
                self.store.delete("container", name)
                self.remove_unit(row["unit"])
            pending = [r for _, r in self.store.rows("container") if r["status"] != "active"]
            if pending:
                raise DockerUnavailable("Container cleanup pending")

    def remove_unit(self, unit):
        path = Path(unit).resolve()
        parent = (self.store.directory / "units").resolve()
        if path.parent != parent or not path.name.startswith("unit-"):
            raise UnsafeIsolation("Unexpected unit directory")
        if path.exists():
            for child in path.iterdir():
                if child.is_file() and not child.is_symlink():
                    child.unlink()
                else:
                    raise UnsafeIsolation("Unexpected unit content")
            path.rmdir()

    def recover(self):
        for name, row in self.store.rows("container"):
            self.store.put("container", name, {**row, "status": "cleanup_pending"})

    def health(self, force=False):
        with self.health_lock:
            if not force and time.monotonic() - self.last_health < 5:
                return
            image = json.loads(self.docker(["image", "inspect", self.cfg["execution"]["image"]]))[0]["Id"]
            if self.identity and image != self.image:
                raise UnsafeIsolation("Image changed; use a new output directory")
            self.image = image
            self.clean()
            obs = self.launch("# probe", {}, None, probe=True)
            facts = obs.get("observation", {}).get("output", {})
            expected = {"uid": 65534, "cap_eff": "0000000000000000", "no_new_privs": "1",
                        "interfaces": ["lo"], "source_read_only": True, "root_read_only": True,
                        "tmp_writable": True, "memory_max": str(self.config["memory_mb"] * 1024 * 1024),
                        "pids_max": str(self.config["pids"])}
            if any(facts.get(k) != value for k, value in expected.items()) or facts.get("python_version") != "3.11.9":
                raise UnsafeIsolation("Isolation probe failed (requires Python 3.11.9)")
            identity = {"image": self.image, "execution": {k: v for k, v in self.config.items() if k != "workers"}, "trace": self.cfg["trace"],
                        "files": {name: digest(content.hex()) for name, content in self.files.items()},
                        "runner": file_digest(Path(__file__)), "base_runner": file_digest(ROOT / "runner.py"),
                        "python": facts["python_version"]}
            old = self.store.get("meta", "environment")
            if old and old != identity:
                raise UnsafeIsolation("Execution configuration changed; choose a new output directory")
            self.store.put("meta", "environment", identity)
            self.identity = identity
            self.last_health = time.monotonic()

    def collect_pair(self, pair):
        result = {}
        for side in ("original", "mutant"):
            if self.stop.is_set():
                raise InterruptedError()
            result[side] = self.collect_source(pair[side + "_code"], pair["execution_contract"], pair["input"])
        return result

    def collect_source(self, source, contract, value):
        key = digest([source, contract, value, self.identity])
        with self.key_lock:
            lock = self.key_locks.setdefault(key, threading.Lock())
        with lock:
            cached = self.store.get("trace", key)
            if cached:
                path = Path(cached["trace_file"])
                if not path.is_file() or file_digest(path) != cached["trace_sha256"]:
                    raise ValueError("Cached trace missing or changed")
                return cached
            directory = self.store.directory / "traces" / key
            directory.mkdir(parents=True, exist_ok=True)
            # A completed file may have been committed just before the process died.
            metadata = directory / "metadata.json"
            if metadata.is_file():
                record = json.loads(metadata.read_text())
                if record.get("trace_key") != key or file_digest(record["trace_file"]) != record["trace_sha256"]:
                    raise ValueError("Recovered trace binding mismatch")
                self.store.put("trace", key, record)
                return record
            baseline_file = directory / "baseline.json"
            payload = {"entry": contract["entry"], "input": value, "trace": self.cfg["trace"]}
            if baseline_file.is_file():
                baseline = json.loads(baseline_file.read_text())
            else:
                baseline = self.launch(source, {**payload, "traced": False}, None)
                write_json(baseline_file, baseline)
            trace_file = directory / (uuid.uuid4().hex + ".jsonl.gz")
            traced = self.launch(source, {**payload, "traced": True}, trace_file)
            project = lambda x: {k: x.get("observation", {}).get(k) for k in
                                 ("status", "output", "exception_class", "program_stdout", "program_stderr")}
            matched = canonical(project(baseline)) == canonical(project(traced))
            usable = (matched and traced.get("events_complete", False) and
                      traced.get("observation", {}).get("status") in ("ok", "program_exception"))
            record = {"trace_key": key, "trace_file": str(trace_file), "trace_sha256": file_digest(trace_file),
                      "baseline": baseline, "traced": traced, "external_results_match": matched,
                      "usable": usable, "repeat_stability": "not_checked", "environment": self.identity}
            write_json(directory / "metadata.json", record)
            self.store.put("trace", key, record)
            return record

    def launch(self, source, payload, trace_path, probe=False):
        cfg = self.config
        if self.stop.is_set():
            raise InterruptedError()
        if shutil.disk_usage(self.store.directory).free < self.cfg["pipeline"]["min_free_disk_mb"] * 1024 * 1024:
            raise DockerUnavailable("Insufficient free disk; collection paused")
        with self.slots:
            name = "light-trace-" + uuid.uuid4().hex
            unit = self.store.directory / "units" / ("unit-" + uuid.uuid4().hex)
            unit.mkdir(parents=True, mode=0o755)
            (unit / "source.py").write_text(source, encoding="utf-8")
            for file, content in self.files.items():
                (unit / file).write_bytes(content)
            for file in unit.iterdir():
                file.chmod(0o444)
            registered = False
            process = None
            threads = []
            result = {}
            started = time.monotonic()
            started_at = time.time()
            try:
                with self.starts:
                    with self.registry_lock:
                        pending = self.store.rows("container")
                        if any(row["status"] != "active" for _, row in pending):
                            raise DockerUnavailable("Pending cleanup blocks new creation")
                        if len(pending) >= self.cfg["pipeline"]["max_unremoved_containers"]:
                            raise DockerUnavailable("Unremoved container limit")
                        self.store.put("container", name, {"unit": str(unit), "status": "active",
                                                          "created_at": time.time()})
                        registered = True
                    create = ["create", "--pull=never", "--name", name, "--label", "light.tracer=" + self.scope,
                              "-i", "--network=none", "--read-only", "--user=65534:65534", "--cap-drop=ALL",
                              "--security-opt=no-new-privileges:true", f"--memory={cfg['memory_mb']}m",
                              f"--memory-swap={cfg['memory_mb']}m", f"--pids-limit={cfg['pids']}",
                              f"--cpus={cfg['cpus']}", "--tmpfs=/tmp:rw,noexec,nosuid,size=64m,uid=65534,gid=65534,mode=700",
                              "--workdir=/tmp", "--mount", f"type=bind,src={unit},dst=/unit,readonly",
                              "--env=PYTHONDONTWRITEBYTECODE=1", self.image, "python", "-I", "-B",
                              "/unit/probe.py" if probe else "/unit/driver.py"]
                    self.docker(create)
                    try:
                        self.verify_config(json.loads(self.docker(["inspect", name]))[0], unit)
                    except IsolationError:
                        raise UnsafeIsolation("Container configuration verification failed") from None
                    process = subprocess.Popen(self.command(["start", "-a", "-i", name]), env=ENV,
                                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    ready = threading.Event()
                    failed = threading.Event()
                    ready_at = [None]
                    target_started_at = [None]
                    target_finished_at = [None]
                    errors = []
                    counts = defaultdict(int)
                    # ASCII framing can expand UTF-8 program output; the adapter
                    # still enforces its original 1 MiB program-output allowance.
                    line_limit = max(self.cfg["trace"]["max_event_bytes"], cfg["output_bytes"] * 6 + 65536)

                    def read_output():
                        handle = None
                        try:
                            if trace_path:
                                handle = gzip.open(trace_path, "wb", compresslevel=3)
                            while True:
                                line = process.stdout.readline(line_limit + 1)
                                if not line:
                                    break
                                if len(line) > line_limit:
                                    raise ValueError("transport_line_limit")
                                counts["wire_bytes"] += len(line)
                                if counts["wire_bytes"] > self.cfg["trace"]["max_trace_bytes"] + cfg["output_bytes"] * 6 + 65536:
                                    raise ValueError("transport_byte_limit")
                                row = json.loads(line)
                                if type(row) is not dict:
                                    raise ValueError("invalid_frame")
                                if probe:
                                    result.update(observation={"status": "ok", "output": row}, events_complete=True)
                                elif row.get("kind") == "ready":
                                    if ready.is_set() or row.get("python") != [3, 11, 9]:
                                        raise ValueError("invalid_ready")
                                    ready_at[0] = time.monotonic()
                                    target_started_at[0] = time.time()
                                    ready.set()
                                elif row.get("kind") == "event":
                                    if result or not ready.is_set() or not payload["traced"] or row.get("event_id") != counts["events"]:
                                        raise ValueError("invalid_event_sequence")
                                    counts["events"] += 1
                                    counts["event_bytes"] += len(line)
                                    if counts["events"] > self.cfg["trace"]["max_events"] or counts["event_bytes"] > self.cfg["trace"]["max_trace_bytes"]:
                                        raise ValueError("trace_limit")
                                    if handle:
                                        handle.write(line)
                                elif row.get("kind") == "result":
                                    if result or not ready.is_set() or row.get("event_count") != counts["events"]:
                                        raise ValueError("invalid_footer")
                                    result.update(row)
                                    target_finished_at[0] = time.time()
                                else:
                                    raise ValueError("unknown_frame")
                        except Exception as exc:
                            errors.append(type(exc).__name__)
                            failed.set()
                        finally:
                            if handle:
                                try:
                                    handle.close()
                                except OSError as exc:
                                    errors.append(type(exc).__name__)
                                    failed.set()

                    def read_errors():
                        # Never persist diagnostic stderr or exception text.
                        while True:
                            chunk = process.stderr.read(4096)
                            if not chunk:
                                return
                            counts["stderr_bytes"] += len(chunk)
                            if counts["stderr_bytes"] > cfg["output_bytes"]:
                                failed.set()
                                return

                    def send():
                        try:
                            process.stdin.write(canonical(payload))
                            process.stdin.close()
                        except (OSError, ValueError):
                            pass

                    threads = [threading.Thread(target=fn, daemon=True) for fn in (read_output, read_errors, send)]
                    for thread in threads:
                        thread.start()
                    # Limit *startup* bursts, not the duration of target execution.
                    deadline = time.monotonic() + cfg["startup_seconds"]
                    while not probe and not ready.is_set() and process.poll() is None and not failed.is_set():
                        if time.monotonic() >= deadline or self.stop.wait(0.05):
                            break
                stop_reason = None
                while process.poll() is None:
                    if self.stop.is_set():
                        stop_reason = "interrupted"
                    elif failed.is_set():
                        stop_reason = "transport_error"
                    elif time.monotonic() - (ready_at[0] or started) > (cfg["wall_seconds"] if ready.is_set() else cfg["startup_seconds"]):
                        stop_reason = "timeout" if ready.is_set() else "startup_timeout"
                    if stop_reason:
                        self.docker(["kill", name], timeout=10)
                        break
                    self.stop.wait(0.05)
                process.wait(timeout=10)
                for thread in threads:
                    thread.join(timeout=5)
                if any(thread.is_alive() for thread in threads):
                    raise DockerUnavailable("Container transport did not finish")
                facts = json.loads(self.docker(["inspect", name]))[0]["State"]
                if facts.get("OOMKilled"):
                    stop_reason = "oom"
                if stop_reason or errors or not result or process.returncode:
                    result = {"observation": {"status": stop_reason or "transport_error", "output": None},
                              "events_complete": False, "stop_reason": stop_reason or "missing_or_invalid_footer",
                              "event_count": counts["events"]}
                result.update(container=name, elapsed_seconds=time.monotonic() - started,
                              started_at=started_at, target_started_at=target_started_at[0],
                              target_finished_at=target_finished_at[0],
                              ended_at=time.time(), wire_bytes=counts["wire_bytes"], image=self.image)
                if trace_path:
                    with Path(trace_path).open("rb") as handle:
                        os.fsync(handle.fileno())
                return result
            finally:
                # Cleanup failures retain registration and never replace a completed result.
                if process is not None:
                    if process.poll() is None:
                        process.kill()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
                    for thread in threads:
                        thread.join(timeout=1)
                    for stream in (process.stdin, process.stdout, process.stderr):
                        if stream and not stream.closed:
                            try:
                                stream.close()
                            except OSError:
                                pass
                if registered:
                    with self.registry_lock:
                        self.store.put("container", name, {"unit": str(unit), "status": "cleanup_pending"})
                        try:
                            self.docker(["rm", "--force", name], timeout=15)
                        except DockerUnavailable:
                            pass
                        else:
                            self.store.delete("container", name)
                            self.remove_unit(unit)
                else:
                    self.remove_unit(unit)
