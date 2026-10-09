"""Docker-only execution. Adapted from motherdata/v112/runner.py; six independent units."""

import json
import math
import os
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path
from adapters import canonical_bytes
from state import digest

ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
HERE = Path(__file__).resolve().parent


class IsolationError(RuntimeError):
    pass


def compare(observation, expected, contract):
    status = observation["status"]
    if (
        status == "program_exception"
        and contract.get("exception_policy") == "forbidden"
    ):
        return "fail"
    if status != "ok":
        return "unknown"
    actual = observation["output"]
    comp = contract["comparison"]
    kind = comp["kind"]
    if kind == "numeric_tolerance":
        if type(actual) not in (int, float) or type(expected) not in (int, float):
            return "unknown"
        return (
            "pass"
            if math.isclose(
                actual,
                expected,
                abs_tol=comp.get("abs_tol") or 0,
                rel_tol=comp.get("rel_tol") or 0,
            )
            else "fail"
        )
    if kind not in ("json_exact", "text_exact", "text_lines", "text_tokens"):
        raise ValueError("unsupported comparison")
    if kind.startswith("text_"):
        if not isinstance(actual, str) or not isinstance(expected, str):
            return "unknown"

        def normalize(s):
            if kind == "text_tokens":
                return s.split()
            if kind == "text_lines":
                lines = [x.rstrip(" \t") for x in s.replace("\r\n", "\n").split("\n")]
                while lines and not lines[-1]:
                    lines.pop()
                return lines
            return s

        actual, expected = normalize(actual), normalize(expected)
    return "pass" if canonical_bytes(actual) == canonical_bytes(expected) else "fail"


class DockerRunner:
    def __init__(self, config, workspace):
        self.config = dict(config)
        self.workspace = Path(workspace).resolve()
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.docker_config = self.workspace / "docker-config"
        self.docker_config.mkdir(exist_ok=True)
        self.image = config["image"]
        self.verified = False
        self.slots = threading.BoundedSemaphore(config["workers"])
        self.cache_key = None

    def command(self, args):
        return [
            self.config["docker"],
            "--host",
            self.config["socket"],
            "--config",
            str(self.docker_config),
            *args,
        ]

    def docker(self, args, timeout=30):
        try:
            result = subprocess.run(
                self.command(args), env=ENV, capture_output=True, timeout=timeout
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise IsolationError("Docker unavailable: " + type(exc).__name__) from None
        if result.returncode:
            raise IsolationError("Docker failed: " + args[0])
        return result.stdout

    def verify_config(self, data, unit):
        h, c = data["HostConfig"], data["Config"]
        memory = self.config["memory_mb"] * 1024 * 1024
        binds = [m for m in data["Mounts"] if m["Type"] == "bind"]
        if (
            h["NetworkMode"] != "none"
            or h["Privileged"]
            or not h["ReadonlyRootfs"]
            or c["User"] != "65534:65534"
            or h["Memory"] != memory
            or h["MemorySwap"] != memory
            or h["PidsLimit"] != self.config["pids"]
            or "ALL" not in (h.get("CapDrop") or [])
            or not any(
                x.startswith("no-new-privileges") for x in h.get("SecurityOpt") or []
            )
            or h.get("PidMode") == "host"
            or h.get("IpcMode") == "host"
            or h.get("Devices")
            or h.get("DeviceRequests")
        ):
            raise IsolationError("Unsafe container configuration")
        if (
            len(binds) != 1
            or binds[0]["Source"] != str(unit)
            or binds[0]["Destination"] != "/unit"
            or binds[0]["RW"]
        ):
            raise IsolationError("Unsafe source mount")
        if (
            not h.get("Tmpfs", {}).get("/tmp")
            or data["Image"] != self.image
            or c["WorkingDir"] != "/tmp"
        ):
            raise IsolationError("Missing isolated tmpfs/image")

    def check(self):
        self.image = json.loads(self.docker(["image", "inspect", self.image]))[0]["Id"]
        obs = self._execute("# isolation probe", {}, probe=True)
        facts = obs.get("output") or {}
        if (
            obs["status"] != "ok"
            or facts.get("uid") != 65534
            or facts.get("cap_eff") != "0000000000000000"
            or facts.get("no_new_privs") != "1"
            or facts.get("interfaces") != ["lo"]
            or not all(
                facts.get(k) is True
                for k in ("source_read_only", "root_read_only", "tmp_writable")
            )
            or facts.get("memory_max") != str(self.config["memory_mb"] * 1024 * 1024)
            or facts.get("pids_max") != str(self.config["pids"])
        ):
            raise IsolationError("Actual isolation probe failed")
        self.verified = True
        self.cache_key = digest(
            {
                "image": self.image,
                "config": self.config,
                "driver": (HERE / "adapters/driver.py").read_text(),
            }
        )
        return facts

    def execute(self, source, entry, value):
        if not self.verified:
            raise IsolationError("Call check() before executing target code")
        with self.slots:
            return self._execute(source, {"entry": entry, "input": value})

    def _execute(self, source, payload, probe=False):
        cfg = self.config
        with tempfile.TemporaryDirectory(prefix="unit-", dir=self.workspace) as tmp:
            unit = Path(tmp)
            unit.chmod(0o755)
            (unit / "source.py").write_text(source)
            (unit / "driver.py").write_bytes(
                (
                    HERE / "adapters" / ("probe.py" if probe else "driver.py")
                ).read_bytes()
            )
            for file in unit.iterdir():
                file.chmod(0o444)
            name = "taco-light-" + uuid.uuid4().hex
            create = [
                "create",
                "--pull=never",
                "--name",
                name,
                "-i",
                "--network=none",
                "--read-only",
                "--user=65534:65534",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges:true",
                f"--memory={cfg['memory_mb']}m",
                f"--memory-swap={cfg['memory_mb']}m",
                f"--pids-limit={cfg['pids']}",
                f"--cpus={cfg['cpus']}",
                "--tmpfs=/tmp:rw,noexec,nosuid,size=64m,uid=65534,gid=65534,mode=700",
                "--workdir=/tmp",
                "--mount",
                f"type=bind,src={unit},dst=/unit,readonly",
                "--env=PYTHONDONTWRITEBYTECODE=1",
                self.image,
                "python",
                "-I",
                "-B",
                "/unit/driver.py",
            ]
            container = None
            process = None
            started = time.monotonic()
            try:
                container = self.docker(create).decode().strip()
                self.verify_config(
                    json.loads(self.docker(["inspect", container]))[0], unit
                )
                process = subprocess.Popen(
                    self.command(["start", "-a", "-i", container]),
                    env=ENV,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                out, err = bytearray(), bytearray()
                ready = threading.Event()
                exceeded = threading.Event()
                ready_at = [None]
                read_lock = threading.Lock()

                def read(stream, target):
                    while True:
                        chunk = os.read(stream.fileno(), 4096)
                        if not chunk:
                            return
                        with read_lock:
                            remaining = max(
                                0, cfg["output_bytes"] - len(out) - len(err)
                            )
                            if len(chunk) > remaining:
                                exceeded.set()
                            target.extend(chunk[:remaining])
                            if not ready.is_set() and b"LIGHT_READY\n" in err:
                                ready_at[0] = time.monotonic()
                                ready.set()

                threads = [
                    threading.Thread(
                        target=read, args=(process.stdout, out), daemon=True
                    ),
                    threading.Thread(
                        target=read, args=(process.stderr, err), daemon=True
                    ),
                ]
                for t in threads:
                    t.start()

                def send():
                    try:
                        process.stdin.write(canonical_bytes(payload))
                        process.stdin.close()
                    except (BrokenPipeError, OSError):
                        pass

                sender = threading.Thread(target=send, daemon=True)
                sender.start()
                stop = None
                while process.poll() is None:
                    elapsed = time.monotonic() - (ready_at[0] or started)
                    if exceeded.is_set():
                        stop = "output_limit"
                    elif elapsed > (
                        cfg["wall_seconds"]
                        if ready.is_set()
                        else cfg["startup_seconds"]
                    ):
                        stop = "timeout" if ready.is_set() else "startup_failed"
                    if stop:
                        self.docker(["kill", container], timeout=10)
                        break
                    time.sleep(0.01)
                process.wait(timeout=10)
                sender.join(timeout=2)
                for t in threads:
                    t.join(timeout=2)
                facts = json.loads(self.docker(["inspect", container]))[0]["State"]
                status = stop or (
                    "output_limit"
                    if exceeded.is_set()
                    else "oom"
                    if facts["OOMKilled"]
                    else "ok"
                    if process.returncode == 0
                    else "wrapper_error"
                )
                observation = {
                    "status": status,
                    "output": None,
                    "exception_class": None,
                }
                if status == "ok":
                    try:
                        data = json.loads(out)
                        if probe:
                            observation["output"] = data
                        elif not isinstance(data, dict) or data.get("status") not in (
                            "ok",
                            "program_exception",
                            "wrapper_error",
                            "output_limit",
                        ):
                            observation["status"] = "wrapper_error"
                        else:
                            observation = data
                    except (ValueError, UnicodeError):
                        observation["status"] = "wrapper_error"
                if not probe and not ready.is_set() and observation["status"] == "ok":
                    observation["status"] = "wrapper_error"
                observation.update(
                    origin="real_container",
                    container_id=container,
                    image=self.image,
                    elapsed_seconds=time.monotonic() - started,
                )
                return observation
            except (OSError, subprocess.SubprocessError) as exc:
                raise IsolationError(
                    "Container execution failed: " + type(exc).__name__
                ) from None
            finally:
                if container:
                    self.docker(["rm", "--force", container], timeout=15)
                if process:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
                    for stream in (process.stdin, process.stdout, process.stderr):
                        if stream and not stream.closed:
                            stream.close()
