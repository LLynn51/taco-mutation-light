"""One coordinator; independently bounded collection, comparison and API pools."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import json
import multiprocessing
from pathlib import Path
import threading
import time
import uuid

from .analysis import analyze
from .compare import compare_pair, comparison_version
from .runner import TraceRunner, DockerUnavailable, UnsafeIsolation
from .storage import file_digest, write_json


def emit(stage, **detail):
    print(json.dumps({"time": time.time(), "stage": stage, **detail}, ensure_ascii=False), flush=True)


def binding(cfg):
    here = Path(__file__).resolve().parent
    files = ["collect.py", "values.py", "runner.py", "checkpoint_io.py", "inputs.py", "storage.py"]
    files += [str(p.relative_to(here)) for p in sorted((here / "adapters").glob("*.py"))]
    return {"schema": "collection_binding_v2", "trace": cfg["trace"],
            "execution": {k: v for k, v in cfg["execution"].items() if k != "workers"},
            "files": {p: file_digest(here / p) for p in files},
            "adapter": file_digest(here.parent / "adapters/driver.py"),
            "base_runner": file_digest(here.parent / "runner.py")}


def run(pairs, cfg, store, *, paid=False, max_requests=0, mode="run"):
    if mode not in ("run", "collect", "compare", "analyze"):
        raise ValueError("Unknown stage")
    if mode in ("run", "collect"):
        frozen = binding(cfg)
        previous = store.get("meta", "binding")
        if previous and previous != frozen:
            raise ValueError("Collection configuration changed; use a new output (compare/analyze can reuse old traces)")
        store.put("meta", "binding", frozen)
    store.recover_requests()
    stop = threading.Event()
    runner = TraceRunner(cfg, store, stop) if mode in ("run", "collect") else None
    if runner:
        runner.recover()
    settings = cfg["pipeline"]
    collect_pool = ThreadPoolExecutor(max_workers=settings["collect_workers"], thread_name_prefix="trace")
    compare_pool = ProcessPoolExecutor(max_workers=settings["compare_workers"],
                                      mp_context=multiprocessing.get_context("spawn"))
    llm_pool = ThreadPoolExecutor(max_workers=settings["llm_workers"], thread_name_prefix="analysis")
    health_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="docker-health")
    active = {}
    futures = {}
    cursor = 0
    healthy = False
    health_future = None
    next_health = 0
    health_delay = settings["docker_retry_seconds"]

    def save(key, job):
        job["updated_at"] = time.time()
        store.put("job", key, {k: v for k, v in job.items() if k != "pair"})

    def log(stage, **detail):
        store.put("event", uuid.uuid4().hex, {"time": time.time(), "stage": stage, **detail})
        emit(stage, **detail)

    def starting(job, stage):
        now = time.time()
        waits = job.setdefault("queue_wait_seconds", {})
        waits[stage] = waits.get(stage, 0) + max(0, now - job.get("ready_at", now))
        job["stage_started_at"] = now

    def admit(pair):
        key = pair["pair_id"]
        directory = store.directory / "pairs" / key
        write_json(directory / "input.json", pair)
        job = store.get("job", key) or {"collect_attempts": 0}
        for record in job.get("traces", {}).values():
            if file_digest(record["trace_file"]) != record["trace_sha256"]:
                raise ValueError("Stored trace missing or changed")
            for cp in record.get("traced", {}).get("checkpoints", {}).values():
                if file_digest(cp["file"]) != cp["sha256"]:
                    raise ValueError("Stored checkpoint missing or changed")
        if mode in ("compare", "analyze") and not job.get("traces"):
            raise ValueError("This stage requires stored traces")
        if mode == "analyze" and not job.get("comparison"):
            raise ValueError("Analyze requires a stored comparison")
        if mode == "collect" and job.get("traces"):
            stage = "collected"
        elif job.get("comparison") and (mode == "analyze" or job.get("comparison_version") == comparison_version(pair, job["traces"], cfg)):
            path = Path(job["comparison"])
            if not path.is_file() or file_digest(path) != job.get("comparison_sha256"):
                raise ValueError("Comparison artifact missing or changed")
            stage = "compared" if mode == "compare" else "analysis_ready"
        elif job.get("traces"):
            stage = "compare_ready"
        elif job.get("stage") in ("collection_failed", "comparison_failed", "interrupted"):
            # Explicitly preserve exhausted attempts across restarts.
            stage = "collection_failed" if job.get("collect_attempts", 0) >= settings["max_local_attempts"] else "collect_ready"
        else:
            stage = "collect_ready"
        job.update(stage=stage, pair=pair, directory=str(directory), ready_at=time.time())
        active[key] = job
        save(key, job)

    try:
        while cursor < len(pairs) or active or futures:
            while cursor < len(pairs) and len(active) < settings["max_inflight"]:
                admit(pairs[cursor])
                cursor += 1
            counts = Counter(stage for _, stage in futures.values())
            needs_collection = any(job["stage"] == "collect_ready" for job in active.values())
            if not healthy and needs_collection and health_future is None and time.monotonic() >= next_health:
                health_future = health_pool.submit(runner.health, True)
            if health_future is not None and health_future.done():
                try:
                    health_future.result()
                    healthy = True
                    health_delay = settings["docker_retry_seconds"]
                    log("docker_ready")
                except DockerUnavailable as exc:
                    next_health = time.monotonic() + health_delay
                    log("collection_paused", reason=str(exc), retry_seconds=health_delay)
                    health_delay = min(60, health_delay * 2)
                finally:
                    health_future = None
            for key, job in list(active.items()):
                stage = job["stage"]
                if stage == "collect_ready" and healthy and counts["collect"] < settings["collect_workers"]:
                    job["collect_attempts"] += 1
                    starting(job, "collect")
                    job["stage"] = "collecting"
                    save(key, job)
                    future = collect_pool.submit(runner.collect_pair, job["pair"])
                    futures[future] = (key, "collect")
                    counts["collect"] += 1
                    log("collecting", pair_id=key)
                elif stage == "compare_ready" and counts["compare"] < settings["compare_workers"]:
                    starting(job, "compare")
                    job["stage"] = "comparing"
                    save(key, job)
                    future = compare_pool.submit(compare_pair, job["pair"], job["traces"], cfg, job["directory"])
                    futures[future] = (key, "compare")
                    counts["compare"] += 1
                    log("comparing", pair_id=key)
                elif stage == "analysis_ready":
                    if not paid:
                        job["stage"] = "awaiting_analysis"
                    elif counts["analysis"] < settings["llm_workers"]:
                        starting(job, "analysis")
                        job["stage"] = "analyzing"
                        save(key, job)
                        future = llm_pool.submit(analyze, job["pair"], job["comparison"], cfg, store, max_requests)
                        futures[future] = (key, "analysis")
                        counts["analysis"] += 1
                        log("analyzing", pair_id=key)
                if job["stage"] in ("complete", "collected", "compared", "awaiting_analysis", "collection_failed", "comparison_failed", "analysis_failed"):
                    save(key, job)
                    write_json(Path(job["directory"]) / "result.json", {k: v for k, v in job.items() if k != "pair"})
                    log(job["stage"], pair_id=key)
                    del active[key]
            for future in list(futures):
                if not future.done():
                    continue
                key, stage = futures.pop(future)
                job = active[key]
                timings = job.setdefault("stage_seconds", {})
                timings[stage] = timings.get(stage, 0) + time.time() - job["stage_started_at"]
                job["ready_at"] = time.time()
                try:
                    value = future.result()
                    if stage == "collect":
                        job.update(traces=value, stage="collected" if mode == "collect" else "compare_ready")
                    elif stage == "compare":
                        if "analysis" in job:
                            job["previous_analysis"] = job.pop("analysis")
                        job.update(comparison=value, comparison_sha256=file_digest(value),
                                   comparison_version=json.loads(Path(value).read_text()).get("version"),
                                   stage="compared" if mode == "compare" else "analysis_ready")
                    else:
                        job.update(analysis=value, stage="complete")
                except DockerUnavailable:
                    # Infrastructure outages do not consume a local program attempt.
                    job["collect_attempts"] -= 1
                    job.update(stage="collect_ready", last_error="docker_unavailable")
                    healthy = False
                    next_health = time.monotonic() + settings["docker_retry_seconds"]
                    log("collection_paused", reason="docker_unavailable")
                except UnsafeIsolation:
                    raise
                except Exception as exc:
                    if stage == "collect" and job["collect_attempts"] < settings["max_local_attempts"]:
                        job["stage"] = "collect_ready"
                    else:
                        job["stage"] = {"collect": "collection_failed", "compare": "comparison_failed", "analysis": "analysis_failed"}[stage]
                    job["last_error"] = type(exc).__name__
                    log("stage_error", pair_id=key, component=stage, error=type(exc).__name__)
                save(key, job)
            time.sleep(0.05)
    finally:
        stop.set()
        log("draining", detail="Waiting for in-flight work; requests are not resent")
        for pool in (collect_pool, compare_pool, llm_pool, health_pool):
            pool.shutdown(wait=True, cancel_futures=True)
        # Retain valid completed work even if Ctrl+C interrupted the coordinator.
        for future, (key, stage) in futures.items():
            if future.cancelled() or not future.done() or key not in active:
                continue
            job = active[key]
            try:
                value = future.result()
                if stage == "collect":
                    job.update(traces=value, stage="collected" if mode == "collect" else "compare_ready")
                elif stage == "compare":
                    if "analysis" in job:
                        job["previous_analysis"] = job.pop("analysis")
                    job.update(comparison=value, comparison_sha256=file_digest(value),
                               comparison_version=json.loads(Path(value).read_text()).get("version"),
                               stage="compared" if mode == "compare" else "analysis_ready")
                else:
                    job.update(analysis=value, stage="complete")
            except Exception:
                job["stage"] = "interrupted"
            save(key, job)
        if runner:
            try:
                runner.clean()
            except DockerUnavailable:
                log("cleanup_pending", detail="Run cleanup after Docker recovers")
    report = {"jobs": dict(Counter(row["stage"] for _, row in store.rows("job"))),
              "analysis": dict(Counter(row.get("analysis", {}).get("status", "not_requested") for _, row in store.rows("job"))),
              "requests": len(store.rows("request")), "unremoved_containers": len(store.rows("container"))}
    report["metrics"] = metrics(store)
    write_json(store.directory / "summary.json", report)
    log("finished", **report)
    return report


def metrics(store):
    traces, comparisons, stages = {}, [], Counter()
    for _, job in store.rows("job"):
        for record in job.get("traces", {}).values():
            if type(record) is dict and "trace_file" in record:
                traces[record["trace_file"]] = record
        for stage, seconds in job.get("stage_seconds", {}).items():
            stages[stage] += seconds
        if job.get("comparison") and Path(job["comparison"]).is_file():
            comparisons.append(json.loads(Path(job["comparison"]).read_text()))
    points = []
    for record in traces.values():
        for kind in ("baseline", "traced"):
            t = record.get(kind, {})
            if t.get("target_started_at") and t.get("target_finished_at"):
                points += [(t["target_started_at"], 1), (t["target_finished_at"], -1)]
    active = peak = 0
    for _, change in sorted(points):
        active += change
        peak = max(peak, active)
    return {"unique_traces": len(traces), "target_interval_peak": peak,
            "interval_basis": "host_ready_to_result_observations_including_reused_traces",
            "stage_worker_seconds": dict(stages),
            "event_bytes": sum(t.get("traced", {}).get("event_bytes", 0) for t in traces.values()),
            "checkpoint_bytes": sum(t.get("traced", {}).get("checkpoint_bytes", 0) for t in traces.values()),
            "compressed_event_bytes": sum(Path(t["trace_file"]).stat().st_size for t in traces.values()),
            "compressed_checkpoint_bytes": sum(Path(cp["file"]).stat().st_size for t in traces.values()
                                                for cp in t.get("traced", {}).get("checkpoints", {}).values()),
            "execution_seconds_including_startup": {
                phase: sum(t.get(phase, {}).get("elapsed_seconds", 0) for t in traces.values())
                for phase in ("baseline", "traced")},
            "incomplete_traces": sum(not t.get("traced", {}).get("events_complete", False) for t in traces.values()),
            "comparison_statuses": dict(Counter(r.get("evidence_status", "unknown") for r in comparisons)),
            "unmatched_events": {s: sum(r.get("coverage", {}).get(s, {}).get("alignment", {}).get("unmatched", 0)
                                         for r in comparisons) for s in ("original", "mutant")}}
