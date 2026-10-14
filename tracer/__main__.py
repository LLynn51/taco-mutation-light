"""Explicit CLI. inspect/status never execute targets, contact Docker or read keys."""
import argparse
from collections import Counter
import json
from pathlib import Path
import sqlite3
import sys
import threading
from contextlib import ExitStack

from .inputs import ROOT, configuration, read_pairs
from .storage import Store, owner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", nargs="?", choices=("inspect", "status", "run", "collect", "compare", "analyze", "cleanup"), default="inspect")
    parser.add_argument("--config", type=Path, default=ROOT / "tracer/config.toml")
    parser.add_argument("--output", type=Path, help="Separate diagnostic output directory")
    parser.add_argument("--source", type=Path, help="Existing diagnostic directory for compare/analyze; defaults to output")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--samples", type=Path, help="JSON array/object or JSONL; also accepts light materials.jsonl")
    group.add_argument("--database", type=Path, help="Read samples from an existing light state.sqlite")
    parser.add_argument("--limit", type=int, help="Select only the first N input pairs")
    parser.add_argument("--paid", action="store_true", help="Enable the optional DeepSeek analysis stage")
    parser.add_argument("--max-requests", type=int, default=0, help="Cumulative analysis attempt cap in this output directory")
    args = parser.parse_args()
    if args.paid and (args.command not in ("run", "analyze") or args.max_requests < 1):
        parser.error("--paid requires run/analyze and positive --max-requests")
    if args.command == "analyze" and not args.paid:
        parser.error("analyze requires explicit --paid and --max-requests")
    if args.source and args.command not in ("compare", "analyze"):
        parser.error("--source is only valid for compare/analyze")
    if args.command in ("compare", "analyze") and (args.samples or args.database):
        parser.error("compare/analyze read frozen --source artifacts, not new samples")
    if args.max_requests < 0 or (args.max_requests and not args.paid):
        parser.error("--max-requests is used only with --paid")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    cfg = configuration(args.config)
    if args.output:
        cfg["output_dir"] = str(args.output.resolve())
    output = Path(cfg["output_dir"])
    light_run = Path(cfg["light_run_dir"])
    if output == light_run:
        parser.error("Diagnostic output must not be the original light run directory")
    if args.command == "status":
        path = output / "state.sqlite"
        if not path.exists():
            print(json.dumps({"status": "not_started", "output": str(output)}))
            return 0
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            rows = [(kind, json.loads(body)) for kind, body in db.execute("SELECT kind,body FROM records")]
        finally:
            db.close()
        print(json.dumps({"jobs": dict(Counter(row.get("stage") for kind, row in rows if kind == "job")),
                          "requests": dict(Counter(row.get("status") for kind, row in rows if kind == "request")),
                          "containers": dict(Counter(row.get("status") for kind, row in rows if kind == "container"))}, indent=2))
        return 0
    pairs = []
    if args.command in ("inspect", "run", "collect"):
        pairs = read_pairs(args.samples, args.database or light_run / "state.sqlite")
        if args.limit:
            pairs = pairs[:args.limit]
        if args.command == "inspect":
            print(json.dumps({"pairs": len(pairs), "output": str(output), "pipeline": cfg["pipeline"],
                              "trace": cfg["trace"], "model": cfg["api"]["model"],
                              "credentials_read": False, "target_executions": 0, "http_requests": 0}, indent=2))
            return 0
    if sys.platform != "linux":
        parser.error("Run diagnostics in WSL/Linux")
    # Also hold light's run lock: the existing runner has only per-instance slots.
    # This prevents the generation pipeline and diagnostics exceeding a joint cap.
    with ExitStack() as locks:
        # Offline stages neither construct a runner nor acquire the generation lock.
        if args.command in ("run", "collect", "cleanup"):
            locks.enter_context(owner(ROOT / "runs/.tracer-execution.lock"))
            locks.enter_context(owner(light_run / "run.lock"))
        locks.enter_context(owner(output / "run.lock"))
        source = (args.source or output).resolve()
        if args.command in ("compare", "analyze") and source != output:
            # Existing lock opened read-only: imported source files stay unchanged.
            import fcntl
            handle = locks.enter_context((source / "run.lock").open("rb"))
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        store = Store(output)
        try:
            if args.command == "cleanup":
                from .runner import TraceRunner
                runner = TraceRunner(cfg, store, threading.Event())
                runner.recover()
                runner.clean()
                print(json.dumps({"unremoved_containers": len(store.rows("container"))}))
            else:
                if args.command in ("compare", "analyze"):
                    from .stages import import_existing
                    pairs = import_existing(source, store, args.limit)
                from .pipeline import run
                run(pairs, cfg, store, paid=args.paid, max_requests=args.max_requests, mode=args.command)
        finally:
            store.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted; persisted stages will be reused on resume", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        # Do not echo task-derived exception text or source snippets.
        print("Tracer stopped: " + type(exc).__name__ + ". Check configuration and tracer/README.md.", file=sys.stderr)
        raise SystemExit(2)
