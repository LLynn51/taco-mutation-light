"""Container-only entrypoint. Reuses the original adapter, without editing it."""
import importlib.util
import io
import json
import sys
import time


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Collector:
    def __init__(self, stream, limits, encoder):
        self.stream, self.limits, self.encoder = stream, limits, encoder
        self.frames = {}
        self.calls = {}
        self.exceptions = set()
        self.count = self.bytes = self.value_gaps = 0
        self.reason = None
        self.unsupported_frames = 0

    def send(self, row):
        line = json.dumps(row, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n"
        self.stream.write(line)
        self.stream.flush()
        return len(line.encode())

    def trace(self, frame, event, arg):
        if frame.f_code.co_filename != "/unit/source.py":
            return None
        if self.reason:
            return None
        # Yield/resume needs a different call-instance model. Do not mislabel it.
        if frame.f_code.co_flags & (0x20 | 0x80 | 0x200):
            self.unsupported_frames += 1
            return None
        if event not in ("call", "line", "return", "exception"):
            return self.trace
        fid = id(frame)
        function = frame.f_code.co_qualname
        if event == "call":
            parent = self.frames.get(id(frame.f_back), "root")
            key = (parent, function)
            self.calls[key] = self.calls.get(key, 0) + 1
            self.frames[fid] = parent + "/" + function + "#" + str(self.calls[key])
        if event == "line":
            self.exceptions.discard(fid)
        if event == "exception":
            self.exceptions.add(fid)
        if self.count >= self.limits["max_events"]:
            self.reason = "event_limit"
            return None
        try:
            encoder = self.encoder(self.limits)
            snapshot = encoder.snapshot(frame.f_locals, arg, event == "return")
            kind = "unwind" if event == "return" and fid in self.exceptions else event
            row = {"kind": "event", "event_id": self.count, "event": kind,
                   "phase": "before" if event in ("line", "call") else "at_event",
                   "line": frame.f_lineno, "function": function,
                   "call_id": self.frames.get(fid, "unmatched"), **snapshot}
            if event == "exception":
                row["exception_class"] = type.__getattribute__(arg[0], "__name__")
            encoded = json.dumps(row, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n"
            size = len(encoded.encode())
            if size > self.limits["max_event_bytes"]:
                self.reason = "event_byte_limit"
                return None
            if self.bytes + size > self.limits["max_trace_bytes"]:
                self.reason = "trace_byte_limit"
                return None
            self.stream.write(encoded)
            self.stream.flush()
            self.bytes += size
            self.count += 1
            self.value_gaps += encoder.missing
        except Exception:
            self.reason = "snapshot_or_transport_error"
        finally:
            if event == "return":
                self.frames.pop(fid, None)
                self.exceptions.discard(fid)
        return self.trace


def main():
    payload = json.load(sys.stdin)
    stream = sys.stdout
    adapter = load("light_adapter", "/unit/adapter.py")
    values = load("trace_values", "/unit/values.py")
    collector = Collector(stream, payload["trace"], values.Encoder)
    original_stdin, original_stdout = sys.stdin, sys.stdout
    captured = adapter.Capture(adapter.OutputBudget())
    adapter_input = json.dumps({"entry": payload["entry"], "input": payload["input"]}).encode("utf-8")
    sys.stdin = io.TextIOWrapper(io.BytesIO(adapter_input), encoding="utf-8")
    sys.stdout = captured
    collector.send({"kind": "ready", "python": list(sys.version_info[:3]), "traced": payload["traced"]})
    started = time.monotonic()
    try:
        if payload["traced"]:
            sys.settrace(collector.trace)
        adapter.main()
    except BaseException:
        collector.reason = collector.reason or "adapter_failed"
    finally:
        if payload["traced"] and sys.gettrace() != collector.trace:
            collector.reason = collector.reason or "trace_hook_changed"
        sys.settrace(None)
        sys.stdin, sys.stdout = original_stdin, original_stdout
    try:
        observation = json.loads(captured.getvalue())
    except (ValueError, TypeError):
        observation = {"status": "wrapper_error", "output": None, "exception_class": None}
    collector.send({"kind": "result", "observation": observation,
                    "event_count": collector.count, "event_bytes": collector.bytes,
                    "events_complete": collector.reason is None,
                    "stop_reason": collector.reason, "value_gaps": collector.value_gaps,
                    "unsupported_frames": collector.unsupported_frames,
                    "scope": "source.py/main_thread/line_boundaries/locals",
                    "elapsed_seconds": time.monotonic() - started})


if __name__ == "__main__":
    main()
