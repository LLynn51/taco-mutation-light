"""Container-only entrypoint. Reuses the original adapter, without editing it."""
import importlib.util
import io
import json
import sys
import time
import dis


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class Collector:
    def __init__(self, stream, limits, encoder, source="", policy=None, checkpoints=None):
        if policy is None:
            from .adapters import policy
        if checkpoints is None:
            from .adapters.checkpoints import Checkpoints
            checkpoints = Checkpoints
        self.stream, self.limits, self.encoder = stream, limits, encoder
        self.policy = policy
        self.omitted_names = policy.runtime_omissions(source)
        self.checkpoints = checkpoints(self.send, limits, encoder)
        self.scopes = {}
        self.checkpoint_gaps = 0
        self.ignored_runtime = self.symbol_summaries = 0
        self.value_bytes = 0
        self.values_exhausted = False
        self.skeleton_events = 0
        self.boundary = 0
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
            counts = self.calls.setdefault(parent, {})
            counts[function] = counts.get(function, 0) + 1
            self.frames[fid] = parent + "/" + function + "#" + str(counts[function])
            if any(function.endswith(x) for x in ("<listcomp>", "<dictcomp>", "<setcomp>")):
                pass  # ephemeral comprehension returns must not produce a full checkpoint each loop
            elif len(self.scopes) < self.limits.get("checkpoint_scopes", 128):
                self.scopes[fid] = {}
            else:
                self.checkpoint_gaps += 1
        if event == "exception":
            self.exceptions.add(fid)
        try:
            self.boundary += 1
            kind = event
            if event == "return" and fid in self.exceptions:
                try:
                    opcode = dis.opname[frame.f_code.co_code[frame.f_lasti]]
                    kind = "return" if opcode == "RETURN_VALUE" else "unwind"
                except (AttributeError, IndexError):
                    kind = "return_or_unwind"
            refs = self.checkpoint_boundary(frame.f_locals, fid, kind, arg)
            if self.count >= self.limits["max_events"]:
                self.reason = self.reason or "event_limit"
            if self.reason:
                return self.trace  # still observe scope exits for final checkpoints
            encoder = self.encoder(self.limits, omitted_names=self.omitted_names)
            snapshot = {} if self.values_exhausted else encoder.snapshot(frame.f_locals, arg, event == "return")
            snapshot_bytes = len(json.dumps(snapshot, ensure_ascii=True).encode())
            if self.values_exhausted or self.value_bytes + snapshot_bytes > self.limits.get("max_value_bytes", self.limits["max_trace_bytes"]):
                self.values_exhausted = True
                snapshot = {"locals": {}, "remaining_locals": {"status": "not_collected", "reason": "value_budget"},
                            "value_gaps": 1, "value_coverage": "not_collected"}
                self.skeleton_events += 1
            else:
                self.value_bytes += snapshot_bytes
            row = {"kind": "event", "event_id": self.count, "event": kind,
                   "phase": "before" if event in ("line", "call") else "at_event",
                   "line": frame.f_lineno, "function": function,
                   "call_id": self.frames.get(fid, "unmatched"), "boundary": self.boundary,
                   "checkpoints": refs, **snapshot}
            if event == "exception":
                row["exception_class"] = type.__getattribute__(arg[0], "__name__")
            encoded = json.dumps(row, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n"
            size = len(encoded.encode())
            if size > self.limits["max_event_bytes"]:
                row.update(locals={}, remaining_locals={"status": "not_collected", "reason": "event_byte_limit"}, value_gaps=1)
                row.pop("detail", None)
                row["checkpoints"] = {"status": "see_checkpoint_stream"}
                encoded = json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n"
                size = len(encoded.encode())
                self.skeleton_events += 1
            if size > self.limits["max_event_bytes"]:
                self.reason = "event_metadata_limit"
                return self.trace
            if self.bytes + size > self.limits["max_trace_bytes"]:
                self.reason = "trace_byte_limit"
                return self.trace
            self.stream.write(encoded)
            self.stream.flush()
            self.bytes += size
            self.count += 1
            self.value_gaps += row["value_gaps"]
            self.ignored_runtime += len(snapshot.get("ignored_runtime_names", []))
            self.symbol_summaries += encoder.symbols
        except Exception:
            self.reason = "snapshot_or_transport_error"
        finally:
            if event == "return":
                self.calls.pop(self.frames.get(fid), None)
                self.frames.pop(fid, None)
                self.exceptions.discard(fid)
                self.scopes.pop(fid, None)
        return self.trace

    def checkpoint_boundary(self, locals_, fid, event, arg):
        scope = self.scopes.get(fid)
        if scope is None:
            return {}
        call_id = self.frames.get(fid, "unmatched")
        refs = {}
        for name in sorted(locals_):
            if name in self.omitted_names:
                continue
            value = locals_[name]
            if name not in scope and len(scope) < self.limits.get("checkpoint_variables", 128):
                scope[name] = {"was_large": False}
                if self.policy.is_large(value, self.limits):
                    refs[name] = self.checkpoints.capture(value, call_id=call_id, name=name,
                                                         phase="first_visible", boundary=self.boundary)
            if name in scope:
                state = scope[name]
                state["was_large"] |= self.policy.is_large(value, self.limits)
            elif self.policy.is_large(value, self.limits):
                refs[name] = {"status": "not_collected", "reason": "checkpoint_variable_limit"}
        if event in ("return", "unwind", "return_or_unwind"):
            captured = {}
            for name, state in scope.items():
                if not state["was_large"]:
                    continue
                value = locals_.get(name)
                if name not in locals_ or type(value) not in (list, tuple):
                    refs[name] = {"status": "not_collected", "reason": "deleted_or_rebound_before_exit"}
                else:
                    ref = self.checkpoints.capture(value, call_id=call_id, name=name,
                                                   phase="scope_exit", boundary=self.boundary)
                    captured[id(value)] = ref
                    refs[name] = ref
            if self.policy.is_large(arg, self.limits):
                refs["<return>"] = captured.get(id(arg)) or self.checkpoints.capture(
                    arg, call_id=call_id, name="<return>", phase="scope_exit", boundary=self.boundary)
        self.checkpoint_gaps += sum(v.get("status") == "not_collected" for v in refs.values())
        return refs


def main():
    payload = json.load(sys.stdin)
    stream = sys.stdout
    adapter = load("light_adapter", "/unit/adapter.py")
    policy = load("trace_policy", "/unit/trace_policy.py")
    load("trace_sequences", "/unit/trace_sequences.py")
    checkpoints = load("trace_checkpoints", "/unit/trace_checkpoints.py")
    values = load("trace_values", "/unit/values.py")
    with open("/unit/source.py", encoding="utf-8") as source:
        collector = Collector(stream, payload["trace"], values.Encoder, source.read(), policy, checkpoints.Checkpoints)
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
                    "ignored_runtime_entries": collector.ignored_runtime,
                    "symbol_summaries": collector.symbol_summaries,
                    "skeleton_events": collector.skeleton_events,
                    "checkpoint_count": collector.checkpoints.count,
                    "checkpoint_bytes": collector.checkpoints.bytes,
                    "checkpoint_partial": collector.checkpoints.partial,
                    "checkpoint_gaps": collector.checkpoint_gaps,
                    "checkpoint_open_scopes": len(collector.scopes),
                    "schema": "line_trace_v2",
                    "scope": "source.py/main_thread/line_boundaries/locals",
                    "elapsed_seconds": time.monotonic() - started})


if __name__ == "__main__":
    main()
