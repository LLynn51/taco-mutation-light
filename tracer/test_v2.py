"""Data-only tests: no target execution, Docker, credentials, or HTTP."""
import copy
import gzip
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor

from .adapters.policy import runtime_omissions
from .analysis import material_for, validate, analyze
from .checkpoint_io import CheckpointSink, checkpoint_items
from .collect import Collector
from .compare import compare_pair, align, leaves, guard_call_instances, compare_checkpoints
from .adapters.checkpoints import Checkpoints
from .facts import oracle_configuration
from .inputs import configuration, ROOT
from .storage import Store, file_digest, write_json, digest
from .stages import import_existing
from .values import Encoder


class V2Tests(unittest.TestCase):
    def setUp(self):
        self.cfg = configuration(ROOT / "tracer/config.toml")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.guard_process = patch("subprocess.Popen", side_effect=AssertionError("No processes in offline tests"))
        self.guard_api = patch("llm.Client.complete", side_effect=AssertionError("No HTTP in offline tests"))
        self.guard_process.start(); self.guard_api.start()
        self.addCleanup(self.guard_process.stop); self.addCleanup(self.guard_api.stop)

    def pair(self):
        return {"pair_id": "fixture", "spec": "Return the input plus one.",
                "original_code": "def f(x):\n return x+1\n", "mutant_code": "def f(x):\n return x-1\n",
                "execution_contract": {"entry": {"mode": "function", "fn_name": "f"}, "comparison": {"kind": "json_exact"}},
                "input": [3], "expected_available": True, "expected": 4, "input_source": "fixture"}

    def event(self, i, event="line", line=2, values=None, detail=None):
        return {"kind": "event", "event_id": i, "event": event, "line": line, "function": "f",
                "call_id": "root/f#1", **Encoder(self.cfg["trace"]).snapshot(values or {"x": 3}, detail, event == "return")}

    def traces(self, left, right):
        records = {}
        for side, rows, output in (("original", left, 4), ("mutant", right, 2)):
            path = self.directory / (side + ".jsonl.gz")
            with gzip.open(path, "wt") as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")
            records[side] = {"trace_file": str(path), "trace_sha256": file_digest(path),
                             "usable": True, "external_results_match": True,
                             "environment": {"trace": self.cfg["trace"]},
                             "traced": {"event_count": len(rows), "events_complete": True, "value_gaps": 0,
                                        "unsupported_frames": 0, "checkpoints": {},
                                        "observation": {"status": "ok", "output": output, "exception_class": None}}}
        return records

    def frame(self, values):
        return SimpleNamespace(f_code=SimpleNamespace(co_filename="/unit/source.py", co_flags=0, co_qualname="f"),
                               f_lineno=1, f_locals=values, f_back=None)

    def test_noise_is_separate_and_explicit_names_are_retained(self):
        omitted = runtime_omissions("def f(x): return x")
        row = Encoder(self.cfg["trace"], omitted_names=omitted).snapshot({"__builtins__": {"huge": [1]*500}, "_data": 2})
        self.assertNotIn("__builtins__", row["locals"])
        self.assertEqual(row["ignored_runtime_names"], ["__builtins__"])
        self.assertEqual(row["value_gaps"], 0)
        self.assertIn("_data", row["locals"])
        self.assertNotIn("__builtins__", runtime_omissions("__builtins__ = {}"))
        self.assertNotIn("__name__", runtime_omissions("def f(__name__): return 1"))
        self.assertNotIn("__name__", runtime_omissions("import math as __name__"))
        self.assertEqual(runtime_omissions("x = globals()"), frozenset())

    def test_large_sequence_windows_are_frozen_and_include_active_index(self):
        arr = [0] * 100001
        arr[99348] = 17
        event = Encoder(self.cfg["trace"]).snapshot({"D": arr, "i": 99348})
        selected = {x["index"]: x["value"]["value"] for x in event["locals"]["D"]["sampled_items"]}
        arr[99348] = 99
        self.assertEqual(selected[99348], 17)
        self.assertIn(0, selected); self.assertIn(100000, selected)
        self.assertLess(len(selected), 64)
        self.assertEqual(event["locals"]["D"]["coverage"], "partial")

    def test_checkpoints_survive_event_limit_without_each_rebinding_checkpoint(self):
        stream = io.StringIO()
        c = Collector(stream, {**self.cfg["trace"], "max_events": 1}, Encoder, "def f(D): return D")
        frame = self.frame({"D": list(range(300))})
        c.trace(frame, "call", None)
        for _ in range(3):
            frame.f_locals["D"] = list(range(400))
            c.trace(frame, "line", None)
        c.trace(frame, "return", frame.f_locals["D"])
        rows = [json.loads(s) for s in stream.getvalue().splitlines()]
        starts = [r for r in rows if r["kind"] == "checkpoint_start"]
        self.assertEqual([r["phase"] for r in starts], ["first_visible", "scope_exit"])
        self.assertEqual(c.reason, "event_limit")
        sink = CheckpointSink(self.directory / "checkpoints", self.cfg["trace"])
        for row in rows:
            if row["kind"].startswith("checkpoint_"):
                sink.accept(row)
        sink.close()
        self.assertTrue(all(r["complete"] for r in sink.records.values()))
        final = list(checkpoint_items(sink.records["cp2"]))
        self.assertEqual(len(final), 400)
        self.assertEqual(final[-1][1]["value"], 399)

    def test_value_budget_keeps_line_events(self):
        stream = io.StringIO()
        c = Collector(stream, {**self.cfg["trace"], "max_value_bytes": 1}, Encoder)
        frame = self.frame({"x": 1})
        for event in ("call", "line", "return"):
            c.trace(frame, event, None)
        self.assertEqual(c.count, 3)
        self.assertEqual(c.skeleton_events, 3)
        self.assertIsNone(c.reason)

    def test_call_counters_do_not_accumulate_after_scope_exit(self):
        c = Collector(io.StringIO(), {**self.cfg["trace"], "max_events": 1}, Encoder)
        for _ in range(20):
            outer = self.frame({"x": 1})
            inner = self.frame({"x": 1})
            inner.f_code.co_qualname = "g"
            inner.f_back = outer
            c.trace(outer, "call", None)
            c.trace(inner, "call", None)
            c.trace(inner, "return", 1)
            c.trace(outer, "return", 1)
        self.assertEqual(c.calls, {"root": {"f": 20}})
        self.assertFalse(c.frames)
        self.assertFalse(c.scopes)

    def test_checkpoint_limit_is_explicit_and_does_not_stop_events(self):
        stream = io.StringIO()
        c = Collector(stream, {**self.cfg["trace"], "checkpoint_max_items": 10}, Encoder)
        frame = self.frame({"a": list(range(300))})
        c.trace(frame, "call", None); c.trace(frame, "return", 1)
        ends = [json.loads(s) for s in stream.getvalue().splitlines() if '"kind":"checkpoint_end"' in s]
        self.assertEqual(len(ends), 2)
        self.assertTrue(all(not e["complete"] and e["items_recorded"] == 10 for e in ends))
        self.assertIsNone(c.reason)

    def test_deleted_large_binding_marks_missing_final_checkpoint(self):
        c = Collector(io.StringIO(), self.cfg["trace"], Encoder)
        frame = self.frame({"a": list(range(200))})
        c.trace(frame, "call", None)
        frame.f_locals.clear()
        c.trace(frame, "return", None)
        self.assertEqual(c.checkpoint_gaps, 1)

    def test_return_on_unmapped_changed_line_is_compared(self):
        pair = self.pair()
        pair["mutant_code"] = "def f(x):\n if x:\n  return False\n return False\n"
        traces = self.traces([self.event(0, "call", 1), self.event(1, "return", 2, detail=True)],
                             [self.event(0, "call", 1), self.event(1, "return", 3, detail=False)])
        path = compare_pair(pair, traces, self.cfg, self.directory)
        report = json.loads(Path(path).read_text())
        self.assertEqual(report["difference_count"], 1)
        self.assertIn("<return>", report["first_divergence"]["candidate"]["variables"])
        self.assertEqual(report["observable_consequence"]["status"], "observed")
        self.assertEqual(compare_pair(pair, traces, self.cfg, self.directory), path)

    def test_different_loop_counts_keep_matches_and_report_unmatched(self):
        def r(i, token):
            return {"id": i, "token": token}
        left = [r(i, t) for i, t in enumerate(["enter", "loop", "body", "loop", "body", "exit"])]
        right = [r(i, t) for i, t in enumerate(["enter", "loop", "body", "exit"])]
        matched, skipped = align(left, right, 64)
        self.assertEqual(len(matched), 4)
        self.assertEqual(len(skipped["original"]), 2)

    def test_extra_calls_require_unique_entry_values(self):
        def call(n, value):
            return {"id": n, "event": "call", "call_id": "root/f#" + str(n),
                    "token": ("call", "f", "root/f#" + str(n), "boundary"),
                    "values": {"x": digest(value)}, "partial_locals": False}
        rows = {"original": [call(1, 8)], "mutant": [call(1, 7), call(2, 8)]}
        guard_call_instances(rows)
        self.assertIsNone(rows["mutant"][0]["token"])
        self.assertEqual(rows["original"][0]["token"], rows["mutant"][1]["token"])
        rows = {"original": [call(1, 8)], "mutant": [call(1, 8), call(2, 8)]}
        guard_call_instances(rows)
        self.assertTrue(all(r["token"] is None for rs in rows.values() for r in rs))

    def test_checkpoint_comparison_budget_and_unmatched_calls(self):
        traces = {}
        for side in ("original", "mutant"):
            stream = io.StringIO()
            capture = Checkpoints(lambda row: stream.write(json.dumps(row) + "\n"), self.cfg["trace"], Encoder)
            capture.capture(list(range(150)), call_id="root/f#1", name="a", phase="scope_exit", boundary=2)
            sink = CheckpointSink(self.directory / side, self.cfg["trace"])
            for line in stream.getvalue().splitlines():
                sink.accept(json.loads(line))
            sink.close()
            traces[side] = {"traced": {"checkpoints": sink.records}}
        cfg = copy.deepcopy(self.cfg); cfg["analysis"]["checkpoint_compare_items"] = 10
        self.assertEqual(compare_checkpoints(traces, cfg, []), [])
        matches = [({"call_id": "root/f#1"}, {"call_id": "root/f#1"})]
        result = compare_checkpoints(traces, cfg, matches)[0]
        self.assertTrue(result["comparison_limited"])
        self.assertEqual(result["compared_items"], 10)
        self.assertEqual(result["status"], "no_difference_in_compared_items")
        result = compare_checkpoints(traces, self.cfg, matches)[0]
        self.assertEqual(result["status"], "equal_contents")

    def test_partial_and_legacy_prefix_differences_are_detected(self):
        a, b = [0] * 300, [0] * 300
        b[250] = 9
        left = self.event(0, values={"D": a, "i": 250})
        right = self.event(0, values={"D": b, "i": 250})
        traces = self.traces([left], [right])
        report = json.loads(Path(compare_pair(self.pair(), traces, self.cfg, self.directory)).read_text())
        self.assertIn("D[250]", report["first_divergence"]["candidate"]["value_paths"])
        legacy = {"locals": {"D": {"type": "list", "length": 300, "object_id": "o1",
                                   "items": [{"type": "int", "value": 7}],
                                   "tail": {"status": "not_collected", "reason": "container_item_limit"}}}}
        self.assertIn("D[0]", leaves(legacy))

    def test_symbol_contents_are_not_treated_as_equal_functions(self):
        row = Encoder(self.cfg["trace"]).snapshot({"f": len})
        self.assertEqual(row["symbol_summaries"], 1)
        self.assertEqual(row["value_gaps"], 0)
        self.assertEqual(leaves(row), {})

    def test_material_budget_retains_both_sides(self):
        pair = self.pair()
        traces = self.traces([self.event(0)], [self.event(0)])
        report = {"traces": traces, "evidence_events": [
            {"ref": s + ":" + str(i), "selection_rank": i, "event": {"padding": "x" * 600}}
            for s in ("original", "mutant") for i in range(6)]}
        cfg = copy.deepcopy(self.cfg); cfg["analysis"]["max_material_bytes"] = 6000
        material = material_for(pair, report, cfg)
        self.assertIsNotNone(material)
        coverage = material["analysis_evidence_coverage"]
        self.assertGreaterEqual(coverage["original"]["sent"], 1)
        self.assertLessEqual(abs(coverage["original"]["sent"] - coverage["mutant"]["sent"]), 1)
        self.assertEqual(len(report["evidence_events"]), 12)

    def reply(self):
        reply = {"summary": "fixture", "uncertainty": "candidate", "requirement": {
            "text": "Return input plus one", "status": "proposed", "spec_basis": [{"quote": "input plus one"}]}}
        for name in ("first_divergence", "propagation", "trigger_condition"):
            reply[name] = {"text": "candidate", "status": "inferred", "evidence": ["original:0", "mutant:0"]}
        return reply

    def test_requirement_rejects_invented_spec_and_oracle_override(self):
        reply = self.reply()
        validate(reply, {"original:0", "mutant:0"}, self.pair()["spec"])
        reply["requirement"]["spec_basis"] = [{"quote": "invented requirement"}]
        with self.assertRaises(ValueError):
            validate(reply, {"original:0", "mutant:0"}, self.pair()["spec"])
        reply = self.reply(); reply["oracle_type"] = "property"
        with self.assertRaises(ValueError):
            validate(reply, {"original:0", "mutant:0"}, self.pair()["spec"])

    def test_analysis_mechanical_fields_and_no_repeat_after_revision(self):
        pair = self.pair()
        traces = self.traces([self.event(0)], [self.event(0)])
        comparison = compare_pair(pair, traces, self.cfg, self.directory)
        store = Store(self.directory / "analysis-store")
        self.addCleanup(store.close)
        with patch("tracer.analysis.Client") as client:
            client.return_value.complete.return_value = {"response": self.reply(), "usage": {"total_tokens": 10}}
            result = analyze(pair, comparison, self.cfg, store, 1)
            self.assertEqual(result["status"], "complete")
            self.assertEqual(result["oracle_type"]["kind"], "exact_value")
            self.assertEqual(result["observable_consequence"]["changes"][0]["original"], 4)
            analyze(pair, comparison, self.cfg, store, 1)
            self.assertEqual(client.return_value.complete.call_count, 1)
            changed = copy.deepcopy(self.cfg); changed["analysis"]["max_tokens"] += 1
            other = analyze(pair, comparison, changed, store, 2)
            self.assertEqual(other["status"], "existing_request_for_other_version")
            self.assertEqual(client.return_value.complete.call_count, 1)

    def test_offline_stage_import_and_compare_do_not_construct_runner(self):
        from . import pipeline
        pair = self.pair()
        traces = self.traces([self.event(0)], [self.event(0, values={"x": 4})])
        source = Store(self.directory / "source")
        output = Store(self.directory / "output")
        self.addCleanup(source.close); self.addCleanup(output.close)
        write_json(source.directory / "pairs/fixture/input.json", pair)
        source.put("job", "fixture", {"traces": traces, "collect_attempts": 1, "stage": "collected",
                                      "analysis": {"status": "complete", "analysis_version": "old"}})
        source.put("request", "unselected-pair", {"status": "unknown"})
        receipt = source.directory / "analysis/unselected-pair/response.json"
        write_json(receipt, {"fixture": True})
        pairs = import_existing(source.directory, output, 1)
        self.assertEqual(output.get("request", "unselected-pair")["status"], "unknown")
        copied = Path(output.get("request", "unselected-pair")["analysis_directory"]) / "response.json"
        self.assertTrue(copied.is_relative_to(output.directory))
        self.assertEqual(file_digest(receipt), file_digest(copied))
        with patch.object(pipeline, "TraceRunner", side_effect=AssertionError("Offline stage constructed Docker runner")), \
             patch.object(pipeline, "ProcessPoolExecutor", lambda max_workers, **kw: ThreadPoolExecutor(max_workers)), \
             patch.object(pipeline, "emit"):
            result = pipeline.run(pairs, self.cfg, output, mode="compare")
        self.assertEqual(result["jobs"], {"compared": 1})
        self.assertEqual(result["requests"], 1)
        self.assertNotIn("analysis", output.get("job", "fixture"))
        self.assertEqual(output.get("job", "fixture")["previous_analysis"]["analysis_version"], "old")
        self.assertEqual(source.get("job", "fixture")["stage"], "collected")

    def test_cli_offline_stage_uses_frozen_source_and_preserves_artifacts(self):
        from . import pipeline, __main__
        from contextlib import redirect_stdout
        source = Store(self.directory / "cli-source")
        pair = self.pair()
        write_json(source.directory / "pairs/fixture/input.json", pair)
        source.put("job", "fixture", {"traces": self.traces([self.event(0)], [self.event(0)]),
                                     "collect_attempts": 1, "stage": "collected"})
        source.close()
        (source.directory / "run.lock").touch()
        before = file_digest(source.directory / "state.sqlite")
        argv = ["tracer", "compare", "--source", str(source.directory), "--output", str(self.directory / "cli-output")]
        with patch("sys.argv", argv), redirect_stdout(io.StringIO()), \
             patch.object(pipeline, "TraceRunner", side_effect=AssertionError("No Docker")), \
             patch.object(pipeline, "ProcessPoolExecutor", lambda max_workers, **kw: ThreadPoolExecutor(max_workers)):
            self.assertEqual(__main__.main(), 0)
        self.assertEqual(file_digest(source.directory / "state.sqlite"), before)

    def test_checkpoint_offset_tampering_is_rejected(self):
        sink = CheckpointSink(self.directory / "bad-checkpoint", self.cfg["trace"])
        self.addCleanup(sink.close)
        sink.accept({"kind": "checkpoint_start", "checkpoint_id": "cp1", "length": 3})
        with self.assertRaises(ValueError):
            sink.accept({"kind": "checkpoint_chunk", "checkpoint_id": "cp1", "offset": 1, "items": []})


if __name__ == "__main__":
    unittest.main()
