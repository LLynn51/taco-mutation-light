"""Offline regression tests. Written for review; do not run Docker or HTTP.

Run explicitly in WSL: python -B -m unittest tracer.test_tracer -v
"""
from concurrent.futures import ThreadPoolExecutor
import copy
import gzip
import io
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from .analysis import validate
from .collect import Collector
from .compare import compare_pair, semantic_values
from .inputs import configuration, ROOT, read_pairs
from .storage import Store, file_digest, write_json
from .values import Encoder


class TracerTests(unittest.TestCase):
    def setUp(self):
        self.cfg = configuration(ROOT / "tracer/config.toml")

    def test_snapshot_freezes_values_and_aliases(self):
        shared = [1]
        frozen = Encoder(self.cfg["trace"]).snapshot({"a": shared, "b": shared})
        shared.append(2)
        self.assertEqual(frozen["locals"]["a"]["length"], 1)
        self.assertEqual(frozen["locals"]["b"], {"ref": frozen["locals"]["a"]["object_id"]})
        cycle = []
        cycle.append(cycle)
        row = Encoder(self.cfg["trace"]).snapshot({"cycle": cycle})
        self.assertIsNone(semantic_values(row)["cycle"])

    def test_no_repr_property_or_equality_invocation(self):
        class Hostile:
            def __repr__(self):
                raise AssertionError("repr executed")

            def __eq__(self, other):
                raise AssertionError("eq executed")

            @property
            def secret(self):
                raise AssertionError("property executed")
        value = Hostile()
        value.x = 3
        row = Encoder(self.cfg["trace"]).snapshot({"x": value})
        self.assertEqual(row["locals"]["x"]["type"], "instance")
        self.assertNotIn("secret", json.dumps(row))

    def test_limits_are_explicit(self):
        cfg = {**self.cfg["trace"], "max_items": 1}
        row = Encoder(cfg).snapshot({"x": [1, 2]})
        self.assertGreater(row["value_gaps"], 0)
        self.assertEqual(row["locals"]["x"]["tail"]["reason"], "container_item_limit")

    def test_line_phase_and_event_limit(self):
        stream = io.StringIO()
        collector = Collector(stream, {**self.cfg["trace"], "max_events": 2}, Encoder)
        frame = SimpleNamespace(f_code=SimpleNamespace(co_filename="/unit/source.py", co_flags=0, co_qualname="f"),
                                f_lineno=1, f_locals={"x": 1}, f_back=None)
        collector.trace(frame, "call", None)
        frame.f_lineno = 2
        collector.trace(frame, "line", None)
        collector.trace(frame, "return", 1)
        rows = [json.loads(line) for line in stream.getvalue().splitlines()]
        self.assertEqual(rows[1]["phase"], "before")
        self.assertEqual(collector.reason, "event_limit")

    def test_compare_is_conservative_and_snapshot_ids_do_not_create_difference(self):
        event = Encoder(self.cfg["trace"]).snapshot({"a": [3]})
        renamed = copy.deepcopy(event)
        renamed["locals"]["a"]["object_id"] = "different-local-id"
        self.assertEqual(semantic_values(event), semantic_values(renamed))
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            traces = {}
            for side, value in (("original", 4), ("mutant", 2)):
                path = directory / (side + ".gz")
                row = {"kind": "event", "event_id": 0, "event": "line", "line": 3,
                       "function": "f", "call_id": "root/f#1",
                       **Encoder(self.cfg["trace"]).snapshot({"x": value})}
                with gzip.open(path, "wt") as handle:
                    handle.write(json.dumps(row) + "\n")
                traces[side] = {"trace_file": str(path), "trace_sha256": file_digest(path),
                                "usable": True, "external_results_match": True, "traced": {}}
            pair = {"pair_id": "test", "original_code": "def f():\n x=4\n return x\n",
                    "mutant_code": "def f():\n x=2\n return x\n"}
            path = compare_pair(pair, traces, self.cfg, directory)
            result = json.loads(Path(path).read_text())
            self.assertEqual(result["first_divergence"]["status"], "observed_candidate")
            self.assertEqual(result["propagation"]["edges"], [])
            self.assertEqual(result["alignment"]["certainty"], "heuristic")

    def test_atomic_request_cap_and_unknown_recovery(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Store(temp)
            try:
                with ThreadPoolExecutor(max_workers=6) as pool:
                    accepted = list(pool.map(lambda i: store.reserve_request(str(i), {}, 3), range(20)))
                self.assertEqual(sum(accepted), 3)
                store.recover_requests()
                self.assertTrue(all(row["status"] == "unknown" for _, row in store.rows("request")))
                self.assertFalse(store.reserve_request(store.rows("request")[0][0], {}, 20))
            finally:
                store.close()

    def test_analysis_rejects_unmeasured_propagation_and_fabricated_refs(self):
        reply = {"summary": "summary", "uncertainty": "limited"}
        for name in ("first_divergence", "propagation", "trigger_condition"):
            reply[name] = {"text": "bounded", "status": "inferred", "evidence": ["original:1"]}
        reply["requirement"] = {"text": "insufficient SPEC", "status": "unknown", "spec_basis": []}
        validate(reply, {"original:1"})
        reply["propagation"]["status"] = "observed"
        with self.assertRaises(ValueError):
            validate(reply, {"original:1"})
        reply["propagation"]["status"] = "inferred"
        with self.assertRaises(ValueError):
            validate(reply, set())

    def test_sample_import_never_requires_expected(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "pair.json"
            write_json(path, {"spec": "Return an integer", "original_code": "def f(x): return x+1",
                              "mutant_code": "def f(x): return x-1", "input": [3],
                              "execution_contract": {"entry": {"mode": "function", "fn_name": "f"}}})
            pair = read_pairs(path)[0]
            self.assertFalse(pair["expected_available"])
            self.assertEqual(pair["input"], [3])

    def test_stdin_entry_and_unknown_request_are_preserved(self):
        from .analysis import analyze
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "pair.json"
            write_json(path, {"spec": "Echo input", "original_code": "print(input())",
                              "mutant_code": "print(input() + '!')", "input": "hello\n",
                              "execution_contract": {"entry": {"mode": "stdin_stdout"}}})
            pair = read_pairs(path)[0]
            self.assertEqual(pair["execution_contract"]["entry"], {"mode": "stdin_stdout"})
            comparison = Path(temp) / "comparison.json"
            write_json(comparison, {"evidence_events": [], "evidence_status": "usable_with_limits", "traces": {}})
            store = Store(Path(temp) / "state")
            try:
                store.put("request", pair["pair_id"], {"status": "unknown"})
                with patch("tracer.analysis.Client") as client:
                    result = analyze(pair, comparison, self.cfg, store, 100)
                client.assert_not_called()
                self.assertEqual(result["status"], "unknown")
            finally:
                store.close()

    def test_cleanup_failure_retains_container_until_confirmed_absent(self):
        from .runner import TraceRunner, DockerUnavailable
        with tempfile.TemporaryDirectory() as temp:
            store = Store(temp)
            try:
                runner = TraceRunner(self.cfg, store, threading.Event())
                unit = store.directory / "units/unit-test"
                unit.mkdir(parents=True)
                name = "light-trace-test"
                store.put("container", name, {"status": "cleanup_pending", "unit": str(unit)})

                def unavailable(args, **kw):
                    if args[0] == "ps":
                        return (name + "\n").encode()
                    raise DockerUnavailable("synthetic cleanup outage")

                with patch.object(runner, "docker", side_effect=unavailable):
                    with self.assertRaises(DockerUnavailable):
                        runner.clean()
                self.assertIsNotNone(store.get("container", name))
                self.assertTrue(unit.exists())
                with patch.object(runner, "docker", return_value=b""):
                    runner.clean()
                self.assertIsNone(store.get("container", name))
                self.assertFalse(unit.exists())
            finally:
                store.close()

    def test_six_collectors_and_overlapping_stages_without_docker_or_http(self):
        from . import pipeline
        barrier = threading.Barrier(6, timeout=5)
        spans = {}
        analysis_times = []

        class FakeRunner:
            def __init__(self, *args):
                pass

            def recover(self):
                pass

            def health(self, *args):
                pass

            def clean(self):
                pass

            def collect_pair(self, pair):
                key = int(pair["pair_id"])
                start = time.monotonic()
                if key < 6:
                    barrier.wait()
                time.sleep(0.05 if key < 6 else 0.4)
                spans[key] = (start, time.monotonic())
                return {"fixture": True}

        def fake_compare(pair, traces, cfg, directory):
            time.sleep(0.02)
            path = Path(directory) / "comparison.json"
            write_json(path, {"fixture": True})
            return str(path)

        def fake_analyze(*args):
            analysis_times.append(time.monotonic())
            return {"status": "complete", "fixture": True}

        with tempfile.TemporaryDirectory() as temp:
            store = Store(temp)
            try:
                with patch.object(pipeline, "TraceRunner", FakeRunner), \
                     patch.object(pipeline, "compare_pair", fake_compare), \
                     patch.object(pipeline, "analyze", fake_analyze), \
                     patch.object(pipeline, "ProcessPoolExecutor", lambda max_workers, **kw: ThreadPoolExecutor(max_workers)), \
                     patch.object(pipeline, "emit"):
                    pipeline.run([{"pair_id": str(i)} for i in range(8)], self.cfg, store, paid=True, max_requests=8)
                self.assertLess(max(spans[i][0] for i in range(6)), min(spans[i][1] for i in range(6)))
                self.assertLess(min(analysis_times), max(end for start, end in spans.values()))
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
