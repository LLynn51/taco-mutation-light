"""OPT-IN real isolation test. Not part of test_tracer. No API/credential reads.

Explicit command after review: python -B -m unittest tracer.test_docker -v
Creates a retained runs/tracer-selftest-* directory with actual evidence.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import unittest

from .inputs import ROOT, configuration
from .compare import events
from .checkpoint_io import checkpoint_items
from .runner import TraceRunner
from .storage import Store, owner, write_json


class DockerTraceTest(unittest.TestCase):
    def test_six_real_isolated_executions_and_immutable_snapshots(self):
        cfg = configuration(ROOT / "tracer/config.toml")
        cfg["pipeline"]["collect_workers"] = 6
        cfg["pipeline"]["max_unremoved_containers"] = 8
        runs = ROOT / "runs"
        runs.mkdir(exist_ok=True)
        output = Path(tempfile.mkdtemp(prefix="tracer-selftest-", dir=runs))
        with owner(runs / ".tracer-execution.lock"), owner(Path(cfg["light_run_dir"]) / "run.lock"):
            store = Store(output)
            stop = threading.Event()
            runner = TraceRunner(cfg, store, stop)
            records = []
            try:
                runner.recover()
                runner.health(True)
                contract = {"entry": {"mode": "function", "target": "function", "fn_name": "solve",
                                      "parameter_order": [0], "output_channel": "return"}}

                def execute(index):
                    source = (f"# synthetic isolation fixture {index}\n"
                              "def solve(x):\n"
                              "    import time\n"
                              "    a = [x]\n"
                              "    time.sleep(2)\n"
                              "    a.append(x + 1)\n"
                              "    return a\n")
                    return runner.collect_source(source, contract, [index])

                with ThreadPoolExecutor(max_workers=6) as pool:
                    records = list(pool.map(execute, range(6)))
                for index, record in enumerate(records):
                    self.assertTrue(record["usable"])
                    self.assertEqual(record["traced"]["observation"]["output"], [index, index + 1])
                    lengths = {row.get("locals", {}).get("a", {}).get("length")
                               for row in events(record["trace_file"], cfg["trace"])}
                    self.assertTrue({1, 2} <= lengths, "Early snapshots must not change after append")
                timeline = []
                for record in records:
                    for phase in ("baseline", "traced"):
                        observation = record[phase]
                        timeline += [(observation["target_started_at"], 1), (observation["target_finished_at"], -1)]
                current = peak = 0
                for _, change in sorted(timeline):
                    current += change
                    peak = max(peak, current)
                write_json(output / "verification.json", {"simulated": False, "api_requests": 0,
                           "peak_overlapping_target_intervals": peak, "records": records})
                self.assertGreaterEqual(peak, 6, "Six real target intervals must overlap; inspect Docker capacity")
                source = ("def solve(x):\n"
                          "    a = [0] * 300\n"
                          "    i = 250\n"
                          "    a[i] = x\n"
                          "    return a[i]\n")
                array_record = runner.collect_source(source, contract, [7])
                self.assertTrue(array_record["usable"])
                checkpoints = array_record["traced"]["checkpoints"]
                self.assertEqual(len(checkpoints), 2)
                initial, final = sorted(checkpoints.values(), key=lambda r: r["boundary"])
                self.assertTrue(initial["complete"] and final["complete"])
                self.assertEqual(dict(checkpoint_items(initial))[250]["value"], 0)
                self.assertEqual(dict(checkpoint_items(final))[250]["value"], 7)
                rows = list(events(array_record["trace_file"], cfg["trace"]))
                observed = [item["value"]["value"] for row in rows
                            for item in row.get("locals", {}).get("a", {}).get("sampled_items", [])
                            if item["index"] == 250]
                self.assertTrue({0, 7} <= set(observed))
                write_json(output / "array-verification.json", {"simulated": False, "record": array_record})
                self.assertFalse(store.rows("container"))
            finally:
                stop.set()
                try:
                    runner.clean()
                finally:
                    store.close()


if __name__ == "__main__":
    unittest.main()
