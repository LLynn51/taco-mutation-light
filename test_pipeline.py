"""One acceptance file: local HTTP only; target snippets execute only in Docker.
Run: python -B -m unittest -v test_pipeline
"""

import copy
import json
import threading
import time
import unittest
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from adapters.interface import freeze_entry
from export import export, public
from llm import Client, RequestFailure, payload
from mutation import rebuild
from pipeline import Pipeline, ROOT, UNSUPPORTED, load_config
from runner import DockerRunner, compare
from state import State
from tasks import normalize

SOURCE = """def solve(xs, n):
    total = 0
    for i in range(n):
        if xs[i] > 0 and n > 1:
            total += xs[i] * 2
    return total
"""
PATCHES = {
    "ROR": {"rule_id": "ROR_GT_GE", "old_fragment": "n > 1", "new_fragment": "n >= 1"},
    "LCR": {
        "rule_id": "LCR_AND_OR",
        "old_fragment": "xs[i] > 0 and n > 1",
        "new_fragment": "xs[i] > 0 or n > 1",
    },
    "AOR": {
        "rule_id": "AOR_MUL_FDIV",
        "old_fragment": "xs[i] * 2",
        "new_fragment": "xs[i] // 2",
    },
    "CLR": {
        "rule_id": "CLR_INT_UP",
        "old_fragment": "total = 0",
        "new_fragment": "total = 1",
    },
    "IBR": {
        "rule_id": "IBR_STOP_DOWN",
        "old_fragment": "range(n)",
        "new_fragment": "range(n - 1)",
    },
    "SDL": {
        "rule_id": "SDL_SIMPLE",
        "old_fragment": "total += xs[i] * 2",
        "new_fragment": "pass",
    },
}


class FixtureServer:
    def __init__(self, failures=None, barrier=False, malformed=False, disconnect=False):
        self.failures = failures or {}
        self.barrier = threading.Barrier(6) if barrier else None
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.counts = Counter()
        self.malformed = malformed
        self.disconnect = disconnect
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                family = json.loads(body["messages"][-1]["content"])["family"]
                with fixture.lock:
                    fixture.counts[family] += 1
                    number = fixture.counts[family]
                    fixture.active += 1
                    fixture.peak = max(fixture.peak, fixture.active)
                try:
                    if fixture.barrier and number == 1:
                        fixture.barrier.wait(timeout=20)
                    time.sleep(0.1)
                    if fixture.disconnect and number <= fixture.failures.get(family, 0):
                        self.connection.shutdown(2)
                        self.connection.close()
                        return
                    finish = (
                        "length"
                        if number <= fixture.failures.get(family, 0)
                        else "stop"
                    )
                    text = (
                        "broken json"
                        if fixture.malformed
                        else json.dumps(PATCHES[family])
                    )
                    choice = {
                        "index": 0,
                        "finish_reason": finish,
                        "delta": {"content": text, "reasoning_content": "fixture"},
                    }
                    data = {
                        "choices": [choice],
                        "usage": {
                            "prompt_tokens": 20,
                            "completion_tokens": 30,
                            "total_tokens": 50,
                        },
                    }
                    if body.get("stream"):
                        raw = (
                            "data: " + json.dumps(data) + "\n\ndata: [DONE]\n\n"
                        ).encode()
                        self.send_response(200)
                        self.send_header("Content-Type", "text/event-stream")
                    else:
                        data["choices"][0] = {
                            "finish_reason": finish,
                            "message": {"content": text},
                        }
                        raw = json.dumps(data).encode()
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                finally:
                    with fixture.lock:
                        fixture.active -= 1

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class Acceptance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config(ROOT / "config.toml")
        cls.directory = ROOT / "runs" / ("selftest-" + uuid.uuid4().hex[:8])
        cls.directory.mkdir(parents=True)
        cls.runner = DockerRunner(cls.config["execution"], cls.directory / "sandbox")
        cls.facts = cls.runner.check()
        cls.evidence = {
            "isolation": cls.facts,
            "paid_requests": 0,
            "credential_files_read": 0,
        }

    @classmethod
    def tearDownClass(cls):
        (cls.directory / "verification.json").write_text(
            json.dumps(cls.evidence, ensure_ascii=False, indent=2)
        )
        print("\nVerification:", cls.directory / "verification.json")

    def test_01_form_and_representation(self):
        for family, patch in PATCHES.items():
            self.assertEqual(rebuild(SOURCE, family, patch)["form"]["family"], family)
        with self.assertRaises(ValueError):
            rebuild(SOURCE, "ROR", PATCHES["AOR"])
        row = {
            "question": "fixture",
            "solutions": [SOURCE],
            "input_output": {"fn_name": "solve", "inputs": [[[2], 1]], "outputs": [0]},
        }
        task = normalize(row, 0, {})
        self.assertEqual(task["status"], "pending")
        row["input_output"]["inputs"] = []
        self.assertEqual(normalize(row, 0, {})["reason"], "no_official_input")
        row = {
            "definition": {
                "task_key": "x",
                "spec": "has textual example",
                "solutions": [SOURCE],
                "official_inputs": [[[2], 1]],
                "official_outputs": [0],
                "official_material": [{"source": "question:Example"}],
            }
        }
        self.assertEqual(normalize(row, 0, {})["reason"], "no_usable_official_cases")
        c = {"comparison": {"kind": "json_exact"}}
        self.assertEqual(compare({"status": "ok", "output": True}, 1, c), "fail")
        self.assertEqual(
            compare({"status": "wrapper_error", "output": None}, None, c), "unknown"
        )

    def run_code(self, source, entry, value):
        return self.runner.execute(source, freeze_entry(entry, source), value)

    def test_02_real_adapters(self):
        obs = self.run_code(
            "import sys\nprint(sum(map(int, sys.stdin.buffer.read().split())))\nsys.exit(0)\n",
            {"mode": "stdin_stdout"},
            "2 3\n",
        )
        self.assertEqual((obs["status"], obs["output"]), ("ok", "5\n"))
        source = "class Solution:\n    def f(self,a,b): return a-b\n"
        obs = self.run_code(
            source,
            {"mode": "function", "fn_name": "f", "parameter_order": [1, 0]},
            [2, 7],
        )
        self.assertEqual(obs["output"], 5)
        entry = {
            "mode": "function",
            "fn_name": "f",
            "output_channel": "mutated_argument",
            "output_argument": 0,
        }
        obs = self.run_code("def f(xs): xs.append(9)\n", entry, [[1]])
        self.assertEqual(obs["output"], [1, 9])
        for kind, encoding, fields, values in [
            ("linked_list", "values", {"value": "val", "next": "next"}, [1, 2, 3]),
            (
                "binary_tree",
                "level_order",
                {"value": "val", "left": "left", "right": "right"},
                [1, 2, 3, None, 4],
            ),
        ]:
            structure = {"kind": kind, "encoding": encoding, "fields": fields}
            entry = {
                "mode": "function",
                "fn_name": "f",
                "structures": {"0": structure},
                "output_structure": structure,
            }
            obs = self.run_code("def f(node): return node\n", entry, [values])
            self.assertEqual(obs["output"], values)
        obs = self.run_code(
            "def f(): raise ValueError('x')\n", {"mode": "function", "fn_name": "f"}, []
        )
        self.assertEqual(
            (obs["status"], obs["exception_class"]), ("program_exception", "ValueError")
        )
        obs = self.run_code(
            "def f(): return (1,2)\n", {"mode": "function", "fn_name": "f"}, []
        )
        self.assertEqual(obs["status"], "wrapper_error")
        row = {
            "question": "return input",
            "source": "geeksforgeeks",
            "solutions": ["class Solution:\n    def f(self,x): return x\n"],
            "input_output": {"inputs": ["[1,2]"], "outputs": ["[1,2]"]},
        }
        adapted = normalize(row, 0, {}, self.config["data"]["source_profiles"])
        self.assertEqual(adapted["cases"][0]["input"], [[1, 2]])
        obs = self.run_code(
            row["solutions"][0],
            adapted["contract"]["entry"],
            adapted["cases"][0]["input"],
        )
        self.assertEqual(
            compare(obs, adapted["cases"][0]["expected"], adapted["contract"]), "pass"
        )
        self.evidence["adapters"] = [
            "stdin.buffer",
            "SystemExit(0)",
            "Solution",
            "parameter_order",
            "mutated_argument",
            "linked_list",
            "binary_tree",
            "exception",
            "unsupported_tuple",
        ]

    def test_03_six_real_containers(self):
        source = "import time\ndef f():\n    start=time.time()\n    time.sleep(1.5)\n    return [start,time.time()]\n"
        barrier = threading.Barrier(6)

        def one(_):
            barrier.wait()
            return self.run_code(source, {"mode": "function", "fn_name": "f"}, [])

        with ThreadPoolExecutor(max_workers=6) as pool:
            observations = list(pool.map(one, range(6)))
        self.assertTrue(all(o["status"] == "ok" for o in observations))
        events = sorted(
            [(o["output"][0], 1) for o in observations]
            + [(o["output"][1], -1) for o in observations]
        )
        active = peak = 0
        for _, delta in events:
            active += delta
            peak = max(peak, active)
        self.assertEqual(peak, 6)
        self.evidence["real_container_peak"] = peak
        self.evidence["real_container_intervals"] = [o["output"] for o in observations]

    def run_fixture(self, failures, pool_size=1, disconnect=False):
        server = FixtureServer(failures, barrier=True, disconnect=disconnect)
        self.addCleanup(server.close)
        cfg = copy.deepcopy(self.config)
        folder = self.directory / ("flow-" + str(len(self.evidence)))
        folder.mkdir()
        row = {
            "task_id": "fixture-task",
            "question": "Return twice the sum of positive entries when n > 1, otherwise zero.",
            "solutions": [SOURCE],
            "input_output": {
                "fn_name": "solve",
                "inputs": [[[2], 1], [[2, 4], 2]],
                "outputs": [0, 12],
            },
            "provenance": {"test_fixture": True},
        }
        rows = [
            {
                **row,
                "task_id": f"fixture-task-{i}",
                "question": row["question"] + f" Fixture {i}.",
            }
            for i in range(pool_size)
        ]
        path = folder / "tasks.json"
        path.write_text(json.dumps(rows))
        cfg["data"].update(source=str(path), run_dir=str(folder), pool_size=pool_size)
        cfg["sampling"].update(target=6, per_family=1, acceptable=4, minimum=2)
        cfg["api"].update(
            base_url=server.url, model="local-fixture", credential_file=""
        )
        state = State(folder)
        self.addCleanup(state.close)
        client = Client(cfg["api"], key_loader=lambda _: "local-fixture-key")
        result = Pipeline(cfg, state, self.runner, client).run()
        self.assertEqual(server.peak, 6)
        counts = dict(server.counts)
        Pipeline(cfg, state, self.runner, client).run()
        self.assertEqual(dict(server.counts), counts, "resume must not resend")
        for sample in state.samples():
            self.assertEqual(sample["label_status"], "unknown")
            self.assertEqual(sample["first_divergence"], UNSUPPORTED)
            self.assertEqual(sample["behavior_abstraction"], UNSUPPORTED)
            view = public(sample)
            self.assertNotIn("mutant_code", view)
            self.assertEqual(view["form"], "盲标阶段隐藏")
        exported = export(state)
        self.evidence[
            "flow-" + str(failures) + ("-disconnect" if disconnect else "")
        ] = {
            "result": result,
            "http_peak": server.peak,
            "requests": counts,
            "export": exported,
        }
        return result, state, server

    def test_04_full_flow_and_retry(self):
        result, state, server = self.run_fixture({"CLR": 1})
        self.assertEqual(result["samples"], 6)
        self.assertEqual(server.counts["CLR"], 2)
        self.assertEqual(len(state.attempts()), 7)
        self.assertEqual(result["outcome"], "target_met")
        coverage = {s["form"]["family"]: s["tested_count"] for s in state.samples()}
        self.assertEqual(coverage["ROR"], 1)
        self.assertEqual(coverage["IBR"], 2)

    def test_05_second_failure_is_retained(self):
        result, state, server = self.run_fixture({"CLR": 2})
        self.assertEqual(result["samples"], 5)
        self.assertEqual(server.counts["CLR"], 2)
        self.assertEqual(result["outcome"], "usable_pilot")
        failures = [a for a in state.attempts() if a["status"] == "failed"]
        self.assertEqual(len(failures), 2)
        self.assertTrue(all(a["local_connection_closed"] for a in failures))

    def test_06_transport_plain_and_parse_failure(self):
        server = FixtureServer()
        self.addCleanup(server.close)
        cfg = {**self.config["api"], "base_url": server.url, "stream": False}
        client = Client(cfg, key_loader=lambda _: "fixture")
        body = payload(cfg, "JSON", {"family": "ROR"})
        self.assertEqual(client.complete(body)["response"], PATCHES["ROR"])
        server.malformed = True
        with self.assertRaises(RequestFailure) as cm:
            client.complete(body)
        self.assertEqual(cm.exception.detail["stage"], "parse")
        self.assertEqual(cm.exception.detail["usage"]["total_tokens"], 50)

    def test_07_interrupted_request_recovery(self):
        state = State(self.directory / "recovery")
        self.addCleanup(state.close)
        state.put("job", "j", {"status": "sending", "tries": 1, "request_id": "a"})
        state.attempt("a", "j", "sending", {"started_at": 0})
        state.recover()
        state.recover()
        self.assertEqual(state.get("job", "j")["tries"], 1)
        self.assertEqual(state.get("job", "j")["status"], "failed")
        self.assertEqual(state.attempts()[0]["status"], "unknown")
        self.assertNotIn("local_connection_closed", state.attempts()[0])

    def test_08_early_stop_leaves_pool_unused(self):
        result, state, server = self.run_fixture({}, pool_size=3)
        self.assertEqual(result["samples"], 6)
        self.assertEqual(len(state.attempts()), 6)
        self.assertEqual(len(state.get("pool", "current")), 3)
        self.assertEqual(len({j["task_id"] for _, j in state.items("job")}), 1)

    def test_09_transport_disconnect_retries_once(self):
        result, state, server = self.run_fixture({"CLR": 1}, disconnect=True)
        self.assertEqual(result["samples"], 6)
        failures = [a for a in state.attempts() if a["status"] == "failed"]
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["stage"], "headers")
        self.assertEqual(failures[0]["error"], "RemoteDisconnected")
        self.assertTrue(failures[0]["local_connection_closed"])
        self.assertEqual(server.counts["CLR"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
