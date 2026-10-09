"""Six independent family workers, shared baselines, one retry pass and resumable jobs."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed, CancelledError
import json
from pathlib import Path
import threading
import time
import tomllib
import uuid

from state import State, digest
from tasks import freeze_pool, source_choices
from mutation import allocated, rebuild
from adapters.form import FAMILIES
from runner import DockerRunner, compare
from llm import Client, RequestFailure, payload

UNSUPPORTED = "taco-mutation-light pipeline不支持"
ROOT = Path(__file__).resolve().parent


def load_config(path):
    path = Path(path).resolve()
    config = tomllib.loads(path.read_text())
    for section, key in [
        ("data", "source"),
        ("data", "run_dir"),
        ("data", "overrides"),
        ("api", "credential_file"),
        ("generation", "prompt"),
    ]:
        value = config[section].get(key)
        if value and not Path(value).is_absolute():
            config[section][key] = str(path.parent / value)
    families = config["generation"]["families"]
    if len(set(families)) != len(families) or set(families) - set(FAMILIES.values()):
        raise ValueError("Invalid families")
    if config["generation"]["strategy"] not in ("per_family", "per_task_random"):
        raise ValueError("Invalid generation strategy")
    for section, key in [
        ("data", "pool_size"),
        ("data", "max_solutions"),
        ("execution", "workers"),
        ("generation", "opportunities"),
        ("sampling", "target"),
        ("sampling", "per_family"),
    ]:
        if type(config[section][key]) is not int or config[section][key] < 1:
            raise ValueError(f"{section}.{key} must be positive")
    if config["generation"]["failure_retries"] not in (0, 1):
        raise ValueError("failure_retries must be 0 or 1")
    if config["sampling"]["target"] > len(families) * config["sampling"]["per_family"]:
        raise ValueError("Target exceeds family quotas")
    return config


class Pipeline:
    def __init__(self, config, state, runner, client):
        self.config = config
        self.state = state
        self.runner = runner
        self.client = client
        self.stop = threading.Event()
        self.task_locks = {}
        self.prompt = Path(config["generation"]["prompt"]).read_text()

    def full(self, family):
        return (
            self.stop.is_set()
            or self.state.count() >= self.config["sampling"]["target"]
            or self.state.count(family) >= self.config["sampling"]["per_family"]
        )

    def execute(self, source, contract, case, repeat=False):
        key = digest([source, contract, case["input"], self.runner.cache_key, repeat])
        saved = self.state.get("execution", key)
        if saved is None:
            saved = self.runner.execute(source, contract["entry"], case["input"])
            self.state.put("execution", key, saved)
        return {**saved, "verdict": compare(saved, case["expected"], contract)}

    def prepare(self, task):
        if task["status"] == "skipped":
            return task
        with self.task_locks[task["task_id"]]:
            key = digest(
                [task, self.runner.cache_key, self.config["data"]["max_solutions"]]
            )
            cached = self.state.get("baseline", key)
            if cached is not None:
                return cached
            failures = []
            for index, source, contract in source_choices(
                task, self.config["data"]["max_solutions"]
            ):
                observations = []
                for case in task["cases"]:
                    if self.stop.is_set():
                        raise CancelledError()
                    obs = self.execute(source, contract, case)
                    observations.append(obs)
                    if obs["verdict"] != "pass":
                        break
                if len(observations) == len(task["cases"]) and all(
                    o["verdict"] == "pass" for o in observations
                ):
                    result = {
                        **task,
                        "status": "ready",
                        "original_code": source,
                        "solution_index": index,
                        "contract": contract,
                        "baseline": observations,
                    }
                    self.state.put("baseline", key, result)
                    return result
                failures.append(
                    {
                        "solution_index": index,
                        "tested": len(observations),
                        "last_status": observations[-1]["status"],
                        "last_verdict": observations[-1]["verdict"],
                    }
                )
            result = {
                **task,
                "status": "skipped",
                "reason": "no_passing_supported_solution",
                "baseline_failures": failures,
            }
            self.state.put("baseline", key, result)
            return result

    def generate(self, job_id, job, task, family):
        previous = [
            {
                "old_fragment": j["candidate"]["form"]["old_fragment"],
                "new_fragment": j["candidate"]["form"]["new_fragment"],
            }
            for _, j in self.state.items("job")
            if j.get("task_id") == task["task_id"]
            and j.get("family") == family
            and j.get("candidate")
        ]
        material = {
            "spec": task["spec"],
            "original_source": task["original_code"],
            "entry": task["contract"]["entry"],
            "family": family,
            "allowed_rules": [r for r, f in FAMILIES.items() if f == family],
            "previous_patches": previous,
            "retry_error": job.get("error"),
        }
        body = payload(self.config["api"], self.prompt, material)
        request_id = str(uuid.uuid4())
        prior = job.get("request_id")
        job.update(
            status="sending", tries=job.get("tries", 0) + 1, request_id=request_id
        )
        job.pop("response", None)
        self.state.put("job", job_id, job)
        request = {
            "started_at": time.time(),
            "retry_of": prior,
            "model": self.config["api"]["model"],
            "base_url": self.config["api"]["base_url"],
            "payload": body,
        }
        self.state.attempt(request_id, job_id, "sending", request)
        try:
            result = self.client.complete(body)
            job.update(status="generated", response=result["response"])
            self.state.put("job", job_id, job)
            self.state.attempt(request_id, job_id, "complete", {**request, **result})
        except RequestFailure as exc:
            job.update(status="failed", error=exc.detail["error"])
            self.state.put("job", job_id, job)
            self.state.attempt(request_id, job_id, "failed", {**request, **exc.detail})
            return False
        return True

    def evaluate(self, task, candidate):
        contract = task["contract"]
        results = []
        witness = None
        for i, case in enumerate(task["cases"]):
            if self.stop.is_set():
                raise CancelledError()
            obs = self.execute(candidate["code"], contract, case)
            results.append({"case_id": case["case_id"], "observation": obs})
            if obs["verdict"] == "fail" and witness is None:
                stable = True
                if self.config["execution"]["repeat_witness"]:
                    again = self.execute(candidate["code"], contract, case, True)
                    original_again = self.execute(
                        task["original_code"], contract, case, True
                    )
                    projection = lambda x: {
                        k: x.get(k) for k in ("status", "output", "exception_class")
                    }
                    stable = (
                        again["verdict"] == "fail"
                        and original_again["verdict"] == "pass"
                        and projection(again) == projection(obs)
                        and projection(original_again)
                        == projection(task["baseline"][i])
                    )
                    results[-1]["repeat"] = {
                        "original": original_again,
                        "mutant": again,
                        "stable": stable,
                    }
                if stable:
                    witness = {**case, "original": task["baseline"][i], "mutant": obs}
                    if not self.config["execution"]["all_official_tests"]:
                        break
        return witness, results

    def sample(self, task, candidate, witness, results):
        channel = (
            task["contract"]["entry"].get("output_channel", "return")
            if task["contract"]["entry"]["mode"] == "function"
            else "stdout"
        )
        original, mutant = witness["original"], witness["mutant"]
        oracle_kind = (
            "approximate_value"
            if task["contract"]["comparison"]["kind"] == "numeric_tolerance"
            else "exact_value"
        )
        return {
            "schema_version": "light-0.1",
            "sample_id": str(uuid.uuid4()),
            "task_id": task["task_id"],
            "mutant_id": digest([task["task_id"], candidate["code_key"]]),
            "spec": task["spec"],
            "original_code": task["original_code"],
            "mutant_code": candidate["code"],
            "task_provenance": task.get("provenance", {}),
            "execution_contract": task["contract"],
            "witness": witness,
            "tested_count": len(results),
            "official_count": len(task["cases"]),
            "raw_official_count": task["raw_official_count"],
            "skipped_cases": task.get("case_skips", []),
            "witness_status": "difference_observed",
            "form": candidate["form"],
            "trigger_condition": UNSUPPORTED,
            "first_divergence": UNSUPPORTED,
            "propagation": UNSUPPORTED,
            "observable_consequence": [
                {
                    "channel": "exception"
                    if mutant["status"] == "program_exception"
                    else channel,
                    "original_status": original["status"],
                    "mutant_status": mutant["status"],
                    "original": original.get("output"),
                    "mutant": mutant.get("output"),
                    "exception_class": mutant.get("exception_class"),
                }
            ],
            "oracle_type": {
                "target": channel,
                "kind": oracle_kind,
                "comparison": task["contract"]["comparison"],
                "requirement": UNSUPPORTED,
            },
            "behavior_abstraction": UNSUPPORTED,
            "label_status": "unknown",
            "trace_status": "not_collected",
        }

    def process(self, task, family, opportunity, retry=False):
        job_id = digest([task["task_id"], family, opportunity])
        job = self.state.get(
            "job",
            job_id,
            {
                "task_id": task["task_id"],
                "family": family,
                "opportunity": opportunity,
                "status": "pending",
                "tries": 0,
            },
        )
        if job["status"] in ("done", "skip"):
            return
        if job["status"] == "failed" and (
            not retry
            or job["tries"] >= 1 + self.config["generation"]["failure_retries"]
        ):
            return
        if job["status"] in ("pending", "failed"):
            if self.full(family) or not self.generate(job_id, job, task, family):
                return
        if not job.get("candidate"):
            try:
                candidate = rebuild(task["original_code"], family, job["response"])
            except (ValueError, SyntaxError, TypeError, KeyError):
                job.update(status="failed", error="invalid_mutation_output")
                self.state.put("job", job_id, job)
                return
            if candidate is None:
                job.update(status="skip", reason="model_no_applicable_mutation")
                self.state.put("job", job_id, job)
                return
            job.update(candidate=candidate, status="generated")
            self.state.put("job", job_id, job)
        candidate = job["candidate"]
        # A prior accepted identical mutant is reused without spending execution time.
        mutant_id = digest([task["task_id"], candidate["code_key"]])
        if any(s["mutant_id"] == mutant_id for s in self.state.samples()):
            job.update(status="done", result="duplicate")
            self.state.put("job", job_id, job)
            return
        witness, results = self.evaluate(task, candidate)
        job.update(status="done", results=results)
        if witness:
            sample = self.sample(task, candidate, witness, results)
            sample["generation"] = {
                "request_id": job["request_id"],
                "model": self.config["api"]["model"],
                "base_url": self.config["api"]["base_url"],
            }
            job["result"] = self.state.add_sample(
                sample,
                self.config["sampling"]["per_family"],
                self.config["sampling"]["target"],
            )
        else:
            job["result"] = (
                "unstable_observation"
                if any(r.get("repeat", {}).get("stable") is False for r in results)
                else "execution_unknown"
                if any(r["observation"]["verdict"] == "unknown" for r in results)
                else "no_difference_observed"
            )
        self.state.put("job", job_id, job)

    def worker(self, family, tasks):
        try:
            prepared = []
            for raw in tasks:
                if self.full(family):
                    return
                task = self.prepare(raw)
                if task["status"] != "ready" or family not in allocated(
                    task, self.config
                ):
                    continue
                prepared.append(task)
                for opportunity in range(self.config["generation"]["opportunities"]):
                    if self.full(family):
                        return
                    if any(
                        s["task_id"] == task["task_id"]
                        and s["form"]["family"] == family
                        for s in self.state.samples()
                    ):
                        break
                    self.process(task, family, opportunity)
            # First pass keeps moving; technical/output failures get one explicit extra attempt.
            for task in prepared:
                for opportunity in range(self.config["generation"]["opportunities"]):
                    if self.full(family):
                        return
                    if any(
                        s["task_id"] == task["task_id"]
                        and s["form"]["family"] == family
                        for s in self.state.samples()
                    ):
                        break
                    self.process(task, family, opportunity, retry=True)
        except BaseException:
            self.stop.set()
            raise

    def run(self):
        with self.state.owner():
            self.state.recover()
            tasks = freeze_pool(self.config, self.state)
            self.task_locks = {t["task_id"]: threading.Lock() for t in tasks}
            self.state.put("run", "last_config", self.config)
            self.state.put(
                "run", "status", {"state": "running", "started_at": time.time()}
            )
            try:
                with ThreadPoolExecutor(
                    max_workers=self.config["execution"]["workers"]
                ) as pool:
                    futures = [
                        pool.submit(self.worker, f, tasks)
                        for f in self.config["generation"]["families"]
                    ]
                    try:
                        for future in as_completed(futures):
                            future.result()
                    except BaseException:
                        self.stop.set()
                        for future in futures:
                            future.cancel()
                        raise
            except BaseException as exc:
                self.state.put(
                    "run",
                    "status",
                    {"state": "interrupted", "error": type(exc).__name__},
                )
                raise
            result = report(self.config, self.state, final=True)
            self.state.put("run", "status", {"state": "finished", "result": result})
            return result


def report(config, state, final=False):
    n = state.count()
    threshold = config["sampling"]
    outcome = (
        "target_met"
        if n >= threshold["target"]
        else "usable_pilot"
        if n >= threshold["acceptable"]
        else "review_shortfall"
        if n >= threshold["minimum"]
        else "revise_pipeline"
    )
    run_status = state.get("run", "status", {}).get("state", "not_started")
    if not final and run_status != "finished":
        outcome = run_status
    attempts = state.attempts()
    usage = {
        k: sum(
            a.get("usage", {}).get(k, 0) or 0
            for a in attempts
            if isinstance(a.get("usage"), dict)
        )
        for k in ("prompt_tokens", "completion_tokens", "total_tokens")
    }
    return {
        "samples": n,
        "families": {f: state.count(f) for f in config["generation"]["families"]},
        "independent_tasks": len({s["task_id"] for s in state.samples()}),
        "outcome": outcome,
        "pool": state.get("pool", "source"),
        "jobs": dict(
            Counter(j.get("result", j["status"]) for _, j in state.items("job"))
        ),
        "requests": len(attempts),
        "request_states": dict(Counter(a["status"] for a in attempts)),
        "known_usage": usage,
        "usage_unknown_requests": sum(a.get("usage") is None for a in attempts),
        "cost": "not_calculated",
        "task_skips": dict(
            Counter(
                t.get("reason")
                for _, t in state.items("task")
                if t["status"] == "skipped"
            )
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=["prepare", "check", "run", "status", "export"]
    )
    parser.add_argument("--config", default=str(ROOT / "config.toml"))
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-prepare before any generation jobs exist",
    )
    parser.add_argument(
        "--paid", action="store_true", help="Explicitly enable API requests for run"
    )
    args = parser.parse_args()
    config = load_config(args.config)
    state = State(config["data"]["run_dir"])
    if args.command == "prepare":
        with state.owner():
            if args.refresh:
                if state.attempts() or state.items("job") or state.count():
                    parser.error(
                        "Existing generation history: use a new run_dir instead of --refresh"
                    )
                with state.lock, state.db:
                    state.db.execute(
                        "DELETE FROM kv WHERE kind IN ('pool','task','baseline')"
                    )
            tasks = freeze_pool(config, state)
        result = {
            "pool_size": len(tasks),
            "states": dict(Counter(t["status"] for t in tasks)),
        }
    elif args.command == "status":
        result = report(config, state)
    elif args.command == "export":
        from export import export

        result = export(state)
    else:
        if args.command == "run" and not args.paid:
            parser.error(
                "run requires --paid; prepare/check/status/export never send requests"
            )
        if args.command == "run" and not config["api"].get("credential_file"):
            parser.error(
                "Set api.credential_file before running; no credentials were read"
            )
        runner = DockerRunner(
            config["execution"], Path(config["data"]["run_dir"]) / "sandbox"
        )
        facts = runner.check()
        state.put("run", "isolation", facts)
        if args.command == "check":
            result = facts
        else:
            result = Pipeline(config, state, runner, Client(config["api"])).run()
            from export import export

            result["export"] = export(state)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    state.close()


if __name__ == "__main__":
    main()
