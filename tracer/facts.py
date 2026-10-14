"""Mechanical observations and existing oracle configuration; never model verdicts."""
from .storage import canonical


def oracle_configuration(pair):
    contract = pair.get("execution_contract", {})
    entry = contract.get("entry", {})
    target = "stdout" if entry.get("mode") == "stdin_stdout" else entry.get("output_channel", "return")
    comparison = contract.get("comparison", {})
    method = comparison.get("kind")
    kind = ("approximate_value" if method == "numeric_tolerance" else
            "exact_value" if method in ("json_exact", "text_exact", "text_lines", "text_tokens") else "unknown")
    return {"target": target, "kind": kind, "comparison": comparison,
            "requirement": None, "spec_basis": [], "requirement_status": "not_analyzed",
            "source": "existing_execution_contract"}


def observable_consequences(pair, traces):
    target = oracle_configuration(pair)["target"]
    obs = {s: {k: v for k, v in traces.get(s, {}).get("traced", {}).get("observation", {}).items()
               if k in ("status", "output", "exception_class", "program_stdout")}
           for s in ("original", "mutant")}
    valid = all(o.get("status") in ("ok", "program_exception") for o in obs.values())
    result = {"status": "observed" if valid else "incomplete", "observations": obs,
              "basis": "recorded_results_not_a_correctness_verdict", "changes": [],
              "scope": [target, "exception", "captured_stdout"], "arbitrary_interactions": "not_collected"}
    if not valid:
        return result
    for field, channel in (("output", target), ("exception_class", "exception"),
                           ("status", "completion"), ("program_stdout", "stdout")):
        if field == "program_stdout" and target == "stdout":
            continue
        if canonical(obs["original"].get(field)) != canonical(obs["mutant"].get(field)):
            result["changes"].append({"channel": channel, "original": obs["original"].get(field),
                                      "mutant": obs["mutant"].get(field)})
    return result
