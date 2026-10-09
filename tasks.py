"""Read local TACO JSON/JSONL/Parquet; use official fields only, never scrape SPEC."""

import ast
import json
import random
from pathlib import Path
from adapters.interface import freeze_entry, input_structure
from adapters.representation import transform
from state import digest


def decoded(value):
    return json.loads(value) if isinstance(value, str) else value


def rows(path):
    path = Path(path)
    paths = sorted(path.glob("*.parquet")) if path.is_dir() else [path]
    for file in paths:
        if file.suffix == ".parquet":
            try:
                import pyarrow.parquet as pq
            except ImportError:
                raise RuntimeError(
                    "Parquet requires pyarrow; install it or use JSON/JSONL"
                ) from None
            for batch in pq.ParquetFile(file).iter_batches(batch_size=128):
                for row in batch.to_pylist():
                    yield row
        elif file.suffix == ".jsonl":
            with file.open() as f:
                for line in f:
                    if line.strip():
                        yield json.loads(line)
        else:
            data = json.loads(file.read_text())
            if isinstance(data, dict):
                data = data.get("tasks", [data])
            yield from data


def single_solution_entry(sources):
    for source in sources[:3]:
        if not isinstance(source, str):
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        # A supplied executable driver means stdin remains the appropriate interface.
        if any(
            not isinstance(
                n, (ast.Import, ast.ImportFrom, ast.ClassDef, ast.FunctionDef)
            )
            and not (
                isinstance(n, ast.Expr)
                and isinstance(n.value, ast.Constant)
                and isinstance(n.value.value, str)
            )
            for n in tree.body
        ):
            continue
        methods = [
            m
            for c in tree.body
            if isinstance(c, ast.ClassDef) and c.name == "Solution"
            for m in c.body
            if isinstance(m, ast.FunctionDef) and not m.name.startswith("_")
        ]
        if len(methods) == 1:
            try:
                return freeze_entry(
                    {
                        "mode": "function",
                        "fn_name": methods[0].name,
                        "target": "solution_method",
                    },
                    source,
                )
            except ValueError:
                continue
    return None


def normalize(row, index, overrides, profiles=None):
    d = row.get("definition", row)
    spec = d.get("spec", d.get("question", ""))
    ident = row.get("trusted_identity") or {}
    task_id = str(
        d.get("task_id")
        or d.get("task_key")
        or ident.get("value")
        or row.get("url")
        or "taco-" + digest(spec)[:16]
    )
    base = {
        "task_id": task_id,
        "spec": spec,
        "source_index": index,
        "provenance": row.get("provenance", {}),
    }
    try:
        if not isinstance(spec, str) or not spec.strip():
            raise ValueError("missing_spec")
        io = decoded(d.get("input_output") or {})
        inputs = d.get("official_inputs", io.get("inputs", []))
        outputs = d.get("official_outputs", io.get("outputs", []))
        if not isinstance(inputs, list) or not inputs:
            raise ValueError("no_official_input")
        if not isinstance(outputs, list):
            raise ValueError("no_official_answers")
        override = overrides.get(task_id, {})
        contract = override.get(
            "execution_contract", d.get("contract", d.get("execution_contract", {}))
        )
        contract = dict(contract)
        sources = decoded(d.get("solutions", []))
        if not sources and d.get("original_code"):
            sources = [d["original_code"]]
        if not isinstance(sources, list):
            raise ValueError("solutions_not_list")
        profile = (
            dict((profiles or {}).get(row.get("source"), {}))
            if "definition" not in row and not contract
            else {}
        )
        inferred = (
            single_solution_entry(sources)
            if profile.get("entry") == "single_solution_method"
            else None
        )
        if profile.get("entry") and inferred is None:
            profile = {}
        entry = (
            contract.get("entry")
            or inferred
            or (
                {"mode": "function", "fn_name": io["fn_name"]}
                if io.get("fn_name")
                else {"mode": "stdin_stdout"}
            )
        )
        contract["entry"] = entry
        contract.setdefault(
            "comparison",
            {
                "kind": "json_exact"
                if entry["mode"] == "function"
                and entry.get("output_channel", "return") != "stdout"
                else "text_lines"
            },
        )
        contract.setdefault("exception_policy", "forbidden")
        conversion = {k: v for k, v in profile.items() if k != "entry"}
        conversion.update(
            override.get("representation", d.get("representation_conversion", {}))
        )
        material = d.get("official_material", [])
        cases = []
        seen = {}
        skips = []
        for i, value in enumerate(inputs):
            info = material[i] if i < len(material) else {}
            src = info.get("source", "official")
            if (
                isinstance(src, str)
                and src.startswith("question:")
                or info.get("answer_source") == "spec_explicit"
            ):
                skips.append({"index": i, "reason": "not_official_input"})
                continue
            if (
                i >= len(outputs)
                or info.get("answer_status", "known") != "known"
                or info.get("original_output_present", True) is False
            ):
                skips.append({"index": i, "reason": "missing_or_uncertain_answer"})
                continue
            if info.get("judgment_kind", "expected") != "expected":
                skips.append({"index": i, "reason": "checker_not_supported"})
                continue
            value = transform(
                value,
                conversion.get("input", "as_stored"),
                terminal=conversion.get("input_terminal_newline"),
            )
            expected = transform(
                outputs[i],
                conversion.get("output", "as_stored"),
                terminal=conversion.get("output_terminal_newline"),
            )
            if entry["mode"] == "stdin_stdout" and not isinstance(value, str):
                raise ValueError("stdin_requires_text_or_explicit_conversion")
            if entry["mode"] == "function" and not isinstance(value, list):
                raise ValueError("function_requires_argument_list")
            key = digest(value)
            if key in seen:
                if digest(cases[seen[key]]["expected"]) != digest(expected):
                    raise ValueError("conflicting_official_answers")
                cases[seen[key]]["source_indices"].append(i)
                continue
            seen[key] = len(cases)
            cases.append(
                {
                    "case_id": key[:16],
                    "source": "official",
                    "source_indices": [i],
                    "input": value,
                    "expected": expected,
                    "answer_source": info.get("answer_source", "original_official"),
                    "raw_input": inputs[i],
                    "raw_expected": outputs[i],
                    "conversion": conversion,
                }
            )
        if not cases:
            raise ValueError("no_usable_official_cases")
        return {
            **base,
            "status": "pending",
            "solutions": sources,
            "contract": contract,
            "format_profile": profile,
            "cases": cases,
            "case_skips": skips,
            "raw_official_count": len(inputs),
        }
    except (ValueError, TypeError, KeyError) as exc:
        return {**base, "status": "skipped", "reason": str(exc)[:150]}


def freeze_pool(config, state):
    cached = state.get("pool", "current")
    if cached is not None:
        return [state.get("task", tid) for tid in cached]
    path = config["data"]["source"]
    overrides = (
        json.loads(Path(config["data"]["overrides"]).read_text())
        if config["data"].get("overrides")
        else {}
    )
    chosen = []
    seen = set()
    ids = set()
    for index, row in enumerate(rows(path)):
        d = row.get("definition", row)
        spec = d.get("spec", d.get("question", ""))
        key = " ".join(spec.split()) if isinstance(spec, str) else str(index)
        if key in seen:
            continue
        seen.add(key)
        task = normalize(
            row, index, overrides, config["data"].get("source_profiles", {})
        )
        if task["task_id"] in ids:
            continue
        ids.add(task["task_id"])
        chosen.append(task)
        if len(chosen) >= config["data"]["pool_size"]:
            break
    random.Random(config["data"]["seed"]).shuffle(chosen)
    for task in chosen:
        state.put("task", task["task_id"], task)
    state.put("pool", "current", [t["task_id"] for t in chosen])
    state.put(
        "pool",
        "source",
        {"path": path, "count": len(chosen), "requested": config["data"]["pool_size"]},
    )
    return chosen


def source_choices(task, maximum):
    for index, source in enumerate(task["solutions"][:maximum]):
        if not isinstance(source, str):
            continue
        try:
            tree = ast.parse(source)
            compile(tree, "candidate", "exec")
            entry = freeze_entry(task["contract"]["entry"], source)
            for case in task["cases"]:
                if entry["mode"] == "function":
                    if len(case["input"]) != len(entry["parameter_order"]):
                        raise ValueError("argument_arity")
                    for i, s in entry.get("structures", {}).items():
                        input_structure(case["input"][int(i)], s)
            yield index, source, {**task["contract"], "entry": entry}
        except (ValueError, SyntaxError, TypeError, KeyError):
            continue
