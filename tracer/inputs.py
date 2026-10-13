"""Read existing light samples or explicit JSON/JSONL pairs. No target execution."""
import ast
import json
from pathlib import Path
import sqlite3
import tomllib

from adapters.interface import freeze_entry, validate_entry
from .storage import digest

ROOT = Path(__file__).resolve().parents[1]


def configuration(path):
    path = Path(path).resolve()
    cfg = tomllib.loads(path.read_text(encoding="utf-8"))
    light_path = (path.parent / cfg["light_config"]).resolve()
    light = tomllib.loads(light_path.read_text(encoding="utf-8"))
    cfg["output_dir"] = str((path.parent / cfg["output_dir"]).resolve())
    cfg["light_run_dir"] = str((light_path.parent / light["data"]["run_dir"]).resolve())
    cfg["execution"] = light["execution"]
    cfg["api"] = {**light["api"], "model": cfg["analysis"]["model"],
                  "max_tokens": cfg["analysis"]["max_tokens"],
                  "credential_file": str(ROOT / ".env")}
    if cfg["api"]["model"] != "deepseek-v4-pro":
        raise ValueError("This diagnostic configuration requires deepseek-v4-pro")
    if cfg["api"].get("extra"):
        raise ValueError("api.extra is not supported in diagnostic mode; keep model/token binding explicit")
    for section in ("pipeline", "trace", "analysis"):
        for name, value in cfg[section].items():
            if name == "model":
                continue
            if type(value) is not int or value < 1:
                raise ValueError(section + "." + name + " must be a positive integer")
    p = cfg["pipeline"]
    if p["max_unremoved_containers"] < p["collect_workers"]:
        raise ValueError("max_unremoved_containers must cover collect_workers")
    if p["max_inflight"] < p["collect_workers"]:
        raise ValueError("max_inflight must cover collect_workers")
    if cfg["trace"]["max_event_bytes"] < cfg["trace"]["max_snapshot_bytes"] + 8192:
        raise ValueError("max_event_bytes too small for snapshot envelope")
    return cfg


def read_pairs(samples=None, database=None):
    if samples:
        path = Path(samples).resolve()
        if path.suffix == ".jsonl":
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        else:
            rows = json.loads(path.read_text(encoding="utf-8"))
            rows = rows if isinstance(rows, list) else [rows]
    else:
        path = Path(database).resolve()
        # mode=ro preserves existing live pipeline state; never recover its jobs.
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            rows = [json.loads(row[0]) for row in db.execute("SELECT body FROM samples ORDER BY rowid")]
        finally:
            db.close()
    pairs = []
    seen = set()
    for row in rows:
        for name in ("spec", "original_code", "mutant_code"):
            if not isinstance(row.get(name), str) or not row[name].strip():
                raise ValueError("Missing " + name)
            if len(row[name].encode("utf-8")) > 2 * 1024 * 1024:
                raise ValueError("Source/SPEC size exceeds the diagnostic import limit")
        for name in ("original_code", "mutant_code"):
            ast.parse(row[name], feature_version=(3, 11))
        witness = row.get("witness", {})
        if "input" not in row and "input" not in witness:
            raise ValueError("Explicit fixed input required")
        contract = row["execution_contract"]
        entry = freeze_entry(contract["entry"], row["original_code"])
        if entry["mode"] == "function":
            validate_entry(entry)
        if freeze_entry(contract["entry"], row["mutant_code"]) != entry:
            raise ValueError("Original/mutant entry mismatch")
        value = row["input"] if "input" in row else witness["input"]
        if entry["mode"] == "stdin_stdout" and type(value) is not str:
            raise ValueError("stdin input must be a complete string")
        if entry["mode"] == "function" and (type(value) is not list or len(value) != len(entry["parameter_order"])):
            raise ValueError("Function input must be the complete positional argument list")
        pair = {"schema": "trace_pair_v1", "spec": row["spec"],
                "original_code": row["original_code"], "mutant_code": row["mutant_code"],
                "execution_contract": {**contract, "entry": entry}, "input": value,
                "expected_available": "expected" in row or "expected" in witness,
                "expected": row.get("expected", witness.get("expected")),
                "input_source": row.get("input_source", witness.get("answer_source", "provided_unknown"))}
        key = digest(pair)
        if key not in seen:
            pairs.append({**pair, "pair_id": key, "source_sample_id": row.get("sample_id")})
            seen.add(key)
    return pairs
