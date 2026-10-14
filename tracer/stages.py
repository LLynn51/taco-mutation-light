"""Import frozen diagnostic artifacts for offline replay. No Docker or credentials."""
import json
from pathlib import Path
import sqlite3

from .storage import file_digest, write_json


def read_store(directory):
    path = Path(directory) / "state.sqlite"
    if not path.is_file():
        raise ValueError("Source diagnostic state is missing")
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        return [(k, i, json.loads(b)) for k, i, b in db.execute("SELECT kind,id,body FROM records ORDER BY rowid")]
    finally:
        db.close()


def import_existing(source, store, limit=None):
    source = Path(source).resolve()
    records = read_store(source)
    # Carry forward ALL historical request slots, including unknowns and pairs not
    # selected by --limit. Changing output directories must not silently resend.
    if source != store.directory:
        for kind, key, row in records:
            if kind == "request" and store.get(kind, key) is None:
                old_directory = Path(row.get("analysis_directory", source / "analysis" / key))
                new_directory = store.directory / "analysis" / key / row.get("analysis_version", "imported_legacy")
                for filename in ("request.json", "response.json", "analysis.json", "failure.json"):
                    original = old_directory / filename
                    if original.is_file():
                        destination = new_directory / filename
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        if destination.exists():
                            if file_digest(original) != file_digest(destination):
                                raise ValueError("Imported request artifact conflict")
                        else:
                            with destination.open("xb") as handle:
                                handle.write(original.read_bytes())
                copied = {**row, "imported_from": str(source),
                          "analysis_directory": str(new_directory)}
                if copied["status"] == "sending":
                    copied.update(status="unknown", retry_allowed=False)
                store.put(kind, key, copied)
    pairs = []
    for kind, key, row in records:
        if kind != "job" or not row.get("traces"):
            continue
        path = source / "pairs" / key / "input.json"
        pair = json.loads(path.read_text())
        if pair["pair_id"] != key:
            raise ValueError("Pair identity mismatch")
        for trace in row["traces"].values():
            if file_digest(trace["trace_file"]) != trace["trace_sha256"]:
                raise ValueError("Source trace changed")
            for checkpoint in trace.get("traced", {}).get("checkpoints", {}).values():
                if file_digest(checkpoint["file"]) != checkpoint["sha256"]:
                    raise ValueError("Source checkpoint changed")
        if row.get("comparison") and file_digest(row["comparison"]) != row.get("comparison_sha256"):
            raise ValueError("Source comparison changed")
        if source != store.directory:
            existing = store.get("job", key)
            if existing and existing.get("traces") != row["traces"]:
                raise ValueError("Output contains another collection for this pair")
            if existing is None:
                store.put("job", key, {**row, "imported_from": str(source)})
                write_json(store.directory / "pairs" / key / "input.json", pair)
        pairs.append(pair)
        if limit is not None and len(pairs) >= limit:
            break
    if not pairs:
        raise ValueError("No completed collection artifacts found")
    return pairs
