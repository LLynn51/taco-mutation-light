"""Bounded-memory line alignment. No target imports or execution on the host."""
from collections import Counter, defaultdict
import difflib
import gzip
import json
import ast
from pathlib import Path

from .storage import canonical, digest, file_digest, write_json
from .adapters.policy import runtime_omissions
from .facts import observable_consequences, oracle_configuration
from .checkpoint_io import checkpoint_items


def events(path, limit):
    total = 0
    with gzip.open(path, "rb") as handle:
        while True:
            line = handle.readline(limit["max_event_bytes"] + 1)
            if not line:
                return
            total += len(line)
            if len(line) > limit["max_event_bytes"] or total > limit["max_trace_bytes"]:
                raise ValueError("Stored trace exceeds configured limit")
            row = json.loads(line)
            if row.get("kind") != "event":
                raise ValueError("Invalid stored trace event")
            yield row


def semantic_values(event):
    """Compare supported contents, not snapshot-local object numbers.

Aliases, cycles, unordered containers and missing values are intentionally not
promoted into reliable scalar/content differences.
"""
    objects = {}

    def register(value):
        if type(value) is dict:
            if "object_id" in value:
                objects[value["object_id"]] = value
            for item in value.values():
                register(item)
        elif type(value) is list:
            for item in value:
                register(item)

    register(event.get("locals", {}))
    register(event.get("detail"))

    remaining = [0]
    total_remaining = [16384]

    def normalize(value, seen=None):
        remaining[0] -= 1
        total_remaining[0] -= 1
        if remaining[0] < 0 or total_remaining[0] < 0:
            raise ValueError("Expanded value comparison limit")
        seen = set() if seen is None else seen
        if type(value) is list:
            return [normalize(item, seen) for item in value]
        if type(value) is not dict:
            return value
        if value.get("status") == "not_collected" or value.get("coverage") == "partial" or value.get("type") in ("set", "frozenset", "symbol"):
            raise ValueError("Unavailable or unordered value")
        if "ref" in value:
            value = objects[value["ref"]]
        oid = value.get("object_id")
        if oid is not None and oid in seen:
            raise ValueError("Cyclic content comparison unsupported")
        next_seen = seen | {oid} if oid else seen
        normalized = {key: normalize(item, next_seen) for key, item in value.items()
                      if key not in ("object_id", "object_id_scope")}
        return normalized

    values = {}
    for name, value in event.get("locals", {}).items():
        try:
            remaining[0] = 4096
            values[name] = digest(normalize(value))
        except (ValueError, KeyError, RecursionError):
            values[name] = None
    if "detail" in event:
        try:
            remaining[0] = 4096
            values["<return>"] = digest(normalize(event["detail"]))
        except (ValueError, KeyError, RecursionError):
            values["<return>"] = None
    if "exception_class" in event:
        values["<exception_class>"] = digest(event["exception_class"])
    return values


def leaves(event):
    """Comparable *observed* paths; missing portions never imply equality."""
    objects, result = {}, {}
    def register(v):
        if type(v) is dict:
            if "object_id" in v:
                objects[v["object_id"]] = v
            for x in v.values():
                register(x)
        elif type(v) is list:
            for x in v:
                register(x)
    register(event.get("locals", {}))
    register(event.get("detail"))
    budget = [16384]
    def visit(v, path, seen):
        budget[0] -= 1
        if budget[0] < 0 or type(v) is not dict or v.get("status") == "not_collected":
            return
        if "ref" in v:
            v = objects.get(v["ref"], {})
        oid = v.get("object_id")
        if oid and oid in seen:
            return
        seen = seen | {oid} if oid else seen
        kind = v.get("type")
        if kind == "symbol":
            return
        if "value" in v:
            result[path] = digest([kind, v["value"]])
        elif kind in ("list", "tuple", "dict", "set", "frozenset", "instance"):
            result[path + ".length"] = digest([kind, v["length"]])
            if kind in ("list", "tuple"):
                for i, x in enumerate(v.get("items", [])):
                    visit(x, path + "[" + str(i) + "]", seen)
                for x in v.get("sampled_items", []):
                    visit(x["value"], path + "[" + str(x["index"]) + "]", seen)
            elif kind in ("dict", "instance"):
                for k, x in v.get("items", []):
                    if "value" in k:
                        visit(x, path + "[" + canonical([k["type"], k["value"]]).decode() + "]", seen)
    for name, v in event.get("locals", {}).items():
        visit(v, name, set())
    if "detail" in event:
        visit(event["detail"], "<return>", set())
    if "exception_class" in event:
        result["<exception_class>"] = digest(event["exception_class"])
    return result


def indexed(path, mapping, limit, omitted):
    records = []
    for row in events(path, limit):
        row = {**row, "locals": {k: v for k, v in row.get("locals", {}).items() if k not in omitted}}
        line = mapping.get(row["line"])
        # Function entry/exit alignment does not depend on the spelling of a return line.
        boundary = "exit" if row["event"] in ("return", "unwind", "return_or_unwind") else row["event"]
        if boundary in ("call", "exit"):
            token = (boundary, row["function"], row["call_id"], "boundary")
        else:
            token = (boundary, row["function"], row["call_id"], line) if line is not None else None
        records.append({"id": row["event_id"], "token": token, "values": semantic_values(row),
                        "leaves": leaves(row), "partial_locals": "remaining_locals" in row,
                        "event": row["event"], "line": row["line"], "call_id": row["call_id"]})
        if len(records) > limit["max_events"]:
            raise ValueError("Stored event count exceeds limit")
    return records


def comparison_version(pair, traces, cfg):
    here = Path(__file__).parent
    return digest({"schema": "trace_comparison_v2", "pair": pair["pair_id"],
                   "traces": traces, "settings": cfg["analysis"],
                   "code": {f: file_digest(here / f) for f in
                            ("compare.py", "facts.py", "checkpoint_io.py", "storage.py", "adapters/policy.py")}})


def line_mapping(a, b):
    mapping, changed = {}, set()
    def kinds(lines):
        out = defaultdict(set)
        for node in ast.walk(ast.parse("\n".join(lines))):
            if isinstance(node, ast.stmt):
                out[node.lineno].add(type(node).__name__)
        return out
    ak, bk = kinds(a), kinds(b)
    for tag, i, j, x, y in difflib.SequenceMatcher(None, a, b, autojunk=True).get_opcodes():
        if tag == "equal":
            for d in range(j - i):
                mapping[x + d + 1] = i + d + 1
        elif tag == "replace" and j - i == y - x:
            for d in range(j - i):
                al, bl = i + d + 1, x + d + 1
                if ak.get(al) and ak.get(al) == bk.get(bl):
                    mapping[bl] = al
                    changed.add(bl)
    return mapping, changed


def align(left, right, window):
    """Bounded greedy resynchronization; never force unmatched events into pairs."""
    i = j = 0
    matches, skipped = [], {"original": [], "mutant": []}
    while i < len(left) and j < len(right):
        a, b = left[i]["token"], right[j]["token"]
        if a is not None and a == b:
            matches.append((left[i], right[j]))
            i += 1
            j += 1
            continue
        next_j = next((k for k in range(j + 1, min(len(right), j + window + 1))
                       if a is not None and right[k]["token"] == a), None)
        next_i = next((k for k in range(i + 1, min(len(left), i + window + 1))
                       if b is not None and left[k]["token"] == b), None)
        if next_j is not None and (next_i is None or next_j - j < next_i - i):
            skipped["mutant"].extend(right[j:next_j]); j = next_j
        elif next_i is not None:
            skipped["original"].extend(left[i:next_i]); i = next_i
        else:
            skipped["original"].append(left[i]); i += 1
            skipped["mutant"].append(right[j]); j += 1
    skipped["original"].extend(left[i:]); skipped["mutant"].extend(right[j:])
    return matches, skipped


def guard_call_instances(rows):
    """Unequal call counts need unique matching entry values, not ordinal identity."""
    groups = {s: defaultdict(list) for s in rows}
    for side, records in rows.items():
        for row in records:
            if row["event"] == "call":
                groups[side][row["token"][1]].append(row)
    ambiguous = {s: set() for s in rows}
    remap = {}
    for function in groups["original"].keys() | groups["mutant"].keys():
        left, right = groups["original"][function], groups["mutant"][function]
        if len(left) == len(right):
            continue
        def signature(row):
            known = {k: v for k, v in row["values"].items() if v is not None}
            return digest(known) if known and len(known) == len(row["values"]) and not row["partial_locals"] else None
        buckets = {}
        for side, calls in (("original", left), ("mutant", right)):
            buckets[side] = defaultdict(list)
            for row in calls:
                buckets[side][signature(row)].append(row["call_id"])
        for side, calls in (("original", left), ("mutant", right)):
            other = "mutant" if side == "original" else "original"
            for row in calls:
                sig = signature(row)
                if sig is None or len(buckets[side][sig]) != 1 or len(buckets[other][sig]) != 1:
                    ambiguous[side].add(row["call_id"])
                elif side == "mutant":
                    remap[row["call_id"]] = buckets[other][sig][0]
    for side, records in rows.items():
        for row in records:
            parts = row["call_id"].split("/")
            ancestors = ["/".join(parts[:i]) for i in range(1, len(parts) + 1)]
            if any(path in ambiguous[side] for path in ancestors):
                row["token"] = None
                row["unmatched_reason"] = "ambiguous_call_instance"
            elif side == "mutant" and row["token"]:
                prefix = next((p for p in reversed(ancestors) if p in remap), None)
                if prefix:
                    token = row["token"]
                    mapped = remap[prefix] + row["call_id"][len(prefix):]
                    row["token"] = (token[0], token[1], mapped, token[3])


def compare_checkpoints(traces, cfg, matches):
    # Checkpoints inherit only call correspondences actually used by event alignment.
    # Unmatched/ambiguous calls retain their files but are not compared by ordinal.
    call_pairs = {(a["call_id"], b["call_id"]) for a, b in matches}
    right_calls = {b: a for a, b in call_pairs}
    left_calls = {a for a, _ in call_pairs}
    groups = {}
    for side in ("original", "mutant"):
        groups[side] = {}
        for r in traces[side].get("traced", {}).get("checkpoints", {}).values():
            call = r["call_id"] if side == "original" else right_calls.get(r["call_id"])
            if call in left_calls:
                groups[side][(call, r["name"], r["phase"])] = r
    report = []
    remaining = cfg["analysis"].get("checkpoint_compare_items", 1000000)
    for key in sorted(groups["original"].keys() & groups["mutant"].keys()):
        a, b = groups["original"][key], groups["mutant"][key]
        count, different, examined = 0, [], 0
        from itertools import islice
        for (i, av), (j, bv) in islice(zip(checkpoint_items(a), checkpoint_items(b)), remaining):
            if i != j:
                raise ValueError("Checkpoint index mismatch")
            ah, bh = semantic_values({"locals": {"v": av}})["v"], semantic_values({"locals": {"v": bv}})["v"]
            examined += 1
            if ah is not None and bh is not None:
                count += 1
                if ah != bh and len(different) < cfg["analysis"]["max_differences"]:
                    different.append(i)
        remaining -= examined
        full = a["complete"] and b["complete"] and count == examined == a["length"] == b["length"]
        report.append({"call_id": key[0], "name": key[1], "phase": key[2],
                       "original_ref": "original:checkpoint:" + a["checkpoint_id"],
                       "mutant_ref": "mutant:checkpoint:" + b["checkpoint_id"],
                       "compared_items": count, "different_indices": different,
                       "comparison_limited": examined < min(a["items_recorded"], b["items_recorded"]),
                       "lengths": [a["length"], b["length"]],
                       "status": "different" if different or a["length"] != b["length"] else
                                 "equal_contents" if full else "no_difference_in_compared_items"})
    return report


def compare_pair(pair, traces, cfg, directory):
    directory = Path(directory)
    version = comparison_version(pair, traces, cfg)
    sources = {s: pair[s + "_code"].splitlines() for s in ("original", "mutant")}
    a, b = sources["original"], sources["mutant"]
    if max(len(a), len(b)) > 10000:
        raise ValueError("Source line limit for comparison")
    mapping, changed = line_mapping(a, b)
    omitted = runtime_omissions(pair["original_code"]) & runtime_omissions(pair["mutant_code"])
    rows = {}
    for side in ("original", "mutant"):
        record = traces[side]
        if file_digest(record["trace_file"]) != record["trace_sha256"]:
            raise ValueError("Trace digest mismatch")
        # Stored collection limits, not the current collector configuration, govern old evidence.
        limits = record.get("environment", {}).get("trace", cfg["trace"])
        rows[side] = indexed(record["trace_file"],
                             {i + 1: i + 1 for i in range(len(a))} if side == "original" else mapping,
                             limits, omitted)
    guard_call_instances(rows)
    matches, skipped = align(rows["original"], rows["mutant"], cfg["analysis"]["alignment_window"])
    differences = []
    difference_count = 0
    comparable_paths = 0
    unknown_variables = 0
    for left, right in matches:
        names = {name for name in left["values"].keys() & right["values"].keys()
                 if left["values"][name] is not None and right["values"][name] is not None
                 and left["values"][name] != right["values"][name]}
        common = left["leaves"].keys() & right["leaves"].keys()
        paths = sorted(p for p in common if left["leaves"][p] != right["leaves"][p])
        comparable_paths += len(common)
        unknown_variables += sum(v is None for r in (left, right) for v in r["values"].values())
        if not left["partial_locals"] and not right["partial_locals"]:
            names.update(name for name in left["values"].keys() ^ right["values"].keys()
                         if (left["values"].get(name) or right["values"].get(name)) is not None)
        if left["event"] != right["event"]:
            names.add("<event_kind>")
        if names or paths:
            difference_count += 1
            if len(differences) < cfg["analysis"]["max_differences"]:
                differences.append({"original_event": left["id"], "mutant_event": right["id"],
                                    "original_line": left["line"], "mutant_line": right["line"],
                                    "event": left["event"], "variables": sorted(names),
                                    "value_paths": paths[:32], "value_paths_truncated": len(paths) > 32,
                                    "alignment": "heuristic"})
    usable = all(t.get("usable", False) for t in traces.values())
    coverage = {}
    for side in ("original", "mutant"):
        t = traces[side].get("traced", {})
        coverage[side] = {
            "events": {"recorded": len(rows[side]), "complete": t.get("events_complete", False),
                       "stop_reason": t.get("stop_reason")},
            "values": {"gaps": t.get("value_gaps"), "skeleton_events": t.get("skeleton_events", 0),
                       "ignored_runtime_entries": t.get("ignored_runtime_entries", "legacy_not_separated"),
                       "symbol_summaries": t.get("symbol_summaries", "legacy_not_separated")},
            "execution_scope": {"name": t.get("scope", "source.py/main_thread/line_boundaries/locals"),
                                "unsupported_frames": t.get("unsupported_frames", 0)},
            "checkpoints": {"recorded": len(t.get("checkpoints", {})),
                            "complete": sum(c.get("complete", False) for c in t.get("checkpoints", {}).values()),
                            "missing": t.get("checkpoint_gaps", "not_collected"),
                            "open_scopes": t.get("checkpoint_open_scopes", "not_collected")},
            "alignment": {"matched": len(matches), "unmatched": len(skipped[side])}}
    report = {
        "schema": "trace_comparison_v2", "version": version, "pair_id": pair["pair_id"],
        "evidence_status": "usable_with_limits" if usable else "incomplete_or_observation_mismatch",
        "coverage": coverage,
        "alignment": {"basis": "source_anchors/compatible_replacements/call_boundaries/bounded_resynchronization",
                      "certainty": "heuristic", "aligned_events": len(matches),
                      "original_events": len(rows["original"]), "mutant_events": len(rows["mutant"]),
                      "changed_lines_mapped": sorted(changed), "comparable_paths": comparable_paths,
                      "whole_values_unavailable": unknown_variables,
                      "unmatched": {s: [{"event_id": r["id"], "line": r["line"], "event": r["event"],
                                       "reason": r.get("unmatched_reason", "source_unmapped" if r["token"] is None else "path_or_occurrence_unmatched")}
                                      for r in skipped[s][:cfg["analysis"]["max_differences"]]]
                                    for s in skipped},
                      "unmatched_lists_truncated": any(len(rs) > cfg["analysis"]["max_differences"] for rs in skipped.values())},
        "first_divergence": {"status": "observed_candidate" if differences and usable else "unknown",
                             "candidate": differences[0] if differences else None,
                             "scope": "earliest_detected_among_compared_snapshots_not_global_first"},
        "propagation": {"status": "unknown", "edges": [], "reason": "dependencies_not_collected",
                        "observed_difference_sequence": differences},
        "difference_count": difference_count,
        "difference_list_truncated": difference_count > len(differences),
        "observable_consequence": observable_consequences(pair, traces),
        "oracle_type": oracle_configuration(pair),
        "checkpoint_comparisons": compare_checkpoints(traces, cfg, matches),
        "limitations": ["line events precede execution", "all cross-program alignment is heuristic",
                        "unmatched events do not establish equality", "equal sampled positions do not imply equal objects",
                        "object identities and causal dependencies not established",
                        "functions snapshot locals, not independent globals; main thread only",
                        "generators/coroutines and external code not traced",
                        "checkpoint comparisons use call/name/phase, not proven object correspondence"],
        "traces": traces}
    # Reserve both sides' evidence slots and prioritize the final result and earliest
    # difference before filling later progression and unmatched neighborhoods.
    evidence = []
    each = max(1, cfg["analysis"]["max_evidence_events"] // 2)
    for side in ("original", "mutant"):
        length = len(rows[side])
        priorities = []
        def choose(i):
            if 0 <= i < length and i not in priorities:
                priorities.append(i)
        if differences:
            first = differences[0][side + "_event"]
            choose(first); choose(first - 1); choose(first + 1)
        choose(length - 1)
        for r in skipped[side][:3]:
            choose(r["id"])
        # Early, middle and late differing snapshots instead of only an early prefix.
        for index in ([0, len(differences) // 2, len(differences) - 1] if differences else []):
            choose(differences[index][side + "_event"])
        choose(0); choose(1)
        for d in differences:
            choose(d[side + "_event"])
        wanted = set(priorities[:each])
        limits = traces[side].get("environment", {}).get("trace", cfg["trace"])
        found = {}
        for event in events(traces[side]["trace_file"], limits):
            if event["event_id"] in wanted:
                # Do not spend the model's context on legacy interpreter dictionaries.
                event = {**event, "locals": {k: v for k, v in event.get("locals", {}).items() if k not in omitted}}
                found[event["event_id"]] = event
        evidence += [{"ref": side + ":" + str(i), "event": found[i], "selection_rank": n}
                     for n, i in enumerate(priorities[:each]) if i in found]
    report["evidence_events"] = evidence
    report["evidence_coverage"] = {s: {"selected": sum(e["ref"].startswith(s + ":") for e in evidence),
                                     "recorded": len(rows[s])} for s in rows}
    path = directory / "comparisons" / version / "comparison.json"
    if path.exists():
        if canonical(json.loads(path.read_text())) != canonical(report):
            raise ValueError("Immutable comparison artifact changed")
    else:
        write_json(path, report)
    return str(path)
