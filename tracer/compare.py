"""Bounded-memory line alignment. No target imports or execution on the host."""
from collections import Counter, defaultdict
import difflib
import gzip
import json
from pathlib import Path

from .storage import canonical, digest, file_digest, write_json


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
        if value.get("status") == "not_collected" or value.get("type") in ("set", "frozenset"):
            raise ValueError("Unavailable or unordered value")
        if "ref" in value:
            value = objects[value["ref"]]
        oid = value.get("object_id")
        if oid is not None and oid in seen:
            raise ValueError("Cyclic content comparison unsupported")
        next_seen = seen | {oid} if oid else seen
        return {key: normalize(item, next_seen) for key, item in value.items()
                if key not in ("object_id", "object_id_scope")}

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


def indexed(path, mapping, limit):
    records = []
    for row in events(path, limit):
        line = mapping.get(row["line"])
        token = (row["event"], row["function"], row["call_id"], line) if line is not None else None
        records.append((row["event_id"], token, semantic_values(row), "remaining_locals" in row))
        if len(records) > limit["max_events"]:
            raise ValueError("Stored event count exceeds limit")
    return records


def compare_pair(pair, traces, cfg, directory):
    directory = Path(directory)
    sources = {side: pair[side + "_code"].splitlines() for side in ("original", "mutant")}
    # Static line alignment only; no claim of a proven cross-program state mapping.
    a, b = sources["original"], sources["mutant"]
    if max(len(a), len(b)) > 10000:
        raise ValueError("Source line limit for comparison")
    matcher = difflib.SequenceMatcher(None, a, b, autojunk=True)
    mapping = {}
    for block in matcher.get_matching_blocks():
        for offset in range(block.size):
            mapping[block.b + offset + 1] = block.a + offset + 1
    rows = {}
    for side in ("original", "mutant"):
        record = traces[side]
        if file_digest(record["trace_file"]) != record["trace_sha256"]:
            raise ValueError("Trace digest mismatch")
        rows[side] = indexed(record["trace_file"], {i + 1: i + 1 for i in range(len(a))}
                             if side == "original" else mapping, cfg["trace"])
    left, right = rows["original"], rows["mutant"]
    lc, rc = Counter(x[1] for x in left if x[1]), Counter(x[1] for x in right if x[1])
    eligible = {key for key in lc if lc[key] == rc.get(key)}
    occurrences = defaultdict(int)
    right_index = {}
    for row in right:
        if row[1] in eligible:
            occurrences[row[1]] += 1
            right_index[(row[1], occurrences[row[1]])] = row
    occurrences.clear()
    last = -1
    differences = []
    aligned = 0
    observed_differences = 0
    chosen = {"original": set(), "mutant": set()}
    for row in left:
        if row[1] not in eligible:
            continue
        occurrences[row[1]] += 1
        other = right_index[(row[1], occurrences[row[1]])]
        if other[0] <= last:
            continue
        last = other[0]
        aligned += 1
        names = sorted(name for name in row[2].keys() & other[2].keys()
                       if row[2][name] is not None and other[2][name] is not None
                       and row[2][name] != other[2][name])
        if not row[3] and not other[3]:
            names += sorted(name for name in row[2].keys() ^ other[2].keys()
                            if (row[2].get(name) or other[2].get(name)) is not None)
        if names:
            observed_differences += 1
            if len(differences) < cfg["analysis"]["max_differences"]:
                differences.append({"original_event": row[0], "mutant_event": other[0], "variables": names,
                                    "original_line": row[1][3], "event": row[1][0]})
                for side, index in (("original", row[0]), ("mutant", other[0])):
                    chosen[side].update(range(max(0, index - 1), index + 2))
    usable = all(traces[side]["usable"] for side in traces)
    report = {"schema": "trace_comparison_v1", "pair_id": pair["pair_id"],
              "evidence_status": "usable_with_limits" if usable else "incomplete_or_observation_mismatch",
              "alignment": {"basis": "equal_source_lines/call_path/equal_occurrence_counts/monotonic_order",
                            "certainty": "heuristic", "aligned_events": aligned,
                            "original_events": len(left), "mutant_events": len(right)},
              "first_divergence": {"status": "observed_candidate" if differences and usable else "unknown",
                                   "candidate": differences[0] if differences else None,
                                   "scope": "earliest_detected_difference_among_compared_snapshots_not_global_first"},
              "propagation": {"status": "unknown", "edges": [], "reason": "read_write_dependencies_not_collected",
                              "observed_difference_sequence": differences},
              "difference_count": observed_differences,
              "difference_list_truncated": observed_differences > len(differences),
              "limitations": ["line events precede execution", "changed/unmatched lines not paired",
                              "loop/call ordinal alignment is heuristic", "alias/cyclic/set equality not established",
                              "globals not included in function snapshots", "main thread only; generators/coroutines unsupported",
                              "one diagnostic run per side; repeat stability not checked",
                              "missing values cannot establish equality"],
              "traces": {side: {key: traces[side][key] for key in
                                 ("trace_file", "trace_sha256", "external_results_match", "usable", "traced")}
                         for side in traces}}
    evidence = []
    for side in ("original", "mutant"):
        if not chosen[side]:
            chosen[side].update([0, 1, max(0, len(rows[side]) - 1)])
        count = 0
        for event in events(traces[side]["trace_file"], cfg["trace"]):
            if event["event_id"] in chosen[side] and count < cfg["analysis"]["max_evidence_events"] // 2:
                evidence.append({"ref": side + ":" + str(event["event_id"]), "event": event})
                count += 1
    report["evidence_events"] = evidence
    write_json(directory / "comparison.json", report)
    return str(directory / "comparison.json")
