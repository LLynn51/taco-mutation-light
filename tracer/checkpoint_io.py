"""Host-side checkpoint transport validation and bounded streaming reads."""
import gzip
import os
from pathlib import Path
import json

from .storage import canonical, file_digest


class CheckpointSink:
    def __init__(self, directory, limits):
        self.directory, self.limits = Path(directory), limits
        self.records = {}
        self.handle = None
        self.current = None
        self.bytes = 0

    def accept(self, row):
        kind, key = row.get("kind"), row.get("checkpoint_id")
        data = canonical(row) + b"\n"
        self.bytes += len(data)
        if self.bytes > self.limits["checkpoint_total_bytes"]:
            raise ValueError("checkpoint_transport_limit")
        if kind == "checkpoint_start":
            if self.current or key != "cp" + str(len(self.records) + 1):
                raise ValueError("checkpoint_sequence")
            if len(self.records) >= self.limits["checkpoint_max_count"]:
                raise ValueError("checkpoint_count")
            self.directory.mkdir(parents=True, exist_ok=True)
            path = self.directory / (key + ".jsonl.gz")
            self.handle = gzip.open(path, "xb", compresslevel=3)
            self.current = key
            self.records[key] = {**row, "file": str(path), "items_recorded": 0,
                                 "complete": False, "transport_complete": False, "bytes": 0,
                                 "max_frame_bytes": self.limits["max_event_bytes"]}
        if key != self.current or self.handle is None:
            raise ValueError("checkpoint_without_start")
        record = self.records[key]
        record["bytes"] += len(data)
        if record["bytes"] > self.limits["checkpoint_max_bytes"]:
            raise ValueError("checkpoint_size")
        if kind == "checkpoint_chunk":
            if row["offset"] != record["items_recorded"] or type(row["items"]) is not list:
                raise ValueError("checkpoint_offset")
            record["items_recorded"] += len(row["items"])
            if record["items_recorded"] > min(record["length"], self.limits["checkpoint_max_items"]):
                raise ValueError("checkpoint_items")
        elif kind == "checkpoint_end":
            if row["items_recorded"] != record["items_recorded"]:
                raise ValueError("checkpoint_footer")
            if row["complete"] and record["items_recorded"] != record["length"]:
                raise ValueError("checkpoint_incomplete")
            record.update(complete=row["complete"], transport_complete=True,
                          stop_reason=row["stop_reason"], value_gaps=row["value_gaps"])
        elif kind != "checkpoint_start":
            raise ValueError("checkpoint_kind")
        self.handle.write(data)
        if kind == "checkpoint_end":
            self._close()

    def _close(self):
        if self.handle:
            self.handle.close()
            record = self.records[self.current]
            with Path(record["file"]).open("rb") as handle:
                os.fsync(handle.fileno())
            record["sha256"] = file_digest(record["file"])
            self.handle = self.current = None

    def close(self):
        self._close()


def checkpoint_items(record):
    if file_digest(record["file"]) != record["sha256"]:
        raise ValueError("Checkpoint digest mismatch")
    consumed = expected = 0
    with gzip.open(record["file"], "rb") as handle:
        while True:
            line = handle.readline(min(record["bytes"] - consumed + 1, record.get("max_frame_bytes", 1048576) + 1))
            if not line:
                break
            consumed += len(line)
            if not line.endswith(b"\n") or consumed > record["bytes"]:
                raise ValueError("Checkpoint size mismatch")
            row = json.loads(line)
            if row["kind"] == "checkpoint_chunk":
                if row["offset"] != expected:
                    raise ValueError("Checkpoint offset mismatch")
                for offset, item in enumerate(row["items"]):
                    yield row["offset"] + offset, item
                expected += len(row["items"])
