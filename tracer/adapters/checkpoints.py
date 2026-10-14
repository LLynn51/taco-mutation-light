"""Stream flat sequence checkpoints; never build an encoded copy of the whole array."""
import json
import time


def wire(row):
    return (json.dumps(row, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n").encode()


class Checkpoints:
    def __init__(self, send, limits, encoder):
        self.send, self.limits, self.encoder = send, limits, encoder
        self.count = self.bytes = self.partial = 0
        self.blocked = False

    def capture(self, value, *, call_id, name, phase, boundary):
        limits = self.limits
        if self.count >= limits.get("checkpoint_max_count", 64) or self.blocked:
            return {"status": "not_collected", "reason": "checkpoint_total_budget"}
        key = "cp" + str(self.count + 1)
        started = time.monotonic()
        size = 0
        remaining = limits.get("checkpoint_total_bytes", 536870912) - self.bytes
        cap = min(limits.get("checkpoint_max_bytes", 268435456), remaining)
        # Reserve bounded space for start/end metadata before transmitting chunks.
        if cap < 8192:
            self.blocked = True
            return {"status": "not_collected", "reason": "checkpoint_total_budget"}

        def emit(row):
            nonlocal size
            n = self.send(row)
            size += n
            self.bytes += n

        start = {"kind": "checkpoint_start", "checkpoint_id": key, "call_id": call_id,
                 "name": name, "phase": phase, "boundary": boundary,
                 "type": type(value).__name__, "length": len(value)}
        if len(wire(start)) > 4096:
            return {"status": "not_collected", "reason": "checkpoint_metadata_limit"}
        self.count += 1
        emit(start)
        offset = 0
        reason = None
        value_gaps = 0
        chunk = []
        chunk_bytes = 0

        def flush():
            nonlocal offset, chunk, chunk_bytes
            if chunk:
                emit({"kind": "checkpoint_chunk", "checkpoint_id": key, "offset": offset, "items": chunk})
                offset += len(chunk)
                chunk, chunk_bytes = [], 0

        for i in range(min(len(value), limits.get("checkpoint_max_items", 10000000))):
            if time.monotonic() - started > limits.get("checkpoint_seconds", 5):
                reason = "checkpoint_time_limit"
                break
            enc = self.encoder(limits, full=True)
            item = enc.value(value[i])
            encoded_size = len(wire(item))
            if encoded_size > limits.get("checkpoint_chunk_bytes", 32768):
                reason = "checkpoint_item_size_limit"
                break
            if size + chunk_bytes + encoded_size + 8192 > cap:
                reason = "checkpoint_byte_limit"
                break
            if chunk and chunk_bytes + encoded_size > limits.get("checkpoint_chunk_bytes", 32768):
                flush()
            chunk.append(item)
            chunk_bytes += encoded_size
            value_gaps += enc.missing + enc.symbols
        flush()
        if offset < len(value) and reason is None:
            reason = "checkpoint_item_limit"
        if value_gaps and reason is None:
            reason = "checkpoint_value_gaps"
        complete = reason is None
        self.partial += not complete
        emit({"kind": "checkpoint_end", "checkpoint_id": key, "items_recorded": offset,
              "complete": complete, "stop_reason": reason, "value_gaps": value_gaps,
              "elapsed_seconds": time.monotonic() - started})
        return {"checkpoint_id": key, "status": "complete" if complete else "partial",
                "reason": reason, "length": len(value), "items_recorded": offset}
