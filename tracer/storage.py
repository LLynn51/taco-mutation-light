"""Small durable stage store; never share its connection with process workers."""
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                      separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def file_digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temp.open("xb") as handle:
        os.chmod(temp, 0o600)
        handle.write(canonical(value) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextlib.contextmanager
def owner(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another pipeline owns " + str(path)) from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


class Store:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.directory / "state.sqlite", check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS records(kind TEXT, id TEXT, body TEXT, PRIMARY KEY(kind,id))")
        self.db.commit()

    def get(self, kind, key):
        with self.lock:
            row = self.db.execute("SELECT body FROM records WHERE kind=? AND id=?", (kind, key)).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, kind, key, body):
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO records VALUES(?,?,?)",
                            (kind, key, canonical(body).decode()))

    def rows(self, kind):
        with self.lock:
            rows = self.db.execute("SELECT id,body FROM records WHERE kind=? ORDER BY rowid", (kind,)).fetchall()
        return [(key, json.loads(body)) for key, body in rows]

    def delete(self, kind, key):
        with self.lock, self.db:
            self.db.execute("DELETE FROM records WHERE kind=? AND id=?", (kind, key))

    def reserve_request(self, key, request, cap):
        with self.lock:
            if self.get("request", key) is not None:
                return False
            if len(self.rows("request")) >= cap:
                return False
            self.put("request", key, {"status": "sending", "created_at": time.time(), "request": request})
            return True

    def recover_requests(self):
        for key, row in self.rows("request"):
            if row["status"] == "sending":
                receipt = self.directory / "analysis" / key / "response.json"
                if receipt.is_file():
                    row.update(status="received", response_file=str(receipt), recovered=True)
                else:
                    row.update(status="unknown", retry_allowed=False)
                self.put("request", key, row)

    def close(self):
        self.db.close()
