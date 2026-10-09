"""SQLite progress; short transactions only, never held across HTTP or execution."""

import contextlib
import fcntl
import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)


def digest(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


class State:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(
            self.directory / "state.sqlite", check_same_thread=False
        )
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS kv (kind TEXT, id TEXT, body TEXT, PRIMARY KEY(kind,id));
            CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY, job TEXT, status TEXT, body TEXT);
            CREATE TABLE IF NOT EXISTS samples (id TEXT PRIMARY KEY, family TEXT, mutant TEXT UNIQUE, body TEXT);
        """)
        self.db.commit()

    def close(self):
        with self.lock:
            self.db.close()

    @contextlib.contextmanager
    def owner(self):
        # Prevent two schedulers claiming the same work; all six workers share this owner.
        with (self.directory / "run.lock").open("a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("This run already has an active scheduler") from None
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def get(self, kind, key, default=None):
        with self.lock:
            row = self.db.execute(
                "SELECT body FROM kv WHERE kind=? AND id=?", (kind, key)
            ).fetchone()
        return json.loads(row[0]) if row else default

    def put(self, kind, key, value):
        with self.lock, self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO kv VALUES(?,?,?)", (kind, key, dumps(value))
            )

    def items(self, kind):
        with self.lock:
            rows = self.db.execute(
                "SELECT id,body FROM kv WHERE kind=? ORDER BY rowid", (kind,)
            ).fetchall()
        return [(k, json.loads(v)) for k, v in rows]

    def attempt(self, request_id, job, status, body):
        with self.lock, self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO attempts VALUES(?,?,?,?)",
                (request_id, job, status, dumps(body)),
            )

    def attempts(self):
        with self.lock:
            rows = self.db.execute(
                "SELECT id,job,status,body FROM attempts ORDER BY rowid"
            ).fetchall()
        return [dict(id=a, job=b, status=c, **json.loads(d)) for a, b, c, d in rows]

    def recover(self):
        for key, job in self.items("job"):
            if job["status"] == "sending":
                job.update(
                    status="failed",
                    error="interrupted_request",
                    response_state="unknown",
                )
                self.put("job", key, job)
        for attempt in self.attempts():
            if attempt["status"] == "sending":
                rid, jid = attempt.pop("id"), attempt.pop("job")
                attempt.pop("status")
                attempt.update(
                    error="interrupted_request",
                    remote_result="unknown",
                    recovered_at=time.time(),
                )
                self.attempt(rid, jid, "unknown", attempt)

    def samples(self):
        with self.lock:
            rows = self.db.execute("SELECT body FROM samples ORDER BY rowid").fetchall()
        return [json.loads(r[0]) for r in rows]

    def count(self, family=None):
        with self.lock:
            sql, args = (
                ("SELECT count(*) FROM samples", ())
                if family is None
                else ("SELECT count(*) FROM samples WHERE family=?", (family,))
            )
            return self.db.execute(sql, args).fetchone()[0]

    def add_sample(self, sample, per_family, target):
        with self.lock, self.db:
            if (
                self.count(sample["form"]["family"]) >= per_family
                or self.count() >= target
            ):
                return "quota_full"
            try:
                self.db.execute(
                    "INSERT INTO samples VALUES(?,?,?,?)",
                    (
                        sample["sample_id"],
                        sample["form"]["family"],
                        sample["mutant_id"],
                        dumps(sample),
                    ),
                )
            except sqlite3.IntegrityError:
                return "duplicate"
            return "accepted"
