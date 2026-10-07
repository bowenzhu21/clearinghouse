"""SQLite transaction boundary for ingestion, journals, and the durable outbox."""

from contextlib import closing, contextmanager
from pathlib import Path
import json
import math
import sqlite3
import time
from typing import Any, Callable, Iterator
from uuid import uuid4

from .models import ConflictError, NotFoundError, PaymentEvent, ValidationError, canonical, identifier, request_hash


def _time(value: float | None) -> float:
    value = time.time() if value is None else value
    if type(value) not in (int, float) or not 0 <= value <= 1_000_000_000_000 or not math.isfinite(value):
        raise ValidationError("now must be a finite nonnegative timestamp")
    return float(value)


def _attempt_limit(value: int) -> int:
    if type(value) is not int or not 1 <= value <= 100:
        raise ValidationError("max_attempts must be an integer from 1 to 100")
    return value


class Ledger:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise RuntimeError(f"unsupported database schema version: {version}")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(Path(__file__).with_name("schema.sql").read_text())

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=FULL")
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as conn:
            # All checks and writes occur while holding SQLite's one writer lock.
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    @staticmethod
    def _cached(conn: sqlite3.Connection, key: str, fingerprint: str) -> dict[str, Any] | None:
        row = conn.execute("SELECT * FROM idempotency WHERE idempotency_key = ?", (key,)).fetchone()
        if row is None:
            return None
        if row["request_hash"] != fingerprint:
            raise ConflictError("idempotency key already used with a different request")
        return json.loads(row["response_json"])

    @staticmethod
    def _remember(conn: sqlite3.Connection, key: str, fingerprint: str, response: dict[str, Any], now: float) -> None:
        conn.execute("INSERT INTO idempotency VALUES (?, ?, ?, ?)", (key, fingerprint, canonical(response), now))

    @staticmethod
    def _enqueue(conn: sqlite3.Connection, topic: str, payload: dict[str, Any], now: float) -> None:
        conn.execute(
            "INSERT INTO outbox(job_id, topic, payload_json, created_at, available_at) VALUES (?, ?, ?, ?, ?)",
            (uuid4().hex, topic, canonical(payload), now, now),
        )

    @staticmethod
    def _journal(
        conn: sqlite3.Connection,
        currency: str,
        lines: list[tuple[str, int, int]],
        reason: str,
        now: float,
        event_id: str | None = None,
        reversal_of: str | None = None,
    ) -> str:
        journal_id = uuid4().hex
        for number, (account, debit, credit) in enumerate(lines, start=1):
            conn.execute("INSERT INTO journal_lines VALUES (?, ?, ?, ?, ?, ?)", (journal_id, number, account, currency, debit, credit))
        # A database trigger checks balance and seals the lines against mutation.
        conn.execute("INSERT INTO journals VALUES (?, ?, ?, ?, ?, ?)", (journal_id, event_id, currency, reversal_of, reason, now))
        return journal_id

    def ingest(self, data: Any, idempotency_key: str, *, now: float | None = None, failpoint: Callable[[str], None] | None = None) -> dict[str, Any]:
        event = PaymentEvent.parse(data)
        key = identifier(idempotency_key, "idempotency_key")
        fingerprint = request_hash({"operation": "ingest", "event": event.as_dict()})
        event_hash = request_hash(event.as_dict())
        timestamp = _time(now)
        with self.transaction() as conn:
            cached = self._cached(conn, key, fingerprint)
            if cached is not None:
                return cached
            existing = conn.execute("SELECT * FROM events WHERE event_id = ?", (event.event_id,)).fetchone()
            if existing is not None:
                if existing["request_hash"] != event_hash:
                    raise ConflictError("event_id already used with a different event")
                response = self._event_response(conn, event.event_id)
            else:
                conn.execute(
                    "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?)",
                    (event.event_id, event.payment_id, event.kind, event.amount_cents, event.currency, event.occurred_at, event_hash, timestamp),
                )
                self._apply(conn, event.event_id, timestamp)
                if event.kind == "payment.captured":
                    pending = conn.execute(
                        "SELECT event_id FROM events WHERE payment_id = ? AND status = 'pending' AND kind = 'payment.refunded' ORDER BY occurred_at, event_id",
                        (event.payment_id,),
                    ).fetchall()
                    for row in pending:
                        self._apply(conn, row["event_id"], timestamp)
                if failpoint is not None:
                    failpoint("before_commit")
                response = self._event_response(conn, event.event_id)
            self._remember(conn, key, fingerprint, response, timestamp)
            return response

    def _reject(self, conn: sqlite3.Connection, event_id: str, reason: str, now: float) -> None:
        conn.execute("UPDATE events SET status = 'rejected', reason = ? WHERE event_id = ?", (reason, event_id))
        self._enqueue(conn, "event.rejected", {"event_id": event_id, "reason": reason}, now)

    def _apply(self, conn: sqlite3.Connection, event_id: str, now: float) -> None:
        event = conn.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
        capture = conn.execute(
            "SELECT * FROM events WHERE payment_id = ? AND kind = 'payment.captured' AND status = 'posted'", (event["payment_id"],)
        ).fetchone()
        amount = event["amount_cents"]
        if event["kind"] == "payment.captured":
            if capture is not None:
                self._reject(conn, event_id, "payment_already_captured", now)
                return
            lines = [("processor_receivable", amount, 0), ("merchant_payable", 0, amount)]
        else:
            if capture is None:
                return  # Durable dependency: a later capture will resume this event.
            if capture["currency"] != event["currency"]:
                self._reject(conn, event_id, "currency_mismatch", now)
                return
            refunded = conn.execute(
                "SELECT COALESCE(SUM(amount_cents), 0) FROM events WHERE payment_id = ? AND kind = 'payment.refunded' AND status = 'posted'",
                (event["payment_id"],),
            ).fetchone()[0]
            if refunded + amount > capture["amount_cents"]:
                self._reject(conn, event_id, "refund_exceeds_capture", now)
                return
            lines = [("merchant_payable", amount, 0), ("processor_receivable", 0, amount)]
        journal_id = self._journal(conn, event["currency"], lines, event["kind"], now, event_id=event_id)
        conn.execute("UPDATE events SET status = 'posted', reason = NULL WHERE event_id = ?", (event_id,))
        self._enqueue(conn, "journal.posted", {"event_id": event_id, "payment_id": event["payment_id"], "journal_id": journal_id, "currency": event["currency"], "amount_cents": amount}, now)

    @staticmethod
    def _event_response(conn: sqlite3.Connection, event_id: str) -> dict[str, Any]:
        row = conn.execute(
            "SELECT e.event_id, e.payment_id, e.status, e.reason, j.journal_id FROM events e LEFT JOIN journals j ON j.event_id = e.event_id WHERE e.event_id = ?",
            (event_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError("event not found")
        return dict(row)

    def event(self, event_id: str) -> dict[str, Any]:
        with self.connection() as conn:
            return self._event_response(conn, event_id)

    def payment(self, payment_id: str) -> dict[str, Any]:
        with self.connection() as conn:
            rows = conn.execute("SELECT event_id, kind, amount_cents, currency, status, reason FROM events WHERE payment_id = ? ORDER BY occurred_at, event_id", (payment_id,)).fetchall()
            if not rows:
                raise NotFoundError("payment not found")
            posted = [row for row in rows if row["status"] == "posted"]
            captured = sum(row["amount_cents"] for row in posted if row["kind"] == "payment.captured")
            refunded = sum(row["amount_cents"] for row in posted if row["kind"] == "payment.refunded")
            return {"payment_id": payment_id, "captured_cents": captured, "refunded_cents": refunded, "net_cents": captured - refunded, "events": [dict(row) for row in rows]}

    def reverse(self, journal_id: str, reason: str, idempotency_key: str, *, now: float | None = None) -> dict[str, Any]:
        identifier(journal_id, "journal_id")
        key = identifier(idempotency_key, "idempotency_key")
        if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
            raise ValidationError("reason must contain 1–500 nonblank characters")
        reason = reason.strip()
        fingerprint = request_hash({"operation": "reverse", "journal_id": journal_id, "reason": reason})
        timestamp = _time(now)
        with self.transaction() as conn:
            cached = self._cached(conn, key, fingerprint)
            if cached is not None:
                return cached
            original = conn.execute("SELECT * FROM journals WHERE journal_id = ?", (journal_id,)).fetchone()
            if original is None:
                raise NotFoundError("journal not found")
            if original["reversal_of"] is not None:
                raise ConflictError("a reversal cannot itself be reversed")
            if conn.execute("SELECT 1 FROM journals WHERE reversal_of = ?", (journal_id,)).fetchone():
                raise ConflictError("journal already reversed")
            lines = conn.execute("SELECT * FROM journal_lines WHERE journal_id = ? ORDER BY line_number", (journal_id,)).fetchall()
            reversal_id = self._journal(conn, original["currency"], [(r["account"], r["credit_cents"], r["debit_cents"]) for r in lines], reason, timestamp, reversal_of=journal_id)
            response = {"journal_id": reversal_id, "reversal_of": journal_id, "reason": reason}
            self._enqueue(conn, "journal.reversed", response, timestamp)
            self._remember(conn, key, fingerprint, response, timestamp)
            return response

    def trial_balance(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            return [dict(row) for row in conn.execute(
                "SELECT currency, account, SUM(debit_cents) AS debit_cents, SUM(credit_cents) AS credit_cents, SUM(debit_cents - credit_cents) AS balance_cents FROM journal_lines GROUP BY currency, account ORDER BY currency, account"
            )]

    def expected_settlements(self) -> dict[str, dict[str, Any]]:
        with self.connection() as conn:
            return {r["event_id"]: {"event_id": r["event_id"], "amount_cents": r["amount_cents"] * (1 if r["kind"] == "payment.captured" else -1), "currency": r["currency"]} for r in conn.execute("SELECT * FROM events WHERE status = 'posted'")}

    def snapshot(self) -> dict[str, Any]:
        with self.connection() as conn:
            # One read transaction makes the operations summary internally consistent.
            conn.execute("BEGIN")
            result = {"events": [dict(r) for r in conn.execute("SELECT * FROM events ORDER BY received_at, event_id")], "journals": [dict(r) for r in conn.execute("SELECT * FROM journals ORDER BY created_at, journal_id")], "outbox": [dict(r) for r in conn.execute("SELECT * FROM outbox ORDER BY created_at, job_id")]}
            result["journal_lines"] = [dict(r) for r in conn.execute("SELECT * FROM journal_lines ORDER BY journal_id, line_number")]
            result["balance"] = [dict(r) for r in conn.execute("SELECT currency, SUM(debit_cents) AS debit_cents, SUM(credit_cents) AS credit_cents, SUM(debit_cents-credit_cents) AS difference_cents FROM journal_lines GROUP BY currency ORDER BY currency")]
            return result

    def claim(self, *, now: float | None = None, lease_seconds: float = 30, max_attempts: int = 5) -> dict[str, Any] | None:
        if type(lease_seconds) not in (int, float) or not 0 < lease_seconds <= 86_400 or not math.isfinite(lease_seconds):
            raise ValidationError("lease_seconds must be finite, positive, and at most 86400")
        _attempt_limit(max_attempts)
        timestamp = _time(now)
        with self.transaction() as conn:
            # A worker that crashes on its final attempt must not strand the job.
            conn.execute(
                "UPDATE outbox SET dead_letter_at = ?, lease_token = NULL, lease_until = NULL, last_error = 'attempt budget exhausted after lease expiry' WHERE delivered_at IS NULL AND dead_letter_at IS NULL AND attempts >= ? AND (lease_until IS NULL OR lease_until <= ?)",
                (timestamp, max_attempts, timestamp),
            )
            row = conn.execute(
                "SELECT * FROM outbox WHERE delivered_at IS NULL AND dead_letter_at IS NULL AND available_at <= ? AND (lease_until IS NULL OR lease_until <= ?) AND attempts < ? ORDER BY available_at, created_at, job_id LIMIT 1",
                (timestamp, timestamp, max_attempts),
            ).fetchone()
            if row is None:
                return None
            token = uuid4().hex
            conn.execute("UPDATE outbox SET lease_token = ?, lease_until = ?, attempts = attempts + 1 WHERE job_id = ?", (token, timestamp + lease_seconds, row["job_id"]))
            result = dict(row)
            result.update(lease_token=token, lease_until=timestamp + lease_seconds, attempts=row["attempts"] + 1, payload=json.loads(row["payload_json"]))
            return result

    def acknowledge(self, job_id: str, token: str, *, now: float | None = None) -> bool:
        timestamp = _time(now)
        with self.transaction() as conn:
            changed = conn.execute("UPDATE outbox SET delivered_at = ?, lease_token = NULL, lease_until = NULL, last_error = NULL WHERE job_id = ? AND lease_token = ? AND lease_until > ? AND delivered_at IS NULL AND dead_letter_at IS NULL", (timestamp, job_id, token, timestamp)).rowcount
            return changed == 1

    def fail(self, job_id: str, token: str, error: str, *, now: float | None = None, max_attempts: int = 5) -> bool:
        timestamp = _time(now)
        _attempt_limit(max_attempts)
        with self.transaction() as conn:
            row = conn.execute("SELECT attempts FROM outbox WHERE job_id = ? AND lease_token = ? AND lease_until > ? AND delivered_at IS NULL AND dead_letter_at IS NULL", (job_id, token, timestamp)).fetchone()
            if row is None:
                return False
            attempts = row["attempts"]
            conn.execute("UPDATE outbox SET available_at = ?, lease_token = NULL, lease_until = NULL, dead_letter_at = ?, last_error = ? WHERE job_id = ?", (timestamp + min(60, 2 ** min(attempts, 10)), timestamp if attempts >= max_attempts else None, str(error)[:500], job_id))
            return True

    def replay_dead_letter(self, job_id: str, reason: str, *, now: float | None = None) -> None:
        if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
            raise ValidationError("replay requires a reason of 1–500 characters")
        timestamp = _time(now)
        with self.transaction() as conn:
            changed = conn.execute("UPDATE outbox SET attempts = 0, dead_letter_at = NULL, available_at = ?, lease_token = NULL, lease_until = NULL WHERE job_id = ? AND dead_letter_at IS NOT NULL", (timestamp, job_id)).rowcount
            if changed != 1:
                raise ConflictError("job is not in the dead-letter queue")
            conn.execute("INSERT INTO dead_letter_replays VALUES (?, ?, ?, ?)", (uuid4().hex, job_id, reason.strip(), timestamp))


class Inbox:
    """Example downstream consumer: deduplicates delivery ID in its own database.

    Real consumers must commit their business side effect in this same transaction.
    In this example, the durable receipt *is* the side effect.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE IF NOT EXISTS receipts(job_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, received_at REAL NOT NULL)")

    def deliver(self, job: dict[str, Any], *, now: float | None = None) -> bool:
        timestamp = _time(now)
        with closing(sqlite3.connect(self.path, timeout=10)) as conn, conn:
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT payload_json FROM receipts WHERE job_id = ?", (job["job_id"],)).fetchone()
            if existing is not None:
                if existing[0] != job["payload_json"]:
                    raise ConflictError("delivery ID reused with different payload")
                return False
            return conn.execute("INSERT INTO receipts VALUES (?, ?, ?)", (job["job_id"], job["payload_json"], timestamp)).rowcount == 1

    def count(self) -> int:
        with closing(sqlite3.connect(self.path)) as conn:
            return conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
