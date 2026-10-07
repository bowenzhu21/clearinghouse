from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from clearinghouse.ledger import Inbox, Ledger
from clearinghouse.models import ConflictError, PaymentEvent, ValidationError


def event(event_id="evt_1", payment_id="pay_1", kind="payment.captured", amount=10000, currency="USD"):
    return {"event_id": event_id, "payment_id": payment_id, "kind": kind, "amount_cents": amount, "currency": currency, "occurred_at": "2026-01-01T00:00:00Z"}


class LedgerFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "ledger.db"
        self.ledger = Ledger(self.path)


class LedgerTest(LedgerFixture):
    def test_capture_and_refund_balances(self):
        self.ledger.ingest(event(), "capture")
        self.ledger.ingest(event("refund", kind="payment.refunded", amount=2500), "refund")
        payment = self.ledger.payment("pay_1")
        self.assertEqual((10000, 2500, 7500), (payment["captured_cents"], payment["refunded_cents"], payment["net_cents"]))
        balance = self.ledger.trial_balance()
        self.assertEqual([("merchant_payable", -7500), ("processor_receivable", 7500)], [(r["account"], r["balance_cents"]) for r in balance])
        self.assertEqual(0, self.ledger.snapshot()["balance"][0]["difference_cents"])

    def test_money_and_schema_are_strict(self):
        for value in (True, 1.5, "100", -1, 0, 10**13, None):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                PaymentEvent.parse(event(amount=value))
        for data in (event(currency="JPY"), {**event(), "unknown": 1}, {**event(), "occurred_at": "2026-01-01"}, {**event(), "event_id": "a/b"}, []):
            with self.subTest(data=data), self.assertRaises(ValidationError):
                PaymentEvent.parse(data)

    def test_equivalent_timestamp_offsets_are_idempotent(self):
        first = self.ledger.ingest(event(), "key")
        second = self.ledger.ingest({**event(), "occurred_at": "2025-12-31T19:00:00-05:00"}, "key")
        self.assertEqual(first, second)

    def test_idempotency_and_event_identity_conflicts(self):
        first = self.ledger.ingest(event(), "key")
        self.assertEqual(first, self.ledger.ingest(event(), "key"))
        self.assertEqual(first, self.ledger.ingest(event(), "other_key"))
        with self.assertRaises(ConflictError):
            self.ledger.ingest(event(amount=9999), "key")
        with self.assertRaises(ConflictError):
            self.ledger.ingest(event(amount=9999), "fresh_key")
        state = self.ledger.snapshot()
        self.assertEqual((1, 1, 1), (len(state["events"]), len(state["journals"]), len(state["outbox"])))

    def test_concurrent_duplicate_ingestion_posts_once(self):
        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(lambda _: self.ledger.ingest(event(), "same_key"), range(24)))
        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(1, len(self.ledger.snapshot()["journals"]))
        self.assertEqual(1, len(self.ledger.snapshot()["outbox"]))

    def test_concurrent_distinct_keys_same_event_posts_once(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda n: self.ledger.ingest(event(), f"key_{n}"), range(16)))
        self.assertEqual(1, len({r["journal_id"] for r in results}))

    def test_pending_refund_resumes_after_restart(self):
        refund = event("refund", kind="payment.refunded", amount=2500)
        pending = self.ledger.ingest(refund, "refund_key")
        self.assertEqual("pending", pending["status"])
        self.assertEqual([], self.ledger.snapshot()["journals"])
        restarted = Ledger(self.path)
        restarted.ingest(event(), "capture_key")
        self.assertEqual("posted", restarted.event("refund")["status"])
        # Idempotency preserves the original HTTP result. Read current state separately.
        self.assertEqual(pending, restarted.ingest(refund, "refund_key"))
        self.assertEqual(7500, restarted.payment("pay_1")["net_cents"])

    def test_pending_invalid_refund_does_not_block_capture(self):
        self.ledger.ingest(event("too_much", kind="payment.refunded", amount=12000), "refund")
        self.assertEqual("posted", self.ledger.ingest(event(), "capture")["status"])
        self.assertEqual("refund_exceeds_capture", self.ledger.event("too_much")["reason"])

    def test_domain_rejections_are_durable_and_emit_outbox(self):
        self.ledger.ingest(event(), "capture")
        result = self.ledger.ingest(event("second_capture"), "duplicate_payment")
        self.assertEqual("payment_already_captured", result["reason"])
        result = self.ledger.ingest(event("bad_currency", kind="payment.refunded", currency="EUR"), "currency")
        self.assertEqual("currency_mismatch", result["reason"])
        self.assertEqual(1, len(self.ledger.snapshot()["journals"]))
        self.assertEqual(3, len(self.ledger.snapshot()["outbox"]))

    def test_concurrent_refunds_cannot_overdraw_capture(self):
        self.ledger.ingest(event(), "capture")
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda n: self.ledger.ingest(event(f"r{n}", kind="payment.refunded", amount=7500), f"r{n}"), range(2)))
        self.assertEqual(["posted", "rejected"], sorted(r["status"] for r in results))
        self.assertEqual(2500, self.ledger.payment("pay_1")["net_cents"])

    def test_failure_rolls_back_event_journal_outbox_and_key(self):
        def crash(stage):
            raise RuntimeError(stage)
        with self.assertRaisesRegex(RuntimeError, "before_commit"):
            self.ledger.ingest(event(), "key", failpoint=crash)
        state = self.ledger.snapshot()
        self.assertEqual((0, 0, 0), (len(state["events"]), len(state["journals"]), len(state["outbox"])))
        with self.ledger.connection() as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM idempotency").fetchone()[0])
        self.assertEqual("posted", self.ledger.ingest(event(), "key")["status"])

    def test_actual_process_exit_after_commit_preserves_outbox(self):
        code = "import json, os, sys; from clearinghouse.ledger import Ledger; Ledger(sys.argv[1]).ingest(json.loads(sys.argv[2]), 'key'); os._exit(23)"
        process = subprocess.run([sys.executable, "-c", code, str(self.path), json.dumps(event())], capture_output=True, text=True)
        self.assertEqual(23, process.returncode, process.stderr)
        reopened = Ledger(self.path)
        self.assertEqual("posted", reopened.event("evt_1")["status"])
        self.assertIsNotNone(reopened.claim())
        self.assertEqual(1, len(reopened.snapshot()["journals"]))

    def test_db_rejects_unbalanced_mixed_currency_or_unsealed_journal(self):
        self.ledger.ingest(event(), "key")
        for currency, debit, credit in (("USD", 100, 99), ("EUR", 100, 100)):
            with self.subTest(currency=currency, credit=credit), self.assertRaises(sqlite3.IntegrityError):
                with self.ledger.transaction() as conn:
                    conn.execute("INSERT INTO journal_lines VALUES ('bad', 1, 'merchant_payable', 'USD', ?, 0)", (debit,))
                    conn.execute("INSERT INTO journal_lines VALUES ('bad', 2, 'processor_receivable', ?, 0, ?)", (currency, credit))
                    conn.execute("INSERT INTO journals VALUES ('bad', NULL, 'USD', ?, 'test', 0)", (self.ledger.event("evt_1")["journal_id"],))
        with self.assertRaises(sqlite3.IntegrityError):
            with self.ledger.transaction() as conn:
                conn.execute("INSERT INTO journal_lines VALUES ('orphan', 1, 'merchant_payable', 'USD', 100, 0)")
        self.assertEqual(1, len(self.ledger.snapshot()["journals"]))

    def test_journals_are_immutable(self):
        self.ledger.ingest(event(), "key")
        for sql in ("DELETE FROM journals", "UPDATE journals SET reason = 'tamper'", "DELETE FROM journal_lines", "UPDATE journal_lines SET debit_cents = 1"):
            with self.subTest(sql=sql), self.assertRaises(sqlite3.IntegrityError):
                with self.ledger.transaction() as conn:
                    conn.execute(sql)
        journal = self.ledger.event("evt_1")["journal_id"]
        with self.assertRaises(sqlite3.IntegrityError):
            with self.ledger.transaction() as conn:
                conn.execute("INSERT INTO journal_lines VALUES (?, 3, 'merchant_payable', 'USD', 100, 0)", (journal,))

    def test_database_rejects_fractional_money(self):
        with self.assertRaises(sqlite3.IntegrityError):
            with self.ledger.transaction() as conn:
                conn.execute("INSERT INTO journal_lines VALUES ('fractional', 1, 'merchant_payable', 'USD', 0.5, 0)")

    def test_reversal_offsets_ledger_but_preserves_provider_history(self):
        original = self.ledger.ingest(event(), "capture")
        reversal = self.ledger.reverse(original["journal_id"], "Incorrect classification; correcting books", "reverse")
        self.assertEqual(reversal, self.ledger.reverse(original["journal_id"], "Incorrect classification; correcting books", "reverse"))
        self.assertTrue(all(row["balance_cents"] == 0 for row in self.ledger.trial_balance()))
        self.assertEqual(10000, self.ledger.payment("pay_1")["net_cents"])
        self.assertEqual(10000, self.ledger.expected_settlements()["evt_1"]["amount_cents"])
        with self.assertRaises(ConflictError):
            self.ledger.reverse(original["journal_id"], "again", "other")
        with self.assertRaises(ConflictError):
            self.ledger.reverse(reversal["journal_id"], "undo", "undo")


class OutboxTest(LedgerFixture):
    def test_lease_reclaim_and_stale_worker_fencing(self):
        self.ledger.ingest(event(), "key", now=100)
        first = self.ledger.claim(now=100, lease_seconds=10)
        self.assertIsNone(self.ledger.claim(now=109))
        second = self.ledger.claim(now=110, lease_seconds=10)
        self.assertEqual(first["job_id"], second["job_id"])
        self.assertNotEqual(first["lease_token"], second["lease_token"])
        self.assertFalse(self.ledger.acknowledge(first["job_id"], first["lease_token"], now=111))
        self.assertFalse(self.ledger.fail(first["job_id"], first["lease_token"], "stale", now=111))
        self.assertTrue(self.ledger.acknowledge(second["job_id"], second["lease_token"], now=111))

    def test_two_workers_cannot_claim_same_live_lease(self):
        self.ledger.ingest(event(), "key", now=100)
        with ThreadPoolExecutor(max_workers=8) as pool:
            jobs = list(pool.map(lambda _: self.ledger.claim(now=100), range(8)))
        self.assertEqual(1, sum(job is not None for job in jobs))

    def test_retry_backoff_dead_letter_and_audited_replay(self):
        self.ledger.ingest(event(), "key", now=100)
        job = self.ledger.claim(now=100, max_attempts=2)
        self.assertTrue(self.ledger.fail(job["job_id"], job["lease_token"], "downstream unavailable", now=100, max_attempts=2))
        self.assertIsNone(self.ledger.claim(now=101, max_attempts=2))
        job = self.ledger.claim(now=102, max_attempts=2)
        self.assertEqual(2, job["attempts"])
        self.ledger.fail(job["job_id"], job["lease_token"], "still unavailable", now=102, max_attempts=2)
        self.assertIsNone(self.ledger.claim(now=200, max_attempts=2))
        self.assertEqual(102, self.ledger.snapshot()["outbox"][0]["dead_letter_at"])
        self.ledger.replay_dead_letter(job["job_id"], "downstream recovered", now=200)
        replay = self.ledger.claim(now=200, max_attempts=2)
        self.assertEqual((job["job_id"], 1), (replay["job_id"], replay["attempts"]))
        with self.ledger.connection() as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM dead_letter_replays").fetchone()[0])

    def test_final_attempt_crash_transitions_to_dead_letter(self):
        self.ledger.ingest(event(), "key", now=100)
        self.ledger.claim(now=100, lease_seconds=1, max_attempts=1)
        self.assertIsNone(self.ledger.claim(now=101, max_attempts=1))
        self.assertEqual(101, self.ledger.snapshot()["outbox"][0]["dead_letter_at"])

    def test_crash_after_consumer_commit_is_deduplicated(self):
        self.ledger.ingest(event(), "key", now=100)
        inbox = Inbox(Path(self.temp.name) / "inbox.db")
        first = self.ledger.claim(now=100, lease_seconds=1)
        self.assertTrue(inbox.deliver(first, now=100))
        # Simulated crash: deliberately omit acknowledgment.
        second = Ledger(self.path).claim(now=101)
        self.assertFalse(inbox.deliver(second, now=101))
        self.assertTrue(self.ledger.acknowledge(second["job_id"], second["lease_token"], now=101))
        self.assertEqual(1, inbox.count())

    def test_inbox_concurrent_payload_conflict(self):
        inbox = Inbox(Path(self.temp.name) / "inbox.db")
        def deliver(payload):
            try:
                return inbox.deliver({"job_id": "same", "payload_json": payload})
            except ConflictError:
                return "conflict"
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(deliver, ['{"amount":1}', '{"amount":2}']))
        self.assertEqual(1, results.count(True))
        self.assertEqual(1, results.count("conflict"))
        self.assertEqual(1, inbox.count())

    def test_invalid_worker_options_never_create_bad_leases(self):
        for value in (float("nan"), float("inf"), True, -1, 0, 90000):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                self.ledger.claim(lease_seconds=value)
        for value in (float("nan"), True, 1.5, 0, 101):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                self.ledger.claim(max_attempts=value)
        for value in (float("nan"), float("inf"), True, -1):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                self.ledger.ingest(event(), "key", now=value)


if __name__ == "__main__":
    unittest.main()
