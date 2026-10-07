"""Reproducible synthetic scenario with an actual terminated child process."""

from datetime import datetime, timezone
from pathlib import Path
import json
import subprocess
import sys
from typing import Any
from uuid import uuid4

from .benchmark import benchmark
from .ledger import Inbox, Ledger
from .models import ConflictError
from .reconcile import reconcile
from .report import render_report


def run_demo(output: str | Path = "demo-output", benchmark_operations: int = 200) -> dict[str, Any]:
    output = Path(output)
    run_dir = output / f"run-{uuid4().hex[:12]}"
    run_dir.mkdir(parents=True)
    ledger_path = run_dir / "ledger.db"
    ledger = Ledger(ledger_path)
    inbox = Inbox(run_dir / "inbox.db")
    checks = []

    def check(condition: bool, name: str, detail: str) -> None:
        if not condition:
            raise AssertionError(f"demo failed: {name}")
        checks.append({"name": name, "detail": detail, "passed": True})

    def event(event_id: str, payment_id: str, amount: int, currency: str = "USD", kind: str = "payment.captured") -> dict[str, Any]:
        return {"event_id": event_id, "payment_id": payment_id, "kind": kind, "amount_cents": amount, "currency": currency, "occurred_at": "2026-01-01T12:00:00Z"}

    refund = event("evt_refund_alpha", "pay_alpha", 2500, kind="payment.refunded")
    check(ledger.ingest(refund, "refund_alpha", now=1000)["status"] == "pending", "Out-of-order refund is parked", "A $25 refund arrived before its $100 capture. No journal was created until the dependency arrived.")
    capture = event("evt_capture_alpha", "pay_alpha", 10000)
    # This child really exits without cleanup immediately after ingest returns.
    code = "import json,os,sys; from clearinghouse.ledger import Ledger; Ledger(sys.argv[1]).ingest(json.loads(sys.argv[2]),'capture_alpha',now=1001); os._exit(23)"
    child = subprocess.run([sys.executable, "-c", code, str(ledger_path.resolve()), json.dumps(capture)], capture_output=True, text=True)
    if child.returncode != 23:
        raise RuntimeError(f"crash subprocess failed unexpectedly: {child.stderr}")
    ledger = Ledger(ledger_path)
    check(ledger.event("evt_capture_alpha")["status"] == "posted" and ledger.event("evt_refund_alpha")["status"] == "posted" and len(ledger.snapshot()["outbox"]) == 2, "Crash after commit loses no delivery", "A child process exited with code 23 after the database commit, before delivery. Restart found both journals and both queued messages.")
    first = ledger.ingest(capture, "capture_alpha", now=1002)
    again = ledger.ingest(capture, "another_provider_retry", now=1002)
    try:
        ledger.ingest({**capture, "amount_cents": 12000}, "capture_alpha", now=1002)
    except ConflictError:
        conflict_rejected = True
    else:
        conflict_rejected = False
    check(first == again and conflict_rejected and len(ledger.snapshot()["journals"]) == 2, "Duplicate and conflicting requests are safe", "Replaying with the same or a new request key produces one journal per provider event. Reusing the key with a changed amount is rejected.")
    for index, (name, amount, currency) in enumerate((("beta", 5000, "USD"), ("gamma", 3000, "USD"), ("delta", 2000, "CAD"), ("epsilon", 4000, "EUR"), ("zeta", 900, "GBP")), 1003):
        ledger.ingest(event(f"evt_capture_{name}", f"pay_{name}", amount, currency), f"capture_{name}", now=index)
    corrected = ledger.ingest(event("evt_capture_correction", "pay_correction", 1000), "capture_correction", now=1008)
    ledger.reverse(corrected["journal_id"], "Synthetic example: reverse an accounting posting without issuing a provider refund", "reverse_correction", now=1008)
    bad_refund = ledger.ingest(event("evt_refund_excess", "pay_alpha", 8000, kind="payment.refunded"), "refund_excess", now=1009)
    ledger.ingest(event("evt_refund_waiting", "pay_waiting", 700, kind="payment.refunded"), "refund_waiting", now=1010)
    check(bad_refund["reason"] == "refund_exceeds_capture" and ledger.payment("pay_alpha")["net_cents"] == 7500, "An excessive refund cannot overdraw", "The second refund would bring refunds above the captured amount. It was durably rejected; the provider net remains $75.")

    job = ledger.claim(now=2000, lease_seconds=2, max_attempts=2)
    assert job is not None
    inbox.deliver(job, now=2000)
    # Simulate the separate crash window after the consumer commit but before ack.
    ledger = Ledger(ledger_path)
    retry = ledger.claim(now=2002, lease_seconds=30, max_attempts=2)
    assert retry is not None
    duplicate_delivery = not inbox.deliver(retry, now=2002)
    acknowledged = ledger.acknowledge(retry["job_id"], retry["lease_token"], now=2002)
    check(job["job_id"] == retry["job_id"] and duplicate_delivery and acknowledged and inbox.count() == 1, "Consumer commit survives an acknowledgment loss", "A committed receipt was delivered again after its lease expired. The consumer's durable inbox suppressed the duplicate side effect.")

    poison = ledger.claim(now=2002, max_attempts=2)
    assert poison is not None
    ledger.fail(poison["job_id"], poison["lease_token"], "Injected downstream outage", now=2002, max_attempts=2)
    while ready := ledger.claim(now=2002, max_attempts=2):
        inbox.deliver(ready, now=2002)
        ledger.acknowledge(ready["job_id"], ready["lease_token"], now=2002)
    retry = ledger.claim(now=2004, max_attempts=2)
    assert retry is not None and retry["job_id"] == poison["job_id"]
    ledger.fail(retry["job_id"], retry["lease_token"], "Injected repeated outage", now=2004, max_attempts=2)
    dead_count = sum(r["dead_letter_at"] is not None for r in ledger.snapshot()["outbox"])
    ledger.replay_dead_letter(poison["job_id"], "Synthetic downstream recovered; operator replay", now=2010)
    replay = ledger.claim(now=2010, max_attempts=2)
    assert replay is not None
    inbox.deliver(replay, now=2010)
    ledger.acknowledge(replay["job_id"], replay["lease_token"], now=2010)
    check(dead_count == 1 and all(r["delivered_at"] is not None for r in ledger.snapshot()["outbox"]), "Retries end in an audited recovery", "One message failed twice, entered the dead-letter queue, and was explicitly replayed with a reason after recovery. All messages now have receipts.")

    csv_text = """settlement_id,event_id,amount_cents,currency
st_01,evt_capture_alpha,10000,USD
st_02,evt_refund_alpha,-2500,USD
st_03,evt_capture_beta,5000,USD
st_04,evt_capture_beta,5000,USD
st_05,evt_capture_gamma,3000,USD
st_06,evt_capture_delta,2100,CAD
st_07,evt_capture_epsilon,4000,USD
st_08,evt_provider_unknown,1200,USD
st_09,evt_capture_correction,1000,USD
"""
    reconciliation = reconcile(csv_text, ledger.expected_settlements())
    check(reconciliation["counts"] == {"exact": 4, "duplicate": 1, "missing": 1, "amount_mismatch": 1, "currency_mismatch": 1, "unexpected": 1}, "Settlement differences are classified exactly", "The provider CSV deliberately contains one duplicate, one missing event, one amount mismatch, one currency mismatch, and one unknown event.")
    state = ledger.snapshot()
    check(all(row["difference_cents"] == 0 for row in state["balance"]) and len(state["journals"]) == 9, "Every currency balances after reversal", "Nine immutable journals, including one accounting reversal, balance across USD, CAD, EUR, and GBP. Provider event history remains intact.")
    report = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "dataset": "synthetic", "checks": checks, "ledger": state, "consumer_receipts": inbox.count(), "reconciliation": reconciliation, "benchmark": benchmark(benchmark_operations)}
    (output / "settlement.csv").write_text(csv_text)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (output / "report.html").write_text(render_report(report))
    (output / "latest-run.txt").write_text(run_dir.name + "\n")
    return report
