"""Small local storage benchmark. Results describe this run, not production capacity."""

from pathlib import Path
import math
import platform
import sqlite3
import statistics
import tempfile
import time
from typing import Any

from .ledger import Ledger
from .models import ValidationError


def environment() -> dict[str, str]:
    return {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version, "os": platform.system(), "os_release": platform.release(), "architecture": platform.machine()}


def benchmark(operations: int = 200) -> dict[str, Any]:
    if type(operations) is not int or not 1 <= operations <= 100_000:
        raise ValidationError("operations must be an integer from 1 to 100000")
    with tempfile.TemporaryDirectory(prefix="clearinghouse-bench-") as temp:
        ledger = Ledger(Path(temp) / "benchmark.db")
        latencies = []
        start = time.perf_counter()
        for index in range(operations):
            event = {"event_id": f"bench_{index}", "payment_id": f"payment_{index}", "kind": "payment.captured", "amount_cents": 12345, "currency": "USD", "occurred_at": "2026-01-01T00:00:00Z"}
            before = time.perf_counter()
            ledger.ingest(event, f"bench_key_{index}")
            latencies.append((time.perf_counter() - before) * 1000)
        elapsed = time.perf_counter() - start
        state = ledger.snapshot()
        ordered = sorted(latencies)
        return {"environment": environment(), "operations": operations, "elapsed_seconds": round(elapsed, 6), "operations_per_second": round(operations / elapsed, 2), "p50_ms": round(statistics.median(latencies), 3), "p95_ms": round(ordered[math.ceil(len(ordered) * .95) - 1], 3), "journal_count": len(state["journals"]), "balanced": all(r["difference_cents"] == 0 for r in state["balance"]), "method": "Sequential unique captures; local SQLite WAL, synchronous=FULL, one connection per ingest; includes event, journal, idempotency and outbox commit; no HTTP, delivery, warmup, or network storage."}
