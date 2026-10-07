"""Strict CSV reconciliation. A duplicate never quietly becomes an exact match."""

from collections import Counter, defaultdict
import csv
from io import StringIO
import re
from typing import Any

from .models import CURRENCIES, MAX_CENTS, ValidationError, identifier


FIELDS = ["settlement_id", "event_id", "amount_cents", "currency"]


def reconcile(csv_text: str, expected: dict[str, dict[str, Any]]) -> dict[str, Any]:
    reader = csv.DictReader(StringIO(csv_text), strict=True)
    rows = []
    try:
        if reader.fieldnames != FIELDS:
            raise ValidationError(f"CSV header must be {','.join(FIELDS)}")
        for line_number, row in enumerate(reader, start=2):
            if None in row or any(value is None for value in row.values()):
                raise ValidationError(f"CSV line {line_number}: wrong number of columns")
            identifier(row["settlement_id"], "settlement_id")
            identifier(row["event_id"], "event_id")
            if not re.fullmatch(r"-?(0|[1-9][0-9]*)", row["amount_cents"]):
                raise ValidationError(f"CSV line {line_number}: amount_cents must be a signed integer")
            if len(row["amount_cents"].lstrip("-")) > 13:
                raise ValidationError(f"CSV line {line_number}: amount is out of range")
            amount = int(row["amount_cents"])
            if abs(amount) > MAX_CENTS:
                raise ValidationError(f"CSV line {line_number}: amount is out of range")
            if row["currency"] not in CURRENCIES:
                raise ValidationError(f"CSV line {line_number}: unsupported currency")
            rows.append({**row, "amount_cents": amount, "line_number": line_number})
    except csv.Error as exc:
        raise ValidationError(f"malformed CSV: {exc}") from exc
    settlement_counts = Counter(row["settlement_id"] for row in rows)
    event_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        event_rows[row["event_id"]].append(row)
    results = []
    for event_id in sorted(set(expected) | set(event_rows)):
        provider = event_rows[event_id]
        target = expected.get(event_id)
        if not provider:
            category = "missing"
        elif len(provider) > 1 or any(settlement_counts[r["settlement_id"]] > 1 for r in provider):
            category = "duplicate"
        elif target is None:
            category = "unexpected"
        elif provider[0]["currency"] != target["currency"]:
            category = "currency_mismatch"
        elif provider[0]["amount_cents"] != target["amount_cents"]:
            category = "amount_mismatch"
        else:
            category = "exact"
        results.append({"event_id": event_id, "category": category, "expected": target, "provider_rows": provider})
    counts = {category: 0 for category in ("exact", "duplicate", "missing", "amount_mismatch", "currency_mismatch", "unexpected")}
    counts.update(Counter(row["category"] for row in results))
    return {"provider_row_count": len(rows), "expected_event_count": len(expected), "counts": counts, "results": results}
