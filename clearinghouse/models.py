"""Strict wire validation. Money is always an integer number of cents."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any


class ValidationError(ValueError):
    """The input violates the public contract."""


class ConflictError(ValueError):
    """A unique identifier was reused with different meaning."""


class NotFoundError(LookupError):
    """A referenced object does not exist."""


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
CURRENCIES = frozenset({"USD", "CAD", "EUR", "GBP"})
MAX_CENTS = 1_000_000_000_000


def identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValidationError(f"{field} must be 1–128 safe identifier characters")
    return value


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def request_hash(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PaymentEvent:
    event_id: str
    payment_id: str
    kind: str
    amount_cents: int
    currency: str
    occurred_at: str

    @classmethod
    def parse(cls, data: Any) -> "PaymentEvent":
        if not isinstance(data, dict):
            raise ValidationError("event must be a JSON object")
        required = {"event_id", "payment_id", "kind", "amount_cents", "currency", "occurred_at"}
        if set(data) != required:
            raise ValidationError(f"expected exactly these fields: {', '.join(sorted(required))}")
        event_id = identifier(data["event_id"], "event_id")
        payment_id = identifier(data["payment_id"], "payment_id")
        if data["kind"] not in ("payment.captured", "payment.refunded"):
            raise ValidationError("kind must be payment.captured or payment.refunded")
        amount = data["amount_cents"]
        if type(amount) is not int or not 0 < amount <= MAX_CENTS:
            raise ValidationError(f"amount_cents must be an integer from 1 to {MAX_CENTS}")
        currency = data["currency"]
        if not isinstance(currency, str) or currency not in CURRENCIES:
            raise ValidationError(f"currency must be one of {', '.join(sorted(CURRENCIES))}")
        timestamp = data["occurred_at"]
        if not isinstance(timestamp, str) or len(timestamp) > 40:
            raise ValidationError("occurred_at must be an ISO 8601 timestamp with timezone")
        try:
            parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("missing timezone")
            timestamp = parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")
        except (ValueError, OverflowError):
            raise ValidationError("occurred_at must be an ISO 8601 timestamp with timezone") from None
        return cls(event_id, payment_id, data["kind"], amount, currency, timestamp)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
