# HTTP API and local workflow

The API accepts synthetic provider-shaped events and exposes their accounting state. It does not initiate real charges or refunds. Start a fresh local database for the examples:

```sh
.venv/bin/clearinghouse serve --db data/api-example.db --host 127.0.0.1 --port 8080
```

## Routes

| Method | Route | Result |
|---|---|---|
| `GET` | `/healthz` | Database connection check |
| `POST` | `/v1/events` | Accept, deduplicate, park, post, or reject an event |
| `GET` | `/v1/events/{event_id}` | Current event status and journal reference |
| `GET` | `/v1/payments/{payment_id}` | Provider capture/refund totals and event history |
| `GET` | `/v1/trial-balance` | Accounting totals by currency and account |
| `POST` | `/v1/journals/{journal_id}/reverse` | Append an offsetting accounting journal |

POST requests require `Content-Type: application/json`, a valid `Content-Length` (curl supplies it), and an `Idempotency-Key`. Bodies are limited to **65,536 bytes**. Chunked transfer encoding, duplicate JSON keys, and unknown fields are rejected. Responses include `Cache-Control: no-store`.

Identifiers and idempotency keys contain 1–128 characters: the first character is ASCII alphanumeric; subsequent characters may also contain `_`, `.`, `:`, or `-`.

## Event contract

Exactly these fields are required:

```json
{
  "event_id": "evt_capture_alpha",
  "payment_id": "pay_alpha",
  "kind": "payment.captured",
  "amount_cents": 10000,
  "currency": "USD",
  "occurred_at": "2026-01-01T12:00:00Z"
}
```

`kind` is `payment.captured` or `payment.refunded`. In both event types, `amount_cents` is a **positive JSON integer** from 1 to 1,000,000,000,000. Strings, booleans, floats, zero, and negative amounts are invalid. Supported currencies are USD, CAD, EUR, and GBP. `occurred_at` must be an ISO 8601 timestamp with a timezone; the service normalizes it to UTC before comparing request identities.

## Exercise an out-of-order refund

First send a $25 refund for a capture the service has not seen:

```sh
curl -i http://127.0.0.1:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: refund-alpha' \
  -d '{"event_id":"evt_refund_alpha","payment_id":"pay_alpha","kind":"payment.refunded","amount_cents":2500,"currency":"USD","occurred_at":"2026-01-01T12:05:00Z"}'
```

It returns HTTP **202** with:

```json
{"event_id":"evt_refund_alpha","payment_id":"pay_alpha","status":"pending","reason":null,"journal_id":null}
```

Now send the $100 capture:

```sh
curl -i http://127.0.0.1:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: capture-alpha' \
  -d '{"event_id":"evt_capture_alpha","payment_id":"pay_alpha","kind":"payment.captured","amount_cents":10000,"currency":"USD","occurred_at":"2026-01-01T12:00:00Z"}'

curl -sS http://127.0.0.1:8080/v1/events/evt_refund_alpha
curl -sS http://127.0.0.1:8080/v1/payments/pay_alpha
curl -sS http://127.0.0.1:8080/v1/trial-balance
```

The capture returns HTTP **200** and its generated journal ID. The refund's current state becomes `posted`, with its own journal ID. Payment totals are `captured_cents: 10000`, `refunded_cents: 2500`, and `net_cents: 7500`. On this fresh example, processor receivable has a 7,500-cent debit balance and merchant payable a 7,500-cent credit balance.

Replaying the original refund POST with `Idempotency-Key: refund-alpha` still returns its **original 202/pending response**. That is intentional response caching. `GET /v1/events/evt_refund_alpha` is the source for current status.

## Duplicate and rejected events

- Same request key and normalized request: return the cached result.
- Same request key with changed content: **409**, no new posting.
- Same provider event ID and content under a new request key: no new posting; return current event state.
- Same provider event ID with changed content: **409**.

A valid event that fails a business rule is recorded durably with `status: rejected` and emits an `event.rejected` outbox message. It receives HTTP **200**, acknowledging that the event was processed; clients must inspect the status. Rejection reasons are `payment_already_captured`, `currency_mismatch`, and `refund_exceeds_capture`. A refund with no capture is pending instead of rejected.

If the connection fails after submitting a POST, retry the **same operation, body, and idempotency key**. The server may already have committed it.

## Accounting reversal

Use the actual `journal_id` returned by an event response in place of `JOURNAL_ID`:

```sh
curl -i http://127.0.0.1:8080/v1/journals/JOURNAL_ID/reverse \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: correction-example-1' \
  -d '{"reason":"Synthetic accounting correction"}'
```

The body contains only `reason`, with 1–500 nonblank characters after trimming. A successful response contains the new `journal_id`, `reversal_of`, and reason. The original journal remains unchanged. Reversing an already reversed journal or reversing a reversal returns **409**. Replaying the original reversal request/key returns its cached success.

This is an **internal book correction**. It does not issue a provider refund or change payment totals and settlement expectations derived from provider events. If a capture already has a refund, reversing its capture journal can make the accounting position differ from the provider net. Record an actual provider refund as a separate `payment.refunded` event.

## Authentication

Loopback use can run without a token. Set `CLEARINGHOUSE_API_TOKEN` before starting the server to require a bearer token on every route, including health checks. Non-loopback binds require it. Tokens must contain at least 24 ASCII characters without whitespace.

For a local example only, set a token in the server terminal:

```sh
export CLEARINGHOUSE_API_TOKEN='local-example-token-replace-before-sharing'
.venv/bin/clearinghouse serve --db data/api-example.db
```

Then use the same value in another terminal:

```sh
export CLEARINGHOUSE_API_TOKEN='local-example-token-replace-before-sharing'
curl -sS http://127.0.0.1:8080/healthz \
  -H "Authorization: Bearer $CLEARINGHOUSE_API_TOKEN"
```

The example token is public documentation, not a deployment credential. This adapter does not implement TLS or provider webhook signatures. Its bearer token is a local access control, not a complete production authentication model.

## Error semantics

| Status | Meaning | Typical response |
|---|---|---|
| `200` | Processed event/read/reversal; an event may be `rejected` | Resource-specific JSON |
| `202` | Event is waiting for its capture | `status: pending` |
| `400` | Invalid request contract | `error: invalid_request`, plus message |
| `401` | Missing or incorrect configured token | `error: unauthorized` |
| `404` | Unknown resource or route | `error: not_found` or `route_not_found` |
| `409` | Conflicting identity or reversal | `error: conflict`, plus message |
| `503` | SQLite operational error | `error: storage_unavailable`; retry with the same key |

The health endpoint verifies a database query succeeds. It does not validate all historical accounting data or attest downstream delivery health.

## Deliver messages and reconcile

The example worker writes receipts to a separate local database:

```sh
.venv/bin/clearinghouse worker --db data/api-example.db --inbox data/api-inbox.db --once
```

`--once` drains jobs currently available and exits. It does not wait for future retry deadlines. Without that flag, the worker polls continuously. The stable outbox `job_id` is the consumer deduplication key.

A dead-letter message can be explicitly requeued using its job ID:

```sh
.venv/bin/clearinghouse replay JOB_ID --db data/api-example.db \
  --reason 'Synthetic downstream recovery verified'
```

For the capture/refund example above, a matching provider CSV is:

```csv
settlement_id,event_id,amount_cents,currency
settlement_capture_alpha,evt_capture_alpha,10000,USD
settlement_refund_alpha,evt_refund_alpha,-2500,USD
```

Save it and compare with the same database:

```sh
.venv/bin/clearinghouse reconcile settlement.csv \
  --db data/api-example.db --output reconciliation.json
```

Refunds are negative in this settlement format, although refund-event amounts are positive. Reconciliation is read-only with respect to accounting; it reports discrepancies and makes no adjustments.
