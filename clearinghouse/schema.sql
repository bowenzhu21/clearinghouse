CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('payment.captured', 'payment.refunded')),
    amount_cents INTEGER NOT NULL CHECK(typeof(amount_cents) = 'integer' AND amount_cents > 0),
    currency TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'posted', 'rejected')),
    reason TEXT,
    received_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS events_payment ON events(payment_id, status, occurred_at, event_id);
CREATE UNIQUE INDEX IF NOT EXISTS one_capture_per_payment
    ON events(payment_id) WHERE kind = 'payment.captured' AND status = 'posted';

CREATE TABLE IF NOT EXISTS journals (
    journal_id TEXT PRIMARY KEY,
    event_id TEXT UNIQUE REFERENCES events(event_id),
    currency TEXT NOT NULL,
    reversal_of TEXT UNIQUE REFERENCES journals(journal_id),
    reason TEXT NOT NULL,
    created_at REAL NOT NULL,
    CHECK ((event_id IS NOT NULL AND reversal_of IS NULL)
        OR (event_id IS NULL AND reversal_of IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS journal_lines (
    journal_id TEXT NOT NULL REFERENCES journals(journal_id) DEFERRABLE INITIALLY DEFERRED,
    line_number INTEGER NOT NULL,
    account TEXT NOT NULL CHECK(account IN ('processor_receivable', 'merchant_payable')),
    currency TEXT NOT NULL,
    debit_cents INTEGER NOT NULL CHECK(typeof(debit_cents) = 'integer' AND debit_cents >= 0),
    credit_cents INTEGER NOT NULL CHECK(typeof(credit_cents) = 'integer' AND credit_cents >= 0),
    CHECK ((debit_cents > 0 AND credit_cents = 0) OR (credit_cents > 0 AND debit_cents = 0)),
    PRIMARY KEY(journal_id, line_number)
);

-- Lines are inserted first, with a deferred FK. Inserting the header seals the
-- journal; its trigger enforces balance before it can exist at commit time.
CREATE TRIGGER IF NOT EXISTS balanced_journal BEFORE INSERT ON journals
BEGIN
    SELECT CASE WHEN
        (SELECT COUNT(*) FROM journal_lines WHERE journal_id = NEW.journal_id) < 2
        OR EXISTS (SELECT 1 FROM journal_lines WHERE journal_id = NEW.journal_id AND currency != NEW.currency)
        OR (SELECT SUM(debit_cents - credit_cents) FROM journal_lines WHERE journal_id = NEW.journal_id) != 0
        THEN RAISE(ABORT, 'journal must have at least two balanced lines in one currency') END;
END;
CREATE TRIGGER IF NOT EXISTS no_append_lines BEFORE INSERT ON journal_lines
WHEN EXISTS (SELECT 1 FROM journals WHERE journal_id = NEW.journal_id)
BEGIN SELECT RAISE(ABORT, 'posted journal is immutable'); END;
CREATE TRIGGER IF NOT EXISTS no_update_lines BEFORE UPDATE ON journal_lines
BEGIN SELECT RAISE(ABORT, 'journal lines are immutable'); END;
CREATE TRIGGER IF NOT EXISTS no_delete_lines BEFORE DELETE ON journal_lines
BEGIN SELECT RAISE(ABORT, 'journal lines are immutable'); END;
CREATE TRIGGER IF NOT EXISTS no_update_journal BEFORE UPDATE ON journals
BEGIN SELECT RAISE(ABORT, 'journal is immutable'); END;
CREATE TRIGGER IF NOT EXISTS no_delete_journal BEFORE DELETE ON journals
BEGIN SELECT RAISE(ABORT, 'journal is immutable'); END;

CREATE TABLE IF NOT EXISTS idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    job_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    available_at REAL NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    lease_token TEXT,
    lease_until REAL,
    delivered_at REAL,
    dead_letter_at REAL,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS outbox_ready ON outbox(available_at, lease_until)
    WHERE delivered_at IS NULL AND dead_letter_at IS NULL;

CREATE TABLE IF NOT EXISTS dead_letter_replays (
    replay_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES outbox(job_id),
    reason TEXT NOT NULL,
    replayed_at REAL NOT NULL
);
PRAGMA user_version = 1;
