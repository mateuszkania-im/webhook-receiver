import sqlite3

_V1 = """
BEGIN IMMEDIATE;
CREATE TABLE webhooks (
    id INTEGER PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    process_count INTEGER NOT NULL DEFAULT 0 CHECK (process_count <= 1),
    next_attempt_at INTEGER,
    lease_token TEXT,
    lease_expires_at INTEGER,
    result TEXT,
    last_error TEXT,
    request_id TEXT,
    received_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    CHECK ((status = 'processing') = (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK (status <> 'pending' OR next_attempt_at IS NOT NULL),
    CHECK (status NOT IN ('completed', 'failed') OR finished_at IS NOT NULL)
) STRICT;
CREATE INDEX ix_webhooks_pending_due ON webhooks (next_attempt_at) WHERE status = 'pending';
CREATE INDEX ix_webhooks_processing_lease ON webhooks (lease_expires_at)
    WHERE status = 'processing';
CREATE INDEX ix_webhooks_status_finished ON webhooks (status, finished_at);
CREATE TABLE meta (id INTEGER PRIMARY KEY CHECK (id = 1), probed_at INTEGER NOT NULL) STRICT;
INSERT INTO meta (id, probed_at) VALUES (1, 0);
PRAGMA user_version = 1;
COMMIT;
"""

_MIGRATIONS = (_V1,)
SCHEMA_VERSION = len(_MIGRATIONS)


def migrate(conn: sqlite3.Connection) -> None:
    current = conn.execute("PRAGMA user_version").fetchone()[0]
    if current > SCHEMA_VERSION:
        raise RuntimeError(f"database schema {current} is newer than this build ({SCHEMA_VERSION})")
    for script in _MIGRATIONS[current:]:
        try:
            conn.executescript(script)
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
