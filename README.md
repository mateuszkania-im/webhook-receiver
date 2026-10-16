# Webhook receiver

An HTTP service that accepts webhooks, stores each one durably in SQLite before
answering, processes it asynchronously exactly once in effect, survives
crashes and restarts without losing accepted events, and lets the sender check
the status of every event by its `idempotency_key`.

Out of scope: exactly-once execution of side effects (the guarantee is
at-least-once execution with exactly-once recorded effect), real business
logic, request signatures, multiple senders, a replay endpoint, horizontal
scaling and ordering between events. These are listed under TODO.

## Quick start

```bash
cp .env.example .env            # sets WEBHOOK_API_KEY=change-me-0123456789
docker compose up --build -d

curl -i http://127.0.0.1:8000/v1/webhooks \
  -H "Authorization: Bearer change-me-0123456789" \
  -H "Content-Type: application/json" \
  -d '{"idempotency_key":"evt-001","event_type":"order.created","payload":{"order_id":"ORD-2024-1234","amount":299.99}}'

curl -s http://127.0.0.1:8000/v1/webhooks/evt-001 \
  -H "Authorization: Bearer change-me-0123456789"
```

Without Docker: `uv sync`, then `WEBHOOK_API_KEY=change-me-0123456789 uv run python -m webhook_receiver`.

## API

`POST /v1/webhooks` with `Content-Type: application/json`:

```json
{"idempotency_key": "evt-001", "event_type": "order.created", "payload": {"order_id": "ORD-2024-1234"}}
```

`idempotency_key` is 1 to 255 characters from `[A-Za-z0-9._:-]`, `event_type`
is dot-separated lowercase segments of `[a-z0-9_]` up to 100 characters, and
`payload` is a JSON object. Other fields are rejected.

| Case | Status | Notes |
| --- | --- | --- |
| New event stored | `202` | `Location: /v1/webhooks/{key}`, body is the status document |
| Same key, same payload | `200` | current status document, `Idempotent-Replayed: true` |
| Same key, different payload | `409` | `idempotency_conflict` |
| Missing or wrong API key | `401` | `unauthorized` |
| Body over 64 KiB | `413` | `payload_too_large`, checked before parsing |
| Not JSON | `415` | `unsupported_media_type` |
| Invalid JSON or schema (`NaN`, `1e999`, deep nesting) | `422` | `validation_failed` with the failing fields |
| Rate limit exceeded | `429` | `rate_limited`, `Retry-After` and `details.retry_after_s` |
| Backlog full | `503` | `backlog_full`, `Retry-After` and `details.retry_after_s`; duplicates still get `200` |

`GET /v1/webhooks/{key}` returns the status document or `404 not_found`:

```json
{
  "idempotency_key": "evt-001",
  "event_type": "order.created",
  "status": "completed",
  "attempts": 1,
  "next_attempt_at": null,
  "last_error": null,
  "result": {"event_type": "order.created", "handled_at": "2026-10-15T12:00:03.512Z"},
  "received_at": "2026-10-15T12:00:00.104Z",
  "started_at": "2026-10-15T12:00:00.120Z",
  "finished_at": "2026-10-15T12:00:03.512Z"
}
```

Every error uses one envelope, and every response carries `X-Request-ID`:

```json
{"error": {"code": "idempotency_conflict", "message": "key evt-001 was already used with a different payload; send a new idempotency_key", "details": {"idempotency_key": "evt-001"}}}
```

## Architecture

```text
 sender                              operator (internal network)
   | POST /v1/webhooks                 | /healthz /readyz /metrics/
   | GET  /v1/webhooks/{key}           |
   v                                   v
+--------------------------------------------------------------+
| FastAPI in one uvicorn process                               |
|   size, content type, API key, schema, global rate limit     |
|   one BEGIN IMMEDIATE: lookup, backlog check, insert, 202    |
+------------------------------+-------------------------------+
                               | one connection on one thread
                               v
+--------------------------------------------------------------+
| SQLite file: WAL, locking_mode=EXCLUSIVE, STRICT tables      |
|   webhooks = idempotency store + durable queue               |
+------------------------------+-------------------------------+
                               ^ claim, renew, complete, retry
                               |
+------------------------------+-------------------------------+
| Worker pool: 100 asyncio tasks, 30 s lease, 10 s heartbeat   |
| Maintenance: retention every 60 s, gauges every 5 s          |
+--------------------------------------------------------------+
```

- **API** validates in a fixed order (size, content type, key, schema, rate
  limit) and then runs lookup, backlog check and insert in one transaction, so
  `202` is sent only after the row is committed. A new row wakes one idle
  worker.
- **Store** is one table that is both the idempotency store and the queue.
  Every SQL call runs on a single dedicated thread with a single connection, so
  transactions never interleave.
- **Worker pool** claims the oldest due row with a random lease token,
  simulates work (sleep 2 to 5 s), renews the lease while working, and writes
  the result only if it still holds the lease.
- **Maintenance** deletes finished rows after the retention period and
  refreshes queue gauges.

## Delivery semantics

Lifecycle: `pending`, `processing`, then `completed` or `failed`. A failed
attempt goes back to `pending` with full-jitter exponential backoff (base 2 s,
factor 2, cap 300 s); after 5 attempts, or on a permanent error, the event is
`failed` and stays visible through `GET` with its `last_error`. A processing
timeout (60 s) counts as a failed attempt.

- **Crash:** a row stays `processing` with a lease. On restart such rows go
  back to `pending` at once, keeping their attempt count: the exclusive lock
  proves no other process can hold them. Nothing is lost because nothing was
  acknowledged before commit.
- **Poison events:** a row whose lease keeps expiring (the payload kills or
  hangs the process) is `failed` once it passes `WEBHOOK_MAX_ATTEMPTS`.
- **Graceful stop (SIGTERM):** in-flight work is cancelled, leases are released,
  the attempt is refunded, and the process exits 0 within 8 s.
- **Duplicates** are answered from the database, also during backpressure, so a
  sender that retries never gets a `503` for an event that is already stored.
- **Same key, new payload** is a `409`: `event_type` and `payload` are
  compared by SHA-256 of canonical JSON (sorted keys, compact, UTF-8; `1` and
  `1.0` differ).

Guarantee: **at-least-once execution, exactly-once recorded effect.** The
result write is fenced by `lease_token`, and the schema enforces
`process_count <= 1`. The honest limit: if the process dies after the
simulated work but before the commit, the work runs again after recovery. A
real handler must therefore be idempotent or pass the lease token downstream.

## Data model

One `STRICT` table `webhooks`: `idempotency_key` (unique), `event_type`,
`payload`, `payload_hash`, `status`, `attempts`, `process_count`,
`next_attempt_at`, `lease_token`, `lease_expires_at`, `result`, `last_error`,
`request_id`, and `received_at`, `started_at`, `finished_at` as epoch
milliseconds. A one-row `meta` table serves the readiness write probe.

Check constraints: `status` is one of the four values; lease columns are set
exactly when the row is `processing`; `pending` rows have `next_attempt_at`;
finished rows have `finished_at`; `process_count <= 1`.

| Index | Used by |
| --- | --- |
| unique `(idempotency_key)` | deduplication and status lookup |
| partial `(next_attempt_at) WHERE status = 'pending'` | claim of due events, lag gauge |
| partial `(lease_expires_at) WHERE status = 'processing'` | claim of expired leases |
| `(status, finished_at)` | status counts and retention deletes |

Every read query, the retention delete and the claim query have an
`EXPLAIN QUERY PLAN` test. The claim query names its two partial indexes with
`INDEXED BY`: without `ANALYZE` statistics SQLite (3.46.1 in the image and
3.53.1 locally) picks `(status, finished_at)` and sorts every pending row.
Claim stays under 5 ms with 100,000 finished rows (`uv run pytest -m slow`).

`locking_mode=EXCLUSIVE` is set before WAL, so a second process opening the
file fails fast instead of risking the WAL-reset corruption bug in SQLite
before 3.51.3 (the image ships 3.46.1).

## Scale

| Quantity | Value | Derivation |
| --- | --- | --- |
| Arrival rate | 16.7 events/s | 1,000 per minute |
| Mean processing time | 3.5 s | uniform 2 to 5 s |
| Events in flight | about 58, worst case 83 | Little's law: 16.7 x 3.5 and 16.7 x 5 |
| Worker pool | 100 | worst case plus headroom |
| Pool throughput | about 28 events/s | 100 / 3.5 |
| Write transactions | about 50/s | intake, claim, complete per event |
| Rows at 7 day retention | about 10 million | 16.7 x 86,400 x 7 |

Measured with `scripts/load.py` (open loop) on an Apple M2 Pro, default
settings:

| Run | Sent | Non-2xx | Intake p50 / p95 / p99 | Final statuses | max process_count |
| --- | --- | --- | --- | --- | --- |
| 16.7/s for 30 s, 20% duplicates | 501 (417 unique) | 0 | 4.4 / 8.1 / 13.0 ms | 417 completed | 1 |
| 33.4/s for 15 s | 501 | 0 | 4.0 / 7.4 / 13.1 ms | 501 completed | 1 |
| 16.7/s for 60 s, 20% duplicates | 1,002 (822 unique) | 0 | 3.4 / 7.1 / 13.9 ms | 822 completed | 1 |
| 33.4/s for 30 s | 1,002 | 0 | 3.2 / 5.9 / 11.7 ms | 1,002 completed | 1 |
| 200/s for 10 s (flood) | 2,000 | 1,401 x `429` | 1.8 / 4.4 / 12.3 ms | 599 completed | 1 |

In the flood the limiter admitted 599 events, one short of the 100 token burst plus 50/s
for 10 s, rejected the rest with `429` and `Retry-After`, and `/readyz` stayed
ready throughout.

The design stops at one writer on one host. The next step is Postgres with
`SELECT ... FOR UPDATE SKIP LOCKED`, so API and workers can scale out.

## Observability

- **Logs:** JSON lines via structlog with `request_id`, `idempotency_key`,
  `webhook_id`, `attempt` and `lease_token` where known. Payloads and the API
  key are never logged, and tracebacks are rendered without frame locals.
- **Metrics** at `/metrics/`: `webhooks_received_total{outcome}`,
  `http_request_duration_seconds{route,method,status}` (unknown methods become
  `OTHER`), `jobs_processed_total{outcome}`, `job_processing_seconds`,
  `job_end_to_end_seconds`, `lease_lost_total`, `worker_errors_total`,
  `queue_depth{status}`, `queue_lag_seconds`, `dead_letter_size`,
  `workers_busy`, `workers_alive`, `retention_deleted_total`.
- **Health:** `/healthz` answers without I/O. `/readyz` checks worker
  heartbeats and writes to `meta` at most once a second (concurrent probes
  share one write), and reports `"database": "error"` without the exception
  text.
- **Example alerts:** `queue_lag_seconds > 60 for 5m`,
  `delta(dead_letter_size[15m]) > 0`, `workers_alive` below `WEBHOOK_WORKERS`
  for 2m,
  `rate(lease_lost_total[5m]) > 0`,
  `rate(webhooks_received_total{outcome="backpressure"}[5m]) > 0`, and
  `/readyz` failing for 1 minute.

`/healthz`, `/readyz` and `/metrics/` are meant for the internal network. The
compose file publishes one port for the demo; in production an ingress or
reverse proxy would expose only `/v1/*`. `/docs`, `/redoc` and
`/openapi.json` are off.

## Configuration

Environment variables, validated at start. Invalid configuration exits with
code 2 and one `location: message` line per error, never the rejected value.

| Variable | Default | Meaning |
| --- | --- | --- |
| `WEBHOOK_HOST`, `WEBHOOK_PORT` | `0.0.0.0`, `8000` | listen address |
| `WEBHOOK_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `WEBHOOK_DB_PATH` | `webhooks.db` | SQLite file (`/data/webhooks.db` in the image) |
| `WEBHOOK_API_KEY` | unset | shared Bearer key for `/v1`, at least 16 characters; unset disables auth |
| `WEBHOOK_WORKERS` | `100` | worker tasks |
| `WEBHOOK_POLL_INTERVAL_S` | `1.0` | idle poll for retries and expired leases |
| `WEBHOOK_PROCESSING_MIN_S`, `WEBHOOK_PROCESSING_MAX_S` | `2.0`, `5.0` | simulated work |
| `WEBHOOK_PROCESSING_TIMEOUT_S` | `60.0` | per attempt |
| `WEBHOOK_SIMULATION_FAULTS` | `false` | honour `payload.simulate` (`fail_attempts`, `permanent`) |
| `WEBHOOK_MAX_ATTEMPTS` | `5` | then `failed` |
| `WEBHOOK_LEASE_S`, `WEBHOOK_HEARTBEAT_S` | `30.0`, `10.0` | lease and renewal |
| `WEBHOOK_BACKOFF_BASE_S`, `WEBHOOK_BACKOFF_CAP_S` | `2.0`, `300.0` | full-jitter backoff, factor 2 |
| `WEBHOOK_MAX_BODY_BYTES` | `65536` | body limit |
| `WEBHOOK_RATE_LIMIT_PER_S`, `WEBHOOK_RATE_LIMIT_BURST` | `50.0`, `100` | global intake token bucket |
| `WEBHOOK_BACKLOG_LIMIT` | `5000` | backpressure, answered with `Retry-After: 5` |
| `WEBHOOK_RETENTION_S` | `604800` | delete finished rows 7 days after `finished_at` |
| `WEBHOOK_RETENTION_INTERVAL_S` | `60.0` | retention sweep, batches of 1,000 |
| `WEBHOOK_READINESS_STALE_S` | `10.0` | heartbeat age that fails `/readyz` |

## Testing

```bash
uv run ruff format --check . && uv run ruff check . && uv run mypy .
uv run pytest --cov --cov-fail-under=90
uv run pytest -m slow     # wall-clock timing checks, excluded by default
uv run python scripts/load.py --api-key change-me-0123456789 --rate 16.7 --duration 300
```

- `tests/unit`: hashing, backoff, token bucket, settings, auth, error envelope,
  logging, entry point. No I/O.
- `tests/integration`: real SQLite and the ASGI app: every response code,
  lease fencing, retries, shutdown release, retention, query plans, claim
  under 5 ms with 100,000 rows.
- `tests/e2e`: a real uvicorn process: duplicates during and after processing,
  200 parallel requests over 20 keys, SIGKILL during processing, SIGTERM,
  retries, backpressure, retention, readiness and metrics.

## Tradeoffs

| Chose | Gave up | Why |
| --- | --- | --- |
| One SQLite table as queue and idempotency store | a separate broker | no dual write, no window between ack and enqueue |
| One process, one connection | horizontal scaling | SQLite has one writer; no lock contention, WAL bug cannot trigger |
| In-process asyncio workers | separate worker scaling | simulated work is I/O bound; 100 tasks cover the load |
| Polling plus wake-one notify | push from the database | simple, and a commit wakes exactly one idle worker |
| Leases with fencing | exactly-once execution | impossible over a network; recorded effect is exactly once |
| `synchronous=NORMAL` | durability on power loss | crash safe in WAL mode; `FULL` is a one-line change |
| One shared API key | per-sender keys and signatures | the brief has one client; see TODO |
| In-memory global token bucket | per-sender fairness, shared limits | one process, one sender |
| Single 7 day retention | early payload purge | covers sender retry windows (Stripe: 3 days) with one rule |
| No ordering guarantee | per-entity ordering | events for one order may complete out of order |

## Development

Rules the code keeps, worth keeping in any change:

- All SQL lives in `src/webhook_receiver/store/` and runs through
  `Database.run` or `Database.transaction` on the single database thread.
- Every write to a claimed row is guarded by `lease_token`.
- Times are integer epoch milliseconds from an injected `Clock`.
- HTTP errors are `ApiError` with a stable `code`; payloads and the API key are
  never logged.

The gates are the commands under Testing; all of them pass on `main`.

## How AI was used

The service was built with AI agents on purpose. The interesting part is not
that AI wrote code, but how the work was steered and checked.

1. **Design before code.** A specification first: goals, non-goals, response
   codes and invariants such as "202 only after COMMIT" and "every write to a
   claimed row checks the lease token". It went through three revisions, one of
   them after query plans showed which indexes the claim and retention need.
   Then a test-first plan of 13 tasks; the first draft had 16 tasks and was cut
   before any code existed. Neither is kept in the repository, so this README
   describes the system rather than how it was made.
2. **An agent implements, tests gate.** Each task started with a failing test.
   Nothing was committed without `ruff format`, `ruff check`, `mypy --strict`
   and `pytest` with a 90% coverage floor. One commit per task.
3. **Fresh reviewers.** Three independent reviews by agents that saw only the
   repository and the brief, never the build conversation, so they did not share
   the author's blind spots. They found 14 defects. Every one was in failure
   handling, none in the happy path: a lone surrogate in a payload became a 500,
   a failed COMMIT left the only connection stuck in a transaction, a poison
   event could loop forever, rows left by a crash waited 30 s for their lease,
   and 50 parallel readiness probes caused 50 writes. Each fix began with a test
   that reproduced the defect and failed.
4. **Verified by running, not by trusting.** Query plans on two SQLite
   versions, SIGKILL and SIGTERM against a real process, `docker kill` and
   restart, the load test at 1x and 2x the target rate and a 200/s flood. Every
   number in this README comes from a run.
5. **Saying no to the AI as well.** Agents add things just in case: five
   settings nobody changes, an unused `updated_at` column, tests that only
   asserted defaults, and thousands of lines of process documents. All of it
   was removed. Some review suggestions were declined with a reason (three
   `json.dumps` calls per request do three different jobs). A full TypeScript
   port was considered and rejected because it would add nothing a reviewer
   needs.

The pattern: AI is fast and reliable on the happy path and weak on what no test
shows. Independent reviews and tests that really kill the process are what
closed that gap.

## TODO

Cut to keep the prototype small, each with why it would matter:

- [ ] HMAC signatures (Standard Webhooks): prove the sender and the body, not only the key holder.
- [ ] Multiple sources with their own keys and per-source key scoping: real deployments have many senders.
- [ ] Replay endpoint and a paginated listing of failed events: operators need to retry the dead letter without SQL.
- [ ] Attempt history table: per-attempt errors and timings for debugging.
- [ ] Per-source rate limits and a separate read limit: one noisy sender or poller should not starve the rest.
- [ ] Two-stage retention (purge payloads after 72 h, delete rows after 7 days): keep personal data shorter than dedup keys.
- [ ] Opt-in API docs setting: useful in development, off in production.
- [ ] Secrets from a file (`*_FILE` settings): keeps keys out of the environment.
- [ ] Docker smoke test in the test suite and GitHub Actions CI: the container is checked by hand today.
- [ ] Per-file coverage gate and `pip-audit`: catch untested modules and vulnerable dependencies.
- [ ] Move the test client to `httpx2`: newer Starlette warns that `httpx` support in `TestClient` is deprecated.

Ideas beyond the brief:

- [ ] Postgres backend with `SKIP LOCKED` and a separate worker process.
- [ ] Leases on a monotonic clock: a wall-clock jump (seen on Docker Desktop) expires leases early and the simulated work runs again; fencing still keeps the recorded effect single.
- [ ] Bearer token on `/metrics/` where the internal network is not trusted.
- [ ] `Retry-After` derived from the measured drain rate instead of a constant.
- [ ] Optional ordering per entity (for example per `order_id`).
- [ ] Batch claims for higher throughput.
- [ ] OpenTelemetry traces linking intake, claim and completion.
- [ ] Property-based tests for the canonical hash.
- [ ] Encryption of payloads at rest and backups with Litestream once the image ships SQLite 3.51.3 or later.
