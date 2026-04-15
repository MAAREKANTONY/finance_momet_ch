# Jobs robustness and manual cleanup

## Manual cleanup (recommended order)

### 1. Inspect Celery
```bash
docker compose exec celery celery -A stockalert inspect active
docker compose exec celery celery -A stockalert inspect reserved
docker compose exec celery celery -A stockalert inspect scheduled
```

### 2. Revoke one task
```bash
docker compose exec celery celery -A stockalert control revoke <TASK_ID>
docker compose exec celery celery -A stockalert control revoke <TASK_ID> --terminate
docker compose exec celery celery -A stockalert control revoke <TASK_ID> --terminate --signal=SIGKILL
```

### 3. Purge pending queue (dangerous: removes queued tasks)
```bash
docker compose exec celery celery -A stockalert purge -f
```

### 4. Mark stuck jobs in DB
```bash
docker compose exec web python manage.py cleanup_processing_jobs --older-than-minutes 120 --include-pending --status FAILED
```

Or target explicit ids:
```bash
docker compose exec web python manage.py cleanup_processing_jobs --ids 12,13,14 --status FAILED
```

### 5. Restart worker if needed
```bash
docker compose restart celery
```

## New application behavior

- **Cancel** is cooperative: it sets `cancel_requested=True` and lets the task stop at checkpoints.
- **Kill** sets `kill_requested=True` and attempts Celery revoke with `SIGTERM`, then `SIGKILL` fallback.
- Pending jobs are immediately marked `CANCELLED` or `KILLED` in DB.
- Running jobs keep their current status until the worker confirms stop at a checkpoint.

## DB outage handling

Long-running tracked tasks now retry transient DB failures (`OperationalError`, `InterfaceError`) when updating `ProcessingJob` rows. This helps when PostgreSQL is briefly restarting or recovering.

Relevant settings:
- `JOB_DB_RETRY_ATTEMPTS`
- `JOB_DB_RETRY_DELAY_SECONDS`
- `JOB_DB_RETRY_BACKOFF_SECONDS`
- `JOB_TASK_RETRY_COUNTDOWN_SECONDS`
- `JOB_TASK_MAX_RETRIES`

## Stale-job watchdog

The existing periodic task `cleanup_stale_processing_jobs_task` remains active and marks zombie jobs as `FAILED` based on heartbeat / age thresholds.


## Recover stale jobs (iteration 4)

Preferred recovery command:
```bash
docker compose exec web python manage.py recover_jobs
```

Useful variants:
```bash
# Preview only
docker compose exec web python manage.py recover_jobs --dry-run

# Recover only explicit ids
docker compose exec web python manage.py recover_jobs --ids 12,15

# More conservative on pending jobs
docker compose exec web python manage.py recover_jobs --no-pending
```

Recovery rules:
- stale `RUNNING` + `kill_requested=True` => `KILLED`
- stale `RUNNING` + `cancel_requested=True` => `CANCELLED`
- stale `RUNNING` without stop request => `FAILED`
- stale `PENDING` with stop request => `CANCELLED` / `KILLED`
- stale `PENDING` without stop request => `FAILED`

The command also re-syncs related business objects so a Backtest is not left stuck in `PENDING`/`RUNNING` after the job becomes terminal.


## Audit active jobs (iteration 3)

Preferred audit command:
```bash
docker compose exec web python manage.py audit_jobs
```

Useful variants:
```bash
# JSON output for tooling
docker compose exec web python manage.py audit_jobs --json

# Audit only explicit ids
docker compose exec web python manage.py audit_jobs --ids 12,15

# Use a single threshold shortcut
docker compose exec web python manage.py audit_jobs --stale-minutes 30

# Ignore queued jobs
docker compose exec web python manage.py audit_jobs --no-pending
```

What it reports:
- `RUNNING` job with stale heartbeat
- `RUNNING` job without heartbeat for too long
- `RUNNING` job still active after cancel / kill request
- `PENDING` job too old
- `PENDING` job still queued after cancel / kill request

The command is read-only and does not modify the database. Use it before `recover_jobs` when you want a clean diagnosis of the queue state.
