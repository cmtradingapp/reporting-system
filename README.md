# reporting-system

Internal reporting & operations platform for CMTrading. Ingests data from CRM
(MySQL replica), Azure MSSQL, and Dealio; warehouses it in PostgreSQL with
materialized views; and serves agent-performance, marketing, retention, FSA,
EEZ, and KPI dashboards to internal staff via server-rendered Jinja pages.

## Stack

- Python 3.11, FastAPI
- gunicorn + uvicorn workers (4 workers, zero-downtime SIGHUP reload)
- PostgreSQL warehouse (primary), MySQL replica (CRM source), MSSQL (Azure), Dealio
- APScheduler for ETL / MV refresh / cache warming
- Server-side Jinja templates (no SPA), GZip middleware
- JWT cookie auth, role + `allowed_pages` permissions

## Architecture

```
  MySQL replica  ──┐
  MSSQL Azure  ────┤                 ┌── MV refresh (every 1 min; mv_mt5_resolved hourly)
  Dealio (MT5)   ──┼─► ETL jobs ───►│
                   │  (APScheduler)  │   PostgreSQL warehouse
                   └─────────────────┘  ├─ raw tables (accounts, users, trades, ...)
                                         ├─ materialized views (mv_mt5_resolved ~8.7M rows)
                                         └─ sync_log
                                          │
                            ┌─────────────┘
                            ▼
                   FastAPI route handlers ──► in-process cache (warmed every 1 min)
                            │
                            ▼
                   Jinja templates ──► internal users (role-gated)
```

Only **one** of the gunicorn workers runs APScheduler — guarded by an `fcntl`
advisory lock on `/tmp/reporting_sched.lock` (see [app/main.py](app/main.py)
`_acquire_scheduler_lock`). Other workers serve HTTP only, keeping their DB
pools free. Schema migrations on startup are gated by a Postgres advisory lock
(`pg_try_advisory_lock(987654321)`) so only one worker runs them.

## Project layout

| Path | Purpose |
|---|---|
| [app/main.py](app/main.py) | App factory, scheduler registration, cache warmer, lifespan |
| [app/db/](app/db/) | Connection helpers per backend (postgres / mysql / mssql / dealio) |
| [app/etl/fetch_and_store.py](app/etl/fetch_and_store.py) | All `run_*_etl` functions |
| [app/auth/](app/auth/) | JWT auth, role filters, permission deps |
| [app/cache.py](app/cache.py) | In-process cache + long-lived fallbacks |
| [app/routes/](app/routes/) | One file per page or sync endpoint |
| [app/templates/](app/templates/) | Jinja pages |
| [qa/](qa/) | QA checks framework (`run_qa.py` + `qa_config.yaml`) |
| [scripts/](scripts/) | Operational scripts (backfills, schema setup) |
| [sql/](sql/) | Materialized view DDL |

## Running locally

```bash
cp .env.example .env       # then fill in real credentials
docker compose up --build
# app on http://localhost:8000  (host networking)
```

`GET /` → `302 /performance`. Default admin seeded on first boot
(see [app/main.py](app/main.py) `seed_admin_user`).

## Required env

See [.env.example](.env.example). At a minimum:

- `MYSQL_HOST` / `MYSQL_PORT` / `MYSQL_USER` / `MYSQL_PASSWORD` / `MYSQL_DB`
- `MSSQL_HOST` / `MSSQL_PORT` / `MSSQL_USER` / `MSSQL_PASSWORD` / `MSSQL_DB`
- `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB`

Optional tuning (defaults shown):

- `SYNC_INTERVAL_MINUTES=1` — base poll cadence for incremental ETLs
- `MV_REFRESH_INTERVAL_MINUTES=1` — fast-MV refresh cadence
- `ACCOUNTS_SYNC_HOURS=6` — lookback per accounts sync run
- `USERS_SYNC_HOURS=6`, `TRANSACTIONS_SYNC_HOURS=6`, `DEALIO_USERS_SYNC_HOURS=6`,
  `DEALIO_TRADES_MT5_SYNC_HOURS=6`, `TRADING_ACCOUNTS_SYNC_HOURS=6`
- `DEALIO_DAILY_PROFITS_SYNC_HOURS=48`

## Scheduled jobs (cadence summary)

Registered in [app/main.py](app/main.py) `lifespan`:

| Cadence | Job |
|---|---|
| Every 1 min (offset start) | accounts, users, transactions, targets, trading_accounts, ftd100, dealio_users, dealio_trades_mt5, dealio_daily_profits, bonus_transactions, campaigns, dealio_positions |
| Every 1 min | `mv_refresh` (fast MVs), `cache_warmer` (dashboard / live EEZ / camp_perf / report APIs) |
| Every 1 hour | `mv_mt5_refresh` (refreshes `mv_mt5_resolved`, ~8.7M rows, with `lock_timeout=0` + CONCURRENTLY) |
| Every 6 hours | `client_classification_sync` |
| Daily 02:00 (Europe/Nicosia) | `age_classification_backfill` |
| Daily 00:05 | `daily_equity_zeroed_snapshot` |

Two startup background threads auto-resume the MT5 + MSSQL full syncs if
`sync_log` shows they have not completed yet.

## Deploy

Two paths, both production:

1. **CI deploy** (default on push to `main`) —
   [.github/workflows/deploy.yml](.github/workflows/deploy.yml) SSHs into the
   server, `git pull`, then `docker compose up --build -d`.
2. **Zero-downtime hot reload** — [deploy.sh](deploy.sh) on the server:
   `git pull`, then `docker exec ... sh -c 'kill -HUP 1'`. `app/` is bind-mounted
   read-only into the container, so gunicorn picks up the new code on graceful
   worker reload — no rebuild, no downtime.

Use `deploy.sh` for code-only changes; let CI do full rebuilds when
`requirements.txt` or `Dockerfile` change.

## QA

The [qa/](qa/) framework runs cross-checks against the warehouse and historical
snapshots:

```bash
python run_qa.py
```

Configuration in [qa_config.yaml](qa_config.yaml). Reports land in
[reports/qa/](reports/qa/).

## One-off scripts

The repo root contains a number of `debug_*.py`, `check_*.py`, `compare_*.py`,
`fix_*.py`, and `analysis*.py` scripts — historical investigation artifacts
kept for reference. Production code is under [app/](app/), [scripts/](scripts/),
and [qa/](qa/).
