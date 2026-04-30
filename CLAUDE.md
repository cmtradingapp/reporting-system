# reporting-system

Internal financial/trading reporting platform for CMTrading. FastAPI app with Jinja2 server-rendered dashboards, multi-DB backend (Postgres data warehouse + MySQL CRM replica + MSSQL + Dealio Postgres), and APScheduler-driven ETL + materialized-view refresh.

## Branch policy

**Always work on `main`. No feature branches.** All work ships directly to main; deploys are zero-downtime via `./deploy.sh` (gunicorn HUP reload). A SessionStart hook in `.claude/settings.json` auto-switches to main if a session starts on another branch.

## Stack

- Python 3.11, FastAPI 0.111, Uvicorn 0.30, Gunicorn 22 (4 workers, uvicorn worker class)
- Postgres (local DW) via `psycopg2` ThreadedConnectionPool
- MySQL CRM replica via `pymysql`, MSSQL via `pymssql`, Dealio PG (SSL cert)
- APScheduler 3.10 for ETL + MV refresh
- Jinja2 + Bootstrap 5 dark theme; no JS framework (inline JS in templates)
- Auth: python-jose JWT + bcrypt

## Entry point + serving

- `app/main.py` — FastAPI instance + APScheduler bootstrap
- Served as `gunicorn app.main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000`
- Scheduler runs in a single worker via file lock at `/tmp/reporting_sched.lock`
- Lifespan: ensures schema, seeds admin, runs one-time MT5/MSSQL backfills if never completed

## Load-bearing files

- `app/main.py` — app + scheduler setup, all job intervals
- `app/db/postgres_conn.py` — pool, schema DDL, MV creation/refresh (~3000+ lines, the heaviest file)
- `app/db/mysql_conn.py`, `app/db/mssql_conn.py` — source connectors (no pooling, retry loops)
- `app/etl/fetch_and_store.py` — every ETL job runner lives here
- `app/auth/role_filters.py` — role-based row filters (admin / cro_sales / cro_retention / retention_* / sales_* / agent / `extra_roles`)
- `app/cache.py` — TTL in-memory cache. **Cache keys must include `extra_roles`** (recent bug — multi-role users got the wrong cached payload)
- `app/routes/` — one file per page (dashboard.py, campaign_performance.py, daily_monthly_performance.py, total_traders.py, scoreboard.py, fsa_report.py, etc.)
- `app/templates/` — Jinja dashboards (Bootstrap 5)
- `sql/setup_materialized_views.sql` — MV definitions
- `deploy.sh` — `git pull origin main` then `kill -HUP 1` to gunicorn master
- `Dockerfile`, `docker-compose.yml` — `app/` is mounted read-only into the container, so HUP picks up new code without rebuild

## Materialized views

In Postgres. All refreshed via APScheduler.

| MV | Refresh cadence | Notes |
|---|---|---|
| `mv_daily_kpis` | every 1 min | agent × date × qual_date — deposits/withdrawals/FTD/FTC |
| `mv_volume_stats` | every 1 min | dealio_trades_mt4 + trading_accounts open volume |
| `mv_sales_bonuses` | every 1 min | derived from ftd100_clients |
| `mv_run_rate` | every 1 min | depends on mv_daily_kpis; aggregated NET + FTD by dept_group |
| `mv_account_stats`, `mv_std_clients` | every 1 min | account aggregates |
| `mv_mt5_resolved` | **hourly** | self-join of dealio_trades_mt5 (8.7M rows). Hourly because no unique index for CONCURRENTLY without it; uses separate advisory lock. |

Never call `refresh_single_mv` from ETL — that was the root cause of MV staleness (see `a4c543f`). Let the scheduler own all refreshes.

## Reports / pages

Performance · Performance CRO (Sales CRO + Retention CRO) · Daily/Monthly Performance (DMP) · Total Traders · Marketing / Campaign Performance (with View By + Sub Group grouping) · Scoreboard · All FTCs · FTC Date · FSA Report · EEZ Comparison · Agent Bonuses · Transaction Report · Admin: Users Management, Data Sync, Holidays.

Page access is governed by `role` + `allowed_pages` overrides on the user. Admin always bypasses `allowed_pages`.

## Conventions

- **Commits**: small, focused, imperative present-tense. Common prefixes: `Fix:`, `Fix <area>:`, `<area>: <change>` (e.g. `Total Traders: replace AVG SCP card with Net Deposits card`).
- **Cache key versioning**: bump the version suffix when output shape changes (`dashboard_v9`, `camp_perf_v14`). Cache keys must include `extra_roles` for any role-filtered endpoint.
- **Test accounts**: cross-checked against MSSQL `ant_acc` and permanently excluded from all data.
- **Bonus transactions**: excluded from deposit calculations across all reports for alignment.
- **Segmentation labels**: `A / B / C / A+ / Unverified` (matches MySQL source). Age-based fallback for Unassigned/Unverified is backfilled nightly at 02:00 UTC.

## Active direction (as of late April 2026)

- KPI-card iteration on Total Traders + Performance CRO (Net Deposits, LTV Traders, LTV Depositors, Daily Traders A/A+)
- LTV 30/60/90/120 + components on the Marketing table
- Multi-role auth: `extra_roles` JSONB, ABJ-NG team roles, page-level `allowed_pages` per user
- Operational hardening: zero-downtime deploys, MV staleness fixes, connection pool leak fixes, age-based classification backfill

## Deploy

```bash
./deploy.sh
# → git pull origin main
# → docker exec <container> sh -c 'kill -HUP 1'
```

No restart, no downtime. The `app/` mount picks up the new code; gunicorn master gracefully recycles workers.

**Deploy is gated by CI.** [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) only runs after [`.github/workflows/ci.yml`](.github/workflows/ci.yml) reports success on the same commit. Red tests = no deploy.

## Tests

Layered pytest suite — see [tests/README.md](tests/README.md).

```bash
make setup        # install dev deps + pre-commit + Playwright Chromium
make test-fast    # unit tier only (sub-15s, what pre-commit runs)
make test         # full pyramid (needs TEST_DATABASE_URL for integration)
make coverage     # opens htmlcov/index.html
make lint         # ruff + mypy
```

Tiers: **unit** (pure logic, no IO) → **contract** (FastAPI routes via httpx, DB mocked) → **integration** (real Postgres in Docker) → **gui** (Playwright). All tests must be independent — `tests/conftest.py` resets `app.cache` around every test and `pytest-randomly` shuffles order to surface hidden coupling.

App import sets `TESTING=1` to short-circuit the lifespan (`app/main.py`): no APScheduler, no DDL, no DB pools when running tests.

When fixing a bug, prefer a generalised invariant test over a one-off pin — see [tests/unit/test_template_state_invariants.py](tests/unit/test_template_state_invariants.py) which catches the entire class of dTradersHQ-style "added to use site but not defaults" bugs across all templates.
