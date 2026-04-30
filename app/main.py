from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from app.routes.accounts import router as accounts_router
from app.routes.users_sync import router as users_sync_router
from app.routes.transactions_sync import router as transactions_sync_router
from app.routes.targets_sync import router as targets_sync_router
from app.routes.trading_accounts_sync import router as trading_accounts_sync_router
from app.routes.ftd100_sync import router as ftd100_sync_router
from app.routes.scoreboard import router as scoreboard_router
from app.routes.ftc_date import router as ftc_date_router
from app.routes.total_traders import router as total_traders_router
from app.routes.agent_bonuses import router as agent_bonuses_router
from app.routes.data_sync import router as data_sync_router
from app.routes.holidays import router as holidays_router
from app.routes.auth import router as auth_router
from app.routes.users_mgmt import router as users_mgmt_router
from app.routes.dashboard import router as dashboard_router, _dashboard_calc
from app.routes.data_sync import warm_data_sync_cache
from app.routes.last_sync import router as last_sync_router
from app.routes.client_classification_sync import router as client_classification_sync_router
from app.routes.dealio_new_sync import router as dealio_new_sync_router
from app.routes.dealio_daily_profits_sync import router as dealio_daily_profits_sync_router
from app.routes.dealio_mt5trades_sync import router as dealio_mt5trades_sync_router
from app.routes.live_equity import router as live_equity_router, _live_calc
from app.routes.eez_comparison import router as eez_comparison_router
from app.routes.eez_old import router as eez_old_router
from app.routes.campaigns_sync import router as campaigns_sync_router
from app.routes.campaign_performance import router as campaign_performance_router, _camp_kpi_calc, _camp_table_calc
from app.routes.all_ftcs import router as all_ftcs_router
from app.routes.transactions_report import router as transactions_report_router
from app.routes.fsa_report import router as fsa_report_router
from app.routes.mssql_dealio_mt5trades_sync import router as mssql_dealio_mt5trades_sync_router
from app.routes.daily_monthly_performance import router as dmp_router
from app.db.postgres_conn import ensure_table, ensure_auth_table, seed_admin_user, seed_company_targets, ensure_client_classification_table, ensure_bonus_transactions_table, ensure_daily_equity_zeroed_table, ensure_materialized_views, refresh_materialized_views, refresh_mv_mt5_resolved, backfill_classification_int, ensure_agent_dept_history_table, ensure_dealio_positions_table, ensure_mssql_dealio_mt5trades_table, ensure_mv_refresh_log, backfill_age_classification
import threading
import fcntl
from app.auth.auth import hash_password
from app.etl.fetch_and_store import run_accounts_etl, run_users_etl, run_transactions_etl, run_targets_etl, run_trading_accounts_etl, run_ftd100_etl, run_client_classification_etl, run_dealio_users_etl, run_dealio_trades_mt4_etl, run_dealio_daily_profits_etl, run_bonus_transactions_etl, run_daily_equity_zeroed_snapshot, run_campaigns_etl, run_dealio_trades_mt5_etl, run_dealio_trades_mt5_full_etl, run_dealio_positions_etl, run_mssql_dealio_mt5trades_full_etl
from app import cache
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# File lock so only one worker process runs the APScheduler background jobs.
# The other worker(s) only serve HTTP requests, keeping their connection pool free.
_SCHED_LOCK_FILE = "/tmp/reporting_sched.lock"
_sched_lock_fd = None

def _acquire_scheduler_lock() -> bool:
    global _sched_lock_fd
    try:
        _sched_lock_fd = open(_SCHED_LOCK_FILE, 'w')
        fcntl.flock(_sched_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except (IOError, OSError):
        if _sched_lock_fd:
            _sched_lock_fd.close()
        _sched_lock_fd = None
        return False

_TZ = ZoneInfo("Europe/Nicosia")


def warm_cache():
    """Refresh cache for dashboard, live EEZ and campaign performance every minute."""
    today = datetime.now(_TZ).date()
    month_start = today.replace(day=1).isoformat()
    today_iso   = today.isoformat()

    _ck = f"dashboard_v9:{today.isoformat()}"
    try:
        cache.set(_ck, _dashboard_calc(today))
    except Exception as e:
        print(f"[warm_cache] dashboard: {e}")

    _ck = f"live_eez_v24:{today}"
    _last_ck = f"live_eez_last_known_v1:{today}"
    try:
        from datetime import datetime as _dt
        result = _live_calc(today)
        result["computed_at"] = _dt.now(_TZ).isoformat(timespec="seconds")
        result["is_stale"] = False
        cache.set(_ck, result)
        cache.set_long(_last_ck, result, ttl=15 * 60)
    except Exception as e:
        print(f"[warm_cache] live_eez: {e}")

    # KPI cards — no filters, current month
    # Key must match camp_performance.py route: camp_perf_v11:{from}:{to}:{class}:{qfrom}:{qto}:{mkt}:{leg}:{name}:{ch}:{sub}:{aff}:{country}:{office}:{agent}:{team}:{seg}
    _ck = f"camp_perf_v14:{month_start}:{today_iso}:None:None:None:::::::::::None"
    try:
        cache.set(_ck, _camp_kpi_calc(month_start, today_iso))
    except Exception as e:
        print(f"[warm_cache] camp_perf: {e}")

    # Table — no groups, period=day, no filters (most common default view)
    # Key must match camp_performance.py route: camp_tbl_v12:{from}:{to}:{g1}:{g2}:{period}:{mkt}:{leg}:{name}:{ch}:{sub}:{aff}:{class}:{ftc}:{qfrom}:{qto}:{country}:{office}:{agent}:{team}:{seg}
    _ck = f"camp_tbl_v18:{month_start}:{today_iso}:none:none:day:::::::None:None:None:None::::None"
    try:
        cache.set(_ck, _camp_table_calc(month_start, today_iso, period="day"))
    except Exception as e:
        print(f"[warm_cache] camp_tbl: {e}")

    try:
        warm_data_sync_cache()
    except Exception as e:
        print(f"[warm_cache] data_sync: {e}")

    # All report pages — warm admin cache so page loads are instant
    try:
        _warm_report_caches(month_start, today_iso)
    except Exception as e:
        print(f"[warm_cache] reports: {e}")

SYNC_INTERVAL_MINUTES          = int(os.getenv("SYNC_INTERVAL_MINUTES", "1"))
TRANSACTIONS_SYNC_INTERVAL_MINUTES = int(os.getenv("TRANSACTIONS_SYNC_INTERVAL_MINUTES", "1"))
MV_REFRESH_INTERVAL_MINUTES    = int(os.getenv("MV_REFRESH_INTERVAL_MINUTES", "1"))
ACCOUNTS_SYNC_HOURS            = int(os.getenv("ACCOUNTS_SYNC_HOURS", "6"))
USERS_SYNC_HOURS               = int(os.getenv("USERS_SYNC_HOURS", "6"))
TRANSACTIONS_SYNC_HOURS        = int(os.getenv("TRANSACTIONS_SYNC_HOURS", "6"))
DEALIO_SYNC_HOURS              = int(os.getenv("DEALIO_SYNC_HOURS", "6"))
DEALIO_USERS_SYNC_HOURS        = int(os.getenv("DEALIO_USERS_SYNC_HOURS", "6"))
DEALIO_TRADES_MT4_SYNC_HOURS   = int(os.getenv("DEALIO_TRADES_MT4_SYNC_HOURS", "6"))
DEALIO_TRADES_MT5_SYNC_HOURS   = int(os.getenv("DEALIO_TRADES_MT5_SYNC_HOURS", "6"))
TRADING_ACCOUNTS_SYNC_HOURS    = int(os.getenv("TRADING_ACCOUNTS_SYNC_HOURS", "6"))
DEALIO_DAILY_PROFIT_SYNC_HOURS = int(os.getenv("DEALIO_DAILY_PROFIT_SYNC_HOURS", "48"))
DEALIO_DAILY_PROFITS_SYNC_HOURS = int(os.getenv("DEALIO_DAILY_PROFITS_SYNC_HOURS", "48"))

scheduler = BackgroundScheduler()


def _auto_mt5_full_sync():
    """On startup, trigger MT5 full sync if no successful full sync has ever completed."""
    import time
    time.sleep(10)  # let scheduler settle first
    try:
        from app.db.postgres_conn import get_connection
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM sync_log
                    WHERE table_name = 'dealio_trades_mt5'
                      AND status = 'success'
                      AND cutoff_used = '1970-01-01 00:00:00'
                    LIMIT 1
                """)
                already_done = cur.fetchone() is not None
        finally:
            conn.close()
        if not already_done:
            print("[auto_mt5_full_sync] No completed full sync found — starting now.")
            run_dealio_trades_mt5_full_etl()
            print("[auto_mt5_full_sync] Full sync completed.")
        else:
            print("[auto_mt5_full_sync] Full sync already completed — skipping.")
    except Exception as e:
        print(f"[auto_mt5_full_sync] Error: {e}")


def _auto_mssql_dmt5_sync():
    """On startup, auto-resume mssql_dealio_mt5trades sync if not yet completed."""
    import time
    time.sleep(15)
    try:
        from app.db.postgres_conn import get_connection
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM sync_log
                    WHERE table_name = 'mssql_dealio_mt5trades'
                      AND status = 'success'
                    LIMIT 1
                """)
                already_done = cur.fetchone() is not None
        finally:
            conn.close()
        if not already_done:
            print("[auto_mssql_dmt5] No completed sync found — starting/resuming now.")
            run_mssql_dealio_mt5trades_full_etl()
            print("[auto_mssql_dmt5] Full sync completed.")
        else:
            print("[auto_mssql_dmt5] Full sync already completed — skipping.")
    except Exception as e:
        print(f"[auto_mssql_dmt5] Error: {e}")


_TESTING = os.environ.get("TESTING") == "1"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Skip every side effect during tests: no DB connection, no DDL, no APScheduler,
    # no background threads. The FastAPI app object is still importable so contract
    # tests can hit routes via httpx.AsyncClient.
    if _TESTING:
        yield
        return

    # Only one worker runs schema migrations — others skip (idempotent DDL already done)
    from app.db.postgres_conn import get_connection as _pgconn
    _mc = _pgconn()
    try:
        with _mc.cursor() as _c:
            _c.execute("SELECT pg_try_advisory_lock(987654321)")
            _run_setup = _c.fetchone()[0]
        _mc.commit()
    finally:
        _mc.close()
    if _run_setup:
        ensure_table()
        ensure_auth_table()
        ensure_client_classification_table()
        ensure_bonus_transactions_table()
        ensure_daily_equity_zeroed_table()
        ensure_agent_dept_history_table()
        ensure_dealio_positions_table()
        ensure_mssql_dealio_mt5trades_table()
        ensure_mv_refresh_log()
        ensure_materialized_views()
        seed_admin_user(hash_password('Admin123!'))
        seed_company_targets()
        threading.Thread(target=backfill_classification_int, daemon=True).start()
    _run_scheduler = _acquire_scheduler_lock()
    if not _run_scheduler:
        # Another worker already holds the scheduler lock — this worker only serves requests.
        yield
        return
    _base = datetime.utcnow()
    scheduler.add_job(
        run_accounts_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": ACCOUNTS_SYNC_HOURS},
        id="accounts_sync",
        start_date=_base + timedelta(seconds=0),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_users_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": USERS_SYNC_HOURS},
        id="users_sync",
        start_date=_base + timedelta(seconds=30),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_transactions_etl,
        "interval",
        minutes=TRANSACTIONS_SYNC_INTERVAL_MINUTES,
        kwargs={"hours": TRANSACTIONS_SYNC_HOURS},
        id="transactions_sync",
        start_date=_base + timedelta(seconds=60),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_targets_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        id="targets_sync",
        start_date=_base + timedelta(seconds=90),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_trading_accounts_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": TRADING_ACCOUNTS_SYNC_HOURS},
        id="trading_accounts_sync",
        start_date=_base + timedelta(seconds=120),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_ftd100_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        id="ftd100_sync",
        start_date=_base + timedelta(seconds=180),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_client_classification_etl,
        "interval",
        hours=6,
        id="client_classification_sync",
        start_date=_base + timedelta(seconds=240),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_dealio_users_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": DEALIO_USERS_SYNC_HOURS},
        id="dealio_users_sync",
        start_date=_base + timedelta(seconds=270),
        replace_existing=True,
        max_instances=1,
    )
    # MT4 trades sync disabled — not needed
    # scheduler.add_job(
    #     run_dealio_trades_mt4_etl,
    #     "interval",
    #     minutes=SYNC_INTERVAL_MINUTES,
    #     kwargs={"hours": DEALIO_TRADES_MT4_SYNC_HOURS},
    #     id="dealio_trades_mt4_sync",
    #     start_date=_base + timedelta(seconds=300),
    #     replace_existing=True,
    #     max_instances=1,
    # )
    scheduler.add_job(
        run_dealio_trades_mt5_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": DEALIO_TRADES_MT5_SYNC_HOURS},
        id="dealio_trades_mt5_sync",
        start_date=_base + timedelta(seconds=360),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_dealio_daily_profits_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": DEALIO_DAILY_PROFITS_SYNC_HOURS},
        id="dealio_daily_profits_sync",
        start_date=_base + timedelta(seconds=330),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_bonus_transactions_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": 6},
        id="bonus_transactions_sync",
        start_date=_base + timedelta(seconds=360),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_campaigns_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        id="campaigns_sync",
        start_date=_base + timedelta(seconds=390),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_dealio_positions_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        id="dealio_positions_sync",
        start_date=_base + timedelta(seconds=420),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        run_daily_equity_zeroed_snapshot,
        "cron",
        hour=0,
        minute=5,
        id="daily_equity_zeroed_snapshot",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        warm_cache,
        "interval",
        minutes=1,
        id="cache_warmer",
        start_date=_base + timedelta(seconds=30),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        refresh_materialized_views,
        "interval",
        minutes=MV_REFRESH_INTERVAL_MINUTES,
        id="mv_refresh",
        start_date=_base + timedelta(seconds=90),
        replace_existing=True,
        max_instances=1,
    )
    # mv_mt5_resolved refreshed hourly on its own schedule — too large (8.7M rows)
    # to include in the per-minute cycle without blocking all other MVs
    scheduler.add_job(
        refresh_mv_mt5_resolved,
        "interval",
        hours=1,
        id="mv_mt5_refresh",
        start_date=_base + timedelta(seconds=120),
        replace_existing=True,
        max_instances=1,
    )
    # Nightly age-based classification backfill — runs at 02:00 server time
    scheduler.add_job(
        backfill_age_classification,
        "cron",
        hour=2, minute=0,
        id="age_classification_backfill",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.start()
    threading.Thread(target=_auto_mt5_full_sync, daemon=True).start()
    threading.Thread(target=_auto_mssql_dmt5_sync, daemon=True).start()
    yield
    scheduler.shutdown()


def _warm_report_caches(date_from: str, date_to: str):
    """Pre-warm all report API caches for admin user via local HTTP calls."""
    import urllib.request
    from app.auth.auth import create_access_token
    token = create_access_token(1)  # admin user id=1
    endpoints = [
        f"/api/performance?date_from={date_from}&date_to={date_to}",
        f"/api/performance/retention?date_from={date_from}&date_to={date_to}",
        f"/api/agent-bonuses/sales?date_from={date_from}&date_to={date_to}",
        f"/api/agent-bonuses/retention?date_from={date_from}&date_to={date_to}",
        f"/api/all-ftcs?date_from={date_from}&date_to={date_to}",
        "/api/eez-comparison",
        f"/api/total-traders?date_from={date_from}&date_to={date_to}&ftc_groups=0+-+7+days%2C8+-+14+days%2C15+-+30+days%2C31+-+60+days%2C61+-+90+days%2C91+-+120+days%2C120%2B+days",
        f"/api/ftc-date?end_date={date_to}",
        f"/api/daily-monthly/sales?date_from={date_from}&date_to={date_to}",
        f"/api/daily-monthly/retention?date_from={date_from}&date_to={date_to}",
    ]
    for ep in endpoints:
        url = f"http://127.0.0.1:8000{ep}"
        req = urllib.request.Request(url)
        req.add_header("Cookie", f"access_token={token}")
        try:
            urllib.request.urlopen(req, timeout=60)
        except Exception as e:
            print(f"[warm_cache] {ep.split('?')[0]}: {e}")


app = FastAPI(title="Agent Performance Report", lifespan=lifespan)

from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import Response
import pathlib
app.add_middleware(GZipMiddleware, minimum_size=500)

_STATIC_DIR = pathlib.Path(__file__).parent / "static"
_STATIC_TYPES = {".svg": "image/svg+xml", ".png": "image/png", ".ico": "image/x-icon", ".webp": "image/webp"}

@app.get("/static/{filename}")
async def static_file(filename: str):
    path = _STATIC_DIR / filename
    if not path.exists() or not path.is_file():
        return Response(status_code=404)
    return Response(content=path.read_bytes(), media_type=_STATIC_TYPES.get(path.suffix.lower(), "application/octet-stream"))


@app.get("/")
async def root(request: Request):
    return RedirectResponse(url="/performance", status_code=302)


app.include_router(accounts_router)
app.include_router(users_sync_router)
app.include_router(transactions_sync_router)
app.include_router(targets_sync_router)
app.include_router(trading_accounts_sync_router)
app.include_router(ftd100_sync_router)
app.include_router(scoreboard_router)
app.include_router(ftc_date_router)
app.include_router(total_traders_router)
app.include_router(agent_bonuses_router)
app.include_router(data_sync_router)
app.include_router(holidays_router)
app.include_router(auth_router)
app.include_router(users_mgmt_router)
app.include_router(dashboard_router)
app.include_router(last_sync_router)
app.include_router(client_classification_sync_router)
app.include_router(dealio_new_sync_router)
app.include_router(dealio_daily_profits_sync_router)
app.include_router(dealio_mt5trades_sync_router)
app.include_router(live_equity_router)
app.include_router(eez_comparison_router)
app.include_router(eez_old_router)
app.include_router(campaigns_sync_router)
app.include_router(campaign_performance_router)
app.include_router(all_ftcs_router)
app.include_router(transactions_report_router)
app.include_router(fsa_report_router)
app.include_router(mssql_dealio_mt5trades_sync_router)
app.include_router(dmp_router)
