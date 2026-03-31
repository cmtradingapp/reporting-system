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
from app.routes.live_equity import router as live_equity_router, _live_calc
from app.routes.eez_comparison import router as eez_comparison_router
from app.routes.eez_old import router as eez_old_router
from app.routes.campaigns_sync import router as campaigns_sync_router
from app.routes.campaign_performance import router as campaign_performance_router, _camp_kpi_calc, _camp_table_calc
from app.db.postgres_conn import ensure_table, ensure_auth_table, seed_admin_user, ensure_client_classification_table, ensure_bonus_transactions_table, ensure_daily_equity_zeroed_table, ensure_materialized_views, refresh_materialized_views, backfill_classification_int
import threading
import fcntl
from app.auth.auth import hash_password
from app.etl.fetch_and_store import run_accounts_etl, run_users_etl, run_transactions_etl, run_targets_etl, run_trading_accounts_etl, run_ftd100_etl, run_client_classification_etl, run_dealio_users_etl, run_dealio_trades_mt4_etl, run_dealio_daily_profits_etl, run_bonus_transactions_etl, run_daily_equity_zeroed_snapshot, run_campaigns_etl
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

    _ck = f"live_eez_v23:{today}"
    try:
        cache.set(_ck, _live_calc(today))
    except Exception as e:
        print(f"[warm_cache] live_eez: {e}")

    # KPI cards — no filters, current month
    # Key must match camp_performance.py route: camp_perf_v9:{from}:{to}:{class}:{qfrom}:{qto}:{mkt}:{leg}:{name}:{ch}:{sub}:{aff}:{country}:{office}:{agent}:{team}:{seg}
    _ck = f"camp_perf_v9:{month_start}:{today_iso}:None:None:None:::::::::::None"
    try:
        cache.set(_ck, _camp_kpi_calc(month_start, today_iso))
    except Exception as e:
        print(f"[warm_cache] camp_perf: {e}")

    # Table — no groups, period=day, no filters (most common default view)
    # Key must match camp_performance.py route: camp_tbl_v10:{from}:{to}:{g1}:{g2}:{period}:{mkt}:{leg}:{name}:{ch}:{sub}:{aff}:{class}:{ftc}:{qfrom}:{qto}:{country}:{office}:{agent}:{team}:{seg}
    _ck = f"camp_tbl_v10:{month_start}:{today_iso}:none:none:day:::::::None:None:None:None::::None"
    try:
        cache.set(_ck, _camp_table_calc(month_start, today_iso, period="day"))
    except Exception as e:
        print(f"[warm_cache] camp_tbl: {e}")

    try:
        warm_data_sync_cache()
    except Exception as e:
        print(f"[warm_cache] data_sync: {e}")

SYNC_INTERVAL_MINUTES          = int(os.getenv("SYNC_INTERVAL_MINUTES", "1"))
TRANSACTIONS_SYNC_INTERVAL_MINUTES = int(os.getenv("TRANSACTIONS_SYNC_INTERVAL_MINUTES", "1"))
MV_REFRESH_INTERVAL_MINUTES    = int(os.getenv("MV_REFRESH_INTERVAL_MINUTES", "1"))
ACCOUNTS_SYNC_HOURS            = int(os.getenv("ACCOUNTS_SYNC_HOURS", "6"))
USERS_SYNC_HOURS               = int(os.getenv("USERS_SYNC_HOURS", "6"))
TRANSACTIONS_SYNC_HOURS        = int(os.getenv("TRANSACTIONS_SYNC_HOURS", "6"))
DEALIO_SYNC_HOURS              = int(os.getenv("DEALIO_SYNC_HOURS", "6"))
DEALIO_USERS_SYNC_HOURS        = int(os.getenv("DEALIO_USERS_SYNC_HOURS", "6"))
DEALIO_TRADES_MT4_SYNC_HOURS   = int(os.getenv("DEALIO_TRADES_MT4_SYNC_HOURS", "6"))
TRADING_ACCOUNTS_SYNC_HOURS    = int(os.getenv("TRADING_ACCOUNTS_SYNC_HOURS", "6"))
DEALIO_DAILY_PROFIT_SYNC_HOURS = int(os.getenv("DEALIO_DAILY_PROFIT_SYNC_HOURS", "48"))
DEALIO_DAILY_PROFITS_SYNC_HOURS = int(os.getenv("DEALIO_DAILY_PROFITS_SYNC_HOURS", "48"))

scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_table()
    ensure_auth_table()
    ensure_client_classification_table()
    ensure_bonus_transactions_table()
    ensure_daily_equity_zeroed_table()
    ensure_materialized_views()
    seed_admin_user(hash_password('Admin123!'))
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
    )
    scheduler.add_job(
        run_users_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": USERS_SYNC_HOURS},
        id="users_sync",
        start_date=_base + timedelta(seconds=30),
        replace_existing=True,
    )
    scheduler.add_job(
        run_transactions_etl,
        "interval",
        minutes=TRANSACTIONS_SYNC_INTERVAL_MINUTES,
        kwargs={"hours": TRANSACTIONS_SYNC_HOURS},
        id="transactions_sync",
        start_date=_base + timedelta(seconds=60),
        replace_existing=True,
    )
    scheduler.add_job(
        run_targets_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        id="targets_sync",
        start_date=_base + timedelta(seconds=90),
        replace_existing=True,
    )
    scheduler.add_job(
        run_trading_accounts_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": TRADING_ACCOUNTS_SYNC_HOURS},
        id="trading_accounts_sync",
        start_date=_base + timedelta(seconds=120),
        replace_existing=True,
    )
    scheduler.add_job(
        run_ftd100_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        id="ftd100_sync",
        start_date=_base + timedelta(seconds=180),
        replace_existing=True,
    )
    scheduler.add_job(
        run_client_classification_etl,
        "interval",
        hours=6,
        id="client_classification_sync",
        start_date=_base + timedelta(seconds=240),
        replace_existing=True,
    )
    scheduler.add_job(
        run_dealio_users_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": DEALIO_USERS_SYNC_HOURS},
        id="dealio_users_sync",
        start_date=_base + timedelta(seconds=270),
        replace_existing=True,
    )
    scheduler.add_job(
        run_dealio_trades_mt4_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": DEALIO_TRADES_MT4_SYNC_HOURS},
        id="dealio_trades_mt4_sync",
        start_date=_base + timedelta(seconds=300),
        replace_existing=True,
    )
    scheduler.add_job(
        run_dealio_daily_profits_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": DEALIO_DAILY_PROFITS_SYNC_HOURS},
        id="dealio_daily_profits_sync",
        start_date=_base + timedelta(seconds=330),
        replace_existing=True,
    )
    scheduler.add_job(
        run_bonus_transactions_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": 6},
        id="bonus_transactions_sync",
        start_date=_base + timedelta(seconds=360),
        replace_existing=True,
    )
    scheduler.add_job(
        run_campaigns_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        id="campaigns_sync",
        start_date=_base + timedelta(seconds=390),
        replace_existing=True,
    )
    scheduler.add_job(
        run_daily_equity_zeroed_snapshot,
        "cron",
        hour=0,
        minute=5,
        id="daily_equity_zeroed_snapshot",
        replace_existing=True,
    )
    scheduler.add_job(
        warm_cache,
        "interval",
        minutes=1,
        id="cache_warmer",
        start_date=_base + timedelta(seconds=30),
        replace_existing=True,
    )
    scheduler.add_job(
        refresh_materialized_views,
        "interval",
        minutes=MV_REFRESH_INTERVAL_MINUTES,
        id="mv_refresh",
        start_date=_base + timedelta(seconds=90),
        replace_existing=True,
    )
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="Agent Performance Report", lifespan=lifespan)


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
app.include_router(live_equity_router)
app.include_router(eez_comparison_router)
app.include_router(eez_old_router)
app.include_router(campaigns_sync_router)
app.include_router(campaign_performance_router)
