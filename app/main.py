from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from app.routes.accounts import router as accounts_router
from app.routes.users_sync import router as users_sync_router
from app.routes.transactions_sync import router as transactions_sync_router
from app.routes.targets_sync import router as targets_sync_router
from app.routes.dealio_mt4trades_sync import router as dealio_mt4trades_sync_router
from app.routes.trading_accounts_sync import router as trading_accounts_sync_router
from app.routes.ftd100_sync import router as ftd100_sync_router
from app.routes.scoreboard import router as scoreboard_router
from app.routes.ftc_date import router as ftc_date_router
from app.routes.agent_bonuses import router as agent_bonuses_router
from app.routes.data_sync import router as data_sync_router
from app.routes.dealio_daily_profit_sync import router as dealio_daily_profit_sync_router
from app.routes.holidays import router as holidays_router
from app.routes.auth import router as auth_router
from app.routes.users_mgmt import router as users_mgmt_router
from app.routes.dashboard import router as dashboard_router
from app.routes.last_sync import router as last_sync_router
from app.db.postgres_conn import ensure_table, ensure_auth_table, seed_admin_user
from app.auth.auth import hash_password
from app.etl.fetch_and_store import run_accounts_etl, run_users_etl, run_transactions_etl, run_targets_etl, run_dealio_mt4trades_etl, run_trading_accounts_etl, run_ftd100_etl, run_dealio_daily_profit_etl
import os
from datetime import datetime, timedelta

SYNC_INTERVAL_MINUTES          = int(os.getenv("SYNC_INTERVAL_MINUTES", "5"))
ACCOUNTS_SYNC_HOURS            = int(os.getenv("ACCOUNTS_SYNC_HOURS", "6"))
USERS_SYNC_HOURS               = int(os.getenv("USERS_SYNC_HOURS", "6"))
TRANSACTIONS_SYNC_HOURS        = int(os.getenv("TRANSACTIONS_SYNC_HOURS", "6"))
DEALIO_SYNC_HOURS              = int(os.getenv("DEALIO_SYNC_HOURS", "6"))
TRADING_ACCOUNTS_SYNC_HOURS    = int(os.getenv("TRADING_ACCOUNTS_SYNC_HOURS", "6"))
DEALIO_DAILY_PROFIT_SYNC_HOURS = int(os.getenv("DEALIO_DAILY_PROFIT_SYNC_HOURS", "48"))

scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_table()
    ensure_auth_table()
    seed_admin_user(hash_password('Admin123!'))
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
        minutes=SYNC_INTERVAL_MINUTES,
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
        run_dealio_mt4trades_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": DEALIO_SYNC_HOURS},
        id="dealio_mt4trades_sync",
        start_date=_base + timedelta(seconds=150),
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
        run_dealio_daily_profit_etl,
        "interval",
        minutes=SYNC_INTERVAL_MINUTES,
        kwargs={"hours": DEALIO_DAILY_PROFIT_SYNC_HOURS},
        id="dealio_daily_profit_sync",
        start_date=_base + timedelta(seconds=210),
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
app.include_router(dealio_mt4trades_sync_router)
app.include_router(trading_accounts_sync_router)
app.include_router(ftd100_sync_router)
app.include_router(scoreboard_router)
app.include_router(ftc_date_router)
app.include_router(agent_bonuses_router)
app.include_router(data_sync_router)
app.include_router(dealio_daily_profit_sync_router)
app.include_router(holidays_router)
app.include_router(auth_router)
app.include_router(users_mgmt_router)
app.include_router(dashboard_router)
app.include_router(last_sync_router)
