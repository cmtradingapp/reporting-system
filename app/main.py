from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from app.routes.report import router
from app.routes.users import router as users_router
from app.routes.accounts import router as accounts_router
from app.routes.users_sync import router as users_sync_router
from app.routes.transactions_sync import router as transactions_sync_router
from app.routes.targets_sync import router as targets_sync_router
from app.routes.dealio_mt4trades_sync import router as dealio_mt4trades_sync_router
from app.routes.trading_accounts_sync import router as trading_accounts_sync_router
from app.routes.data_sync import router as data_sync_router
from app.db.postgres_conn import ensure_table
from app.etl.fetch_and_store import run_accounts_etl, run_users_etl, run_transactions_etl, run_targets_etl, run_dealio_mt4trades_etl, run_trading_accounts_etl
import os

ACCOUNTS_SYNC_HOURS = int(os.getenv("ACCOUNTS_SYNC_HOURS", "24"))
ACCOUNTS_SYNC_INTERVAL_HOURS = int(os.getenv("ACCOUNTS_SYNC_INTERVAL_HOURS", "1"))
USERS_SYNC_HOURS = int(os.getenv("USERS_SYNC_HOURS", "24"))
USERS_SYNC_INTERVAL_HOURS = int(os.getenv("USERS_SYNC_INTERVAL_HOURS", "1"))
TRANSACTIONS_SYNC_HOURS = int(os.getenv("TRANSACTIONS_SYNC_HOURS", "24"))
TRANSACTIONS_SYNC_INTERVAL_HOURS = int(os.getenv("TRANSACTIONS_SYNC_INTERVAL_HOURS", "1"))
TARGETS_SYNC_INTERVAL_HOURS = int(os.getenv("TARGETS_SYNC_INTERVAL_HOURS", "1"))
DEALIO_SYNC_HOURS = int(os.getenv("DEALIO_SYNC_HOURS", "24"))
DEALIO_SYNC_INTERVAL_HOURS = int(os.getenv("DEALIO_SYNC_INTERVAL_HOURS", "1"))
TRADING_ACCOUNTS_SYNC_HOURS = int(os.getenv("TRADING_ACCOUNTS_SYNC_HOURS", "24"))
TRADING_ACCOUNTS_SYNC_INTERVAL_HOURS = int(os.getenv("TRADING_ACCOUNTS_SYNC_INTERVAL_HOURS", "1"))

scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_table()
    scheduler.add_job(
        run_accounts_etl,
        "interval",
        hours=ACCOUNTS_SYNC_INTERVAL_HOURS,
        kwargs={"hours": ACCOUNTS_SYNC_HOURS},
        id="accounts_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        run_users_etl,
        "interval",
        hours=USERS_SYNC_INTERVAL_HOURS,
        kwargs={"hours": USERS_SYNC_HOURS},
        id="users_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        run_transactions_etl,
        "interval",
        hours=TRANSACTIONS_SYNC_INTERVAL_HOURS,
        kwargs={"hours": TRANSACTIONS_SYNC_HOURS},
        id="transactions_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        run_targets_etl,
        "interval",
        hours=TARGETS_SYNC_INTERVAL_HOURS,
        id="targets_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        run_trading_accounts_etl,
        "interval",
        hours=TRADING_ACCOUNTS_SYNC_INTERVAL_HOURS,
        kwargs={"hours": TRADING_ACCOUNTS_SYNC_HOURS},
        id="trading_accounts_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        run_dealio_mt4trades_etl,
        "interval",
        hours=DEALIO_SYNC_INTERVAL_HOURS,
        kwargs={"hours": DEALIO_SYNC_HOURS},
        id="dealio_mt4trades_sync",
        replace_existing=True,
    )
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="Agent Performance Report", lifespan=lifespan)

app.include_router(router)
app.include_router(users_router)
app.include_router(accounts_router)
app.include_router(users_sync_router)
app.include_router(transactions_sync_router)
app.include_router(targets_sync_router)
app.include_router(dealio_mt4trades_sync_router)
app.include_router(trading_accounts_sync_router)
app.include_router(data_sync_router)
