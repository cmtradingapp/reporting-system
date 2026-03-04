from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from app.routes.report import router
from app.routes.users import router as users_router
from app.routes.accounts import router as accounts_router
from app.db.postgres_conn import ensure_table
from app.etl.fetch_and_store import run_accounts_etl
import os

ACCOUNTS_SYNC_HOURS = int(os.getenv("ACCOUNTS_SYNC_HOURS", "24"))
ACCOUNTS_SYNC_INTERVAL_HOURS = int(os.getenv("ACCOUNTS_SYNC_INTERVAL_HOURS", "1"))

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
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="Agent Performance Report", lifespan=lifespan)

app.include_router(router)
app.include_router(users_router)
app.include_router(accounts_router)
