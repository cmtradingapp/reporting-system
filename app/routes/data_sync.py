import os
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.db.postgres_conn import fetch_accounts_stats, fetch_sync_log
from app.etl.fetch_and_store import run_accounts_etl
from datetime import datetime, timezone, timedelta

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

ACCOUNTS_SYNC_INTERVAL_HOURS = int(os.getenv("ACCOUNTS_SYNC_INTERVAL_HOURS", "1"))
ACCOUNTS_SYNC_HOURS = int(os.getenv("ACCOUNTS_SYNC_HOURS", "24"))


@router.get("/data-sync", response_class=HTMLResponse)
async def data_sync_page(request: Request):
    stats = fetch_accounts_stats()
    sync_log = fetch_sync_log("crm_accounts", limit=50)

    healthy = False
    if sync_log and sync_log[0]["status"] == "success":
        last_ran = datetime.strptime(sync_log[0]["ran_at"], "%Y-%m-%d %H:%M:%S")
        threshold = datetime.utcnow() - timedelta(hours=ACCOUNTS_SYNC_INTERVAL_HOURS + 1)
        healthy = last_ran >= threshold

    tables = [
        {
            "key": "crm_accounts",
            "label": "crm_accounts",
            "stats": stats,
            "sync_log": sync_log,
            "healthy": healthy,
            "sync_interval_hours": ACCOUNTS_SYNC_INTERVAL_HOURS,
            "lookback_hours": ACCOUNTS_SYNC_HOURS,
            "primary_key": "accountid",
            "incremental_columns": "last_update_time, last_communication_time",
        }
    ]

    return templates.TemplateResponse("data_sync.html", {
        "request": request,
        "tables": tables,
    })
