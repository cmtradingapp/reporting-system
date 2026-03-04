import os
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.db.postgres_conn import fetch_accounts_stats, fetch_crm_users_stats, fetch_transactions_stats, fetch_sync_log
from datetime import datetime, timezone, timedelta

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

ACCOUNTS_SYNC_INTERVAL_HOURS = int(os.getenv("ACCOUNTS_SYNC_INTERVAL_HOURS", "1"))
ACCOUNTS_SYNC_HOURS = int(os.getenv("ACCOUNTS_SYNC_HOURS", "24"))
USERS_SYNC_INTERVAL_HOURS = int(os.getenv("USERS_SYNC_INTERVAL_HOURS", "1"))
USERS_SYNC_HOURS = int(os.getenv("USERS_SYNC_HOURS", "24"))
TRANSACTIONS_SYNC_INTERVAL_HOURS = int(os.getenv("TRANSACTIONS_SYNC_INTERVAL_HOURS", "1"))
TRANSACTIONS_SYNC_HOURS = int(os.getenv("TRANSACTIONS_SYNC_HOURS", "24"))


def _is_healthy(sync_log: list, interval_hours: int) -> bool:
    if not sync_log or sync_log[0]["status"] != "success":
        return False
    last_ran = datetime.strptime(sync_log[0]["ran_at"], "%Y-%m-%d %H:%M:%S")
    threshold = datetime.utcnow() - timedelta(hours=interval_hours + 1)
    return last_ran >= threshold


@router.get("/data-sync", response_class=HTMLResponse)
async def data_sync_page(request: Request):
    accounts_stats = fetch_accounts_stats()
    accounts_log = fetch_sync_log("crm_accounts", limit=50)

    users_stats = fetch_crm_users_stats()
    users_log = fetch_sync_log("crm_users", limit=50)

    tx_stats = fetch_transactions_stats()
    tx_log = fetch_sync_log("transactions", limit=50)

    tables = [
        {
            "key": "crm_accounts",
            "label": "crm_accounts",
            "last_synced_at": accounts_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Records",      "value": accounts_stats["total_records"],    "color": "text-info",    "icon": "bi-database"},
                {"label": "Funded Accounts",    "value": accounts_stats["funded_accounts"],  "color": "text-success", "icon": "bi-currency-dollar"},
                {"label": "Sales Accounts",     "value": accounts_stats["sales_accounts"],   "color": "text-warning", "icon": "bi-graph-up"},
                {"label": "Retention Accounts", "value": accounts_stats["retention_accounts"],"color": "text-primary","icon": "bi-arrow-repeat"},
            ],
            "sync_log": accounts_log,
            "healthy": _is_healthy(accounts_log, ACCOUNTS_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": ACCOUNTS_SYNC_INTERVAL_HOURS,
            "lookback_hours": ACCOUNTS_SYNC_HOURS,
            "primary_key": "accountid",
            "incremental_columns": "last_update_time, last_communication_time",
            "source": "crmdb.users + joins",
        },
        {
            "key": "crm_users",
            "label": "crm_users",
            "last_synced_at": users_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Records",  "value": users_stats["total_records"],  "color": "text-info",    "icon": "bi-database"},
                {"label": "Active Users",   "value": users_stats["active_users"],   "color": "text-success", "icon": "bi-person-check"},
                {"label": "Unique Desks",   "value": users_stats["unique_desks"],   "color": "text-warning", "icon": "bi-diagram-3"},
                {"label": "Unique Offices", "value": users_stats["unique_offices"], "color": "text-primary", "icon": "bi-building"},
            ],
            "sync_log": users_log,
            "healthy": _is_healthy(users_log, USERS_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": USERS_SYNC_INTERVAL_HOURS,
            "lookback_hours": USERS_SYNC_HOURS,
            "primary_key": "id",
            "incremental_columns": "o.last_update_time, d.last_update_time",
            "source": "UNION of v_ant_operators + desk",
        },
        {
            "key": "transactions",
            "label": "transactions",
            "last_synced_at": tx_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Records",           "value": tx_stats["total_records"], "color": "text-info",    "icon": "bi-database"},
                {"label": "Approved",                "value": tx_stats["approved"],      "color": "text-success", "icon": "bi-check-circle"},
                {"label": "FTDs",                    "value": tx_stats["ftd_count"],     "color": "text-warning", "icon": "bi-star"},
                {"label": "Total USD Volume",        "value": tx_stats["total_usd"],     "color": "text-primary", "icon": "bi-currency-dollar"},
            ],
            "sync_log": tx_log,
            "healthy": _is_healthy(tx_log, TRANSACTIONS_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": TRANSACTIONS_SYNC_INTERVAL_HOURS,
            "lookback_hours": TRANSACTIONS_SYNC_HOURS,
            "primary_key": "mttransactionsid",
            "incremental_columns": "modifiedtime, confirmation_time",
            "source": "crmdb.broker_banking + v_ant_broker_user + autolut",
        },
    ]

    return templates.TemplateResponse("data_sync.html", {
        "request": request,
        "tables": tables,
    })
