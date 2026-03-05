import os
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.db.postgres_conn import fetch_accounts_stats, fetch_crm_users_stats, fetch_transactions_stats, fetch_targets_stats, fetch_dealio_mt4trades_stats, fetch_trading_accounts_stats, fetch_sync_log
from datetime import datetime, timezone, timedelta

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

ACCOUNTS_SYNC_INTERVAL_HOURS = int(os.getenv("ACCOUNTS_SYNC_INTERVAL_HOURS", "1"))
ACCOUNTS_SYNC_HOURS = int(os.getenv("ACCOUNTS_SYNC_HOURS", "24"))
USERS_SYNC_INTERVAL_HOURS = int(os.getenv("USERS_SYNC_INTERVAL_HOURS", "1"))
USERS_SYNC_HOURS = int(os.getenv("USERS_SYNC_HOURS", "24"))
TRANSACTIONS_SYNC_INTERVAL_HOURS = int(os.getenv("TRANSACTIONS_SYNC_INTERVAL_HOURS", "1"))
TRANSACTIONS_SYNC_HOURS = int(os.getenv("TRANSACTIONS_SYNC_HOURS", "24"))
TARGETS_SYNC_INTERVAL_HOURS = int(os.getenv("TARGETS_SYNC_INTERVAL_HOURS", "1"))
DEALIO_SYNC_INTERVAL_HOURS = int(os.getenv("DEALIO_SYNC_INTERVAL_HOURS", "1"))
DEALIO_SYNC_HOURS = int(os.getenv("DEALIO_SYNC_HOURS", "24"))
TRADING_ACCOUNTS_SYNC_INTERVAL_HOURS = int(os.getenv("TRADING_ACCOUNTS_SYNC_INTERVAL_HOURS", "1"))
TRADING_ACCOUNTS_SYNC_HOURS = int(os.getenv("TRADING_ACCOUNTS_SYNC_HOURS", "24"))


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

    targets_stats = fetch_targets_stats()
    targets_log = fetch_sync_log("targets", limit=50)

    dealio_stats = fetch_dealio_mt4trades_stats()
    dealio_log = fetch_sync_log("dealio_mt4trades", limit=50)

    ta_stats = fetch_trading_accounts_stats()
    ta_log = fetch_sync_log("trading_accounts", limit=50)

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
        {
            "key": "trading_accounts",
            "label": "trading_accounts",
            "last_synced_at": ta_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Accounts",    "value": ta_stats["total_records"],    "color": "text-info",    "icon": "bi-database"},
                {"label": "Enabled Accounts",  "value": ta_stats["enabled_accounts"], "color": "text-success", "icon": "bi-person-check"},
                {"label": "Total Balance",     "value": ta_stats["total_balance"],    "color": "text-warning", "icon": "bi-wallet2"},
                {"label": "Total Equity",      "value": ta_stats["total_equity"],     "color": "text-primary", "icon": "bi-graph-up"},
            ],
            "sync_log": ta_log,
            "healthy": _is_healthy(ta_log, TRADING_ACCOUNTS_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": TRADING_ACCOUNTS_SYNC_INTERVAL_HOURS,
            "lookback_hours": TRADING_ACCOUNTS_SYNC_HOURS,
            "primary_key": "trading_account_id",
            "incremental_columns": "last_update_time",
            "source": "MySQL → v_ant_broker_user + v_ant_users",
        },
        {
            "key": "dealio_mt4trades",
            "label": "dealio_mt4trades",
            "last_synced_at": dealio_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Records",   "value": dealio_stats["total_records"],  "color": "text-info",    "icon": "bi-database"},
                {"label": "Unique Logins",   "value": dealio_stats["unique_logins"],  "color": "text-success", "icon": "bi-person-badge"},
                {"label": "Total Volume",    "value": dealio_stats["total_volume"],   "color": "text-warning", "icon": "bi-graph-up"},
                {"label": "Total Profit",    "value": dealio_stats["total_profit"],   "color": "text-primary", "icon": "bi-currency-dollar"},
            ],
            "sync_log": dealio_log,
            "healthy": _is_healthy(dealio_log, DEALIO_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": DEALIO_SYNC_INTERVAL_HOURS,
            "lookback_hours": DEALIO_SYNC_HOURS,
            "primary_key": "ticket",
            "incremental_columns": "last_modified, updated_at",
            "source": "MSSQL → report.dealio_mt4trades",
        },
        {
            "key": "targets",
            "label": "targets",
            "last_synced_at": targets_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Records",  "value": targets_stats["total_records"],  "color": "text-info",    "icon": "bi-database"},
                {"label": "Unique Agents",  "value": targets_stats["unique_agents"],  "color": "text-success", "icon": "bi-person-badge"},
                {"label": "Total FTC",      "value": targets_stats["total_ftc"],      "color": "text-warning", "icon": "bi-bullseye"},
                {"label": "Total NET",      "value": targets_stats["total_net"],      "color": "text-primary", "icon": "bi-currency-dollar"},
            ],
            "sync_log": targets_log,
            "healthy": _is_healthy(targets_log, TARGETS_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": TARGETS_SYNC_INTERVAL_HOURS,
            "lookback_hours": "All",
            "primary_key": "(date, agent_id)",
            "incremental_columns": "N/A — full refresh (no timestamp column)",
            "source": "MSSQL → report.target",
        },
    ]

    return templates.TemplateResponse("data_sync.html", {
        "request": request,
        "tables": tables,
    })
