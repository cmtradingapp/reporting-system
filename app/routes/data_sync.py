import os
import time as _time
from concurrent.futures import ThreadPoolExecutor
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import fetch_accounts_stats, fetch_crm_users_stats, fetch_transactions_stats, fetch_targets_stats, fetch_dealio_mt4trades_stats, fetch_trading_accounts_stats, fetch_ftd100_stats, fetch_sync_log, fetch_dealio_daily_profit_stats, fetch_dealio_users_stats, fetch_dealio_trades_mt4_stats, fetch_dealio_daily_profits_stats
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

_stats_cache: dict = {}
_CACHE_TTL = 60  # seconds


def _cached(key: str, fn):
    now = _time.monotonic()
    if key in _stats_cache:
        val, ts = _stats_cache[key]
        if now - ts < _CACHE_TTL:
            return val
    val = fn()
    _stats_cache[key] = (val, now)
    return val

_TZ = ZoneInfo("Europe/Nicosia")

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
DEALIO_USERS_SYNC_INTERVAL_HOURS = int(os.getenv("DEALIO_USERS_SYNC_INTERVAL_HOURS", "1"))
DEALIO_USERS_SYNC_HOURS = int(os.getenv("DEALIO_USERS_SYNC_HOURS", "24"))
DEALIO_TRADES_MT4_SYNC_INTERVAL_HOURS = int(os.getenv("DEALIO_TRADES_MT4_SYNC_INTERVAL_HOURS", "1"))
DEALIO_TRADES_MT4_SYNC_HOURS = int(os.getenv("DEALIO_TRADES_MT4_SYNC_HOURS", "24"))
DEALIO_DAILY_PROFIT_SYNC_INTERVAL_HOURS = int(os.getenv("DEALIO_DAILY_PROFIT_SYNC_INTERVAL_HOURS", "1"))
DEALIO_DAILY_PROFIT_SYNC_HOURS = int(os.getenv("DEALIO_DAILY_PROFIT_SYNC_HOURS", "48"))
DEALIO_DAILY_PROFITS_SYNC_INTERVAL_HOURS = int(os.getenv("DEALIO_DAILY_PROFITS_SYNC_INTERVAL_HOURS", "1"))
DEALIO_DAILY_PROFITS_SYNC_HOURS = int(os.getenv("DEALIO_DAILY_PROFITS_SYNC_HOURS", "48"))
TRADING_ACCOUNTS_SYNC_INTERVAL_HOURS = int(os.getenv("TRADING_ACCOUNTS_SYNC_INTERVAL_HOURS", "1"))
TRADING_ACCOUNTS_SYNC_HOURS = int(os.getenv("TRADING_ACCOUNTS_SYNC_HOURS", "24"))
FTD100_SYNC_INTERVAL_HOURS = int(os.getenv("FTD100_SYNC_INTERVAL_HOURS", "1"))


def _is_healthy(sync_log: list, interval_hours: int) -> bool:
    if not sync_log or sync_log[0]["status"] != "success":
        return False
    last_ran = datetime.strptime(sync_log[0]["ran_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=_TZ)
    threshold = datetime.now(_TZ) - timedelta(hours=interval_hours + 1)
    return last_ran >= threshold


@router.get("/data-sync", response_class=HTMLResponse)
async def data_sync_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if user.get("role") != "admin":
        return RedirectResponse(url="/performance", status_code=302)
    jobs = {
        "accounts_stats":  lambda: _cached("accounts_stats",  fetch_accounts_stats),
        "users_stats":     lambda: _cached("users_stats",     fetch_crm_users_stats),
        "tx_stats":        lambda: _cached("tx_stats",        fetch_transactions_stats),
        "targets_stats":   lambda: _cached("targets_stats",   fetch_targets_stats),
        "dealio_stats":    lambda: _cached("dealio_stats",    fetch_dealio_mt4trades_stats),
        "ta_stats":        lambda: _cached("ta_stats",        fetch_trading_accounts_stats),
        "ftd100_stats":    lambda: _cached("ftd100_stats",    fetch_ftd100_stats),
        "ddp_stats":       lambda: _cached("ddp_stats",       fetch_dealio_daily_profit_stats),
        "du_stats":        lambda: _cached("du_stats",        fetch_dealio_users_stats),
        "dtm4_stats":      lambda: _cached("dtm4_stats",      fetch_dealio_trades_mt4_stats),
        "ddps_stats":      lambda: _cached("ddps_stats",      fetch_dealio_daily_profits_stats),
        "accounts_log":    lambda: fetch_sync_log("crm_accounts",      limit=50),
        "users_log":       lambda: fetch_sync_log("crm_users",         limit=50),
        "tx_log":          lambda: fetch_sync_log("transactions",       limit=50),
        "targets_log":     lambda: fetch_sync_log("targets",           limit=50),
        "dealio_log":      lambda: fetch_sync_log("dealio_mt4trades",  limit=50),
        "ta_log":          lambda: fetch_sync_log("trading_accounts",  limit=50),
        "ftd100_log":      lambda: fetch_sync_log("ftd100_clients",    limit=50),
        "ddp_log":         lambda: fetch_sync_log("dealio_daily_profit", limit=50),
        "du_log":          lambda: fetch_sync_log("dealio_users",      limit=50),
        "dtm4_log":        lambda: fetch_sync_log("dealio_trades_mt4", limit=50),
        "ddps_log":        lambda: fetch_sync_log("dealio_daily_profits", limit=50),
    }

    results = {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(fn): key for key, fn in jobs.items()}
        for future in futures:
            results[futures[future]] = future.result()

    accounts_stats = results["accounts_stats"]
    accounts_log   = results["accounts_log"]
    users_stats    = results["users_stats"]
    users_log      = results["users_log"]
    tx_stats       = results["tx_stats"]
    tx_log         = results["tx_log"]
    targets_stats  = results["targets_stats"]
    targets_log    = results["targets_log"]
    dealio_stats   = results["dealio_stats"]
    dealio_log     = results["dealio_log"]
    ta_stats       = results["ta_stats"]
    ta_log         = results["ta_log"]
    ftd100_stats   = results["ftd100_stats"]
    ftd100_log     = results["ftd100_log"]
    ddp_stats      = results["ddp_stats"]
    ddp_log        = results["ddp_log"]
    du_stats       = results["du_stats"]
    du_log         = results["du_log"]
    dtm4_stats     = results["dtm4_stats"]
    dtm4_log       = results["dtm4_log"]
    ddps_stats     = results["ddps_stats"]
    ddps_log       = results["ddps_log"]

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
                {"label": "Unique Logins",      "value": ta_stats["unique_logins"],    "color": "text-success", "icon": "bi-person-badge"},
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
            "key": "ftd100_clients",
            "label": "ftd100_clients",
            "last_synced_at": ftd100_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Clients",      "value": ftd100_stats["total_records"],      "color": "text-info",    "icon": "bi-database"},
                {"label": "Sales",              "value": ftd100_stats["sales_count"],         "color": "text-success", "icon": "bi-graph-up"},
                {"label": "Retention",          "value": ftd100_stats["retention_count"],     "color": "text-warning", "icon": "bi-arrow-repeat"},
                {"label": "Total Net Deposits", "value": ftd100_stats["total_net_deposits"],  "color": "text-primary", "icon": "bi-currency-dollar"},
            ],
            "sync_log": ftd100_log,
            "healthy": _is_healthy(ftd100_log, FTD100_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": FTD100_SYNC_INTERVAL_HOURS,
            "lookback_hours": "All",
            "primary_key": "accountid",
            "incremental_columns": "N/A — full refresh (running total requires full recalc)",
            "source": "PostgreSQL → transactions + accounts (CTE)",
        },
        {
            "key": "dealio_daily_profit",
            "label": "dealio_daily_profit",
            "last_synced_at": ddp_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Records",     "value": ddp_stats["total_records"],     "color": "text-info",    "icon": "bi-database"},
                {"label": "Unique Logins",     "value": ddp_stats["unique_logins"],     "color": "text-success", "icon": "bi-person-badge"},
                {"label": "Total Closed PnL",  "value": ddp_stats["total_closed_pnl"],  "color": "text-warning", "icon": "bi-graph-up"},
                {"label": "Total Net Deposit", "value": ddp_stats["total_net_deposit"], "color": "text-primary", "icon": "bi-currency-dollar"},
            ],
            "sync_log": ddp_log,
            "healthy": _is_healthy(ddp_log, DEALIO_DAILY_PROFIT_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": DEALIO_DAILY_PROFIT_SYNC_INTERVAL_HOURS,
            "lookback_hours": DEALIO_DAILY_PROFIT_SYNC_HOURS,
            "primary_key": "login",
            "incremental_columns": "date",
            "source": "MSSQL → report.dealio_daily_profit",
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
        {
            "key": "dealio_users",
            "label": "dealio_users",
            "last_synced_at": du_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Records",     "value": du_stats["total_records"],     "color": "text-info",    "icon": "bi-database"},
                {"label": "Unique Groups",     "value": du_stats["unique_groups"],     "color": "text-success", "icon": "bi-diagram-3"},
                {"label": "Unique Currencies", "value": du_stats["unique_currencies"], "color": "text-warning", "icon": "bi-currency-exchange"},
                {"label": "With Balance",      "value": du_stats["users_with_balance"], "color": "text-primary", "icon": "bi-wallet2"},
            ],
            "sync_log": du_log,
            "healthy": _is_healthy(du_log, DEALIO_USERS_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": DEALIO_USERS_SYNC_INTERVAL_HOURS,
            "lookback_hours": DEALIO_USERS_SYNC_HOURS,
            "primary_key": "login",
            "incremental_columns": "lastupdate",
            "source": "Dealio PG → dealio.users",
        },
        {
            "key": "dealio_trades_mt4",
            "label": "dealio_trades_mt4",
            "last_synced_at": dtm4_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Records",  "value": dtm4_stats["total_records"],  "color": "text-info",    "icon": "bi-database"},
                {"label": "Unique Logins",  "value": dtm4_stats["unique_logins"],  "color": "text-success", "icon": "bi-person-badge"},
                {"label": "Total Profit",   "value": dtm4_stats["total_profit"],   "color": "text-warning", "icon": "bi-currency-dollar"},
                {"label": "Unique Symbols", "value": dtm4_stats["unique_symbols"], "color": "text-primary", "icon": "bi-graph-up"},
            ],
            "sync_log": dtm4_log,
            "healthy": _is_healthy(dtm4_log, DEALIO_TRADES_MT4_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": DEALIO_TRADES_MT4_SYNC_INTERVAL_HOURS,
            "lookback_hours": DEALIO_TRADES_MT4_SYNC_HOURS,
            "primary_key": "ticket",
            "incremental_columns": "last_modified",
            "source": "Dealio PG → dealio.trades_mt4",
        },
        {
            "key": "dealio_daily_profits",
            "label": "dealio_daily_profits",
            "last_synced_at": ddps_stats["last_synced_at"],
            "stat_cards": [
                {"label": "Total Records",    "value": ddps_stats["total_records"],    "color": "text-info",    "icon": "bi-database"},
                {"label": "Unique Logins",    "value": ddps_stats["unique_logins"],    "color": "text-success", "icon": "bi-person-badge"},
                {"label": "Total Closed PnL", "value": ddps_stats["total_closed_pnl"], "color": "text-warning", "icon": "bi-currency-dollar"},
                {"label": "Latest Date",      "value": ddps_stats["latest_date"],      "color": "text-primary", "icon": "bi-calendar-date"},
            ],
            "sync_log": ddps_log,
            "healthy": _is_healthy(ddps_log, DEALIO_DAILY_PROFITS_SYNC_INTERVAL_HOURS),
            "sync_interval_hours": DEALIO_DAILY_PROFITS_SYNC_INTERVAL_HOURS,
            "lookback_hours": DEALIO_DAILY_PROFITS_SYNC_HOURS,
            "primary_key": "(date, login, sourceid)",
            "incremental_columns": "date",
            "source": "Dealio PG → dealio.daily_profits",
        },
    ]

    return templates.TemplateResponse("data_sync.html", {
        "request": request,
        "current_user": user,
        "tables": tables,
    })
