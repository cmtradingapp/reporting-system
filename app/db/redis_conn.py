"""
Redis connection and data helpers for live MT5 data (MT5 Bridge).

Redis keys (all JSON strings):
  position:{ticket}         — open position (pnl, swap, login, symbol, cmd, ...)
  closed_position:{ticket}  — closed position (profit, commission, swap, ...)
  account:{login}           — account (balance, equity, credit, floating, ...)
  rate:{symbol}             — bid, ask, time

Index sets:
  positions:tickets, closed_positions:tickets, accounts:logins, rates:symbols

Timestamps:
  positions:last_update, closed_positions:last_update, accounts:last_update
"""
import json
import threading
import redis
from app.config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD, REDIS_ENABLED

_pool = None
_pool_lock = threading.Lock()

# Same exclusion list as dealio_conn.py
_EXCLUDED_SYMBOLS = {
    "Cashback", "CFDRollover", "CommEUR", "CommUSD",
    "CorrectiEUR", "CorrectiGBP", "CorrectiJPY", "Correction",
    "CredExp", "CredExpEUR", "CredExpGBP", "CredExpJPY",
    "Dividend", "DividendEUR", "DividendGBP", "DividendJPY",
    "Dormant", "EarnedCr", "EarnedCrEUR", "FEE", "INACT-FEE",
    "Inactivity", "Rollover", "SPREAD",
    "ZeroingEUR", "ZeroingGBP", "ZeroingJPY", "ZeroingKES",
    "ZeroingNGN", "ZeroingUSD", "ZeroingZAR",
}

_BATCH = 5000


def _get_pool():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = redis.ConnectionPool(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    db=REDIS_DB,
                    password=REDIS_PASSWORD or None,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=10,
                    max_connections=20,
                )
    return _pool


def get_redis():
    return redis.Redis(connection_pool=_get_pool())


def is_redis_healthy():
    """PING + verify positions data exists."""
    if not REDIS_ENABLED:
        return False
    try:
        r = get_redis()
        return r.ping() and r.exists("positions:last_update")
    except Exception:
        return False


def get_last_update():
    """Return last update timestamps for all Redis data types."""
    r = get_redis()
    return {
        "positions": r.get("positions:last_update"),
        "closed_positions": r.get("closed_positions:last_update"),
        "accounts": r.get("accounts:last_update"),
        "rates": r.get("rates:last_update"),
    }


def get_floating_pnl_by_login(equity_logins):
    """
    Sum of (pnl + swap) per login from open positions, filtered by cmd<2
    and excluding non-trading symbols.

    Replaces: SELECT login, SUM(computedcommission+computedprofit+computedswap)
              FROM dealio.positions WHERE login=ANY(...) AND cmd<2 ...
    """
    r = get_redis()
    equity_set = set(equity_logins)
    floating_map = {}

    tickets = r.smembers("positions:tickets")
    if not tickets:
        return floating_map

    ticket_list = list(tickets)
    for i in range(0, len(ticket_list), _BATCH):
        batch = ticket_list[i:i + _BATCH]
        pipe = r.pipeline(transaction=False)
        for t in batch:
            pipe.get(f"position:{t}")
        results = pipe.execute()

        for raw in results:
            if not raw:
                continue
            pos = json.loads(raw)
            login = int(pos.get("login", 0))
            if login not in equity_set:
                continue
            if int(pos.get("cmd", 99)) >= 2:
                continue
            if pos.get("symbol", "") in _EXCLUDED_SYMBOLS:
                continue
            pnl = float(pos.get("pnl", 0))
            swap = float(pos.get("swap", 0))
            floating_map[login] = floating_map.get(login, 0.0) + pnl + swap

    return floating_map


def get_balances_by_login(equity_logins):
    """
    Fetch balance per login from Redis accounts.

    Redis account.balance = Dealio compbalance (pure balance, credit separate).
    Replaces: SELECT login, compbalance FROM dealio.users WHERE login=ANY(...)
    """
    r = get_redis()
    bal_map = {}

    pipe = r.pipeline(transaction=False)
    for login in equity_logins:
        pipe.get(f"account:{login}")
    results = pipe.execute()

    for login, raw in zip(equity_logins, results):
        if not raw:
            continue
        acct = json.loads(raw)
        bal_map[int(login)] = float(acct.get("balance", 0))

    return bal_map


# ── Aggregate stats for /redis-perf page ─────────────────────────────────────

def get_all_open_positions_stats(period_start_ts=None, period_end_ts=None):
    """Scan all open positions → totals + per-symbol breakdown.

    period_start_ts/period_end_ts: unix timestamps to filter by open_time.
    If provided, only positions with open_time in [start, end) count toward volume.
    Floating/EEZ always uses ALL open positions (regardless of period).
    """
    r = get_redis()
    tickets = r.smembers("positions:tickets")
    if not tickets:
        return {"total_floating": 0, "total_volume": 0, "trader_count": 0,
                "position_count": 0, "by_symbol": {}, "top_logins": []}

    total_floating = 0.0
    total_volume = 0.0
    logins = set()
    by_symbol = {}
    by_login = {}

    ticket_list = list(tickets)
    for i in range(0, len(ticket_list), _BATCH):
        batch = ticket_list[i:i + _BATCH]
        pipe = r.pipeline(transaction=False)
        for t in batch:
            pipe.get(f"position:{t}")
        results = pipe.execute()

        for raw in results:
            if not raw:
                continue
            pos = json.loads(raw)
            cmd = int(pos.get("cmd", 99))
            if cmd >= 2:
                continue
            symbol = pos.get("symbol", "")
            if symbol in _EXCLUDED_SYMBOLS:
                continue
            login = int(pos.get("login", 0))
            pnl = float(pos.get("pnl", 0))
            swap = float(pos.get("swap", 0))
            vol = float(pos.get("notional_value", 0))
            flt = pnl + swap

            # Floating/EEZ always counts all positions
            total_floating += flt
            logins.add(login)

            if login not in by_login:
                by_login[login] = {"floating": 0.0, "volume": 0.0, "count": 0}
            by_login[login]["floating"] += flt
            by_login[login]["count"] += 1

            # Volume only counts if open_time is in period
            open_time = int(pos.get("open_time", 0))
            in_period = True
            if period_start_ts and open_time < period_start_ts:
                in_period = False
            if period_end_ts and open_time >= period_end_ts:
                in_period = False

            if in_period:
                total_volume += vol
                by_login[login]["volume"] += vol

                if symbol not in by_symbol:
                    by_symbol[symbol] = {"volume": 0.0, "floating": 0.0, "count": 0}
                by_symbol[symbol]["volume"] += vol
                by_symbol[symbol]["floating"] += flt
                by_symbol[symbol]["count"] += 1
            else:
                # Still track symbol floating even if outside period
                if symbol not in by_symbol:
                    by_symbol[symbol] = {"volume": 0.0, "floating": 0.0, "count": 0}
                by_symbol[symbol]["floating"] += flt
                by_symbol[symbol]["count"] += 1

    # Top 20 logins by volume
    top = sorted(by_login.items(), key=lambda x: x[1]["volume"], reverse=True)[:20]
    top_logins = [{"login": l, **v} for l, v in top]

    return {
        "total_floating": round(total_floating, 2),
        "total_volume": round(total_volume, 2),
        "trader_count": len(logins),
        "position_count": len(ticket_list),
        "by_symbol": {s: {k: round(v, 2) if isinstance(v, float) else v
                          for k, v in d.items()}
                      for s, d in sorted(by_symbol.items(),
                                         key=lambda x: x[1]["volume"], reverse=True)},
        "top_logins": top_logins,
        "_login_floating": by_login,  # internal, for EEZ calc
    }


def get_closed_volume_for_period(period_start_ts, period_end_ts):
    """Scan closed positions and sum notional_value where open_time is in [start, end).

    Also returns closed PnL stats for positions closed today.
    """
    from datetime import datetime, timezone

    r = get_redis()
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today_ts = int(today_start.timestamp())

    tickets = r.smembers("closed_positions:tickets")
    if not tickets:
        return {"closed_volume": 0, "closed_volume_count": 0,
                "total_pnl": 0, "trade_count": 0, "total_commission": 0, "total_swap": 0}

    closed_volume = 0.0
    closed_volume_count = 0
    total_pnl = 0.0
    total_commission = 0.0
    total_swap = 0.0
    trade_count = 0

    ticket_list = list(tickets)
    for i in range(0, len(ticket_list), _BATCH):
        batch = ticket_list[i:i + _BATCH]
        pipe = r.pipeline(transaction=False)
        for t in batch:
            pipe.get(f"closed_position:{t}")
        results = pipe.execute()

        for raw in results:
            if not raw:
                continue
            pos = json.loads(raw)
            open_time = int(pos.get("open_time", 0))
            close_time = int(pos.get("close_time", 0))
            entry = int(pos.get("entry", -1))
            symbol = pos.get("symbol", "")

            if symbol in _EXCLUDED_SYMBOLS:
                continue
            if entry != 1:
                continue

            # Volume: opened in period
            if period_start_ts <= open_time < period_end_ts:
                closed_volume += float(pos.get("notional_value", 0))
                closed_volume_count += 1

            # PnL: closed today
            if close_time >= today_ts:
                profit = float(pos.get("profit", 0))
                commission = float(pos.get("commission", 0))
                swap = float(pos.get("swap", 0))
                total_pnl += profit + commission + swap
                total_commission += commission
                total_swap += swap
                trade_count += 1

    return {
        "closed_volume": round(closed_volume, 2),
        "closed_volume_count": closed_volume_count,
        "total_pnl": round(total_pnl, 2),
        "trade_count": trade_count,
        "total_commission": round(total_commission, 2),
        "total_swap": round(total_swap, 2),
    }


def get_all_account_stats(login_floating=None):
    """Compute balance/equity/EEZ stats for all accounts with open positions or balance>0.

    login_floating: dict from get_all_open_positions_stats()._login_floating
    """
    r = get_redis()

    # Get logins that have open positions
    pos_logins = set()
    if login_floating:
        pos_logins = set(login_floating.keys())

    # Also scan for accounts with balance > 0 using SSCAN
    target_logins = set(pos_logins)

    # Fetch accounts for position logins first
    total_balance = 0.0
    total_equity_computed = 0.0
    eez_no_bonus = 0.0
    count = 0

    if not target_logins:
        return {"total_balance": 0, "total_equity_computed": 0,
                "eez_no_bonus": 0, "account_count": 0}

    login_list = list(target_logins)
    for i in range(0, len(login_list), _BATCH):
        batch = login_list[i:i + _BATCH]
        pipe = r.pipeline(transaction=False)
        for l in batch:
            pipe.get(f"account:{l}")
        results = pipe.execute()

        for login, raw in zip(batch, results):
            if not raw:
                continue
            acct = json.loads(raw)
            bal = float(acct.get("balance", 0))
            if bal <= 0 and login not in pos_logins:
                continue
            flt = login_floating.get(login, {}).get("floating", 0.0) if login_floating else 0.0
            eq = bal + flt
            eez = max(0.0, eq)

            total_balance += bal
            total_equity_computed += eq
            eez_no_bonus += eez
            count += 1

    return {
        "total_balance": round(total_balance, 2),
        "total_equity_computed": round(total_equity_computed, 2),
        "eez_no_bonus": round(eez_no_bonus, 2),
        "account_count": count,
    }


def get_today_closed_stats():
    """Legacy wrapper — use get_closed_volume_for_period instead."""
    from datetime import datetime, timezone
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    # Use a wide period so all closed positions are checked for today's PnL
    result = get_closed_volume_for_period(0, int(datetime.now(timezone.utc).timestamp()) + 86400)
    return {
        "total_pnl": result["total_pnl"],
        "trade_count": result["trade_count"],
        "total_commission": result["total_commission"],
        "total_swap": result["total_swap"],
    }


def get_rates():
    """Return all rate symbols with bid/ask/time."""
    r = get_redis()
    symbols = r.smembers("rates:symbols")
    if not symbols:
        return []

    pipe = r.pipeline(transaction=False)
    sym_list = sorted(symbols)
    for s in sym_list:
        pipe.get(f"rate:{s}")
    results = pipe.execute()

    rates = []
    for sym, raw in zip(sym_list, results):
        if not raw:
            continue
        data = json.loads(raw)
        rates.append({
            "symbol": sym,
            "bid": float(data.get("bid", 0)),
            "ask": float(data.get("ask", 0)),
            "spread": round(float(data.get("ask", 0)) - float(data.get("bid", 0)), 5),
            "time": int(data.get("time", 0)),
        })
    return rates
