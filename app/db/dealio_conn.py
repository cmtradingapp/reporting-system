"""
Connection and fetch functions for the Dealio PostgreSQL replica.

Source: cmtrading-replicadb.dealio.ai
Schema: dealio

Tables fetched into local warehouse:
  - dealio.users          (incremental by lastupdate)
  - dealio.trades_mt4     (incremental by last_modified; cmd IN (0,1); exclude bad symbols)
  - dealio.daily_profits  (incremental by date)

Live (no local copy):
  - dealio.positions    (open trades — queried directly via get_dealio_connection())
"""
import pandas as pd
import psycopg2
from datetime import datetime, timedelta, timezone

from app.config import (
    DEALIO_PG_HOST, DEALIO_PG_PORT, DEALIO_PG_USER,
    DEALIO_PG_PASSWORD, DEALIO_PG_DB,
    DEALIO_PG_SSLCERT, DEALIO_PG_SSLKEY, DEALIO_PG_SSLROOTCERT,
)

# ── Symbols to exclude from trades_mt4 ──────────────────────────────────────
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

_EXCLUDED_SYMBOLS_TUPLE = tuple(_EXCLUDED_SYMBOLS)

_CHUNK_SIZE = 50_000


def get_dealio_connection():
    """Return a live psycopg2 connection to the Dealio PG replica."""
    return psycopg2.connect(
        host=DEALIO_PG_HOST,
        port=DEALIO_PG_PORT,
        user=DEALIO_PG_USER,
        password=DEALIO_PG_PASSWORD,
        dbname=DEALIO_PG_DB,
        connect_timeout=15,
        options="-c statement_timeout=120000",
        sslmode="require",
        sslcert=DEALIO_PG_SSLCERT,
        sslkey=DEALIO_PG_SSLKEY,
        sslrootcert=DEALIO_PG_SSLROOTCERT,
        client_encoding="utf8",
    )


# ── dealio.users ─────────────────────────────────────────────────────────────

_USERS_COLS = """
    login,
    sourceid,
    sourcename,
    sourcetype,
    groupname,
    groupcurrency,
    name,
    email,
    country,
    city,
    zipcode,
    address,
    phone,
    comment,
    balance,
    credit,
    leverage,
    status,
    regdate,
    lastdate,
    lastupdate,
    agentaccount,
    isenabled
"""

def get_dealio_users(hours: int = 24) -> pd.DataFrame:
    """Fetch dealio.users updated within the last N hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    sql = f"""
        SELECT {_USERS_COLS}
        FROM dealio.users
        WHERE lastupdate >= %(cutoff)s
        ORDER BY lastupdate
    """
    conn = get_dealio_connection()
    try:
        df = pd.read_sql(sql, conn, params={"cutoff": cutoff})
        return df
    finally:
        conn.close()


def get_dealio_users_full():
    """Full fetch of dealio.users in chunks. Reconnects per chunk to avoid SSL timeout."""
    last_login = 0
    while True:
        sql = f"""
            SELECT {_USERS_COLS}
            FROM dealio.users
            WHERE login > %(last_login)s
            ORDER BY login
            LIMIT {_CHUNK_SIZE}
        """
        conn = get_dealio_connection()
        try:
            df = pd.read_sql(sql, conn, params={"last_login": last_login})
        finally:
            conn.close()
        if df.empty:
            break
        yield df
        if len(df) < _CHUNK_SIZE:
            break
        last_login = int(df["login"].max())


# ── dealio.trades_mt4 ────────────────────────────────────────────────────────

_TRADES_COLS = """
    ticket,
    source_id,
    login,
    cmd,
    volume,
    open_time,
    close_time,
    last_modified,
    profit,
    computed_profit,
    symbol,
    core_symbol,
    book,
    open_price,
    close_price,
    commission,
    swaps,
    comment,
    group_name,
    group_currency,
    source_name,
    source_type,
    reason,
    notional_value,
    computed_swap,
    computed_commission,
    spread
"""

def get_dealio_trades_mt4(hours: int = 24) -> pd.DataFrame:
    """Fetch dealio.trades_mt4 updated within the last N hours (cmd 0/1, filtered symbols)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    sql = f"""
        SELECT {_TRADES_COLS}
        FROM dealio.trades_mt4
        WHERE last_modified >= %(cutoff)s
          AND cmd IN (0, 1)
          AND symbol NOT IN %(excluded)s
        ORDER BY last_modified
    """
    conn = get_dealio_connection()
    try:
        df = pd.read_sql(sql, conn, params={"cutoff": cutoff, "excluded": _EXCLUDED_SYMBOLS_TUPLE})
        return df
    finally:
        conn.close()


def get_dealio_trades_mt4_missing(start_ticket: int):
    """Fetch only rows with ticket > start_ticket (to add missing rows without re-syncing everything)."""
    last_ticket = start_ticket
    while True:
        sql = f"""
            SELECT {_TRADES_COLS}
            FROM dealio.trades_mt4
            WHERE ticket > %(last_ticket)s
              AND cmd IN (0, 1)
              AND symbol NOT IN %(excluded)s
            ORDER BY ticket
            LIMIT {_CHUNK_SIZE}
        """
        conn = get_dealio_connection()
        try:
            df = pd.read_sql(sql, conn, params={
                "last_ticket": last_ticket,
                "excluded": _EXCLUDED_SYMBOLS_TUPLE,
            })
        finally:
            conn.close()
        if df.empty:
            break
        yield df
        if len(df) < _CHUNK_SIZE:
            break
        last_ticket = int(df["ticket"].max())


def get_dealio_trades_mt4_full():
    """Full fetch of dealio.trades_mt4 in chunks (cmd 0/1, filtered symbols).
    Reconnects per chunk to avoid SSL timeout on long-running syncs."""
    last_ticket = 0
    while True:
        sql = f"""
            SELECT {_TRADES_COLS}
            FROM dealio.trades_mt4
            WHERE ticket > %(last_ticket)s
              AND cmd IN (0, 1)
              AND symbol NOT IN %(excluded)s
            ORDER BY ticket
            LIMIT {_CHUNK_SIZE}
        """
        conn = get_dealio_connection()
        try:
            df = pd.read_sql(sql, conn, params={
                "last_ticket": last_ticket,
                "excluded": _EXCLUDED_SYMBOLS_TUPLE,
            })
        finally:
            conn.close()
        if df.empty:
            break
        yield df
        if len(df) < _CHUNK_SIZE:
            break
        last_ticket = int(df["ticket"].max())


# ── dealio.daily_profits ──────────────────────────────────────────────────────

_DAILY_PROFITS_COLS = """
    date,
    login,
    sourceid,
    sourcename,
    sourcetype,
    book,
    closedpnl,
    convertedclosedpnl,
    calculationcurrency,
    floatingpnl,
    convertedfloatingpnl,
    netdeposit,
    convertednetdeposit,
    equity,
    convertedequity,
    balance,
    convertedbalance,
    groupcurrency,
    conversionratio,
    equityprevday,
    groupname,
    deltafloatingpnl,
    converteddeltafloatingpnl
"""


def get_dealio_daily_profits(hours: int = 48) -> pd.DataFrame:
    """Fetch dealio.daily_profits rows with date >= NOW() - N hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    sql = f"""
        SELECT {_DAILY_PROFITS_COLS}
        FROM dealio.daily_profits
        WHERE date >= %(cutoff)s
        ORDER BY date, login
    """
    conn = get_dealio_connection()
    try:
        df = pd.read_sql(sql, conn, params={"cutoff": cutoff})
        return df
    finally:
        conn.close()


def get_dealio_daily_profits_daterange(date_from: str, date_to: str):
    """Fetch dealio.daily_profits for a specific date range in chunks."""
    last_date = f"{date_from} 00:00:00"
    last_login = 0
    last_sourceid = ""
    while True:
        sql = f"""
            SELECT {_DAILY_PROFITS_COLS}
            FROM dealio.daily_profits
            WHERE date::date >= %(date_from)s
              AND date::date <= %(date_to)s
              AND (date, login, sourceid) > (%(last_date)s::timestamptz, %(last_login)s, %(last_sourceid)s)
            ORDER BY date, login, sourceid
            LIMIT {_CHUNK_SIZE}
        """
        conn = get_dealio_connection()
        try:
            df = pd.read_sql(sql, conn, params={
                "date_from": date_from,
                "date_to": date_to,
                "last_date": last_date,
                "last_login": last_login,
                "last_sourceid": last_sourceid,
            })
        finally:
            conn.close()
        if df.empty:
            break
        yield df
        if len(df) < _CHUNK_SIZE:
            break
        last_date = str(df.iloc[-1]["date"])
        last_login = int(df.iloc[-1]["login"])
        last_sourceid = str(df.iloc[-1]["sourceid"])


def get_dealio_daily_profits_full():
    """Full fetch of dealio.daily_profits in chunks.
    Paginated by (date, login, sourceid) — the full PK — so no rows are
    skipped when multiple sourceids exist for the same (date, login)."""
    last_date = "1970-01-01 00:00:00"
    last_login = 0
    last_sourceid = ""
    while True:
        sql = f"""
            SELECT {_DAILY_PROFITS_COLS}
            FROM dealio.daily_profits
            WHERE (date, login, sourceid) > (%(last_date)s::timestamptz, %(last_login)s, %(last_sourceid)s)
            ORDER BY date, login, sourceid
            LIMIT {_CHUNK_SIZE}
        """
        conn = get_dealio_connection()
        try:
            df = pd.read_sql(sql, conn, params={
                "last_date": last_date,
                "last_login": last_login,
                "last_sourceid": last_sourceid,
            })
        finally:
            conn.close()
        if df.empty:
            break
        yield df
        if len(df) < _CHUNK_SIZE:
            break
        last_date = str(df.iloc[-1]["date"])
        last_login = int(df.iloc[-1]["login"])
        last_sourceid = str(df.iloc[-1]["sourceid"])


# ── Live equity helpers ───────────────────────────────────────────────────────

def get_dealio_users_comp():
    """Fetch login, compprevequity, compcredit for users with compprevequity > 0."""
    sql = """
        SELECT login, compprevequity, compcredit
        FROM dealio.users
        WHERE compprevequity > 0
    """
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_users_balance():
    """Fetch (login, compprevbalance) from live dealio.users for EEZ calculation."""
    sql = """
        SELECT login, compprevbalance
        FROM dealio.users
        WHERE compprevbalance > 0
    """
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_equity_credit_for_logins(logins: list):
    """Fetch (login, compprevequity, compcredit) from dealio.users for Group A logins."""
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT login, compprevequity, compcredit FROM dealio.users WHERE login = ANY(%s)",
                (logins,)
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_balance_for_logins(logins: list):
    """Fetch (login, compprevbalance) from dealio.users for a specific list of logins."""
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT login, compprevbalance FROM dealio.users WHERE login = ANY(%s)",
                (logins,)
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_compbalance_for_logins(logins: list):
    """Fetch (login, compbalance) from dealio.users — live computed balance."""
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT login, compbalance FROM dealio.users WHERE login = ANY(%s)",
                (logins,)
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_closed_pnl_for_logins_date(logins: list, date: str):
    """Fetch closed trade PnL from dealio.trades_mt4 for a specific close date."""
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login,
                       SUM(COALESCE(computed_commission, 0)
                         + COALESCE(computed_profit, 0)
                         + COALESCE(computed_swap, 0)) AS closed_pnl
                FROM dealio.trades_mt4
                WHERE login = ANY(%s)
                  AND close_time::date = %s
                  AND cmd < 2
                  AND symbol NOT IN %s
                GROUP BY login
            """, (logins, date, _EXCLUDED_SYMBOLS_TUPLE))
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_floating_pnl_for_logins(logins: list):
    """Fetch floating PnL from dealio.trades_mt4 (open trades) for a specific list of logins."""
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login,
                       SUM(COALESCE(computed_commission, 0)
                         + COALESCE(computed_profit, 0)
                         + COALESCE(computed_swap, 0)) AS floatingpnl
                FROM dealio.trades_mt4
                WHERE login = ANY(%s)
                  AND close_time = '1970-01-01 00:00:00'
                  AND cmd < 2
                  AND symbol NOT IN %s
                GROUP BY login
            """, (logins, _EXCLUDED_SYMBOLS_TUPLE))
            return cur.fetchall()
    finally:
        conn.close()
