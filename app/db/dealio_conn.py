"""
Connection and fetch functions for the Dealio PostgreSQL replica.

Source: cmtrading-replicadb.dealio.ai
Schema: dealio

Tables fetched into local warehouse:
  - dealio.users        (incremental by lastupdate)
  - dealio.trades_mt4   (incremental by last_modified; cmd IN (0,1); exclude bad symbols)

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
    reason
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
