"""
Connection and fetch functions for the Dealio PostgreSQL replica.

Source: cmtrading-replicadb.dealio.ai
Schema: dealio

Tables fetched into local warehouse:
  - dealio.users          (incremental by lastupdate)
  - dealio.trades_mt4     (incremental by last_modified; cmd IN (0,1); exclude bad symbols)
  - dealio.daily_profits  (incremental by date)
  - dealio.positions      (full replace every sync — live open positions snapshot)

Live (no local copy):
  - dealio.positions    (open trades — queried directly via get_dealio_connection())
"""

from datetime import UTC, datetime, timedelta

import pandas as pd
import psycopg2

from app.config import (
    DEALIO_PG_DB,
    DEALIO_PG_HOST,
    DEALIO_PG_PASSWORD,
    DEALIO_PG_PORT,
    DEALIO_PG_SSLCERT,
    DEALIO_PG_SSLKEY,
    DEALIO_PG_SSLROOTCERT,
    DEALIO_PG_USER,
)

# ── Symbols to exclude from trades_mt4 ──────────────────────────────────────
_EXCLUDED_SYMBOLS = {
    "Cashback",
    "CFDRollover",
    "CommEUR",
    "CommUSD",
    "CorrectiEUR",
    "CorrectiGBP",
    "CorrectiJPY",
    "Correction",
    "CredExp",
    "CredExpEUR",
    "CredExpGBP",
    "CredExpJPY",
    "Dividend",
    "DividendEUR",
    "DividendGBP",
    "DividendJPY",
    "Dormant",
    "EarnedCr",
    "EarnedCrEUR",
    "FEE",
    "INACT-FEE",
    "Inactivity",
    "Rollover",
    "SPREAD",
    "ZeroingEUR",
    "ZeroingGBP",
    "ZeroingJPY",
    "ZeroingKES",
    "ZeroingNGN",
    "ZeroingUSD",
    "ZeroingZAR",
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
        connect_timeout=12,
        # Cap remote statement time to 10s so a slow/cancelled query fails over
        # to the local snapshot quickly instead of blocking the user request.
        options="-c statement_timeout=10000",
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
    compbalance,
    compcredit,
    leverage,
    status,
    regdate,
    lastdate,
    lastupdate,
    agentaccount,
    isenabled
"""


def get_dealio_users(hours: int = 24, since: datetime | None = None, statement_timeout_ms: int = 60000) -> pd.DataFrame:
    """Fetch dealio.users updated since a cutoff.

    If `since` is provided, uses it directly (incremental sync). Otherwise
    falls back to `now - hours`. `statement_timeout_ms` overrides the
    connection default so ETL backfills don't get killed at 10s.
    """
    cutoff = since if since is not None else (datetime.now(UTC) - timedelta(hours=hours))
    sql = f"""
        SELECT {_USERS_COLS}
        FROM dealio.users
        WHERE lastupdate >= %(cutoff)s
        ORDER BY lastupdate
    """
    conn = get_dealio_connection()
    try:
        with conn.cursor() as _cur:
            _cur.execute(f"SET statement_timeout={int(statement_timeout_ms)}")
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
    cutoff = datetime.now(UTC) - timedelta(hours=hours)
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
            df = pd.read_sql(
                sql,
                conn,
                params={
                    "last_ticket": last_ticket,
                    "excluded": _EXCLUDED_SYMBOLS_TUPLE,
                },
            )
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
            df = pd.read_sql(
                sql,
                conn,
                params={
                    "last_ticket": last_ticket,
                    "excluded": _EXCLUDED_SYMBOLS_TUPLE,
                },
            )
        finally:
            conn.close()
        if df.empty:
            break
        yield df
        if len(df) < _CHUNK_SIZE:
            break
        last_ticket = int(df["ticket"].max())


def get_dealio_trades_mt4_by_open_time(from_date: str):
    """Fetch trades where open_time (UTC+2) >= from_date, in chunks ordered by ticket.
    Used for targeted re-sync after timezone fix."""
    last_ticket = 0
    while True:
        sql = f"""
            SELECT {_TRADES_COLS}
            FROM dealio.trades_mt4
            WHERE ticket > %(last_ticket)s
              AND open_time >= %(from_date)s::timestamp - INTERVAL '3 hours'
              AND cmd IN (0, 1)
              AND symbol NOT IN %(excluded)s
            ORDER BY ticket
            LIMIT {_CHUNK_SIZE}
        """
        conn = get_dealio_connection()
        try:
            df = pd.read_sql(
                sql,
                conn,
                params={
                    "last_ticket": last_ticket,
                    "from_date": from_date,
                    "excluded": _EXCLUDED_SYMBOLS_TUPLE,
                },
            )
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
    cutoff = datetime.now(UTC) - timedelta(hours=hours)
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
            df = pd.read_sql(
                sql,
                conn,
                params={
                    "date_from": date_from,
                    "date_to": date_to,
                    "last_date": last_date,
                    "last_login": last_login,
                    "last_sourceid": last_sourceid,
                },
            )
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
            df = pd.read_sql(
                sql,
                conn,
                params={
                    "last_date": last_date,
                    "last_login": last_login,
                    "last_sourceid": last_sourceid,
                },
            )
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


# ── dealio.trades_mt5 ────────────────────────────────────────────────────────

_TRADES_MT5_COLS = """
    ticket,
    login,
    symbol,
    digit,
    cmd,
    volume,
    (opentime  AT TIME ZONE 'EET') AT TIME ZONE 'UTC' AS open_time,
    openprice  AS open_price,
    (closetime AT TIME ZONE 'EET') AT TIME ZONE 'UTC' AS close_time,
    closeprice AS close_price,
    reason,
    commission,
    agentid    AS agent_id,
    swap,
    profit,
    comment,
    computedprofit      AS computed_profit,
    computedswap        AS computed_swap,
    computedcommission  AS computed_commission,
    groupname           AS group_name,
    groupcurrency       AS group_currency,
    book,
    notionalvalue       AS notional_value,
    sourcename          AS source_name,
    sourcetype          AS source_type,
    sourceid            AS source_id,
    positionid          AS position_id,
    entry,
    volumeclosed        AS volume_closed,
    synctime            AS sync_time,
    isfinalized         AS is_finalized,
    spread::text        AS spread,
    conversionrate      AS conversion_rate
"""


def get_dealio_trades_mt5(hours: int = 24) -> pd.DataFrame:
    """Fetch dealio.trades_mt5 updated within the last N hours (cmd 0/1, filtered symbols)."""
    cutoff = datetime.now(UTC) - timedelta(hours=hours)
    sql = f"""
        SELECT {_TRADES_MT5_COLS}
        FROM dealio.trades_mt5
        WHERE synctime >= %(cutoff)s
          AND cmd IN (0, 1)
          AND symbolplain NOT IN %(excluded)s
        ORDER BY synctime
    """
    conn = get_dealio_connection()
    try:
        df = pd.read_sql(sql, conn, params={"cutoff": cutoff, "excluded": _EXCLUDED_SYMBOLS_TUPLE})
        return df
    finally:
        conn.close()


def get_dealio_trades_mt5_full():
    """Full fetch of dealio.trades_mt5 in chunks (cmd 0/1, filtered symbols).
    Reconnects per chunk and retries up to 3 times on SSL errors."""
    import time

    last_ticket = 0
    while True:
        sql = f"""
            SELECT {_TRADES_MT5_COLS}
            FROM dealio.trades_mt5
            WHERE ticket > %(last_ticket)s
              AND cmd IN (0, 1)
              AND symbolplain NOT IN %(excluded)s
            ORDER BY ticket
            LIMIT {_CHUNK_SIZE}
        """
        df = None
        for attempt in range(3):
            conn = get_dealio_connection()
            try:
                df = pd.read_sql(sql, conn, params={"last_ticket": last_ticket, "excluded": _EXCLUDED_SYMBOLS_TUPLE})
                break
            except Exception as e:
                if attempt < 2:
                    print(f"[get_dealio_trades_mt5_full] attempt {attempt+1} failed: {e} — retrying in 5s")
                    time.sleep(5)
                else:
                    raise
            finally:
                conn.close()
        if df.empty:
            break
        yield df
        if len(df) < _CHUNK_SIZE:
            break
        last_ticket = int(df["ticket"].max())


def get_dealio_trades_mt5_missing(start_ticket: int):
    """Fetch only rows with ticket > start_ticket."""
    last_ticket = start_ticket
    while True:
        sql = f"""
            SELECT {_TRADES_MT5_COLS}
            FROM dealio.trades_mt5
            WHERE ticket > %(last_ticket)s
              AND cmd IN (0, 1)
              AND symbolplain NOT IN %(excluded)s
            ORDER BY ticket
            LIMIT {_CHUNK_SIZE}
        """
        conn = get_dealio_connection()
        try:
            df = pd.read_sql(sql, conn, params={"last_ticket": last_ticket, "excluded": _EXCLUDED_SYMBOLS_TUPLE})
        finally:
            conn.close()
        if df.empty:
            break
        yield df
        if len(df) < _CHUNK_SIZE:
            break
        last_ticket = int(df["ticket"].max())


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
            cur.execute("SELECT login, compprevequity, compcredit FROM dealio.users WHERE login = ANY(%s)", (logins,))
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_balance_for_logins(logins: list):
    """Fetch (login, compprevbalance) from dealio.users for a specific list of logins."""
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT login, compprevbalance FROM dealio.users WHERE login = ANY(%s)", (logins,))
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_compbalance_for_logins(logins: list):
    """Fetch (login, compbalance) from dealio.users — live computed balance."""
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT login, compbalance FROM dealio.users WHERE login = ANY(%s)", (logins,))
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_compbalance_credit_for_logins(logins: list):
    """Fetch (login, compbalance, compcredit) from dealio.users — live USD-converted balance + credit."""
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT login, compbalance, compcredit FROM dealio.users WHERE login = ANY(%s)", (logins,))
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_closed_pnl_for_logins_date(logins: list, date: str):
    """Fetch closed trade PnL from dealio.trades_mt4 for a specific close date."""
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
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
            """,
                (logins, date, _EXCLUDED_SYMBOLS_TUPLE),
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_dealio_floating_pnl_for_logins(logins: list):
    """Fetch floating PnL from dealio.positions (live open positions) for a specific list of logins."""
    conn = get_dealio_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT login,
                       SUM(COALESCE(computedcommission, 0)
                         + COALESCE(computedprofit, 0)
                         + COALESCE(computedswap, 0)) AS floatingpnl
                FROM dealio.positions
                WHERE login = ANY(%s)
                  AND cmd < 2
                  AND symbol NOT IN %s
                GROUP BY login
            """,
                (logins, _EXCLUDED_SYMBOLS_TUPLE),
            )
            return cur.fetchall()
    finally:
        conn.close()


# ── dealio.positions ──────────────────────────────────────────────────────────


def get_dealio_positions() -> pd.DataFrame:
    """Fetch all open positions from dealio.positions (live snapshot, full replace)."""
    sql = """
        SELECT
            id,
            login,
            cmd,
            volume,
            symbol,
            coresymbol          AS core_symbol,
            book,
            openprice           AS open_price,
            closeprice          AS close_price,
            profit,
            computedprofit      AS computed_profit,
            swap,
            computedswap        AS computed_swap,
            commission,
            computedcommission  AS computed_commission,
            comment,
            groupname           AS group_name,
            groupcurrency       AS group_currency,
            notionalvalue       AS notional_value,
            contractsize        AS contract_size,
            sourcename          AS source_name,
            sourcetype          AS source_type,
            sourceid            AS source_id,
            opentime            AS open_time,
            lastupdate          AS last_update,
            reason,
            conversionrate      AS conversion_rate,
            calculationcurrency AS calculation_currency,
            currencybase        AS currency_base,
            currencyprofit      AS currency_profit,
            exposurebase        AS exposure_base,
            exposureprofit      AS exposure_profit
        FROM dealio.positions
        WHERE cmd IN (0, 1)
          AND symbol NOT IN %(excluded)s
        ORDER BY id
    """
    conn = get_dealio_connection()
    try:
        df = pd.read_sql(sql, conn, params={"excluded": _EXCLUDED_SYMBOLS_TUPLE})
        return df
    finally:
        conn.close()
