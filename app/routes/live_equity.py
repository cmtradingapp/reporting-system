from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from app import cache
from datetime import date, datetime
from zoneinfo import ZoneInfo
import traceback
import time

_TZ = ZoneInfo("Europe/Nicosia")

router = APIRouter()

_RETRYABLE_ERRORS = ("conflict with recovery", "ssl syscall error", "eof detected", "timeout expired")

def _with_retry(fn, *args, retries=2, delay=0.5):
    """Retry fn on transient dealio replica errors (replication conflict, SSL drop, timeout)."""
    for attempt in range(retries):
        try:
            return fn(*args)
        except Exception as e:
            msg = str(e).lower()
            if attempt < retries - 1 and any(s in msg for s in _RETRYABLE_ERRORS):
                time.sleep(delay)
                continue
            raise


@router.get("/api/live-equity-zeroed")
async def live_equity_zeroed(request: Request, date: str = None):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    today = datetime.now(_TZ).date()
    if not date:
        d = today
    else:
        try:
            d = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            return JSONResponse(status_code=400, content={"detail": "Invalid date"})

    is_current_month = (d.year == today.year and d.month == today.month)
    _ck = f"live_eez_v24:{d}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        if is_current_month:
            try:
                result = _with_retry(_live_calc, d)
            except Exception as live_err:
                # Dealio unreachable — fall back to historical (local postgres only)
                traceback.print_exc()
                result = _historical_calc(d)
                result["dealio_error"] = str(live_err)
        else:
            result = _with_retry(_historical_calc, d)
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})

    if result.get("is_live", False):
        cache.set(_ck, result)
    return JSONResponse(content=result)


def _historical_calc(d) -> dict:
    """Use dealio_daily_profits with same EEZ formula as eez_comparison page."""
    # Performance: previously did DISTINCT ON over 13.4M dealio_daily_profits rows
    # (full seq scan + disk sort ~18s). Rewritten as LATERAL index scan driven from
    # trading_accounts (~50K non-test/non-deleted rows) using idx_ddps_login_date_desc.
    sql = """
        WITH bonus_bal AS (
            SELECT login, SUM(net_amount) AS old_bonus_balance
            FROM bonus_transactions
            WHERE confirmation_time::date <= %(d)s
            GROUP BY login
        )
        SELECT COALESCE(SUM(
            CASE
                WHEN COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0) <= 0 THEN 0
                ELSE GREATEST(
                    COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0)
                        - COALESCE(b.old_bonus_balance, 0),
                    0
                )
            END
        ), 0) AS total_eez
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
                       AND a.is_test_account = 0
        CROSS JOIN LATERAL (
            SELECT convertedbalance, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE login = ta.login::bigint
              AND date::date <= %(d)s
            ORDER BY date DESC
            LIMIT 1
        ) d
        LEFT JOIN bonus_bal b ON b.login = ta.login::bigint
        WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"d": str(d)})
            row = cur.fetchone()
            total = float(row[0] or 0)
            cur.execute("""
                SELECT COALESCE(SUM(end_equity_zeroed), 0)
                FROM daily_equity_zeroed
                WHERE day = %(d)s::date - INTERVAL '1 day'
                  AND login IN (
                      SELECT login::bigint FROM trading_accounts
                      WHERE vtigeraccountid IS NOT NULL
                        AND (deleted = 0 OR deleted IS NULL)
                  )
            """, {"d": str(d)})
            start_row = cur.fetchone()
            start_eez = float(start_row[0] or 0)
        return {"total": round(total), "start_equity_zeroed": round(start_eez), "pnl_cash": None, "net_deposits_today": None, "is_live": False, "date": str(d)}
    finally:
        conn.close()


def _live_calc(d) -> dict:
    """Live EEZ: MAX(0, compbalance + live_floating - cumulative_bonus).
    Matches snapshot formula: MAX(0, convertedbalance + convertedfloatingpnl - bonus).
    compbalance = pure balance (no credit included) — no credit deduction needed.
    compbalance includes today's closed PnL (balance updates when trades close).
    Only includes logins where ta.equity > 0 (avoids stale dealio values for dormant accounts).
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ta.login::bigint
                FROM trading_accounts ta
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
                  AND a.is_test_account = 0
                  AND ta.vtigeraccountid IS NOT NULL
            """)
            valid_logins = [int(r[0]) for r in cur.fetchall()]

            if not valid_logins:
                return {"total": 0, "start_equity_zeroed": 0, "net_deposits_today": 0, "pnl_cash": 0, "is_live": True, "date": str(d)}

            # Start EEZ per login (yesterday)
            cur.execute("""
                SELECT login, end_equity_zeroed
                FROM daily_equity_zeroed
                WHERE day = %(d)s::date - INTERVAL '1 day'
                  AND login = ANY(%(logins)s)
            """, {"d": str(d), "logins": valid_logins})
            start_eez_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

            # Aggregate start EEZ total (for display)
            cur.execute("""
                SELECT COALESCE(SUM(end_equity_zeroed), 0)
                FROM daily_equity_zeroed
                WHERE day = %(d)s::date - INTERVAL '1 day'
                  AND login IN (
                      SELECT login::bigint FROM trading_accounts
                      WHERE vtigeraccountid IS NOT NULL
                        AND (deleted = 0 OR deleted IS NULL)
                  )
            """, {"d": str(d)})
            start_eez_total = float(cur.fetchone()[0] or 0)

            # Net deposits today — read from MV (pre-filtered, indexed on tx_date)
            cur.execute("""
                SELECT COALESCE(SUM(net_usd), 0)
                FROM mv_daily_kpis
                WHERE tx_date = %(d)s::date
            """, {"d": str(d)})
            net_deposits_today = float(cur.fetchone()[0] or 0)

            # Logins with equity > 0 from live trading_accounts
            cur.execute("""
                SELECT ta.login::bigint
                FROM trading_accounts ta
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE ta.equity > 0
                  AND (ta.deleted = 0 OR ta.deleted IS NULL)
                  AND a.is_test_account = 0
            """)
            equity_logins = [int(r[0]) for r in cur.fetchall()]

            # Cumulative bonus per login up to today (for equity_logins only)
            cur.execute("""
                SELECT login, SUM(net_amount)
                FROM bonus_transactions
                WHERE confirmation_time::date <= %(d)s
                  AND login = ANY(%(logins)s)
                GROUP BY login
            """, {"d": str(d), "logins": equity_logins})
            bonus_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

            # Daily start net equity: MAX(0, convertedbalance + convertedfloatingpnl)
            # from dealio_daily_profits for yesterday, same equity_logins set
            cur.execute("""
                SELECT COALESCE(SUM(CASE
                    WHEN COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0) <= 0 THEN 0
                    ELSE COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0)
                END), 0)
                FROM (
                    SELECT DISTINCT ON (login) login, convertedbalance, convertedfloatingpnl
                    FROM dealio_daily_profits
                    WHERE date::date = %(d)s::date - INTERVAL '1 day'
                    ORDER BY login, date DESC
                ) d
                WHERE d.login = ANY(%(logins)s)
            """, {"d": str(d), "logins": equity_logins})
            start_net_equity = float(cur.fetchone()[0] or 0)

            # Today's bonuses (for daily pnl cash)
            cur.execute("""
                SELECT COALESCE(SUM(net_amount), 0)
                FROM bonus_transactions
                WHERE confirmation_time::date = %(d)s
            """, {"d": str(d)})
            today_bonuses = float(cur.fetchone()[0] or 0)

    finally:
        conn.close()

    # Single dealio connection for all live queries — reduces connection overhead.
    floating_map     = {}
    bal_map          = {}
    today_closed_pnl = 0.0
    if equity_logins:
        dc = get_dealio_connection()
        try:
            with dc.cursor() as cur:
                cur.execute("""
                    SELECT login,
                           SUM(COALESCE(computedcommission,0)
                             + COALESCE(computedprofit,0)
                             + COALESCE(computedswap,0))
                    FROM dealio.positions
                    WHERE login = ANY(%s) AND cmd < 2 AND symbol NOT IN %s
                    GROUP BY login
                """, (equity_logins, _EXCLUDED_SYMBOLS_TUPLE))
                floating_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

                cur.execute(
                    "SELECT login, compbalance FROM dealio.users WHERE login = ANY(%s)",
                    (equity_logins,)
                )
                bal_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

                cur.execute("""
                    SELECT login,
                           SUM(COALESCE(computedcommission,0)
                             + COALESCE(computedprofit,0)
                             + COALESCE(computedswap,0))
                    FROM dealio.trades_mt5
                    WHERE login = ANY(%s)
                      AND entry = 1
                      AND closetime >= %s::date
                      AND closetime <  %s::date + INTERVAL '1 day'
                      AND cmd < 2
                      AND symbol NOT IN %s
                    GROUP BY login
                """, (equity_logins, str(d), str(d), _EXCLUDED_SYMBOLS_TUPLE))
                today_closed_pnl = sum(float(r[1] or 0) for r in cur.fetchall())
        finally:
            dc.close()

    current_floating = sum(floating_map.values())
    open_logins      = list(floating_map.keys())

    # EEZ: MAX(0, compbalance + live_floating - bonus)
    # compbalance = pure balance (no credit) — matches snapshot formula exactly.
    # Daily end net equity: same without bonus deduction.
    grand_total = 0.0
    daily_end_net_equity = 0.0
    for login, balance in bal_map.items():
        flt    = floating_map.get(login, 0.0)
        net_eq = balance + flt
        bonus  = max(0.0, bonus_map.get(login, 0.0))
        grand_total          += max(0.0, net_eq - bonus)
        daily_end_net_equity += max(0.0, net_eq)

    # Query eod_floating_yesterday only for currently-open logins
    eod_floating_yesterday = 0.0
    if open_logins:
        conn2 = get_connection()
        try:
            with conn2.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(COALESCE(d.convertedfloatingpnl, 0)), 0)
                    FROM (
                        SELECT DISTINCT ON (login) login, convertedfloatingpnl
                        FROM dealio_daily_profits
                        WHERE date::date = %(d)s::date - INTERVAL '1 day'
                        ORDER BY login, date DESC
                    ) d
                    WHERE d.login = ANY(%(logins)s)
                """, {"d": str(d), "logins": open_logins})
                eod_floating_yesterday = float(cur.fetchone()[0] or 0)
        finally:
            conn2.close()

    delta_floating   = current_floating - eod_floating_yesterday
    daily_pnl        = round(delta_floating + today_closed_pnl)

    pnl_cash       = round(start_eez_total - grand_total - net_deposits_today)
    daily_pnl_cash = round(daily_end_net_equity - start_net_equity - net_deposits_today - today_bonuses)
    return {
        "total":                  round(grand_total),
        "start_equity_zeroed":    round(start_eez_total),
        "net_deposits_today":     round(net_deposits_today),
        "pnl_cash":               pnl_cash,
        "daily_pnl_cash":         daily_pnl_cash,
        "daily_pnl":              daily_pnl,
        "current_floating":       round(current_floating),
        "eod_floating_yesterday": round(eod_floating_yesterday),
        "today_closed_pnl":       round(today_closed_pnl),
        "daily_end_net_equity":   round(daily_end_net_equity),
        "daily_start_net_equity": round(start_net_equity),
        "is_live":                True,
        "date":                   str(d),
    }
