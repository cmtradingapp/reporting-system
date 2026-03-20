from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_floating_pnl_for_logins, get_dealio_equity_credit_for_logins, get_dealio_closed_pnl_for_logins_date
from app import cache
from datetime import date, datetime
from zoneinfo import ZoneInfo
import traceback

_TZ = ZoneInfo("Europe/Nicosia")

router = APIRouter()


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
    _ck = f"live_eez_v19:{d}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        if is_current_month:
            result = _live_calc(d)
        else:
            result = _historical_calc(d)
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})

    cache.set(_ck, result)
    return JSONResponse(content=result)


def _historical_calc(d) -> dict:
    """Use dealio_daily_profits with same EEZ formula as eez_comparison page."""
    sql = """
        WITH bonus_bal AS (
            SELECT login, SUM(net_amount) AS old_bonus_balance
            FROM bonus_transactions
            WHERE confirmation_time::date <= %(d)s
            GROUP BY login
        ),
        test_flags AS (
            SELECT ta.login::bigint AS login,
                   MAX(a.is_test_account) AS is_test
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
            GROUP BY ta.login::bigint
        ),
        latest_equity AS (
            SELECT DISTINCT ON (login)
                login, convertedbalance, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date <= %(d)s
            ORDER BY login, date DESC
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
        FROM latest_equity d
        LEFT JOIN bonus_bal b  ON b.login = d.login
        JOIN test_flags tf ON tf.login = d.login
        WHERE tf.is_test = 0
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
    """Live EEZ: MAX(0, compprevequity - compcredit - GREATEST(0, cumulative_bonus)).
    Only includes logins where ta.equity > 0 (avoids stale dealio values for dormant accounts).
    cumulative_bonus = SUM(net_amount) from bonus_transactions up to today.
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

            # Net deposits per login (today, excl. bonuses via comment filter)
            cur.execute("""
                SELECT ta.login::bigint,
                       COALESCE(SUM(CASE
                           WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                           WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                       END), 0)
                FROM transactions t
                JOIN crm_users u ON u.id = t.original_deposit_owner
                JOIN accounts a  ON a.accountid = t.vtigeraccountid
                JOIN trading_accounts ta ON ta.vtigeraccountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
                  AND t.confirmation_time::date = %(d)s::date
                  AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
                  AND t.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0
                  AND (ta.deleted = 0 OR ta.deleted IS NULL)
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
                  AND ta.login::bigint = ANY(%(logins)s)
                GROUP BY ta.login::bigint
            """, {"d": str(d), "logins": valid_logins})
            net_deposits_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

            # Aggregate net deposits (for display)
            cur.execute("""
                SELECT COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                END), 0)
                FROM transactions t
                JOIN crm_users u ON u.id = t.original_deposit_owner
                JOIN accounts a  ON a.accountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
                  AND t.confirmation_time::date = %(d)s::date
                  AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
                  AND t.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
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

    # EEZ: MAX(0, compprevequity - compcredit - GREATEST(0, cumulative_bonus))
    # Daily end net equity: MAX(0, compprevequity - compcredit) — raw, no bonus deduction
    # Both computed in one dealio pass for efficiency.
    grand_total = 0.0
    daily_end_net_equity = 0.0
    if equity_logins:
        for login, equity, credit in get_dealio_equity_credit_for_logins(equity_logins):
            net_eq = float(equity or 0) - float(credit or 0)
            bonus  = max(0.0, bonus_map.get(int(login), 0.0))
            grand_total          += max(0.0, net_eq - bonus)
            daily_end_net_equity += max(0.0, net_eq)

    # Daily PnL: delta_floating + today_closed_pnl
    # IMPORTANT: eod_floating_yesterday must use the SAME login set as current_floating
    # (only logins with currently open positions). Using all equity_logins creates a
    # mismatch: accounts that closed positions inflate the delta.
    open_pnl_rows    = get_dealio_floating_pnl_for_logins(equity_logins)
    current_floating = sum(float(r[1] or 0) for r in open_pnl_rows)
    open_logins      = [int(r[0]) for r in open_pnl_rows]

    today_closed_pnl = sum(float(r[1] or 0) for r in get_dealio_closed_pnl_for_logins_date(equity_logins, str(d)))

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
