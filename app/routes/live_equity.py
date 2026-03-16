from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_users_comp
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
    _ck = f"live_eez_v3:{d}"
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
            SELECT login,
                   SUM(net_amount) AS old_bonus_balance
            FROM bonus_transactions
            WHERE confirmation_time::date <= %(d)s
            GROUP BY login
        ),
        test_flags AS (
            SELECT ta.login::bigint AS login,
                   MAX(a.is_test_account) AS is_test
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
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
            GREATEST(
                GREATEST(0, COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0))
                    - GREATEST(0, COALESCE(b.old_bonus_balance, 0)),
                0
            )
        ), 0) AS total_eez
        FROM latest_equity d
        LEFT JOIN bonus_bal b  ON b.login = d.login
        LEFT JOIN test_flags tf ON tf.login = d.login
        WHERE COALESCE(tf.is_test, 0) = 0
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"d": str(d)})
            row = cur.fetchone()
        total = float(row[0] or 0)
        return {"total": round(total), "is_live": False, "date": str(d)}
    finally:
        conn.close()


def _live_calc(d) -> dict:
    """Live Group A/B calculation using Dealio PG + local PG."""
    # Step 1: get users with compprevequity > 0 from Dealio PG
    dealio_users = get_dealio_users_comp()
    if not dealio_users:
        return {"total": 0, "is_live": True, "date": str(d)}

    logins = [int(r[0]) for r in dealio_users]

    conn = get_connection()
    try:
        # Step 2: lifetime net deposits per login
        sql_nd = """
            SELECT ta.login::bigint,
                   SUM(CASE WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN t.usdamount ELSE 0 END) -
                   SUM(CASE WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN t.usdamount ELSE 0 END) AS lifetime_nd
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            JOIN trading_accounts ta ON ta.vtigeraccountid = a.accountid
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND ta.login::bigint = ANY(%(logins)s)
              AND a.is_test_account = 0
            GROUP BY ta.login::bigint
        """
        # Step 3: trade PnL per login (local dealio_trades_mt4, symbols already filtered by ETL)
        sql_pnl = """
            SELECT d.login::bigint,
                   SUM(COALESCE(d.computed_profit, 0) + COALESCE(d.computed_swap, 0) + COALESCE(d.computed_commission, 0)) AS trade_pnl
            FROM dealio_trades_mt4 d
            WHERE d.login::bigint = ANY(%(logins)s)
              AND EXISTS (
                  SELECT 1 FROM trading_accounts ta
                  JOIN accounts a ON a.accountid = ta.vtigeraccountid
                  WHERE ta.login::bigint = d.login::bigint
                    AND a.is_test_account = 0
              )
            GROUP BY d.login::bigint
        """
        # Step 4: total old bonus per login from bonus_transactions table
        sql_bonus = """
            SELECT login, SUM(net_amount) AS total_bonus
            FROM bonus_transactions
            WHERE login = ANY(%(logins)s)
            GROUP BY login
        """
        with conn.cursor() as cur:
            cur.execute(sql_nd, {"logins": logins})
            nd_rows = cur.fetchall()
            cur.execute(sql_pnl, {"logins": logins})
            pnl_rows = cur.fetchall()
            cur.execute(sql_bonus, {"logins": logins})
            bonus_rows = cur.fetchall()
    finally:
        conn.close()

    nd_map    = {int(r[0]): float(r[1] or 0) for r in nd_rows}
    pnl_map   = {int(r[0]): float(r[1] or 0) for r in pnl_rows}
    bonus_map = {int(r[0]): float(r[1] or 0) for r in bonus_rows}

    grand_total = 0.0
    for row in dealio_users:
        login          = int(row[0])
        compprevequity = float(row[1])
        compcredit     = float(row[2])

        if login in bonus_map:
            # Group B: has bonus transactions
            nd          = nd_map.get(login, 0.0)
            pnl         = pnl_map.get(login, 0.0)
            total_bonus = bonus_map[login]
            val = max(0.0, nd + pnl - compcredit - total_bonus)
        else:
            # Group A: no bonus transactions
            val = max(0.0, compprevequity - compcredit)

        grand_total += val

    return {"total": round(grand_total), "is_live": True, "date": str(d)}
