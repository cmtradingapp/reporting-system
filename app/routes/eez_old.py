from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app import cache

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/api/debug-login/{login}")
async def debug_login(login: int, request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT date::date, convertedbalance, convertedfloatingpnl, convertedequity
                FROM dealio_daily_profit WHERE login = %s ORDER BY date DESC LIMIT 5
            """, (login,))
            old = [{"date": str(r[0]), "bal": float(r[1] or 0), "flt": float(r[2] or 0), "eq": float(r[3] or 0)} for r in cur.fetchall()]
            cur.execute("""
                SELECT date::date, convertedbalance, convertedfloatingpnl, convertedequity
                FROM dealio_daily_profits WHERE login = %s ORDER BY date DESC LIMIT 5
            """, (login,))
            new = [{"date": str(r[0]), "bal": float(r[1] or 0), "flt": float(r[2] or 0), "eq": float(r[3] or 0)} for r in cur.fetchall()]
            cur.execute("""
                SELECT transactiontype, usdamount, confirmation_time::date, transactionapproval, deleted
                FROM transactions
                WHERE login::bigint = %s
                  AND transactiontype IN ('FRF Commission','Bonus','FRF Commission Cancelled','BonusCancelled')
                ORDER BY confirmation_time DESC
                LIMIT 20
            """, (login,))
            bonus_txns = [{"type": r[0], "amount": float(r[1] or 0), "date": str(r[2]), "approval": r[3], "deleted": r[4]} for r in cur.fetchall()]
            cur.execute("""
                SELECT SUM(CASE WHEN transactiontype IN ('FRF Commission','Bonus') THEN usdamount ELSE 0 END)
                     - SUM(CASE WHEN transactiontype IN ('FRF Commission Cancelled','BonusCancelled') THEN usdamount ELSE 0 END)
                  AS bonus_total
                FROM transactions
                WHERE login::bigint = %s
                  AND transactionapproval = 'Approved'
                  AND (deleted = 0 OR deleted IS NULL)
                  AND transactiontype IN ('FRF Commission','Bonus','FRF Commission Cancelled','BonusCancelled')
            """, (login,))
            bonus_total = float(cur.fetchone()[0] or 0)
            cur.execute("""
                SELECT transactiontype, COUNT(*), SUM(usdamount)
                FROM transactions
                WHERE login::bigint = %s
                GROUP BY transactiontype
                ORDER BY COUNT(*) DESC
            """, (login,))
            all_types = [{"type": r[0], "count": r[1], "total_usd": float(r[2] or 0)} for r in cur.fetchall()]
            cur.execute("""
                SELECT transactiontype, usdamount, confirmation_time::date, comment, transactionapproval
                FROM transactions
                WHERE login::bigint = %s
                  AND transactionapproval = 'Approved'
                ORDER BY usdamount DESC
                LIMIT 30
            """, (login,))
            deposits_with_comments = [{"type": r[0], "amount": float(r[1] or 0), "date": str(r[2]), "comment": r[3], "approval": r[4]} for r in cur.fetchall()]
    finally:
        conn.close()
    return JSONResponse(content={"login": login, "old_table": old, "new_table": new, "bonus_transactions": bonus_txns, "bonus_total_applied": bonus_total, "all_transaction_types": all_types, "approved_deposits_with_comments": deposits_with_comments})


@router.get("/eez-old", response_class=HTMLResponse)
async def eez_old_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if user.get("role") != "admin":
        return RedirectResponse(url="/performance")
    return templates.TemplateResponse("eez_old.html", {"request": request, "current_user": user})


@router.get("/api/eez-old")
async def eez_old_api(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    _ck = "eez_old_v8"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    sql = """
        WITH last_date AS (
            SELECT MAX(date::date) AS last_dt
            FROM dealio_daily_profit
            WHERE EXTRACT(YEAR  FROM date) = EXTRACT(YEAR  FROM CURRENT_DATE)
              AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)
        ),
        bonus_bal AS (
            SELECT t.login::bigint AS login,
                   SUM(CASE WHEN t.transactiontype IN ('FRF Commission','Bonus')
                              OR (t.transactiontype = 'Deposit' AND (t.comment ILIKE '%bonus%' OR t.comment ILIKE '%FRF%'))
                            THEN t.usdamount ELSE 0 END)
                 - SUM(CASE WHEN t.transactiontype IN ('FRF Commission Cancelled','BonusCancelled')
                              OR (t.transactiontype = 'Withdrawal' AND (t.comment ILIKE '%bonus%' OR t.comment ILIKE '%FRF%'))
                            THEN t.usdamount ELSE 0 END) AS old_bonus_balance
            FROM transactions t
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND (
                  t.transactiontype IN ('FRF Commission','Bonus','FRF Commission Cancelled','BonusCancelled')
                  OR t.comment ILIKE '%bonus%' OR t.comment ILIKE '%FRF%'
              )
              AND t.confirmation_time::date <= (SELECT last_dt FROM last_date)
            GROUP BY t.login::bigint
        ),
        test_flags AS (
            SELECT ta.login::bigint AS login,
                   MAX(a.is_test_account) AS is_test
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            GROUP BY ta.login::bigint
        ),
        daily_start AS (
            SELECT
                ds.login,
                GREATEST(0, COALESCE(ds.convertedequity, 0))                                          AS daily_start_equity,
                GREATEST(0, COALESCE(ds.convertedbalance, 0) + COALESCE(ds.convertedfloatingpnl, 0)) AS daily_start_net_equity
            FROM dealio_daily_profit ds
            WHERE ds.date::date = (
                SELECT MAX(date::date) FROM dealio_daily_profit
                WHERE date::date < DATE_TRUNC('month', CURRENT_DATE)
            )
        )
        SELECT
            d.login,
            COALESCE(tf.is_test, 0)                                             AS is_test,
            ROUND(GREATEST(
                GREATEST(0, COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0))
                    - COALESCE(b.old_bonus_balance, 0),
                0)::numeric, 2)                                                  AS eez,
            ROUND(COALESCE(st.daily_start_equity,     0)::numeric, 2)           AS daily_start_equity,
            ROUND(COALESCE(st.daily_start_net_equity, 0)::numeric, 2)           AS daily_start_net_equity
        FROM dealio_daily_profit d
        LEFT JOIN bonus_bal b  ON b.login  = d.login
        LEFT JOIN test_flags tf ON tf.login = d.login
        LEFT JOIN daily_start st ON st.login = d.login
        WHERE d.date::date = (SELECT last_dt FROM last_date)
        ORDER BY eez DESC
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()

    data = []
    total = 0.0
    for r in rows:
        row = dict(zip(cols, r))
        eez = float(row["eez"] or 0)
        data.append({
            "login":                  int(row["login"]) if row["login"] is not None else None,
            "is_test":                int(row["is_test"]),
            "eez":                    eez,
            "daily_start_equity":     float(row["daily_start_equity"] or 0),
            "daily_start_net_equity": float(row["daily_start_net_equity"] or 0),
        })
        total += eez

    result = {"rows": data, "total": round(total, 2)}
    cache.set(_ck, result)
    return JSONResponse(content=result)
