from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app import cache

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/eez-comparison", response_class=HTMLResponse)
async def eez_comparison_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if user.get("role") != "admin":
        return RedirectResponse(url="/performance")
    return templates.TemplateResponse("eez_comparison.html", {"request": request, "current_user": user})


@router.get("/api/eez-comparison")
async def eez_comparison_api(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    _ck = "eez_comparison"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    sql = """
        WITH last_date AS (
            SELECT MAX(date::date) AS last_dt
            FROM dealio_daily_profits
            WHERE EXTRACT(YEAR  FROM date) = EXTRACT(YEAR  FROM CURRENT_DATE)
              AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)
        ),
        bonus_bal AS (
            SELECT t.login::bigint AS login,
                   SUM(CASE WHEN t.transactiontype IN ('FRF Commission', 'Bonus')
                            THEN t.usdamount ELSE 0 END)
                 - SUM(CASE WHEN t.transactiontype IN ('FRF Commission Cancelled', 'BonusCancelled')
                            THEN t.usdamount ELSE 0 END) AS total_bonus
            FROM transactions t
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('FRF Commission','Bonus','FRF Commission Cancelled','BonusCancelled')
              AND t.confirmation_time::date <= (SELECT last_dt FROM last_date)
            GROUP BY t.login::bigint
        ),
        test_flags AS (
            SELECT ta.login::bigint AS login,
                   MAX(a.is_test_account) AS is_test
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            GROUP BY ta.login::bigint
        )
        SELECT
            d.login,
            COALESCE(tf.is_test, 0)                                             AS is_test,
            ROUND(
                GREATEST(
                    GREATEST(d.convertedbalance + d.convertedfloatingpnl, 0)
                        - COALESCE(b.total_bonus, 0),
                    0
                )::numeric, 2
            )                                                                    AS eez
        FROM dealio_daily_profits d
        LEFT JOIN bonus_bal b ON b.login = d.login
        LEFT JOIN test_flags tf ON tf.login = d.login
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
            "login":   int(row["login"]) if row["login"] is not None else None,
            "is_test": int(row["is_test"]),
            "eez":     eez,
        })
        total += eez

    result = {"rows": data, "total": round(total, 2)}
    cache.set(_ck, result)
    return JSONResponse(content=result)
