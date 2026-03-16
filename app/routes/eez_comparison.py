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
        old_data AS (
            SELECT
                d.login,
                COALESCE(d.convertedequity, d.equity, 0)                              AS equity_val,
                GREATEST(0, COALESCE(d.convertedequity, d.equity, 0))                 AS eez_old
            FROM dealio_daily_profit d
        ),
        new_data AS (
            SELECT
                d.login,
                d.convertedbalance,
                d.convertedfloatingpnl,
                COALESCE(b.total_bonus, 0)                                             AS bonus_deducted,
                GREATEST(
                    GREATEST(d.convertedbalance + d.convertedfloatingpnl, 0)
                        - COALESCE(b.total_bonus, 0),
                    0
                )                                                                      AS eez_new
            FROM dealio_daily_profits d
            LEFT JOIN bonus_bal b ON b.login = d.login
            WHERE d.date::date = (SELECT last_dt FROM last_date)
        ),
        test_flags AS (
            SELECT ta.login::bigint AS login,
                   MAX(a.is_test_account) AS is_test
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            GROUP BY ta.login::bigint
        )
        SELECT
            COALESCE(o.login, n.login)                                                  AS login,
            COALESCE(tf.is_test, 0)                                                     AS is_test,
            ROUND(COALESCE(o.eez_old,       0)::numeric, 2)                             AS eez_old,
            ROUND(COALESCE(n.eez_new,       0)::numeric, 2)                             AS eez_new,
            ROUND((COALESCE(n.eez_new,0) - COALESCE(o.eez_old,0))::numeric, 2)         AS diff,
            ROUND(COALESCE(o.equity_val,    0)::numeric, 2)                             AS raw_equity,
            ROUND(COALESCE(n.convertedbalance,    0)::numeric, 2)                       AS new_balance,
            ROUND(COALESCE(n.convertedfloatingpnl,0)::numeric, 2)                       AS new_floating,
            ROUND(COALESCE(n.bonus_deducted,0)::numeric, 2)                             AS bonus_deducted,
            CASE
                WHEN o.login IS NULL THEN 'new_only'
                WHEN n.login IS NULL THEN 'old_only'
                ELSE 'both'
            END                                                                          AS presence
        FROM old_data o
        FULL OUTER JOIN new_data n ON n.login = o.login
        LEFT JOIN test_flags tf ON tf.login = COALESCE(o.login, n.login)
        ORDER BY ABS(COALESCE(n.eez_new,0) - COALESCE(o.eez_old,0)) DESC
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()

    data = []
    total_old = total_new = 0.0
    for r in rows:
        row = dict(zip(cols, r))
        row = {k: float(v) if v is not None else 0 for k, v in row.items()
               if k not in ("presence", "login")} | {
            "login":    int(row["login"]) if row["login"] is not None else None,
            "presence": row["presence"],
            "is_test":  int(row["is_test"]),
        }
        total_old += row["eez_old"]
        total_new += row["eez_new"]
        data.append(row)

    result = {
        "rows":      data,
        "total_old": round(total_old, 2),
        "total_new": round(total_new, 2),
        "diff":      round(total_new - total_old, 2),
    }
    cache.set(_ck, result)
    return JSONResponse(content=result)
