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

    _ck = "eez_comparison_v18"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    sql = """
        WITH latest_day AS (
            SELECT MAX(day) AS day FROM daily_equity_zeroed
        ),
        test_flags AS (
            SELECT ta.login::bigint AS login,
                   MAX(a.is_test_account) AS is_test
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            GROUP BY ta.login::bigint
        )
        SELECT
            e.login,
            COALESCE(e.end_equity_zeroed, 0)  AS end_equity_zeroed,
            COALESCE(s.end_equity_zeroed, 0)  AS start_equity_zeroed,
            (SELECT day FROM latest_day)       AS snapshot_date
        FROM daily_equity_zeroed e
        LEFT JOIN daily_equity_zeroed s
            ON s.login = e.login
            AND s.day  = e.day - INTERVAL '1 day'
        LEFT JOIN test_flags tf ON tf.login = e.login
        WHERE e.day = (SELECT day FROM latest_day)
          AND COALESCE(tf.is_test, 0) = 0
        ORDER BY e.end_equity_zeroed DESC
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
    total_end = 0.0
    total_start = 0.0
    snapshot_date = None
    for r in rows:
        row = dict(zip(cols, r))
        end_eez   = float(row["end_equity_zeroed"] or 0)
        start_eez = float(row["start_equity_zeroed"] or 0)
        if snapshot_date is None and row["snapshot_date"]:
            snapshot_date = str(row["snapshot_date"])
        data.append({
            "login":               int(row["login"]) if row["login"] is not None else None,
            "end_equity_zeroed":   end_eez,
            "start_equity_zeroed": start_eez,
        })
        total_end   += end_eez
        total_start += start_eez

    result = {
        "rows":          data,
        "total_end":     round(total_end, 2),
        "total_start":   round(total_start, 2),
        "snapshot_date": snapshot_date,
    }
    cache.set(_ck, result)
    return JSONResponse(content=result)
