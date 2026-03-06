from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from app.db.postgres_conn import get_connection
from datetime import datetime, timedelta, date as date_type


def count_working_days(start: date_type, end: date_type) -> int:
    """Count Mon–Fri days between start and end (inclusive)."""
    if end < start:
        return 0
    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5:
            count += 1
        current += timedelta(days=1)
    return count

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")





@router.get("/scoreboard", response_class=HTMLResponse)
def scoreboard_page(request: Request):
    return templates.TemplateResponse("scoreboard.html", {"request": request})


@router.get("/api/scoreboard")
def scoreboard_api(date_from: str, date_to: str):
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt_to   = datetime.strptime(date_to,   "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    today = datetime.utcnow().date()
    working_days        = count_working_days(dt_from, dt_to)
    working_days_passed = count_working_days(dt_from, min(dt_to, today))

    sql = """
        SELECT
            COALESCE(u.office_name, 'N/A')              AS office_name,
            COALESCE(u.agent_name, u.full_name, 'N/A')  AS agent_name,
            COALESCE(u.department_, '')                  AS department_,
            COALESCE(ftc.cnt, 0)                         AS ftc,
            COALESCE(tgt.target_ftc, 0)                  AS target_ftc,
            COALESCE(f100.ftd100_cnt, 0)                 AS ftd100
        FROM crm_users u
        LEFT JOIN (
            SELECT
                t.original_deposit_owner          AS agent_id,
                COUNT(DISTINCT t.vtigeraccountid) AS cnt
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype = 'Deposit'
              AND t.ftd = 1
              AND a.client_qualification_date IS NOT NULL
              AND a.client_qualification_date >= %(date_from)s
              AND a.client_qualification_date <  %(date_to_excl)s
            GROUP BY t.original_deposit_owner
        ) ftc ON ftc.agent_id = u.id
        LEFT JOIN (
            SELECT agent_id::bigint, SUM(ftc)::int AS target_ftc
            FROM targets
            WHERE date >= %(date_from)s
              AND date <  %(date_to_excl)s
            GROUP BY agent_id
        ) tgt ON tgt.agent_id = u.id
        LEFT JOIN (
            SELECT
                f.original_deposit_owner          AS agent_id,
                COUNT(DISTINCT f.accountid)       AS ftd100_cnt
            FROM ftd100_clients f
            WHERE f.ftd_100_date >= %(date_from)s
              AND f.ftd_100_date <  %(date_to_excl)s
            GROUP BY f.original_deposit_owner
        ) f100 ON f100.agent_id = u.id
        WHERE u.status = 'Active'
          AND u.department_ = 'Sales'
          AND u.team = 'Conversion'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
        ORDER BY u.office_name NULLS LAST, COALESCE(ftc.cnt, 0) DESC, u.agent_name
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            rows = cur.fetchall()
        data = [
            {
                "office_name": r[0],
                "agent_name":  r[1],
                "department":  r[2],
                "ftc":         r[3],
                "target_ftc":  r[4],
                "ftd100":      r[5],
            }
            for r in rows
        ]
        return JSONResponse(content={
            "rows":                 data,
            "total_ftc":            sum(r["ftc"] for r in data),
            "total_target_ftc":     sum(r["target_ftc"] for r in data),
            "total_ftd100":         sum(r["ftd100"] for r in data),
            "working_days":         working_days,
            "working_days_passed":  working_days_passed,
            "date_from":            date_from,
            "date_to":              date_to,
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
