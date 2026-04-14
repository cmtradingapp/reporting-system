from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from datetime import date

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

FSA_COUNTRIES = ('CM','KE','SE','ZM','DK','NL','ES','FI','NO')

def _quarter_dates(year: int, quarter: int):
    q_start_month = (quarter - 1) * 3 + 1
    q_start = date(year, q_start_month, 1)
    q_end_month = q_start_month + 2
    if q_end_month == 3:
        q_end = date(year, 3, 31)
    elif q_end_month == 6:
        q_end = date(year, 6, 30)
    elif q_end_month == 9:
        q_end = date(year, 9, 30)
    else:
        q_end = date(year, 12, 31)
    q_end_excl = date(year + (1 if quarter == 4 else 0),
                      1 if quarter == 4 else q_end_month + 1, 1)
    return q_start, q_end, q_end_excl


@router.get("/fsa-report", response_class=HTMLResponse)
async def fsa_report_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if user.get("role") != "admin":
        return RedirectResponse(url="/performance")
    return templates.TemplateResponse("fsa_report.html", {"request": request, "current_user": user})


@router.get("/api/fsa-report/section3")
async def fsa_report_section3(request: Request, year: int = 2026, quarter: int = 1):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if user.get("role") != "admin":
        return JSONResponse({"error": "forbidden"}, status_code=403)

    q_start, q_end, q_end_excl = _quarter_dates(year, quarter)

    base_filter = """
        funded = 1
        AND is_test_account = 0
        AND (sales_rep_id IS NULL OR sales_rep_id != 3303)
        AND country_iso IN ('CM','KE','SE','ZM','DK','NL','ES','FI','NO')
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Query 1: Active/Inactive counts BOP + EOP
            cur.execute(f"""
                SELECT
                  SUM(CASE WHEN compliance_status IN ('4','9') AND createdtime < %(q_start)s THEN 1 ELSE 0 END) AS active_bop,
                  SUM(CASE WHEN compliance_status NOT IN ('4','9') AND createdtime < %(q_start)s THEN 1 ELSE 0 END) AS inactive_bop,
                  SUM(CASE WHEN compliance_status IN ('4','9') AND createdtime < %(q_end_excl)s THEN 1 ELSE 0 END) AS active_eop,
                  SUM(CASE WHEN compliance_status NOT IN ('4','9') AND createdtime < %(q_end_excl)s THEN 1 ELSE 0 END) AS inactive_eop
                FROM accounts
                WHERE {base_filter}
            """, {"q_start": q_start, "q_end_excl": q_end_excl})
            row = cur.fetchone()
            counts = {
                "active_bop": row[0] or 0,
                "inactive_bop": row[1] or 0,
                "active_eop": row[2] or 0,
                "inactive_eop": row[3] or 0,
            }

            # Query 2: Clients' Funds from daily_equity_zeroed (last day of quarter)
            # Find the latest snapshot day on or before quarter end
            cur.execute(f"""
                SELECT COALESCE(SUM(GREATEST(dez.end_equity_zeroed, 0)), 0)
                FROM daily_equity_zeroed dez
                JOIN trading_accounts ta ON ta.login::bigint = dez.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE dez.day = (
                    SELECT MAX(day) FROM daily_equity_zeroed WHERE day <= %(q_end)s
                )
                  AND a.is_test_account = 0
                  AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
                  AND a.country_iso IN ('CM','KE','SE','ZM','DK','NL','ES','FI','NO')
            """, {"q_end": q_end})
            clients_funds = float(cur.fetchone()[0])

            # Query 3: Age groups
            cur.execute(f"""
                SELECT
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) < 18 THEN 1 ELSE 0 END) AS under_18,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) BETWEEN 36 AND 45 THEN 1 ELSE 0 END) AS age_36_45,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) BETWEEN 46 AND 55 THEN 1 ELSE 0 END) AS age_46_55,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) BETWEEN 56 AND 65 THEN 1 ELSE 0 END) AS age_56_65,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) > 65 THEN 1 ELSE 0 END) AS age_over_65
                FROM accounts
                WHERE {base_filter}
                  AND compliance_status IN ('4','9')
                  AND createdtime < %(q_end_excl)s
                  AND birth_date IS NOT NULL
            """, {"q_end": q_end, "q_end_excl": q_end_excl})
            age_row = cur.fetchone()
            age_groups = {
                "under_18": age_row[0] or 0,
                "18_25": age_row[1] or 0,
                "26_35": age_row[2] or 0,
                "36_45": age_row[3] or 0,
                "46_55": age_row[4] or 0,
                "56_65": age_row[5] or 0,
                "over_65": age_row[6] or 0,
            }

            # Query 4: Classification of active clients (PEP + total active EOP)
            cur.execute(f"""
                SELECT
                  COUNT(*) AS total_active,
                  SUM(CASE WHEN pep_sanctions = 1 THEN 1 ELSE 0 END) AS pep_count
                FROM accounts
                WHERE {base_filter}
                  AND compliance_status IN ('4','9')
                  AND createdtime < %(q_end_excl)s
            """, {"q_end_excl": q_end_excl})
            cls_row = cur.fetchone()
            classification = {
                "total_active": cls_row[0] or 0,
                "pep": cls_row[1] or 0,
            }

        return JSONResponse({
            "counts": counts,
            "clients_funds": clients_funds,
            "age_groups": age_groups,
            "classification": classification,
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        conn.close()
