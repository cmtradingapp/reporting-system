"""
Total Number of Traders report.

Mirrors the Power BI "Total Traders" view:
- KPIs: Total Traders, Total Depositors (unique accounts over the date range)
- Daily line chart: traders (FTC Date logic) + depositors (Approved Deposit tx)
- Filters: date range (end date + lookback days), office, team, client classification, FTC groups

Trader definition (same as FTC Date / Marketing Performance):
  accounts having at least one trade in dealio_positions OR a closed
  dealio_trades_mt5 pair (entry=1 joined to entry=0) with notional_value > 0,
  grouped by d.open_time::date.

Depositor definition:
  accounts with at least one Approved Deposit transaction in the period.
"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app import cache

_TZ = ZoneInfo("Europe/Nicosia")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


_FTC_GROUP_SQL = {
    "0 - 7 days":    "(%(ref_date)s::date - a.client_qualification_date::date) BETWEEN 0 AND 7",
    "8 - 14 days":   "(%(ref_date)s::date - a.client_qualification_date::date) BETWEEN 8 AND 14",
    "15 - 30 days":  "(%(ref_date)s::date - a.client_qualification_date::date) BETWEEN 15 AND 30",
    "31 - 60 days":  "(%(ref_date)s::date - a.client_qualification_date::date) BETWEEN 31 AND 60",
    "61 - 90 days":  "(%(ref_date)s::date - a.client_qualification_date::date) BETWEEN 61 AND 90",
    "91 - 120 days": "(%(ref_date)s::date - a.client_qualification_date::date) BETWEEN 91 AND 120",
    "120+ days":     "(%(ref_date)s::date - a.client_qualification_date::date) > 120",
}
_ALL_FTC_GROUPS = set(_FTC_GROUP_SQL.keys())


def _has_access(user) -> bool:
    if user.get("role") == "admin":
        return True
    ap = user.get("allowed_pages_list")
    return ap is not None and "total_traders" in ap


@router.get("/total-traders", response_class=HTMLResponse)
async def total_traders_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if not _has_access(user):
        return RedirectResponse(url="/performance", status_code=302)
    return templates.TemplateResponse("total_traders.html", {
        "request": request,
        "current_user": user,
    })


@router.get("/api/total-traders/options")
async def total_traders_options(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    if not _has_access(user):
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})

    _ck = "total_traders_opts_v1"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    sql = """
        SELECT DISTINCT u.office_name, u.department
        FROM crm_users u
        WHERE u.id IN (SELECT DISTINCT assigned_to FROM accounts WHERE assigned_to IS NOT NULL)
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        offices = sorted({r[0] for r in rows if r[0]})
        teams   = sorted({r[1] for r in rows if r[1]})
        result = {"offices": offices, "teams": teams}
        cache.set(_ck, result)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


def _build_filters(office, team, classification, ftc_groups_list, params):
    """Filters on `a.*` and `u.*` (crm_users as u). Returns (where_sql, needs_cu_join)."""
    clauses = []
    needs_cu = False
    if office:
        clauses.append("AND u.office_name = ANY(%(f_office)s)")
        params["f_office"] = office
        needs_cu = True
    if team:
        clauses.append("AND u.department = ANY(%(f_team)s)")
        params["f_team"] = team
        needs_cu = True
    if classification == "High Quality":
        clauses.append("AND a.classification_int BETWEEN 6 AND 10")
    elif classification == "Low Quality":
        clauses.append("AND a.classification_int BETWEEN 1 AND 5")
    elif classification == "No segmentation":
        clauses.append("AND (a.classification_int IS NULL OR a.classification_int NOT BETWEEN 1 AND 10)")
    if ftc_groups_list:
        or_clauses = [_FTC_GROUP_SQL[g] for g in ftc_groups_list if g in _FTC_GROUP_SQL]
        if or_clauses:
            clauses.append("AND a.client_qualification_date IS NOT NULL")
            clauses.append("AND (" + " OR ".join(or_clauses) + ")")
    return ("\n      ".join(clauses), needs_cu)


@router.get("/api/total-traders")
async def total_traders_api(
    request: Request,
    end_date: str = None,
    days_back: int = 30,
    f_office: Optional[List[str]] = Query(default=None),
    f_team: Optional[List[str]] = Query(default=None),
    f_classification: str = None,
    ftc_groups: str = None,
):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    if not _has_access(user):
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})

    if not end_date:
        end_date = datetime.now(_TZ).date().strftime("%Y-%m-%d")
    try:
        dt_end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid end_date"})

    days_back = max(1, min(int(days_back or 30), 365))
    dt_from = dt_end - timedelta(days=days_back - 1)
    dt_excl = dt_end + timedelta(days=1)
    date_from = dt_from.strftime("%Y-%m-%d")
    date_to_excl = dt_excl.strftime("%Y-%m-%d")

    ftc_groups_list = None
    if ftc_groups:
        parsed = [g.strip() for g in ftc_groups.split(",") if g.strip()]
        parsed_set = set(parsed)
        if parsed_set and parsed_set != _ALL_FTC_GROUPS:
            ftc_groups_list = parsed

    def _ck_part(v): return ",".join(sorted(v)) if v else ""
    _ck = (f"total_traders_v1:{end_date}:{days_back}:{_ck_part(f_office)}:{_ck_part(f_team)}"
           f":{f_classification}:{ftc_groups}")
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    params = {
        "date_from": date_from,
        "date_to_excl": date_to_excl,
        "ref_date": end_date,
    }
    filters_sql, needs_cu = _build_filters(f_office, f_team, f_classification, ftc_groups_list, params)
    cu_join = "LEFT JOIN crm_users u ON u.id = a.assigned_to" if needs_cu else ""

    # ── Traders: daily + total ────────────────────────────────────────────
    traders_daily_sql = f"""
        WITH d AS (
            SELECT p.login, p.notional_value, p.open_time FROM dealio_positions p
            UNION ALL
            SELECT ex.login, ex.notional_value, en.open_time
            FROM dealio_trades_mt5 ex
            JOIN dealio_trades_mt5 en
              ON en.position_id = ex.position_id
             AND en.source_id   = ex.source_id
             AND en.entry       = 0
            WHERE ex.entry = 1 AND ex.close_time > '1971-01-01'
        )
        SELECT d.open_time::date AS day, COUNT(DISTINCT a.accountid) AS cnt
        FROM d
        JOIN trading_accounts ta ON ta.login::bigint = d.login::bigint
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        {cu_join}
        WHERE d.notional_value > 0
          AND ta.vtigeraccountid IS NOT NULL AND ta.vtigeraccountid::text != ''
          AND a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)
          AND d.open_time::date >= %(date_from)s
          AND d.open_time::date <  %(date_to_excl)s
          {filters_sql}
        GROUP BY 1
    """

    traders_total_sql = f"""
        WITH d AS (
            SELECT p.login, p.notional_value, p.open_time FROM dealio_positions p
            UNION ALL
            SELECT ex.login, ex.notional_value, en.open_time
            FROM dealio_trades_mt5 ex
            JOIN dealio_trades_mt5 en
              ON en.position_id = ex.position_id
             AND en.source_id   = ex.source_id
             AND en.entry       = 0
            WHERE ex.entry = 1 AND ex.close_time > '1971-01-01'
        )
        SELECT COUNT(DISTINCT a.accountid)
        FROM d
        JOIN trading_accounts ta ON ta.login::bigint = d.login::bigint
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        {cu_join}
        WHERE d.notional_value > 0
          AND ta.vtigeraccountid IS NOT NULL AND ta.vtigeraccountid::text != ''
          AND a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)
          AND d.open_time::date >= %(date_from)s
          AND d.open_time::date <  %(date_to_excl)s
          {filters_sql}
    """

    # ── Depositors: daily + total (distinct accountids with Approved Deposit) ─
    depositors_daily_sql = f"""
        SELECT t.confirmation_time::date AS day, COUNT(DISTINCT a.accountid) AS cnt
        FROM transactions t
        JOIN accounts a ON a.accountid = t.vtigeraccountid
        {cu_join}
        WHERE t.transactionapproval = 'Approved'
          AND (t.deleted = 0 OR t.deleted IS NULL)
          AND t.transaction_type_name = 'Deposit'
          AND t.vtigeraccountid IS NOT NULL
          AND a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)
          AND t.confirmation_time::date >= %(date_from)s
          AND t.confirmation_time::date <  %(date_to_excl)s
          {filters_sql}
        GROUP BY 1
    """

    depositors_total_sql = f"""
        SELECT COUNT(DISTINCT a.accountid)
        FROM transactions t
        JOIN accounts a ON a.accountid = t.vtigeraccountid
        {cu_join}
        WHERE t.transactionapproval = 'Approved'
          AND (t.deleted = 0 OR t.deleted IS NULL)
          AND t.transaction_type_name = 'Deposit'
          AND t.vtigeraccountid IS NOT NULL
          AND a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)
          AND t.confirmation_time::date >= %(date_from)s
          AND t.confirmation_time::date <  %(date_to_excl)s
          {filters_sql}
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(traders_daily_sql, params)
            traders_map = {r[0].strftime("%Y-%m-%d"): int(r[1]) for r in cur.fetchall()}

            cur.execute(traders_total_sql, params)
            traders_total = int(cur.fetchone()[0] or 0)

            cur.execute(depositors_daily_sql, params)
            deps_map = {r[0].strftime("%Y-%m-%d"): int(r[1]) for r in cur.fetchall()}

            cur.execute(depositors_total_sql, params)
            deps_total = int(cur.fetchone()[0] or 0)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()

    labels, traders_series, deps_series = [], [], []
    cur_d = dt_from
    while cur_d <= dt_end:
        key = cur_d.strftime("%Y-%m-%d")
        labels.append(key)
        traders_series.append(traders_map.get(key, 0))
        deps_series.append(deps_map.get(key, 0))
        cur_d += timedelta(days=1)

    result = {
        "date_from": date_from,
        "date_to": end_date,
        "days_back": days_back,
        "totals": {
            "traders": traders_total,
            "depositors": deps_total,
        },
        "series": {
            "labels": labels,
            "traders": traders_series,
            "depositors": deps_series,
        },
    }
    cache.set(_ck, result)
    return JSONResponse(content=result)
