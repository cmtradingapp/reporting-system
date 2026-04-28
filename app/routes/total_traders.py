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
from app.auth.role_filters import get_role_filter
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
        "is_admin": user.get("role") == "admin",
    })


@router.get("/api/total-traders/options")
async def total_traders_options(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    if not _has_access(user):
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})

    _ck = "total_traders_opts_v2"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    # All offices/teams where at least one Retention agent exists (matches the page filter).
    sql = """
        SELECT DISTINCT u.office_name, u.department
        FROM crm_users u
        WHERE u.department_ = 'Retention'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%'
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
    date_from: str = None,
    date_to: str = None,
    end_date: str = None,
    days_back: int = None,
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

    try:
        if date_from and date_to:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
            dt_end  = datetime.strptime(date_to,   "%Y-%m-%d").date()
            if dt_from > dt_end:
                dt_from, dt_end = dt_end, dt_from
        else:
            if not end_date:
                end_date = datetime.now(_TZ).date().strftime("%Y-%m-%d")
            dt_end = datetime.strptime(end_date, "%Y-%m-%d").date()
            days_back_val = max(1, min(int(days_back or 30), 365))
            dt_from = dt_end - timedelta(days=days_back_val - 1)
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date"})

    # Apply optional "days back" shrink within the chosen range
    if days_back:
        try:
            db = max(1, min(int(days_back), 365))
            new_from = dt_end - timedelta(days=db - 1)
            if new_from > dt_from:
                dt_from = new_from
        except (TypeError, ValueError):
            pass

    dt_excl = dt_end + timedelta(days=1)
    date_from    = dt_from.strftime("%Y-%m-%d")
    end_date     = dt_end.strftime("%Y-%m-%d")
    date_to_excl = dt_excl.strftime("%Y-%m-%d")

    ftc_groups_list = None
    if ftc_groups:
        parsed = [g.strip() for g in ftc_groups.split(",") if g.strip()]
        parsed_set = set(parsed)
        if parsed_set and parsed_set != _ALL_FTC_GROUPS:
            ftc_groups_list = parsed

    def _ck_part(v): return ",".join(sorted(v)) if v else ""
    _user_role = user.get("role", "")
    _ck = (f"total_traders_v9:{date_from}:{end_date}:{_ck_part(f_office)}:{_ck_part(f_team)}"
           f":{f_classification}:{ftc_groups}:{_user_role}")
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    params = {
        "date_from": date_from,
        "date_to_excl": date_to_excl,
        "ref_date": end_date,
    }
    filters_sql, _ = _build_filters(f_office, f_team, f_classification, ftc_groups_list, params)

    # Apply role-based filter (e.g. retention_cy sees only Cyprus office)
    rf = get_role_filter(user)
    role_sql = ""
    if rf['crm_where']:
        role_sql = rf['crm_where']
        for i, val in enumerate(rf['crm_params']):
            key = f'_rf{i}'
            role_sql = role_sql.replace('%s', f'%({key})s', 1)
            params[key] = val
    # Always join crm_users so we can apply the standard "test agent" exclusion used on other pages
    cu_join = "LEFT JOIN crm_users u ON u.id = a.assigned_to"
    base_excl = (
        "AND a.accountid IS NOT NULL AND a.accountid::text != ''\n          "
        "AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'\n          "
        "AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'\n          "
        "AND u.department_ = 'Retention'"
    )

    # ── Shared SQL fragments ──
    _score_case = (
        "CASE WHEN a.classification_int IS NOT NULL AND a.classification_int > 0"
        " THEN a.classification_int"
        " WHEN a.birth_date IS NOT NULL THEN CASE"
        " WHEN DATE_PART('year', AGE({ref}, a.birth_date::date)) BETWEEN 25 AND 29 THEN 4"
        " WHEN DATE_PART('year', AGE({ref}, a.birth_date::date)) BETWEEN 30 AND 34 THEN 5"
        " WHEN DATE_PART('year', AGE({ref}, a.birth_date::date)) BETWEEN 35 AND 44 THEN 6"
        " WHEN DATE_PART('year', AGE({ref}, a.birth_date::date)) >= 45 THEN 7"
        " ELSE NULL END ELSE NULL END"
    )
    _rt_where = f"""rt.accountid IS NOT NULL AND rt.accountid::text != ''
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
          AND u.department_ = 'Retention'
          AND rt.day >= %(date_from)s AND rt.day < %(date_to_excl)s
          {filters_sql}
          {role_sql}"""
    _txn_where = f"""t.transactionapproval = 'Approved'
          AND (t.deleted = 0 OR t.deleted IS NULL)
          AND t.transaction_type_name IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
          AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
          AND t.vtigeraccountid IS NOT NULL
          AND a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)
          AND a.accountid IS NOT NULL AND a.accountid::text != ''
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
          AND u.department_ = 'Retention'
          AND t.confirmation_time >= %(date_from)s
          AND t.confirmation_time < %(date_to_excl)s
          {filters_sql}
          {role_sql}"""
    _net_agg = """COALESCE(SUM(CASE
             WHEN t.transaction_type_name IN ('Deposit','Withdrawal Cancelled') THEN t.usdamount
             WHEN t.transaction_type_name IN ('Withdrawal','Deposit Cancelled') THEN -t.usdamount
           END), 0)::float"""

    is_admin = user.get("role") == "admin"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Q1: Traders daily + avg score daily + high quality traders daily
            cur.execute(f"""
                SELECT rt.day, COUNT(DISTINCT rt.accountid),
                       ROUND(AVG({_score_case.format(ref='rt.day')}), 2),
                       COUNT(DISTINCT CASE WHEN a.classification_int BETWEEN 6 AND 10 THEN rt.accountid END)
                FROM mv_retention_traders rt
                JOIN accounts a ON a.accountid = rt.accountid
                LEFT JOIN crm_users u ON u.id = rt.assigned_to
                WHERE {_rt_where}
                GROUP BY 1
            """, params)
            _rows = cur.fetchall()
            traders_map    = {r[0].strftime("%Y-%m-%d"): int(r[1]) for r in _rows}
            avg_score_map  = {r[0].strftime("%Y-%m-%d"): float(r[2]) for r in _rows if r[2] is not None}
            high_traders_map = {r[0].strftime("%Y-%m-%d"): int(r[3]) for r in _rows}

            # Q2: Traders total + avg score total (combined, was 2 queries)
            cur.execute(f"""
                SELECT COUNT(DISTINCT rt.accountid),
                       ROUND(AVG({_score_case.format(ref="%(ref_date)s::date")}), 2)
                FROM mv_retention_traders rt
                JOIN accounts a ON a.accountid = rt.accountid
                LEFT JOIN crm_users u ON u.id = rt.assigned_to
                WHERE {_rt_where}
            """, params)
            row = cur.fetchone()
            traders_total = int(row[0] or 0)
            avg_score_total = float(row[1]) if row[1] is not None else 0.0

            # Q3: Depositors daily + NET daily (combined, was 2 queries)
            cur.execute(f"""
                SELECT t.confirmation_time::date AS day,
                       COUNT(DISTINCT a.accountid) FILTER (WHERE t.transaction_type_name = 'Deposit'),
                       {_net_agg}
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                LEFT JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE {_txn_where}
                GROUP BY 1
            """, params)
            _rows = cur.fetchall()
            deps_map = {r[0].strftime("%Y-%m-%d"): int(r[1]) for r in _rows}
            net_map = {r[0].strftime("%Y-%m-%d"): round(float(r[2]), 2) for r in _rows}

            # Q4: Depositors total (COUNT DISTINCT across all days)
            cur.execute(f"""
                SELECT COUNT(DISTINCT a.accountid)
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                LEFT JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transaction_type_name = 'Deposit'
                  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
                  AND t.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)
                  AND a.accountid IS NOT NULL AND a.accountid::text != ''
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                  AND u.department_ = 'Retention'
                  AND t.confirmation_time >= %(date_from)s
                  AND t.confirmation_time < %(date_to_excl)s
                  {filters_sql}
                  {role_sql}
            """, params)
            deps_total = int(cur.fetchone()[0] or 0)

            # Q5: Matrix agents (separate table — crm_users)
            cur.execute(f"""
                SELECT u.id, COALESCE(u.office_name, 'N/A'),
                       COALESCE(u.department, 'N/A'),
                       COALESCE(u.agent_name, u.full_name, 'N/A')
                FROM crm_users u
                WHERE u.department_ = 'Retention'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                  {role_sql}
                ORDER BY u.office_name, u.department, u.agent_name
            """, params)
            agents = [{"id": r[0], "office_name": r[1], "dept_name": r[2], "agent_name": r[3]}
                      for r in cur.fetchall()]

            # Q6: Matrix traders + avg score per agent/day (combined, was 2 queries)
            cur.execute(f"""
                SELECT rt.assigned_to, rt.day, COUNT(DISTINCT rt.accountid),
                       ROUND(AVG({_score_case.format(ref='rt.day')}), 2)
                FROM mv_retention_traders rt
                JOIN accounts a ON a.accountid = rt.accountid
                LEFT JOIN crm_users u ON u.id = rt.assigned_to
                WHERE {_rt_where}
                GROUP BY 1, 2
            """, params)
            m_traders, m_avg_score = {}, {}
            for r in cur.fetchall():
                aid = int(r[0])
                day_str = r[1].strftime("%Y-%m-%d")
                m_traders.setdefault(aid, {})[day_str] = int(r[2])
                if r[3] is not None:
                    m_avg_score.setdefault(aid, {})[day_str] = float(r[3])

            # Q7: Matrix traders total + avg score total per agent (combined, was 2 queries)
            cur.execute(f"""
                SELECT rt.assigned_to, COUNT(DISTINCT rt.accountid),
                       ROUND(AVG({_score_case.format(ref="%(ref_date)s::date")}), 2)
                FROM mv_retention_traders rt
                JOIN accounts a ON a.accountid = rt.accountid
                LEFT JOIN crm_users u ON u.id = rt.assigned_to
                WHERE {_rt_where}
                GROUP BY 1
            """, params)
            m_traders_total, m_avg_score_total = {}, {}
            for r in cur.fetchall():
                aid = int(r[0])
                m_traders_total[aid] = int(r[1])
                if r[2] is not None:
                    m_avg_score_total[aid] = float(r[2])

            # Q8: Matrix depositors + NET per agent/day (combined, was 2 queries)
            cur.execute(f"""
                SELECT t.original_deposit_owner, t.confirmation_time::date,
                       COUNT(DISTINCT a.accountid) FILTER (WHERE t.transaction_type_name = 'Deposit'),
                       {_net_agg}
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                LEFT JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE {_txn_where}
                GROUP BY 1, 2
            """, params)
            m_deps, m_net = {}, {}
            for r in cur.fetchall():
                aid = int(r[0])
                day_str = r[1].strftime("%Y-%m-%d")
                if r[2]:
                    m_deps.setdefault(aid, {})[day_str] = int(r[2])
                m_net.setdefault(aid, {})[day_str] = round(float(r[3]), 2)

            # Q9: Matrix depositors total + NET total per agent (combined, was 2 queries)
            cur.execute(f"""
                SELECT t.original_deposit_owner,
                       COUNT(DISTINCT a.accountid) FILTER (WHERE t.transaction_type_name = 'Deposit'),
                       {_net_agg}
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                LEFT JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE {_txn_where}
                GROUP BY 1
            """, params)
            m_deps_total, m_net_total = {}, {}
            for r in cur.fetchall():
                aid = int(r[0])
                m_deps_total[aid] = int(r[1]) if r[1] else 0
                m_net_total[aid] = round(float(r[2]), 2)

            matrix_data = {
                "agents": agents,
                "traders": {str(k): v for k, v in m_traders.items()},
                "depositors": {str(k): v for k, v in m_deps.items()},
                "net": {str(k): v for k, v in m_net.items()},
                "totals_traders": {str(k): v for k, v in m_traders_total.items()},
                "totals_depositors": {str(k): v for k, v in m_deps_total.items()},
                "totals_net": {str(k): v for k, v in m_net_total.items()},
                "avg_score": {str(k): v for k, v in m_avg_score.items()},
                "totals_avg_score": {str(k): v for k, v in m_avg_score_total.items()},
            }
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()

    labels, traders_series, deps_series, avg_score_series, net_series, high_traders_series = [], [], [], [], [], []
    cur_d = dt_from
    while cur_d <= dt_end:
        # Skip weekends (Saturday=5, Sunday=6)
        if cur_d.weekday() < 5:
            key = cur_d.strftime("%Y-%m-%d")
            labels.append(key)
            traders_series.append(traders_map.get(key, 0))
            deps_series.append(deps_map.get(key, 0))
            avg_score_series.append(avg_score_map.get(key, 0))
            net_series.append(net_map.get(key, 0))
            high_traders_series.append(high_traders_map.get(key, 0))
        cur_d += timedelta(days=1)

    result = {
        "date_from": date_from,
        "date_to": end_date,
        "totals": {
            "traders": traders_total,
            "depositors": deps_total,
            "depositor_pct": round((deps_total / traders_total * 100), 2) if traders_total else 0.0,
            "avg_score": avg_score_total,
        },
        "series": {
            "labels": labels,
            "traders": traders_series,
            "depositors": deps_series,
            "avg_score": avg_score_series,
            "net": net_series,
            "high_traders": high_traders_series,
        },
    }
    if matrix_data:
        result["matrix"] = matrix_data
    cache.set(_ck, result)
    return JSONResponse(content=result)
