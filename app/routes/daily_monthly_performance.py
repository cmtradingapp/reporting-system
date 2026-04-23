from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.auth.role_filters import get_role_filter
from app.db.postgres_conn import get_connection
from app import cache
from datetime import datetime, timedelta, date as date_type
from app.routes.scoreboard import (
    _apply_role_filter, _build_cls_filter, count_working_days,
    last_day_of_month, _CLS_KPIS_SQL, _TZ, _TARGETS_CUTOFF,
)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

_PAGE_URLS = {
    "performance": "/performance", "agent_bonuses": "/agent-bonuses",
    "marketing": "/campaign-performance", "dashboard": "/dashboard",
    "total_traders": "/total-traders", "data_sync": "/data-sync",
}


@router.get("/daily-monthly", response_class=HTMLResponse)
async def daily_monthly_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    role = user.get("role", "")
    ap = user.get("allowed_pages_list")
    if role != "admin" and (ap is None or "daily_monthly" not in ap):
        for p, url in _PAGE_URLS.items():
            if ap is None or p in ap:
                return RedirectResponse(url=url, status_code=302)
        return RedirectResponse(url="/performance", status_code=302)
    if role == "agent":
        dept = user.get("department_") or ""
        show_sales = dept != "Retention"
        show_retention = dept != "Sales"
    else:
        show_sales = not role.startswith("retention_")
        show_retention = not role.startswith("sales_")
    return templates.TemplateResponse("daily_monthly_performance.html", {
        "request": request,
        "current_user": user,
        "show_sales": show_sales,
        "show_retention": show_retention,
    })


@router.get("/api/daily-monthly/sales")
async def dmp_sales_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    role_filter = get_role_filter(user)
    cls_where, cls_params, cls_suffix = _build_cls_filter(request)
    has_cls = bool(cls_where)
    _ck = f"dmp_sales_v2:{user.get('role','')}:{date_from}:{date_to}{cls_suffix}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt_to   = datetime.strptime(date_to,   "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    _kpi_tbl = "_cls_kpis" if has_cls else "mv_daily_kpis"

    sql = """
        SELECT
            COALESCE(u.office_name, 'N/A')              AS office_name,
            COALESCE(u.agent_name, u.full_name, 'N/A')  AS agent_name,
            COALESCE(u.department_, '')                  AS department_,
            COALESCE(mv.ftc, 0)::int                     AS ftc,
            COALESCE(tgt.target_ftc, 0)                  AS target_ftc,
            COALESCE(f100.ftd100_cnt, 0)                 AS ftd100,
            COALESCE(mv.net_usd, 0)::float               AS net_deposits,
            COALESCE(mv.ftd_count, 0)::int               AS ftd_count,
            COALESCE(dk.daily_ftd, 0)::int               AS daily_ftd,
            COALESCE(dk.daily_ftc, 0)::int               AS daily_ftc,
            COALESCE(dk.daily_net, 0)::float             AS daily_net,
            COALESCE(u.status, '')                        AS status,
            scp.avg_scp                                   AS avg_scp,
            daily_scp.daily_avg_scp                       AS daily_avg_scp
        FROM crm_users u
        LEFT JOIN (
            SELECT
                k.agent_id,
                SUM(CASE WHEN k.qual_date >= %(date_from)s AND k.qual_date < %(date_to_excl)s
                         THEN k.ftc_count ELSE 0 END)::int  AS ftc,
                SUM(CASE WHEN k.tx_date  >= %(date_from)s AND k.tx_date  < %(date_to_excl)s
                         THEN k.net_usd  ELSE 0 END)        AS net_usd,
                SUM(CASE WHEN k.tx_date  >= %(date_from)s AND k.tx_date  < %(date_to_excl)s
                         THEN k.ftd_count ELSE 0 END)::int  AS ftd_count
            FROM {_kpi_tbl} k
            WHERE (k.qual_date >= %(date_from)s AND k.qual_date < %(date_to_excl)s)
               OR (k.tx_date   >= %(date_from)s AND k.tx_date   < %(date_to_excl)s)
            GROUP BY k.agent_id
        ) mv ON mv.agent_id = u.id
        LEFT JOIN (
            SELECT
                k.agent_id,
                SUM(CASE WHEN k.tx_date  = %(date_to)s THEN k.ftd_count ELSE 0 END)::int   AS daily_ftd,
                SUM(CASE WHEN k.qual_date = %(date_to)s THEN k.ftc_count ELSE 0 END)::int   AS daily_ftc,
                SUM(CASE WHEN k.tx_date  = %(date_to)s THEN k.net_usd   ELSE 0 END)::float  AS daily_net
            FROM {_kpi_tbl} k
            WHERE k.tx_date = %(date_to)s OR k.qual_date = %(date_to)s
            GROUP BY k.agent_id
        ) dk ON dk.agent_id = u.id
        LEFT JOIN ({tgt_subq}) tgt ON tgt.agent_id = u.id
        LEFT JOIN (
            SELECT f.original_deposit_owner AS agent_id,
                   COUNT(DISTINCT f.accountid) AS ftd100_cnt
            FROM ftd100_clients f
            {ftd100_cls_join}
            WHERE f.ftd_100_date >= %(date_from)s
              AND f.ftd_100_date <  %(date_to_excl)s
              {ftd100_cls_where}
            GROUP BY f.original_deposit_owner
        ) f100 ON f100.agent_id = u.id
        LEFT JOIN (
            SELECT sub.agent_id,
                   ROUND(AVG(sub.score)::numeric, 1) AS avg_scp
            FROM (
                SELECT t.original_deposit_owner AS agent_id,
                       t.vtigeraccountid AS accountid,
                       CASE
                         WHEN a.classification_int IS NOT NULL AND a.classification_int > 0
                           THEN a.classification_int
                         WHEN a.birth_date IS NOT NULL THEN CASE
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 25 AND 29 THEN 4
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 30 AND 34 THEN 5
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 35 AND 44 THEN 6
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) >= 45 THEN 7
                           ELSE NULL END
                         ELSE NULL END AS score
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transaction_type_name = 'Deposit'
                  AND t.ftd = 1
                  AND t.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0
                  AND t.confirmation_time >= %(date_from)s::timestamp
                  AND t.confirmation_time <  %(date_to_excl)s::timestamp
                GROUP BY t.original_deposit_owner, t.vtigeraccountid,
                         a.classification_int, a.birth_date
            ) sub
            WHERE sub.score IS NOT NULL
            GROUP BY sub.agent_id
        ) scp ON scp.agent_id = u.id
        LEFT JOIN (
            SELECT sub.agent_id,
                   ROUND(AVG(sub.score)::numeric, 1) AS daily_avg_scp
            FROM (
                SELECT t.original_deposit_owner AS agent_id,
                       t.vtigeraccountid AS accountid,
                       CASE
                         WHEN a.classification_int IS NOT NULL AND a.classification_int > 0
                           THEN a.classification_int
                         WHEN a.birth_date IS NOT NULL THEN CASE
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 25 AND 29 THEN 4
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 30 AND 34 THEN 5
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 35 AND 44 THEN 6
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) >= 45 THEN 7
                           ELSE NULL END
                         ELSE NULL END AS score
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transaction_type_name = 'Deposit'
                  AND t.ftd = 1
                  AND t.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0
                  AND t.confirmation_time >= %(date_to)s::timestamp
                  AND t.confirmation_time <  (%(date_to)s::date + 1)::timestamp
                GROUP BY t.original_deposit_owner, t.vtigeraccountid,
                         a.classification_int, a.birth_date
            ) sub
            WHERE sub.score IS NOT NULL
            GROUP BY sub.agent_id
        ) daily_scp ON daily_scp.agent_id = u.id
        WHERE u.department_ = 'Sales'
          AND u.team = 'Conversion'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
          {role_filter}
        ORDER BY u.office_name NULLS LAST, COALESCE(mv.ftc, 0) DESC, u.agent_name
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT holiday_date FROM public_holidays")
                holidays = {row[0] for row in cur.fetchall()}
            except Exception:
                conn.rollback()
                holidays = set()

            if dt_from >= _TARGETS_CUTOFF:
                _tgt_subq = """SELECT crm_user_id AS agent_id, monthly_ftd100_target AS target_ftc
                    FROM agent_targets_history
                    WHERE report_month = DATE_TRUNC('month', %(date_from)s::date)
                      AND crm_user_id IS NOT NULL"""
            else:
                _tgt_subq = """SELECT agent_id::int AS agent_id, ftc::int AS target_ftc
                    FROM targets
                    WHERE date = DATE_TRUNC('month', %(date_from)s::date)"""
            base_params = {"date_from": date_from, "date_to_excl": date_to_exclusive, "date_to": date_to}

            if has_cls:
                cur.execute(
                    _CLS_KPIS_SQL.replace("{cls_where}", cls_where),
                    {**base_params, **cls_params},
                )

            _ftd100_join = "JOIN accounts a ON a.accountid = f.accountid" if has_cls else ""
            _ftd100_where = cls_where if has_cls else ""
            _prepared_sql = (sql
                .replace("{_kpi_tbl}", _kpi_tbl)
                .replace("{tgt_subq}", _tgt_subq)
                .replace("{ftd100_cls_join}", _ftd100_join)
                .replace("{ftd100_cls_where}", _ftd100_where))
            final_sql, final_params = _apply_role_filter(_prepared_sql, {**base_params, **cls_params}, role_filter)
            cur.execute(final_sql, final_params)
            rows = cur.fetchall()

        data = [
            {
                "office_name":  r[0],
                "agent_name":   r[1],
                "department":   r[2],
                "ftc":          r[3],
                "target_ftc":   r[4],
                "ftd100":       r[5],
                "net_deposits": round(r[6], 2),
                "ftd_count":    r[7],
                "daily_ftd":    int(r[8] or 0),
                "daily_ftc":    int(r[9] or 0),
                "daily_net":    round(float(r[10] or 0), 2),
                "status":       r[11] or '',
                "avg_scp":       round(float(r[12]), 1) if r[12] is not None else None,
                "daily_avg_scp": round(float(r[13]), 1) if r[13] is not None else None,
            }
            for r in rows
        ]

        today = datetime.now(_TZ).date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        month_start   = dt_from.replace(day=1)
        wd_full_month = count_working_days(month_start, month_end, holidays)
        wd_in_range   = count_working_days(dt_from, dt_to, holidays)
        target_ratio  = 1.0 if dt_from.day == 1 or wd_full_month == 0 else round(wd_in_range / wd_full_month, 6)

        # ── Unassigned Sales Target ────────────────────────────────
        with conn.cursor() as cur2:
            cur2.execute(
                "SELECT sales_ftc FROM company_targets WHERE month = DATE_TRUNC('month', %s::date)",
                (date_from,),
            )
            ct_row = cur2.fetchone()
        if ct_row and ct_row[0]:
            company_ftc = float(ct_row[0])
            agent_raw_sum = sum(r["target_ftc"] for r in data)
            gap = company_ftc - agent_raw_sum
            if gap > 0:
                data.append({
                    "office_name":  "Unassigned Sales Target",
                    "agent_name":   "Unassigned Sales Target",
                    "department":   "Sales",
                    "ftc":          0,
                    "target_ftc":   round(gap),
                    "ftd100":       0,
                    "net_deposits": 0,
                    "ftd_count":    0,
                    "daily_ftd":    0,
                    "daily_ftc":    0,
                    "daily_net":    0,
                    "status":       "Active",
                    "avg_scp":      None,
                })

        # ── Global totals (all agents, all departments — for KPI cards) ──
        g_daily_ftd = g_monthly_ftd = g_daily_ftc = g_monthly_ftc = 0
        g_daily_net = g_monthly_net = 0.0
        try:
            with conn.cursor() as cur_g:
                cur_g.execute("""
                    SELECT
                        COALESCE(SUM(CASE WHEN k.tx_date = %(date_to)s  THEN k.ftd_count ELSE 0 END), 0)::int,
                        COALESCE(SUM(CASE WHEN k.tx_date >= %(date_from)s AND k.tx_date < %(date_to_excl)s THEN k.ftd_count ELSE 0 END), 0)::int,
                        COALESCE(SUM(CASE WHEN k.qual_date = %(date_to)s THEN k.ftc_count ELSE 0 END), 0)::int,
                        COALESCE(SUM(CASE WHEN k.qual_date >= %(date_from)s AND k.qual_date < %(date_to_excl)s THEN k.ftc_count ELSE 0 END), 0)::int,
                        COALESCE(SUM(CASE WHEN k.tx_date = %(date_to)s  THEN k.net_usd ELSE 0 END), 0)::float,
                        COALESCE(SUM(CASE WHEN k.tx_date >= %(date_from)s AND k.tx_date < %(date_to_excl)s THEN k.net_usd ELSE 0 END), 0)::float
                    FROM mv_daily_kpis k
                    LEFT JOIN crm_users u ON u.id = k.agent_id
                    WHERE TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                      AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
                """, {"date_from": date_from, "date_to_excl": date_to_exclusive, "date_to": date_to})
                gr = cur_g.fetchone()
                if gr:
                    g_daily_ftd, g_monthly_ftd, g_daily_ftc, g_monthly_ftc = int(gr[0]), int(gr[1]), int(gr[2]), int(gr[3])
                    g_daily_net, g_monthly_net = round(float(gr[4]), 2), round(float(gr[5]), 2)
        except Exception as _ge:
            print(f"[dmp_sales] global totals query failed: {_ge}")

        _result = {
            "rows":                 data,
            "working_days":         working_days,
            "working_days_passed":  working_days_passed,
            "working_days_left":    working_days_left,
            "target_ratio":         target_ratio,
            "wd_in_range":          wd_in_range,
            "date_from":            date_from,
            "date_to":              date_to,
            "global_daily_ftd":     g_daily_ftd,
            "global_monthly_ftd":   g_monthly_ftd,
            "global_daily_ftc":     g_daily_ftc,
            "global_monthly_ftc":   g_monthly_ftc,
            "global_daily_net":     g_daily_net,
            "global_monthly_net":   g_monthly_net,
        }
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


@router.get("/api/daily-monthly/retention")
async def dmp_retention_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    role_filter = get_role_filter(user)
    cls_where, cls_params, cls_suffix = _build_cls_filter(request)
    has_cls = bool(cls_where)
    _ck = f"dmp_ret_v2:{user.get('role','')}:{date_from}:{date_to}{cls_suffix}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt_to   = datetime.strptime(date_to,   "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
        last_day = last_day_of_month(dt_from).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    _kpi_tbl = "_cls_kpis" if has_cls else "mv_daily_kpis"

    sql = """
        SELECT
            COALESCE(u.office_name, 'N/A')                   AS office_name,
            COALESCE(u.department, 'N/A')                     AS dept_name,
            COALESCE(u.agent_name, u.full_name, 'N/A')        AS agent_name,
            COALESCE(tgt.monthly_target_net, 0)::float        AS target_net,
            COALESCE(mv.net_usd, 0)::float                    AS net_usd,
            COALESCE(mv.deposit_usd, 0)::float                AS deposit_usd,
            COALESCE(vol.open_volume_usd, 0)::float           AS open_volume_usd,
            COALESCE(dk.daily_net_usd, 0)::float              AS daily_net_usd,
            COALESCE(u.status, '')                             AS status,
            COALESCE(std.std_count, 0)::int                   AS std_count,
            COALESCE(trd.traders_count, 0)::int               AS traders_count,
            u.id                                              AS user_id,
            COALESCE(trd.daily_traders, 0)::int               AS daily_traders,
            COALESCE(loads.daily_loads, 0)::int               AS daily_loads,
            COALESCE(dt_std.daily_std, 0)::int                AS daily_std,
            COALESCE(loads.monthly_loads, 0)::int             AS monthly_loads,
            daily_scp.daily_avg_scp                           AS daily_avg_scp,
            monthly_scp.monthly_avg_scp                       AS monthly_avg_scp
        FROM crm_users u
        LEFT JOIN ({tgt_subq}) tgt ON tgt.agent_id = u.id
        LEFT JOIN (
            SELECT k.agent_id,
                   SUM(k.net_usd)     AS net_usd,
                   SUM(k.deposit_usd) AS deposit_usd
            FROM {_kpi_tbl} k
            WHERE k.tx_date >= %(date_from)s AND k.tx_date < %(date_to_excl)s
            GROUP BY k.agent_id
        ) mv ON mv.agent_id = u.id
        LEFT JOIN (
            SELECT agent_id, SUM(notional_usd)::float AS open_volume_usd
            FROM mv_volume_stats
            WHERE open_date >= %(date_from)s AND open_date <= %(date_to)s
            GROUP BY agent_id
        ) vol ON vol.agent_id = u.id
        LEFT JOIN (
            SELECT agent_id, SUM(net_usd)::float AS daily_net_usd
            FROM {_kpi_tbl}
            WHERE tx_date = %(date_to)s
            GROUP BY agent_id
        ) dk ON dk.agent_id = u.id
        LEFT JOIN (
            SELECT assigned_to AS agent_id, COUNT(DISTINCT accountid) AS std_count
            FROM mv_std_clients
            WHERE has_second_deposit = 1
              AND second_deposit_date::date >= %(date_from)s::date
              AND second_deposit_date::date < %(date_to_excl)s::date
            GROUP BY assigned_to
        ) std ON std.agent_id = u.id
        LEFT JOIN (
            SELECT rt.assigned_to AS agent_id,
                   COUNT(DISTINCT rt.accountid) AS traders_count,
                   COUNT(DISTINCT CASE WHEN rt.day = %(date_to)s::date THEN rt.accountid END)::int AS daily_traders
            FROM mv_retention_traders rt
            JOIN accounts a ON a.accountid = rt.accountid
            WHERE rt.day >= %(date_from)s::date
              AND rt.day < %(date_to_excl)s::date
              AND a.is_test_account = 0
              AND (a.is_demo = 0 OR a.is_demo IS NULL)
              AND a.accountid IS NOT NULL AND a.accountid::text != ''
            GROUP BY rt.assigned_to
        ) trd ON trd.agent_id = u.id
        LEFT JOIN (
            SELECT t.original_deposit_owner AS agent_id,
                   COUNT(DISTINCT a.accountid)::int AS monthly_loads,
                   COUNT(DISTINCT CASE WHEN t.confirmation_time >= %(date_to)s::timestamp
                                        AND t.confirmation_time < (%(date_to)s::date + 1)::timestamp
                                  THEN a.accountid END)::int AS daily_loads
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transaction_type_name = 'Deposit'
              AND t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
              AND a.is_test_account = 0
              AND (a.is_demo = 0 OR a.is_demo IS NULL)
              AND a.accountid IS NOT NULL AND a.accountid::text != ''
              AND t.vtigeraccountid IS NOT NULL
              AND t.confirmation_time >= %(date_from)s::timestamp
              AND t.confirmation_time < %(date_to_excl)s::timestamp
            GROUP BY t.original_deposit_owner
        ) loads ON loads.agent_id = u.id
        LEFT JOIN (
            SELECT assigned_to AS agent_id, COUNT(DISTINCT accountid)::int AS daily_std
            FROM mv_std_clients
            WHERE has_second_deposit = 1
              AND second_deposit_date::date = %(date_to)s::date
            GROUP BY assigned_to
        ) dt_std ON dt_std.agent_id = u.id
        LEFT JOIN (
            SELECT sub.agent_id,
                   ROUND(AVG(sub.score)::numeric, 1) AS daily_avg_scp
            FROM (
                SELECT rt.assigned_to AS agent_id,
                       a.accountid,
                       CASE
                         WHEN a.classification_int IS NOT NULL AND a.classification_int > 0
                           THEN a.classification_int
                         WHEN a.birth_date IS NOT NULL THEN CASE
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 25 AND 29 THEN 4
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 30 AND 34 THEN 5
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 35 AND 44 THEN 6
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) >= 45 THEN 7
                           ELSE NULL END
                         ELSE NULL END AS score
                FROM mv_retention_traders rt
                JOIN accounts a ON a.accountid = rt.accountid
                WHERE rt.day = %(date_to)s::date
                  AND a.is_test_account = 0
                  AND (a.is_demo = 0 OR a.is_demo IS NULL)
                  AND a.accountid IS NOT NULL AND a.accountid::text != ''
                GROUP BY rt.assigned_to, a.accountid, a.classification_int, a.birth_date
            ) sub
            WHERE sub.score IS NOT NULL
            GROUP BY sub.agent_id
        ) daily_scp ON daily_scp.agent_id = u.id
        LEFT JOIN (
            SELECT sub.agent_id,
                   ROUND(AVG(sub.score)::numeric, 1) AS monthly_avg_scp
            FROM (
                SELECT rt.assigned_to AS agent_id,
                       a.accountid,
                       CASE
                         WHEN a.classification_int IS NOT NULL AND a.classification_int > 0
                           THEN a.classification_int
                         WHEN a.birth_date IS NOT NULL THEN CASE
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 25 AND 29 THEN 4
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 30 AND 34 THEN 5
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) BETWEEN 35 AND 44 THEN 6
                           WHEN DATE_PART('year', AGE(CURRENT_DATE, a.birth_date::date)) >= 45 THEN 7
                           ELSE NULL END
                         ELSE NULL END AS score
                FROM mv_retention_traders rt
                JOIN accounts a ON a.accountid = rt.accountid
                WHERE rt.day >= %(date_from)s::date
                  AND rt.day < %(date_to_excl)s::date
                  AND a.is_test_account = 0
                  AND (a.is_demo = 0 OR a.is_demo IS NULL)
                  AND a.accountid IS NOT NULL AND a.accountid::text != ''
                GROUP BY rt.assigned_to, a.accountid, a.classification_int, a.birth_date
            ) sub
            WHERE sub.score IS NOT NULL
            GROUP BY sub.agent_id
        ) monthly_scp ON monthly_scp.agent_id = u.id
        WHERE (
              u.department_ = 'Retention'
              OR u.id IN (
                  SELECT agent_id FROM agent_dept_history
                  WHERE report_dept = 'Retention'
                    AND effective_from <= %(date_to_excl)s::date - 1
                    AND (effective_to IS NULL OR effective_to >= %(date_from)s::date)
              )
          )
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
          {role_filter}
        ORDER BY u.office_name NULLS LAST, dept_name, u.agent_name
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT holiday_date FROM public_holidays")
                holidays = {row[0] for row in cur.fetchall()}
            except Exception:
                conn.rollback()
                holidays = set()

            if dt_from >= _TARGETS_CUTOFF:
                _tgt_subq = """SELECT crm_user_id AS agent_id, monthly_net_target::float AS monthly_target_net
                    FROM agent_targets_history
                    WHERE report_month = DATE_TRUNC('month', %(date_from)s::date)
                      AND crm_user_id IS NOT NULL"""
            else:
                _tgt_subq = """SELECT agent_id::int AS agent_id, net::float AS monthly_target_net
                    FROM targets
                    WHERE date = DATE_TRUNC('month', %(date_from)s::date)"""
            base_params = {
                "date_from":    date_from,
                "date_to_excl": date_to_exclusive,
                "date_to":      date_to,
                "last_day":     last_day,
            }

            if has_cls:
                cur.execute(
                    _CLS_KPIS_SQL.replace("{cls_where}", cls_where),
                    {**base_params, **cls_params},
                )

            _prepared_sql = sql.replace("{_kpi_tbl}", _kpi_tbl).replace("{tgt_subq}", _tgt_subq)
            final_sql, final_params = _apply_role_filter(_prepared_sql, {**base_params, **cls_params}, role_filter)
            cur.execute(final_sql, final_params)
            rows = cur.fetchall()

            _gp = {**base_params, **cls_params}

            # RDP net
            rdp_map = {}
            try:
                _rdp_sql = f"""
                    SELECT a.assigned_to AS agent_id,
                           SUM(CASE WHEN t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled')
                                    THEN t.usdamount ELSE 0 END)
                           - SUM(CASE WHEN t.transaction_type_name IN ('Withdrawal', 'Deposit Cancelled')
                                      THEN t.usdamount ELSE 0 END) AS rdp_net
                    FROM transactions t
                    JOIN accounts a ON a.accountid = t.vtigeraccountid
                    WHERE t.transactionapproval = 'Approved'
                      AND (t.deleted = 0 OR t.deleted IS NULL)
                      AND a.client_qualification_date IS NOT NULL
                      AND t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                      AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
                      AND t.confirmation_time >= %(date_from)s::timestamp
                      AND t.confirmation_time <  %(date_to_excl)s::timestamp
                      AND t.confirmation_time >  a.client_qualification_date + INTERVAL '1 day' - INTERVAL '1 second'
                      {cls_where}
                    GROUP BY a.assigned_to
                """
                cur.execute(_rdp_sql, _gp)
                rdp_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
            except Exception as _rdp_err:
                conn.rollback()
                print(f"[dmp_ret] RDP query failed: {_rdp_err}")

        data = [
            {
                "office_name":     r[0],
                "dept_name":       r[1],
                "agent_name":      r[2],
                "target_net":      round(r[3], 2),
                "net_usd":         round(r[4], 2),
                "deposit_usd":     round(r[5], 2),
                "open_volume_usd": round(r[6], 2),
                "daily_net_usd":   round(float(r[7] or 0), 2),
                "status":          r[8] or '',
                "std_count":       int(r[9] or 0),
                "traders_count":   int(r[10] or 0),
                "rdp_net":         round(rdp_map.get(int(r[11] or 0), 0.0), 2),
                "daily_traders":   int(r[12] or 0),
                "daily_loads":     int(r[13] or 0),
                "daily_std":       int(r[14] or 0),
                "monthly_loads":   int(r[15] or 0),
                "daily_avg_scp":   round(float(r[16]), 1) if r[16] is not None else None,
                "monthly_avg_scp": round(float(r[17]), 1) if r[17] is not None else None,
            }
            for r in rows
        ]

        # ── Global summary counts (matching Total Traders logic) ──────
        _global_base = """
            AND rt.accountid IS NOT NULL AND rt.accountid::text != ''
            AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
            AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
            AND u.department_ = 'Retention'
        """
        with conn.cursor() as cur2:
            # Monthly traders (global distinct)
            cur2.execute(f"""
                SELECT COUNT(DISTINCT rt.accountid)
                FROM mv_retention_traders rt
                JOIN accounts a ON a.accountid = rt.accountid
                LEFT JOIN crm_users u ON u.id = rt.assigned_to
                WHERE rt.day >= %(date_from)s::date AND rt.day < %(date_to_excl)s::date
                  AND a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)
                  {_global_base}
            """, _gp)
            _monthly_traders_global = int(cur2.fetchone()[0] or 0)

            # Daily traders (global distinct)
            cur2.execute(f"""
                SELECT COUNT(DISTINCT rt.accountid)
                FROM mv_retention_traders rt
                JOIN accounts a ON a.accountid = rt.accountid
                LEFT JOIN crm_users u ON u.id = rt.assigned_to
                WHERE rt.day = %(date_to)s::date
                  AND a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)
                  {_global_base}
            """, _gp)
            _daily_traders_global = int(cur2.fetchone()[0] or 0)

            # Monthly depositors (global distinct)
            cur2.execute(f"""
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
                  AND t.confirmation_time >= %(date_from)s::timestamp
                  AND t.confirmation_time < %(date_to_excl)s::timestamp
            """, _gp)
            _monthly_loads_global = int(cur2.fetchone()[0] or 0)

            # Daily depositors (global distinct)
            cur2.execute(f"""
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
                  AND t.confirmation_time >= %(date_to)s::timestamp
                  AND t.confirmation_time < (%(date_to)s::date + 1)::timestamp
            """, _gp)
            _daily_loads_global = int(cur2.fetchone()[0] or 0)

        today               = datetime.now(_TZ).date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        month_start   = dt_from.replace(day=1)
        wd_full_month = count_working_days(month_start, month_end, holidays)
        wd_in_range   = count_working_days(dt_from, dt_to, holidays)
        target_ratio  = 1.0 if dt_from.day == 1 or wd_full_month == 0 else round(wd_in_range / wd_full_month, 6)

        _result = {
            "rows":                    data,
            "working_days":            working_days,
            "working_days_passed":     working_days_passed,
            "working_days_left":       working_days_left,
            "target_ratio":            target_ratio,
            "wd_in_range":             wd_in_range,
            "global_monthly_traders":  _monthly_traders_global,
            "global_daily_traders":    _daily_traders_global,
            "global_monthly_loads":    _monthly_loads_global,
            "global_daily_loads":      _daily_loads_global,
        }
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
