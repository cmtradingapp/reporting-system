from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.auth.role_filters import get_role_filter
from app.db.postgres_conn import get_connection
from app import cache
from datetime import datetime, timedelta, date as date_type
from zoneinfo import ZoneInfo
import calendar

_TZ = ZoneInfo("Europe/Nicosia")
_TARGETS_CUTOFF = date_type(2026, 5, 1)


def _apply_role_filter(sql: str, params: dict, role_filter: dict) -> tuple[str, dict]:
    if not role_filter['crm_where']:
        return sql.replace('{role_filter}', ''), params
    named_where = role_filter['crm_where']
    extra = {}
    for i, val in enumerate(role_filter['crm_params']):
        key = f'_rf{i}'
        named_where = named_where.replace('%s', f'%({key})s', 1)
        extra[key] = val
    return sql.replace('{role_filter}', named_where), {**params, **extra}


def _build_cls_filter(request: Request) -> tuple[str, dict, str]:
    """Parse classification filter from query params.
    Returns (sql_where_fragment, params, cache_suffix).
    sql_where_fragment is '' when no filter, or 'AND (...)' when active."""
    scp_cat = request.query_params.get("scp_cat", "").strip()
    scp = request.query_params.get("scp", "").strip()
    parts = []
    params = {}
    if scp_cat:
        cats = [c.strip().lower() for c in scp_cat.split(",") if c.strip()]
        cat_parts = []
        if "high" in cats:
            cat_parts.append("a.classification_int BETWEEN 6 AND 10")
        if "low" in cats:
            cat_parts.append("a.classification_int BETWEEN 1 AND 5")
        if "none" in cats:
            cat_parts.append("(a.classification_int IS NULL OR a.classification_int NOT BETWEEN 1 AND 10)")
        if cat_parts:
            parts.append("(" + " OR ".join(cat_parts) + ")")
    if scp:
        vals = [int(v) for v in scp.split(",") if v.strip().isdigit() and 1 <= int(v.strip()) <= 10]
        if vals:
            params["_cls_vals"] = tuple(vals)
            parts.append("a.classification_int IN %(_cls_vals)s")
    if not parts:
        return "", {}, ""
    return "AND " + " AND ".join(parts), params, f":{scp_cat}:{scp}"


# SQL to create temp table mimicking mv_daily_kpis with classification filter
_CLS_KPIS_SQL = """
CREATE TEMP TABLE _cls_kpis ON COMMIT DROP AS
SELECT
    t.original_deposit_owner                                             AS agent_id,
    t.confirmation_time::date                                            AS tx_date,
    a.client_qualification_date::date                                    AS qual_date,
    COALESCE(SUM(CASE
        WHEN t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled')  THEN  t.usdamount
        WHEN t.transaction_type_name IN ('Withdrawal', 'Deposit Cancelled')  THEN -t.usdamount
    END), 0)                                                             AS net_usd,
    COALESCE(SUM(CASE
        WHEN t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled')  THEN t.usdamount
        ELSE 0
    END), 0)                                                             AS deposit_usd,
    SUM(CASE WHEN t.transaction_type_name = 'Deposit' AND t.ftd = 1 THEN 1 ELSE 0 END)::int AS ftd_count,
    COUNT(DISTINCT CASE WHEN t.transaction_type_name = 'Deposit' AND t.ftd = 1
                        THEN t.vtigeraccountid END)::int                 AS ftc_count
FROM transactions t
JOIN accounts  a  ON a.accountid = t.vtigeraccountid
WHERE t.transactionapproval = 'Approved'
  AND (t.deleted = 0 OR t.deleted IS NULL)
  AND t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
  AND t.vtigeraccountid IS NOT NULL
  AND a.is_test_account = 0
  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%bonus%'
  {cls_where}
  AND (
      (a.client_qualification_date >= %(date_from)s AND a.client_qualification_date < %(date_to_excl)s)
      OR
      (t.confirmation_time >= %(date_from)s AND t.confirmation_time < %(date_to_excl)s)
  )
GROUP BY t.original_deposit_owner, t.confirmation_time::date, a.client_qualification_date::date
"""


def last_day_of_month(d: date_type) -> date_type:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def count_working_days(start: date_type, end: date_type, holidays: set) -> int:
    """Count Mon–Fri non-holiday days between start and end (inclusive)."""
    if end < start:
        return 0
    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in holidays:
            count += 1
        current += timedelta(days=1)
    return count


router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/performance", response_class=HTMLResponse)
async def scoreboard_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    role = user.get("role", "")
    ap = user.get("allowed_pages_list")
    if role == "marketing" and ap is None:
        return RedirectResponse(url="/campaign-performance", status_code=302)
    if ap is not None and "performance" not in ap:
        # Prefer daily_monthly as first fallback (renamed to "Performance")
        if "daily_monthly" in ap:
            return RedirectResponse(url="/daily-monthly", status_code=302)
        _page_urls = {"agent_bonuses": "/agent-bonuses", "marketing": "/campaign-performance",
                       "dashboard": "/dashboard", "total_traders": "/total-traders",
                       "performance": "/performance", "data_sync": "/data-sync"}
        for p in ap:
            if p in _page_urls:
                return RedirectResponse(url=_page_urls[p], status_code=302)
        return RedirectResponse(url="/agent-bonuses", status_code=302)
    if role == "agent":
        dept = user.get("department_") or ""
        show_sales = dept != "Retention"
        show_retention = dept != "Sales"
    else:
        show_sales = not role.startswith("retention_")
        show_retention = not role.startswith("sales_")

    # CRO view: admin@cmtrading.com sees both; sales_all sees sales CRO; retention_all sees retention CRO
    _is_cro_admin = user.get("email", "") == "admin@cmtrading.com"
    show_cro_sales     = _is_cro_admin or role == "sales_all"
    show_cro_retention = _is_cro_admin or role == "retention_all"

    return templates.TemplateResponse("scoreboard.html", {
        "request": request,
        "current_user": user,
        "show_sales": show_sales,
        "show_retention": show_retention,
        "show_cro_sales": show_cro_sales,
        "show_cro_retention": show_cro_retention,
    })


@router.get("/api/performance")
async def scoreboard_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    role_filter = get_role_filter(user)
    cls_where, cls_params, cls_suffix = _build_cls_filter(request)
    has_cls = bool(cls_where)
    _ck = f"perf_v26:{user.get('role','')}:{date_from}:{date_to}{cls_suffix}"
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

    # ── Main sales table: Sales/Conversion agents ─────────────────────────────
    # mv_daily_kpis replaces 3 separate transactions subqueries (FTC, NET, FTD).
    # A single OR-filtered scan of mv_daily_kpis handles both the tx_date axis
    # (NET, FTD) and the qual_date axis (FTC) in one pass.
    # When classification filter is active, a temp table _cls_kpis is used instead.
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
            COALESCE(u.status, '')                        AS status
        FROM crm_users u
        LEFT JOIN (
            -- Combined FTC + NET + FTD from mv_daily_kpis in a single scan
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
            -- Per-agent daily stats for the date_to day only
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
        WHERE u.department_ = 'Sales'
          AND u.team = 'Conversion'
          AND u.status = 'Active'
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

            # When classification filter is active, create a temp table
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

            _gp = {**base_params, **cls_params}

            # ── Combined KPI aggregates (replaces 6 separate queries) ──
            cur.execute(f"""
                SELECT
                    COALESCE(SUM(CASE WHEN qual_date >= %(date_from)s AND qual_date < %(date_to_excl)s
                                      THEN ftc_count ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
                                      THEN ftd_count ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN tx_date  = %(date_to)s THEN ftd_count ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN qual_date = %(date_to)s THEN ftc_count ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
                                      THEN net_usd ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN tx_date  = %(date_to)s THEN net_usd ELSE 0 END), 0)
                FROM {_kpi_tbl}
                WHERE (qual_date >= %(date_from)s AND qual_date < %(date_to_excl)s)
                   OR (tx_date  >= %(date_from)s AND tx_date  < %(date_to_excl)s)
                   OR  tx_date  = %(date_to)s
                   OR  qual_date = %(date_to)s
            """, _gp)
            _kpi = cur.fetchone()
            grand_ftc = int(_kpi[0])
            grand_ftd = int(_kpi[1])
            daily_ftd = int(_kpi[2])
            daily_ftc = int(_kpi[3])
            if has_cls:
                grand_net = float(_kpi[4])
                daily_net = float(_kpi[5])
            else:
                # NET from mv_run_rate (pre-aggregated by dept_group)
                cur.execute("""
                    SELECT
                        COALESCE(SUM(CASE WHEN tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
                                          THEN net_usd ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN tx_date = %(date_to)s THEN net_usd ELSE 0 END), 0)
                    FROM mv_run_rate
                    WHERE dept_group = 'all'
                      AND ((tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s) OR tx_date = %(date_to)s)
                """, _gp)
                _net = cur.fetchone()
                grand_net = float(_net[0])
                daily_net = float(_net[1])

            # Open Volume — from mv_volume_stats
            cur.execute("""
                SELECT COALESCE(SUM(notional_usd), 0)
                FROM mv_volume_stats
                WHERE open_date >= %(date_from)s AND open_date <= %(date_to)s
            """, {"date_from": date_from, "date_to": date_to})
            open_volume = float(cur.fetchone()[0] or 0)

            # End Equity Zeroed — heavy CTE (deadlock-prone during MV refresh)
            try:
                cur.execute("""
                    WITH latest_equity AS (
                        SELECT DISTINCT ON (login)
                            login, convertedbalance, convertedfloatingpnl
                        FROM dealio_daily_profits
                        WHERE date >= date_trunc('month', %(date_to)s::date)
                          AND date <  date_trunc('month', %(date_to)s::date) + INTERVAL '1 month'
                        ORDER BY login, date DESC
                    ),
                    old_bonus_balance AS (
                        SELECT login, SUM(net_amount) AS old_bonus_balance
                        FROM bonus_transactions
                        WHERE confirmation_time < %(date_to)s::date + INTERVAL '1 day'
                        GROUP BY login
                    )
                    SELECT COALESCE(SUM(
                        CASE
                            WHEN COALESCE(d.convertedbalance, 0) + COALESCE(d.convertedfloatingpnl, 0) <= 0 THEN 0
                            ELSE GREATEST(
                                COALESCE(d.convertedbalance, 0) + COALESCE(d.convertedfloatingpnl, 0)
                                    - COALESCE(ob.old_bonus_balance, 0),
                                0
                            )
                        END
                    ), 0)
                    FROM latest_equity d
                    JOIN trading_accounts ta  ON ta.login::bigint = d.login
                    JOIN accounts a           ON a.accountid = ta.vtigeraccountid
                    JOIN crm_users u          ON u.id = a.assigned_to
                    LEFT JOIN old_bonus_balance ob ON ob.login::bigint = d.login
                    WHERE a.is_test_account = 0
                      AND (ta.deleted = 0 OR ta.deleted IS NULL)
                """, {"date_to": date_to})
                end_equity_zeroed = float(cur.fetchone()[0] or 0)
            except Exception as _eez_err:
                conn.rollback()
                print(f"[perf] End Equity Zeroed query failed (lock?): {_eez_err}")
                end_equity_zeroed = 0.0

            # New Leads + Live Accounts
            cur.execute("""
                SELECT new_leads_today, new_leads_month, new_live_today, new_live_month
                FROM mv_account_stats
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                new_leads_today, new_leads_month, new_live_today, new_live_month = (
                    int(row[0] or 0), int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)
                )
            else:
                new_leads_today = new_leads_month = new_live_today = new_live_month = 0

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
            }
            for r in rows
        ]

        # For restricted users, derive card values from filtered rows
        if not role_filter['is_full_access']:
            grand_ftc  = sum(r["ftc"] for r in data)
            grand_net  = sum(r["net_deposits"] for r in data)
            grand_ftd  = sum(r["ftd_count"] for r in data)
            daily_ftd  = sum(r["daily_ftd"] for r in data)
            daily_ftc  = sum(r["daily_ftc"] for r in data)
            daily_net  = sum(r["daily_net"] for r in data)
            open_volume = 0.0
            end_equity_zeroed = 0.0
            new_leads_today = new_leads_month = new_live_today = new_live_month = 0

        today = datetime.now(_TZ).date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        # Pro-rate targets for sub-month ranges
        month_start   = dt_from.replace(day=1)
        wd_full_month = count_working_days(month_start, month_end, holidays)
        wd_in_range   = count_working_days(dt_from, dt_to, holidays)
        target_ratio  = 1.0 if dt_from.day == 1 or wd_full_month == 0 else round(wd_in_range / wd_full_month, 6)

        safe_wdp        = working_days_passed if working_days_passed > 0 else 1
        grand_ftc_rr    = round(grand_ftc  / safe_wdp * working_days)
        grand_ftd_rr    = round(grand_ftd  / safe_wdp * working_days)
        grand_net_rr    = round(grand_net  / safe_wdp * working_days, 2)
        open_volume_rr  = round(open_volume / safe_wdp * working_days) if open_volume > 0 else round(open_volume)

        _result = {
            "rows":                 data,
            "total_ftc":            sum(r["ftc"] for r in data),
            "total_target_ftc":     sum(r["target_ftc"] for r in data),
            "total_ftd100":         sum(r["ftd100"] for r in data),
            "total_net_deposits":   round(sum(r["net_deposits"] for r in data), 2),
            "total_ftd_count":      sum(r["ftd_count"] for r in data),
            "grand_ftc":            grand_ftc,
            "grand_ftc_rr":         grand_ftc_rr,
            "grand_ftd_rr":         grand_ftd_rr,
            "grand_net":            round(grand_net, 2),
            "grand_net_rr":         grand_net_rr,
            "open_volume":          round(open_volume, 2),
            "open_volume_rr":       open_volume_rr,
            "end_equity_zeroed":    round(end_equity_zeroed, 2),
            "grand_ftd":            grand_ftd,
            "daily_net":            round(daily_net, 2),
            "daily_ftd":            daily_ftd,
            "daily_ftc":            daily_ftc,
            "new_leads":            {"daily": new_leads_today, "monthly": new_leads_month},
            "new_live":             {"daily": new_live_today,  "monthly": new_live_month},
            "working_days":         working_days,
            "working_days_passed":  working_days_passed,
            "working_days_left":    working_days_left,
            "target_ratio":         target_ratio,
            "wd_in_range":          wd_in_range,
            "date_from":            date_from,
            "date_to":              date_to,
        }
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


@router.get("/api/performance/retention")
async def scoreboard_retention_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    role_filter = get_role_filter(user)
    cls_where, cls_params, cls_suffix = _build_cls_filter(request)
    has_cls = bool(cls_where)
    _ck = f"perf_ret_v21:{user.get('role','')}:{date_from}:{date_to}{cls_suffix}"
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

    # ── Retention table: mv_daily_kpis replaces 2 transaction subqueries (NET,
    #    DEPOSIT).  Open volume comes from mv_volume_stats.
    #    RDP (transactions JOIN accounts) is fetched separately to avoid deadlocks.
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
            u.id                                              AS user_id
        FROM crm_users u
        LEFT JOIN ({tgt_subq}) tgt ON tgt.agent_id = u.id
        LEFT JOIN (
            -- NET + DEPOSIT in one scan
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
            SELECT rt.assigned_to AS agent_id, COUNT(DISTINCT rt.accountid) AS traders_count
            FROM mv_retention_traders rt
            JOIN accounts a ON a.accountid = rt.accountid
            WHERE rt.day >= %(date_from)s::date
              AND rt.day < %(date_to_excl)s::date
              AND a.is_test_account = 0
              AND (a.is_demo = 0 OR a.is_demo IS NULL)
              AND a.accountid IS NOT NULL AND a.accountid::text != ''
            GROUP BY rt.assigned_to
        ) trd ON trd.agent_id = u.id
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

            # When classification filter is active, create a temp table
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

            # ── Combined KPI aggregates (replaces 9 separate queries) ──
            cur.execute(f"""
                SELECT
                    COALESCE(SUM(CASE WHEN qual_date >= %(date_from)s AND qual_date < %(date_to_excl)s
                                      THEN ftc_count ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
                                      THEN ftd_count ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN tx_date  = %(date_to)s THEN ftd_count ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN qual_date = %(date_to)s THEN ftc_count ELSE 0 END), 0)::int,
                    COALESCE(SUM(CASE WHEN tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
                                      THEN net_usd ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN tx_date  = %(date_to)s THEN net_usd ELSE 0 END), 0)
                FROM {_kpi_tbl}
                WHERE (qual_date >= %(date_from)s AND qual_date < %(date_to_excl)s)
                   OR (tx_date  >= %(date_from)s AND tx_date  < %(date_to_excl)s)
                   OR  tx_date  = %(date_to)s
                   OR  qual_date = %(date_to)s
            """, _gp)
            _kpi = cur.fetchone()
            grand_ftc = int(_kpi[0])
            grand_ftd = int(_kpi[1])
            daily_ftd = int(_kpi[2])
            daily_ftc = int(_kpi[3])
            if has_cls:
                grand_net = float(_kpi[4])
                daily_net = float(_kpi[5])
                daily_net_retention = daily_net  # same table, no dept split
            else:
                # NET from mv_run_rate (with dept_group split for retention daily)
                cur.execute("""
                    SELECT
                        COALESCE(SUM(CASE WHEN dept_group = 'all' AND tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
                                          THEN net_usd ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN dept_group = 'all' AND tx_date = %(date_to)s
                                          THEN net_usd ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN dept_group = 'retention' AND tx_date = %(date_to)s
                                          THEN net_usd ELSE 0 END), 0)
                    FROM mv_run_rate
                    WHERE dept_group IN ('all', 'retention')
                      AND ((tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s) OR tx_date = %(date_to)s)
                """, _gp)
                _net = cur.fetchone()
                grand_net = float(_net[0])
                daily_net = float(_net[1])
                daily_net_retention = float(_net[2])

            cur.execute("""
                SELECT COALESCE(SUM(notional_usd), 0)
                FROM mv_volume_stats
                WHERE open_date >= %(date_from)s AND open_date <= %(date_to)s
            """, _gp)
            open_volume = float(cur.fetchone()[0] or 0)

            # New Leads + Live Accounts
            cur.execute("""
                SELECT new_leads_today, new_leads_month, new_live_today, new_live_month
                FROM mv_account_stats LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                new_leads_today, new_leads_month, new_live_today, new_live_month = (
                    int(row[0] or 0), int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)
                )
            else:
                new_leads_today = new_leads_month = new_live_today = new_live_month = 0

            # RDP net — separate query (fix: range comparison instead of ::date cast)
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
                print(f"[perf_ret] RDP query failed (lock?): {_rdp_err}")

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
            }
            for r in rows
        ]

        # For restricted users, derive card values from filtered rows
        if not role_filter['is_full_access']:
            grand_net  = sum(r["net_usd"] for r in data)
            grand_ftc  = 0
            grand_ftd  = 0
            daily_net  = sum(r["daily_net_usd"] for r in data)
            daily_net_retention = daily_net
            daily_ftd  = 0
            daily_ftc  = 0
            open_volume = sum(r["open_volume_usd"] for r in data)
            new_leads_today = new_leads_month = new_live_today = new_live_month = 0

        today               = datetime.now(_TZ).date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        # Pro-rate targets for sub-month ranges
        month_start   = dt_from.replace(day=1)
        wd_full_month = count_working_days(month_start, month_end, holidays)
        wd_in_range   = count_working_days(dt_from, dt_to, holidays)
        target_ratio  = 1.0 if dt_from.day == 1 or wd_full_month == 0 else round(wd_in_range / wd_full_month, 6)

        safe_wdp       = working_days_passed if working_days_passed > 0 else 1
        grand_ftc_rr   = round(grand_ftc   / safe_wdp * working_days)
        grand_ftd_rr   = round(grand_ftd   / safe_wdp * working_days)
        grand_net_rr   = round(grand_net   / safe_wdp * working_days, 2)
        open_volume_rr = round(open_volume / safe_wdp * working_days) if open_volume > 0 else round(open_volume)

        _result = {
            "rows":                    data,
            "daily_net_retention":     round(daily_net_retention, 2),
            "working_days":            working_days,
            "working_days_passed":     working_days_passed,
            "working_days_left":       working_days_left,
            "target_ratio":            target_ratio,
            "wd_in_range":             wd_in_range,
            # Global KPI stats (for retention-only users who don't call /api/performance)
            "grand_ftc":               grand_ftc,
            "grand_ftc_rr":            grand_ftc_rr,
            "grand_ftd":               grand_ftd,
            "grand_ftd_rr":            grand_ftd_rr,
            "grand_net":               round(grand_net, 2),
            "grand_net_rr":            grand_net_rr,
            "open_volume":             round(open_volume, 2),
            "open_volume_rr":          open_volume_rr,
            "daily_ftd":               daily_ftd,
            "daily_ftc":               daily_ftc,
            "daily_net":               round(daily_net, 2),
            "new_leads":               {"daily": new_leads_today, "monthly": new_leads_month},
            "new_live":                {"daily": new_live_today,  "monthly": new_live_month},
        }
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
