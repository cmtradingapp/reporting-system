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
        return RedirectResponse(url="/agent-bonuses", status_code=302)
    if role == "agent":
        dept = user.get("department_") or ""
        show_sales = dept != "Retention"
        show_retention = dept != "Sales"
    else:
        show_sales = not role.startswith("retention_")
        show_retention = not role.startswith("sales_")
    return templates.TemplateResponse("scoreboard.html", {
        "request": request,
        "current_user": user,
        "show_sales": show_sales,
        "show_retention": show_retention,
    })


@router.get("/api/performance")
async def scoreboard_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    role_filter = get_role_filter(user)
    _ck = f"perf_v19:{user.get('role','')}:{date_from}:{date_to}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt_to   = datetime.strptime(date_to,   "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    # ── Main sales table: Sales/Conversion agents ─────────────────────────────
    # mv_daily_kpis replaces 3 separate transactions subqueries (FTC, NET, FTD).
    # A single OR-filtered scan of mv_daily_kpis handles both the tx_date axis
    # (NET, FTD) and the qual_date axis (FTC) in one pass.
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
            COALESCE(rdp.rdp_net, 0)::float               AS rdp_net
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
            FROM mv_daily_kpis k
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
            FROM mv_daily_kpis k
            WHERE k.tx_date = %(date_to)s OR k.qual_date = %(date_to)s
            GROUP BY k.agent_id
        ) dk ON dk.agent_id = u.id
        LEFT JOIN ({tgt_subq}) tgt ON tgt.agent_id = u.id
        LEFT JOIN (
            SELECT f.original_deposit_owner AS agent_id,
                   COUNT(DISTINCT f.accountid) AS ftd100_cnt
            FROM ftd100_clients f
            WHERE f.ftd_100_date >= %(date_from)s
              AND f.ftd_100_date <  %(date_to_excl)s
            GROUP BY f.original_deposit_owner
        ) f100 ON f100.agent_id = u.id
        LEFT JOIN (
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
              AND t.confirmation_time::date >= %(date_from)s
              AND t.confirmation_time::date < %(date_to_excl)s
              AND t.confirmation_time::date > a.client_qualification_date
            GROUP BY a.assigned_to
        ) rdp ON rdp.agent_id = u.id
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
            final_sql, final_params = _apply_role_filter(sql.replace('{tgt_subq}', _tgt_subq), base_params, role_filter)
            cur.execute(final_sql, final_params)
            rows = cur.fetchall()

            # Grand FTC — company-wide, qual_date axis
            cur.execute("""
                SELECT COALESCE(SUM(ftc_count), 0)
                FROM mv_daily_kpis
                WHERE qual_date >= %(date_from)s AND qual_date < %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            grand_ftc = int(cur.fetchone()[0] or 0)

            # Grand NET — company-wide, tx_date axis (from mv_run_rate 'all')
            cur.execute("""
                SELECT COALESCE(SUM(net_usd), 0)
                FROM mv_run_rate
                WHERE dept_group = 'all'
                  AND tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            grand_net = float(cur.fetchone()[0] or 0)

            # Open Volume — from mv_volume_stats (test-filter already baked in)
            cur.execute("""
                SELECT COALESCE(SUM(notional_usd), 0)
                FROM mv_volume_stats
                WHERE open_date >= %(date_from)s AND open_date <= %(date_to)s
            """, {"date_from": date_from, "date_to": date_to})
            open_volume = float(cur.fetchone()[0] or 0)

            # End Equity Zeroed — live (real-time snapshot, not suitable for MV)
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

            # Daily NET — from mv_run_rate 'all', single day
            cur.execute("""
                SELECT COALESCE(SUM(net_usd), 0)
                FROM mv_run_rate
                WHERE dept_group = 'all' AND tx_date = %(date_to)s
            """, {"date_to": date_to})
            daily_net = float(cur.fetchone()[0] or 0)

            # Grand FTD — company-wide, tx_date axis
            cur.execute("""
                SELECT COALESCE(SUM(ftd_count), 0)
                FROM mv_daily_kpis
                WHERE tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            grand_ftd = int(cur.fetchone()[0] or 0)

            # Daily FTD — single day
            cur.execute("""
                SELECT COALESCE(SUM(ftd_count), 0)
                FROM mv_daily_kpis
                WHERE tx_date = %(date_to)s
            """, {"date_to": date_to})
            daily_ftd = int(cur.fetchone()[0] or 0)

            # Daily FTC — qual_date = date_to
            cur.execute("""
                SELECT COALESCE(SUM(ftc_count), 0)
                FROM mv_daily_kpis
                WHERE qual_date = %(date_to)s
            """, {"date_to": date_to})
            daily_ftc = int(cur.fetchone()[0] or 0)

            # New Leads + Live Accounts (mv_account_stats — always today/MTD)
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

        today = datetime.now(_TZ).date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        safe_wdp        = working_days_passed if working_days_passed > 0 else 1
        grand_ftc_rr    = round(grand_ftc  / safe_wdp * working_days)
        grand_ftd_rr    = round(grand_ftd  / safe_wdp * working_days)
        grand_net_rr    = round(grand_net  / safe_wdp * working_days, 2)
        open_volume_rr  = round(open_volume / safe_wdp * working_days) if open_volume > 0 else round(open_volume)

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
                "rdp_net":      round(float(r[12] or 0), 2),
            }
            for r in rows
        ]
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
    _ck = f"perf_ret_v15:{user.get('role','')}:{date_from}:{date_to}"
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

    # ── Retention table: mv_daily_kpis replaces 2 transaction subqueries (NET,
    #    DEPOSIT).  Open volume comes from mv_volume_stats.
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
            COALESCE(rdp.rdp_net, 0)::float                   AS rdp_net
        FROM crm_users u
        LEFT JOIN ({tgt_subq}) tgt ON tgt.agent_id = u.id
        LEFT JOIN (
            -- NET + DEPOSIT in one mv_daily_kpis scan
            SELECT k.agent_id,
                   SUM(k.net_usd)     AS net_usd,
                   SUM(k.deposit_usd) AS deposit_usd
            FROM mv_daily_kpis k
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
            FROM mv_daily_kpis
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
              AND t.confirmation_time::date >= %(date_from)s
              AND t.confirmation_time::date < %(date_to_excl)s
              AND t.confirmation_time::date > a.client_qualification_date
            GROUP BY a.assigned_to
        ) rdp ON rdp.agent_id = u.id
        WHERE u.department_ = 'Retention'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
          AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Retention%%'
          AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Conversion%%'
          AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Support%%'
          AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%General%%'
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
            final_sql, final_params = _apply_role_filter(sql.replace('{tgt_subq}', _tgt_subq), base_params, role_filter)
            cur.execute(final_sql, final_params)
            rows = cur.fetchall()

            # Daily net retention — from mv_run_rate 'retention', single day
            cur.execute("""
                SELECT COALESCE(SUM(net_usd), 0)
                FROM mv_run_rate
                WHERE dept_group = 'retention' AND tx_date = %(date_to)s
            """, {"date_to": date_to})
            daily_net_retention = float(cur.fetchone()[0] or 0)

            # Global KPI stats (same as /api/performance — needed for retention-only users)
            cur.execute("""
                SELECT COALESCE(SUM(ftc_count), 0)
                FROM mv_daily_kpis
                WHERE qual_date >= %(date_from)s AND qual_date < %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            grand_ftc = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COALESCE(SUM(net_usd), 0)
                FROM mv_run_rate
                WHERE dept_group = 'all'
                  AND tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            grand_net = float(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COALESCE(SUM(notional_usd), 0)
                FROM mv_volume_stats
                WHERE open_date >= %(date_from)s AND open_date <= %(date_to)s
            """, {"date_from": date_from, "date_to": date_to})
            open_volume = float(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COALESCE(SUM(ftd_count), 0)
                FROM mv_daily_kpis
                WHERE tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            grand_ftd = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COALESCE(SUM(ftd_count), 0)
                FROM mv_daily_kpis WHERE tx_date = %(date_to)s
            """, {"date_to": date_to})
            daily_ftd = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COALESCE(SUM(ftc_count), 0)
                FROM mv_daily_kpis WHERE qual_date = %(date_to)s
            """, {"date_to": date_to})
            daily_ftc = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COALESCE(SUM(net_usd), 0)
                FROM mv_run_rate WHERE dept_group = 'all' AND tx_date = %(date_to)s
            """, {"date_to": date_to})
            daily_net = float(cur.fetchone()[0] or 0)

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

        today               = datetime.now(_TZ).date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        safe_wdp       = working_days_passed if working_days_passed > 0 else 1
        grand_ftc_rr   = round(grand_ftc   / safe_wdp * working_days)
        grand_ftd_rr   = round(grand_ftd   / safe_wdp * working_days)
        grand_net_rr   = round(grand_net   / safe_wdp * working_days, 2)
        open_volume_rr = round(open_volume / safe_wdp * working_days) if open_volume > 0 else round(open_volume)

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
                "rdp_net":         round(float(r[10] or 0), 2),
            }
            for r in rows
        ]
        _result = {
            "rows":                    data,
            "daily_net_retention":     round(daily_net_retention, 2),
            "working_days":            working_days,
            "working_days_passed":     working_days_passed,
            "working_days_left":       working_days_left,
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
