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
_TARGETS_CUTOFF = date_type(2026, 4, 1)


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


OFFICE_GROUP_A = {'GMT', 'CY', 'BU'}
OFFICE_GROUP_B = {'ABJ-NG', 'SA', 'LAG-NG'}


def get_office_group(office: str) -> str:
    if office in OFFICE_GROUP_A:
        return 'A'
    if office in OFFICE_GROUP_B:
        return 'B'
    return 'other'


def get_net_bonus_pct(net_usd: float, group: str) -> float:
    """Tiered bonus % on net USD."""
    if group == 'A':
        if net_usd >= 200_000: return 0.04
        if net_usd >= 150_000: return 0.0375
        if net_usd >= 100_000: return 0.035
        if net_usd >= 75_000:  return 0.03
        if net_usd >= 50_000:  return 0.02
        if net_usd >= 20_000:  return 0.015
        return 0.0
    if group == 'B':
        if net_usd >= 100_000: return 0.035
        if net_usd >= 80_000:  return 0.03
        if net_usd >= 60_000:  return 0.028
        if net_usd >= 50_000:  return 0.025
        if net_usd >= 40_000:  return 0.02
        if net_usd >= 30_000:  return 0.018
        if net_usd >= 20_000:  return 0.015
        if net_usd >= 10_000:  return 0.01
        return 0.0
    return 0.0


def get_vol_bonus_pct(vol_pct: float, group: str) -> float:
    """Tiered bonus % on volume.  vol_pct is ratio: open_vol / target_vol."""
    if group == 'A':
        if vol_pct >= 2.0:  return 0.015
        if vol_pct >= 1.5:  return 0.0125
        if vol_pct >= 1.0:  return 0.01
        if vol_pct >= 0.75: return 0.005
        if vol_pct >= 0.5:  return 0.002
        return 0.0
    return 0.0


def get_sales_multiplier(ftd100: int) -> int:
    """Per-FTD100 $ amount based on count tier."""
    if ftd100 >= 48: return 65
    if ftd100 >= 44: return 60
    if ftd100 >= 40: return 55
    if ftd100 >= 36: return 50
    if ftd100 >= 32: return 45
    if ftd100 >= 28: return 40
    if ftd100 >= 24: return 35
    if ftd100 >= 20: return 30
    if ftd100 >= 15: return 25
    if ftd100 >= 10: return 20
    if ftd100 >= 5:  return 15
    return 0


def get_sales_target_bonus(ftd100_actual: int, target_ftc: int) -> int:
    """Flat $ bonus paid when ftd100_actual >= target_ftc."""
    if target_ftc <= 0 or ftd100_actual < target_ftc:
        return 0
    n = ftd100_actual
    if n >= 60: return 1500
    if n >= 50: return 1000
    if n >= 35: return 500
    if n >= 30: return 300
    if n >= 25: return 200
    if n >= 20: return 150
    if n >= 5:  return 100
    return 0


def last_day_of_month(d: date_type) -> date_type:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def count_working_days(start: date_type, end: date_type, holidays: set) -> int:
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


@router.get("/agent-bonuses", response_class=HTMLResponse)
async def agent_bonuses_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    role = user.get("role", "")
    ap = user.get("allowed_pages_list")
    if role == "marketing" and ap is None:
        return RedirectResponse(url="/campaign-performance", status_code=302)
    if ap is not None and "agent_bonuses" not in ap:
        return RedirectResponse(url="/performance", status_code=302)
    if role == "agent":
        dept = user.get("department_") or ""
        show_sales = dept != "Retention"
        show_retention = dept != "Sales"
    else:
        show_sales = not role.startswith("retention_")
        show_retention = not role.startswith("sales_")
    return templates.TemplateResponse("agent_bonuses.html", {
        "request": request,
        "current_user": user,
        "show_sales": show_sales,
        "show_retention": show_retention,
    })


@router.get("/api/agent-bonuses/retention")
async def agent_bonuses_retention_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    role_filter = get_role_filter(user)
    _ck = f"bon_ret_v9:{user.get('role','')}:{date_from}:{date_to}"
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

    # mv_daily_kpis replaces transactions NET subquery.
    # mv_volume_stats replaces dealio_trades_mt4 open-volume subquery.
    sql = """
        SELECT
            COALESCE(u.office_name, 'N/A')                   AS office_name,
            COALESCE(u.department, 'N/A')                     AS dept_name,
            COALESCE(u.agent_name, u.full_name, 'N/A')        AS agent_name,
            COALESCE(u.office, '')                             AS office,
            COALESCE(tgt.monthly_target_net, 0)::float        AS target_net,
            COALESCE(mv.net_usd, 0)::float                    AS net_usd,
            COALESCE(vol.open_volume_usd, 0)::float           AS open_volume_usd,
            COALESCE(u.status, '')                             AS status
        FROM crm_users u
        LEFT JOIN auth_users au ON au.crm_user_id = u.id
        LEFT JOIN ({tgt_subq}) tgt ON tgt.agent_id = u.id
        LEFT JOIN (
            SELECT k.agent_id, SUM(k.net_usd) AS net_usd
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
        WHERE u.department_ = 'Retention'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
          AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Retention%%'
          AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Conversion%%'
          AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%Support%%'
          AND TRIM(COALESCE(u.department, '')) NOT ILIKE '%%General%%'
          AND (%(is_admin)s OR au.role IS NULL OR au.role NOT IN ('admin', 'general'))
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
                "is_admin":     user.get("role") in ("admin", "general"),
            }
            final_sql, final_params = _apply_role_filter(sql.replace('{tgt_subq}', _tgt_subq), base_params, role_filter)
            cur.execute(final_sql, final_params)
            rows = cur.fetchall()

        today               = datetime.now(_TZ).date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        data = []
        for r in rows:
            office_name     = r[0]
            dept_name       = r[1]
            agent_name      = r[2]
            office          = r[3]
            target_net      = round(float(r[4]), 2)
            net_usd         = round(float(r[5]), 2)
            open_volume_usd = round(float(r[6]), 2)
            status          = r[7] or ''

            target_vol = target_net * 1650

            group          = get_office_group(office)
            target_net_pct = net_usd / target_net if target_net > 0 else None
            target_vol_pct = open_volume_usd / target_vol if target_vol > 0 else None

            pct_on_net        = get_net_bonus_pct(net_usd, group)
            pct_on_target_net = 0.005 if (
                group == 'A' and target_net_pct is not None and target_net_pct >= 1.0
            ) else 0.0
            pct_on_target_vol = (
                get_vol_bonus_pct(target_vol_pct, group) if target_vol_pct is not None else 0.0
            )
            total_bonus_pct = pct_on_net + pct_on_target_net + pct_on_target_vol
            basic_bonus_usd = round(total_bonus_pct * net_usd, 2)

            data.append({
                "office_name":       office_name,
                "dept_name":         dept_name,
                "agent_name":        agent_name,
                "office":            office,
                "target_net":        target_net,
                "net_usd":           net_usd,
                "target_net_pct":    round(target_net_pct, 6) if target_net_pct is not None else None,
                "target_vol":        float(target_vol),
                "open_volume_usd":   open_volume_usd,
                "target_vol_pct":    round(target_vol_pct, 6) if target_vol_pct is not None else None,
                "pct_on_net":        pct_on_net,
                "pct_on_target_net": pct_on_target_net,
                "pct_on_target_vol": pct_on_target_vol,
                "total_bonus_pct":   total_bonus_pct,
                "basic_bonus_usd":   basic_bonus_usd,
                "status":            status,
            })

        _result = {
            "rows":                data,
            "working_days":        working_days,
            "working_days_passed": working_days_passed,
            "working_days_left":   working_days_left,
        }
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


@router.get("/api/agent-bonuses/sales")
async def agent_bonuses_sales_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    role_filter = get_role_filter(user)
    _ck = f"bon_sales_v14:{user.get('role','')}:{date_from}:{date_to}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)
    try:
        dt_from           = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt_to             = datetime.strptime(date_to, "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    # mv_daily_kpis replaces the FTC transactions subquery.
    # mv_sales_bonuses replaces 3 ftd100_clients subqueries (ftd100_count,
    #   total_sales_net, ftd_amount_bonus).
    # ftc_net_usd still uses a live transactions query because its filter
    #   (qual_date >= tx_date OR ftd=1) cannot be pre-aggregated cleanly.
    sql = """
        SELECT
            COALESCE(u.office_name, 'N/A')                       AS office_name,
            COALESCE(u.agent_name, u.full_name, 'N/A')            AS agent_name,
            COALESCE(tgt.target_ftc, 0)::int                      AS target_ftc,
            COALESCE(ftc.ftc_count, 0)::int                       AS ftc_count,
            COALESCE(bon.ftd100_count, 0)::int                    AS ftd100_count,
            COALESCE(bon.ftd100_full_count, 0)::int               AS ftd100_full_count,
            COALESCE(bon.ftd100_half_count, 0)::int               AS ftd100_half_count,
            COALESCE(ftc_net.net_usd, 0)::float                   AS ftc_net_usd,
            COALESCE(bon.total_sales_net, 0)::float               AS total_sales_net,
            COALESCE(bon.ftd_amount_bonus, 0)::float              AS ftd_amount_bonus_sql,
            COALESCE(u.status, '')                                 AS status
        FROM crm_users u
        LEFT JOIN auth_users au ON au.crm_user_id = u.id
        LEFT JOIN ({tgt_subq}) tgt ON tgt.agent_id = u.id
        LEFT JOIN (
            -- FTC from mv_daily_kpis (qual_date axis)
            SELECT k.agent_id, SUM(k.ftc_count)::int AS ftc_count
            FROM mv_daily_kpis k
            WHERE k.qual_date >= %(date_from)s AND k.qual_date < %(date_to_excl)s
            GROUP BY k.agent_id
        ) ftc ON ftc.agent_id = u.id
        LEFT JOIN (
            -- FTD100 count + net_until_qualification + FTD amount bonus from mv_sales_bonuses
            SELECT agent_id,
                   SUM(ftd100_count)       AS ftd100_count,
                   SUM(ftd100_full_count)  AS ftd100_full_count,
                   SUM(ftd100_half_count)  AS ftd100_half_count,
                   SUM(total_sales_net)    AS total_sales_net,
                   SUM(ftd_amount_bonus)   AS ftd_amount_bonus
            FROM mv_sales_bonuses
            WHERE ftd_100_date >= %(date_from)s AND ftd_100_date < %(date_to_excl)s
            GROUP BY agent_id
        ) bon ON bon.agent_id = u.id
        LEFT JOIN (
            -- FTC net USD: live query — depends on (qual_date >= tx_date OR ftd=1)
            SELECT t.original_deposit_owner AS agent_id,
                   SUM(CASE WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN  t.usdamount
                            WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled')  THEN -t.usdamount END)::float AS net_usd
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
              AND a.client_qualification_date IS NOT NULL
              AND a.client_qualification_date >= %(date_from)s
              AND a.client_qualification_date <  %(date_to_excl)s
              AND (a.client_qualification_date >= t.confirmation_time::date OR t.ftd = 1)
              AND a.is_test_account = 0
              AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
            GROUP BY t.original_deposit_owner
        ) ftc_net ON ftc_net.agent_id = u.id
        WHERE (
            (u.department_ = 'Sales' AND u.team = 'Conversion' {role_filter})
            OR u.id IN (3750, 3614, 6119, 6479, 6492, 6355, 6666, 6694)
          )
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
          AND (%(is_admin)s OR au.role IS NULL OR au.role NOT IN ('admin', 'general'))
        ORDER BY u.office_name NULLS LAST, COALESCE(bon.ftd100_count, 0) DESC, u.agent_name
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
            base_params = {
                "date_from":    date_from,
                "date_to_excl": date_to_exclusive,
                "is_admin":     user.get("role") in ("admin", "general"),
            }
            final_sql, final_params = _apply_role_filter(sql.replace('{tgt_subq}', _tgt_subq), base_params, role_filter)
            cur.execute(final_sql, final_params)
            rows = cur.fetchall()

        today               = datetime.now(_TZ).date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        data = []
        for r in rows:
            office_name          = r[0]
            agent_name           = r[1]
            target_ftc           = int(r[2])
            ftc_count            = int(r[3])
            ftd100_count         = int(r[4])
            ftd100_full_count    = int(r[5])
            ftd100_half_count    = int(r[6])
            ftc_net_usd          = round(float(r[7]), 2)
            total_sales_net      = round(float(r[8]), 2)
            ftd_amount_bonus_raw = round(float(r[9]), 2)
            status               = r[10] or ''

            target_pct = ftc_count / target_ftc if target_ftc > 0 else None

            if target_ftc == 0:
                qualify = ftd100_count >= 5
            else:
                qualify = ftd100_count >= 0.50 * target_ftc

            multiplier         = get_sales_multiplier(ftd100_count)
            basic_bonus        = (ftd100_full_count * multiplier + ftd100_half_count * multiplier / 2) if qualify else 0
            _eff_target        = target_ftc if target_ftc > 0 else 1
            sales_target_bonus = get_sales_target_bonus(ftd100_count, _eff_target) if qualify else 0
            ftd_amount_bonus   = ftd_amount_bonus_raw if qualify else 0
            total_sales_bonus  = basic_bonus + sales_target_bonus + ftd_amount_bonus

            data.append({
                "office_name":        office_name,
                "agent_name":         agent_name,
                "target_ftc":         target_ftc,
                "ftc_count":          ftc_count,
                "ftd100_count":       ftd100_count,
                "ftd100_full_count":  ftd100_full_count,
                "ftd100_half_count":  ftd100_half_count,
                "ftc_net_usd":        ftc_net_usd,
                "total_sales_net":    total_sales_net,
                "basic_bonus":        round(basic_bonus, 2),
                "sales_target_bonus": sales_target_bonus,
                "ftd_amount_bonus":   ftd_amount_bonus,
                "total_sales_bonus":  round(total_sales_bonus, 2),
                "target_pct":         round(target_pct, 6) if target_pct is not None else None,
                "multiplier":         multiplier,
                "status":             status,
            })

        _result = {
            "rows":                data,
            "working_days":        working_days,
            "working_days_passed": working_days_passed,
            "working_days_left":   working_days_left,
        }
        # Don't cache if mv_sales_bonuses is empty/stale (all agents showing 0 FTD100s)
        if any(r["ftd100_count"] > 0 for r in data):
            cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


@router.get("/api/agent-bonuses/sales-accounts")
async def agent_bonuses_sales_accounts_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    role_filter = get_role_filter(user)
    _ck = f"bon_sales_acct_v10:{user.get('role','')}:{date_from}:{date_to}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)
    try:
        dt_from           = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt_to             = datetime.strptime(date_to, "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    sql = """
        WITH
        ftc_accs AS (
            SELECT a.accountid,
                   COALESCE(td.original_deposit_owner, a.assigned_to) AS agent_id,
                   1 AS is_ftc
            FROM accounts a
            LEFT JOIN (
                SELECT DISTINCT ON (vtigeraccountid)
                       vtigeraccountid, original_deposit_owner
                FROM transactions
                WHERE ftd = 1
                  AND transactionapproval = 'Approved'
                  AND (deleted = 0 OR deleted IS NULL)
                ORDER BY vtigeraccountid, confirmation_time ASC
            ) td ON td.vtigeraccountid = a.accountid
            WHERE a.client_qualification_date >= %(date_from)s
              AND a.client_qualification_date <  %(date_to_excl)s
              AND a.is_test_account = 0
              AND a.accountid IS NOT NULL
        ),
        ftd100_accs AS (
            SELECT f.accountid,
                   f.original_deposit_owner                                    AS agent_id,
                   1                                                            AS is_ftd100,
                   CASE WHEN f.ftd_100_amount >= 240 THEN 'full' ELSE 'half' END AS ftd100_type,
                   CASE WHEN f.ftd_100_amount < 500  THEN 0
                        WHEN f.ftd_100_amount < 1000 THEN 10
                        WHEN f.ftd_100_amount < 5000 THEN 20
                        ELSE 50 END::float                                     AS ftd_amount_bonus_raw
            FROM ftd100_clients f
            WHERE f.ftd_100_date >= %(date_from)s
              AND f.ftd_100_date <  %(date_to_excl)s
              AND f.original_deposit_owner IS NOT NULL
        ),
        combined AS (
            SELECT COALESCE(f.accountid, t.accountid)       AS accountid,
                   COALESCE(f.agent_id,  t.agent_id)        AS agent_id,
                   COALESCE(t.is_ftc,    0)                 AS is_ftc,
                   COALESCE(f.is_ftd100, 0)                 AS is_ftd100,
                   f.ftd100_type,
                   COALESCE(f.ftd_amount_bonus_raw, 0)      AS ftd_amount_bonus_raw
            FROM ftd100_accs f
            FULL OUTER JOIN ftc_accs t ON t.accountid = f.accountid
        ),
        ftc_net AS (
            SELECT t.vtigeraccountid        AS accountid,
                   t.original_deposit_owner AS agent_id,
                   SUM(CASE WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN  t.usdamount
                            WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled')  THEN -t.usdamount
                       END)::float AS net_usd
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
              AND a.client_qualification_date IS NOT NULL
              AND a.client_qualification_date >= %(date_from)s
              AND a.client_qualification_date <  %(date_to_excl)s
              AND (a.client_qualification_date >= t.confirmation_time::date OR t.ftd = 1)
              AND a.is_test_account = 0
              AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
            GROUP BY t.vtigeraccountid, t.original_deposit_owner
        ),
        agent_totals AS (
            SELECT bon.agent_id,
                   SUM(bon.ftd100_count)::int        AS ftd100_total,
                   COALESCE(tgt.target_ftc, 0)::int  AS target_ftc
            FROM (
                SELECT agent_id, SUM(ftd100_count) AS ftd100_count
                FROM mv_sales_bonuses
                WHERE ftd_100_date >= %(date_from)s AND ftd_100_date < %(date_to_excl)s
                GROUP BY agent_id
            ) bon
            LEFT JOIN ({tgt_subq}) tgt ON tgt.agent_id = bon.agent_id
            GROUP BY bon.agent_id, tgt.target_ftc
        )
        SELECT
            COALESCE(u.office_name, 'N/A')              AS office_name,
            COALESCE(u.agent_name, u.full_name, 'N/A')  AS agent_name,
            c.accountid,
            c.is_ftc,
            c.is_ftd100,
            c.ftd100_type,
            COALESCE(fn.net_usd, 0)::float              AS ftc_net_usd,
            c.ftd_amount_bonus_raw,
            COALESCE(at.ftd100_total, 0)::int           AS ftd100_total,
            COALESCE(at.target_ftc,  0)::int            AS target_ftc,
            COALESCE(u.status, '')                      AS status
        FROM combined c
        JOIN crm_users u ON u.id = c.agent_id
        LEFT JOIN auth_users au ON au.crm_user_id = u.id
        LEFT JOIN ftc_net fn ON fn.accountid = c.accountid AND fn.agent_id = c.agent_id
        LEFT JOIN agent_totals at ON at.agent_id = c.agent_id
        WHERE (
            (u.department_ = 'Sales' AND u.team = 'Conversion' {role_filter})
            OR u.id IN (3750, 3614, 6119, 6479, 6492, 6355, 6666, 6694)
          )
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
          AND (%(is_admin)s OR au.role IS NULL OR au.role NOT IN ('admin', 'general'))
        ORDER BY u.office_name NULLS LAST, u.agent_name, c.accountid
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if dt_from >= _TARGETS_CUTOFF:
                _tgt_subq = """SELECT crm_user_id AS agent_id, monthly_ftd100_target AS target_ftc
                    FROM agent_targets_history
                    WHERE report_month = DATE_TRUNC('month', %(date_from)s::date)
                      AND crm_user_id IS NOT NULL"""
            else:
                _tgt_subq = """SELECT agent_id::int AS agent_id, ftc::int AS target_ftc
                    FROM targets
                    WHERE date = DATE_TRUNC('month', %(date_from)s::date)"""
            base_params = {
                "date_from":    date_from,
                "date_to_excl": date_to_exclusive,
                "is_admin":     user.get("role") in ("admin", "general"),
            }
            final_sql, final_params = _apply_role_filter(sql.replace('{tgt_subq}', _tgt_subq), base_params, role_filter)
            cur.execute(final_sql, final_params)
            rows = cur.fetchall()

        data = []
        for r in rows:
            office_name          = r[0]
            agent_name           = r[1]
            accountid            = r[2]
            is_ftc               = int(r[3])
            is_ftd100            = int(r[4])
            ftd100_type          = r[5]
            ftc_net_usd          = round(float(r[6]), 2)
            ftd_amount_bonus_raw = round(float(r[7]), 2)
            ftd100_total         = int(r[8])
            target_ftc           = int(r[9])
            status               = r[10] or ''

            if target_ftc == 0:
                qualify = ftd100_total >= 5
            else:
                qualify = ftd100_total >= 0.50 * target_ftc
            multiplier = get_sales_multiplier(ftd100_total)

            if is_ftd100 and qualify:
                basic_bonus      = multiplier if ftd100_type == 'full' else round(multiplier / 2, 2)
                ftd_amount_bonus = ftd_amount_bonus_raw
            else:
                basic_bonus      = 0
                ftd_amount_bonus = 0

            data.append({
                "office_name":      office_name,
                "agent_name":       agent_name,
                "accountid":        accountid,
                "is_ftc":           is_ftc,
                "is_ftd100":        is_ftd100,
                "ftc_net_usd":      ftc_net_usd,
                "basic_bonus":      basic_bonus,
                "ftd_amount_bonus": ftd_amount_bonus,
                "status":           status,
            })

        _result = {"rows": data}
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
