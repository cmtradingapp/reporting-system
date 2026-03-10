from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.auth.role_filters import get_role_filter
from app.db.postgres_conn import get_connection
from datetime import datetime, timedelta, date as date_type
import calendar


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


@router.get("/scoreboard", response_class=HTMLResponse)
async def scoreboard_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse("scoreboard.html", {"request": request, "current_user": user})


@router.get("/api/scoreboard")
async def scoreboard_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    role_filter = get_role_filter(user)
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt_to   = datetime.strptime(date_to,   "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    sql = """
        SELECT
            COALESCE(u.office_name, 'N/A')              AS office_name,
            COALESCE(u.agent_name, u.full_name, 'N/A')  AS agent_name,
            COALESCE(u.department_, '')                  AS department_,
            COALESCE(ftc.cnt, 0)                         AS ftc,
            COALESCE(tgt.target_ftc, 0)                  AS target_ftc,
            COALESCE(f100.ftd100_cnt, 0)                 AS ftd100,
            COALESCE(net.net_usd, 0)::float              AS net_deposits,
            COALESCE(ftd_cnt.cnt, 0)                     AS ftd_count
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
        LEFT JOIN (
            SELECT
                t.original_deposit_owner                         AS agent_id,
                SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                END)                                             AS net_usd
            FROM transactions t
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
              AND t.confirmation_time >= %(date_from)s
              AND t.confirmation_time <  %(date_to_excl)s
            GROUP BY t.original_deposit_owner
        ) net ON net.agent_id = u.id
        LEFT JOIN (
            SELECT
                t.original_deposit_owner          AS agent_id,
                COUNT(t.mttransactionsid)         AS cnt
            FROM transactions t
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype = 'Deposit'
              AND t.ftd = 1
              AND t.confirmation_time >= %(date_from)s
              AND t.confirmation_time <  %(date_to_excl)s
            GROUP BY t.original_deposit_owner
        ) ftd_cnt ON ftd_cnt.agent_id = u.id
        WHERE u.department_ = 'Sales'
          AND u.team = 'Conversion'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
          {role_filter}
        ORDER BY u.office_name NULLS LAST, COALESCE(ftc.cnt, 0) DESC, u.agent_name
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Fetch public holidays (graceful fallback if table missing)
            try:
                cur.execute("SELECT holiday_date FROM public_holidays")
                holidays = {row[0] for row in cur.fetchall()}
            except Exception:
                conn.rollback()
                holidays = set()

            base_params = {"date_from": date_from, "date_to_excl": date_to_exclusive}
            final_sql, final_params = _apply_role_filter(sql, base_params, role_filter)
            cur.execute(final_sql, final_params)
            rows = cur.fetchall()

            # Grand FTC — all departments/teams
            cur.execute("""
                SELECT COUNT(DISTINCT t.vtigeraccountid)
                FROM transactions t
                JOIN accounts a ON a.accountid = t.vtigeraccountid
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype = 'Deposit'
                  AND t.ftd = 1
                  AND a.client_qualification_date IS NOT NULL
                  AND a.client_qualification_date >= %(date_from)s
                  AND a.client_qualification_date <  %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            grand_ftc = int(cur.fetchone()[0] or 0)

            # Grand NET $ — all departments, year > 2024, no blank accountid, no test agents
            cur.execute("""
                SELECT COALESCE(SUM(CASE
                    WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled') THEN  t.usdamount
                    WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled') THEN -t.usdamount
                END), 0)
                FROM transactions t
                JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                  AND t.confirmation_time >= %(date_from)s
                  AND t.confirmation_time <  %(date_to_excl)s
                  AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
                  AND t.vtigeraccountid IS NOT NULL
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            grand_net = float(cur.fetchone()[0] or 0)

            # Open Volume — join via trading_accounts.login, filter year > 2024, no blank accountid, no test agents
            cur.execute("""
                SELECT COALESCE(SUM(d.notional_value), 0)
                FROM dealio_mt4trades d
                JOIN trading_accounts ta ON ta.login::bigint = d.login
                JOIN accounts a          ON a.accountid = ta.vtigeraccountid
                LEFT JOIN crm_users u    ON u.id = a.assigned_to
                WHERE d.open_time::date >= %(date_from)s
                  AND d.open_time::date <= %(date_to)s
                  AND EXTRACT(YEAR FROM d.open_time) >= 2024
                  AND ta.vtigeraccountid IS NOT NULL
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
            """, {"date_from": date_from, "date_to": date_to})
            open_volume = float(cur.fetchone()[0] or 0)

            # End Equity Zeroed — last available date in the selected month/year
            cur.execute("""
                WITH last_date AS (
                    SELECT MAX(date) AS last_available_date
                    FROM dealio_daily_profit
                    WHERE EXTRACT(YEAR  FROM date) = EXTRACT(YEAR  FROM %(date_to)s::date)
                      AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM %(date_to)s::date)
                ),
                old_bal_bonus AS (
                    SELECT
                        t.login,
                        t.confirmation_time::date AS bonus_date,
                        SUM(CASE WHEN t.transactiontype IN ('FRF Commission', 'Bonus')                          THEN t.usdamount ELSE 0 END)
                      - SUM(CASE WHEN t.transactiontype IN ('FRF Commission Cancelled', 'BonusCancelled') THEN t.usdamount ELSE 0 END)
                            AS old_bonus_usd
                    FROM transactions t
                    WHERE t.transactionapproval = 'Approved'
                      AND (t.deleted = 0 OR t.deleted IS NULL)
                      AND t.transactiontype IN ('FRF Commission', 'Bonus', 'FRF Commission Cancelled', 'BonusCancelled')
                    GROUP BY t.login, t.confirmation_time::date
                ),
                old_bonus_balance AS (
                    SELECT
                        login,
                        SUM(old_bonus_usd) AS old_bonus_balance
                    FROM old_bal_bonus
                    WHERE bonus_date <= (SELECT last_available_date FROM last_date)
                    GROUP BY login
                )
                SELECT COALESCE(SUM(
                    GREATEST(
                        GREATEST(d.convertedbalance + d.convertedfloatingpnl, 0)
                            - COALESCE(ob.old_bonus_balance, 0),
                        0
                    )
                ), 0) AS end_equity_zeroed
                FROM dealio_daily_profit d
                JOIN trading_accounts ta  ON ta.login::bigint = d.login
                JOIN accounts a           ON a.accountid = ta.vtigeraccountid
                JOIN crm_users u          ON u.id = d.assigned_to
                LEFT JOIN old_bonus_balance ob ON ob.login::bigint = d.login
                WHERE d.date = (SELECT last_available_date FROM last_date)
            """, {"date_to": date_to})
            end_equity_zeroed = float(cur.fetchone()[0] or 0)

        today = datetime.utcnow().date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        safe_wdp        = working_days_passed if working_days_passed > 0 else 1
        grand_ftc_rr    = round(grand_ftc  / safe_wdp * working_days)
        grand_net_rr    = round(grand_net  / safe_wdp * working_days, 2)
        open_volume_rr  = round(open_volume / safe_wdp * working_days) if open_volume > 0 else round(open_volume)

        data = [
            {
                "office_name": r[0],
                "agent_name":  r[1],
                "department":  r[2],
                "ftc":          r[3],
                "target_ftc":   r[4],
                "ftd100":       r[5],
                "net_deposits": round(r[6], 2),
                "ftd_count":    r[7],
            }
            for r in rows
        ]
        return JSONResponse(content={
            "rows":                 data,
            "total_ftc":            sum(r["ftc"] for r in data),
            "total_target_ftc":     sum(r["target_ftc"] for r in data),
            "total_ftd100":         sum(r["ftd100"] for r in data),
            "total_net_deposits":   round(sum(r["net_deposits"] for r in data), 2),
            "total_ftd_count":      sum(r["ftd_count"] for r in data),
            "grand_ftc":            grand_ftc,
            "grand_ftc_rr":         grand_ftc_rr,
            "grand_net":            round(grand_net, 2),
            "grand_net_rr":         grand_net_rr,
            "open_volume":          round(open_volume, 2),
            "open_volume_rr":       open_volume_rr,
            "end_equity_zeroed":    round(end_equity_zeroed, 2),
            "working_days":         working_days,
            "working_days_passed":  working_days_passed,
            "working_days_left":    working_days_left,
            "date_from":            date_from,
            "date_to":              date_to,
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


@router.get("/api/scoreboard/retention")
async def scoreboard_retention_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    role_filter = get_role_filter(user)
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt_to   = datetime.strptime(date_to,   "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
        last_day = last_day_of_month(dt_from).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    sql = """
        SELECT
            COALESCE(u.office_name, 'N/A')                   AS office_name,
            COALESCE(u.department, 'N/A')                     AS dept_name,
            COALESCE(u.agent_name, u.full_name, 'N/A')        AS agent_name,
            COALESCE(tgt.monthly_target_net, 0)::float        AS target_net,
            COALESCE(net.net_usd, 0)::float                   AS net_usd,
            COALESCE(dep.deposit_usd, 0)::float               AS deposit_usd,
            COALESCE(vol.open_volume_usd, 0)::float           AS open_volume_usd
        FROM crm_users u
        LEFT JOIN (
            SELECT agent_id::bigint, SUM(net)::float AS monthly_target_net
            FROM targets
            WHERE date >= %(date_from)s AND date <= %(last_day)s AND agent_id IS NOT NULL
            GROUP BY agent_id
        ) tgt ON tgt.agent_id = u.id
        LEFT JOIN (
            SELECT a.assigned_to AS agent_id,
                   SUM(CASE WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN t.usdamount
                            WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled')  THEN -t.usdamount END) AS net_usd
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved' AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
              AND t.confirmation_time >= %(date_from)s AND t.confirmation_time < %(date_to_excl)s
              AND COALESCE(t.comment, '') NOT ILIKE '%%bonus%%'
            GROUP BY a.assigned_to
        ) net ON net.agent_id = u.id
        LEFT JOIN (
            SELECT a.assigned_to AS agent_id, SUM(t.usdamount)::float AS deposit_usd
            FROM transactions t JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved' AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit','Withdrawal Cancelled')
              AND t.confirmation_time >= %(date_from)s AND t.confirmation_time < %(date_to_excl)s
              AND COALESCE(t.comment, '') NOT ILIKE '%%bonus%%'
            GROUP BY a.assigned_to
        ) dep ON dep.agent_id = u.id
        LEFT JOIN (
            SELECT a.assigned_to AS agent_id, SUM(d.notional_value)::float AS open_volume_usd
            FROM dealio_mt4trades d
            JOIN trading_accounts ta ON ta.login::bigint = d.login
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE d.open_time::date >= %(date_from)s AND d.open_time::date <= %(date_to)s
              AND EXTRACT(YEAR FROM d.open_time) >= 2024
              AND ta.vtigeraccountid IS NOT NULL
            GROUP BY a.assigned_to
        ) vol ON vol.agent_id = u.id
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

            base_params = {
                "date_from":    date_from,
                "date_to_excl": date_to_exclusive,
                "date_to":      date_to,
                "last_day":     last_day,
            }
            final_sql, final_params = _apply_role_filter(sql, base_params, role_filter)
            cur.execute(final_sql, final_params)
            rows = cur.fetchall()

        today               = datetime.utcnow().date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        data = [
            {
                "office_name":     r[0],
                "dept_name":       r[1],
                "agent_name":      r[2],
                "target_net":      round(r[3], 2),
                "net_usd":         round(r[4], 2),
                "deposit_usd":     round(r[5], 2),
                "open_volume_usd": round(r[6], 2),
            }
            for r in rows
        ]
        return JSONResponse(content={
            "rows":                data,
            "working_days":        working_days,
            "working_days_passed": working_days_passed,
            "working_days_left":   working_days_left,
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
