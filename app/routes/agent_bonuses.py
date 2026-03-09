from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from app.db.postgres_conn import get_connection
from datetime import datetime, timedelta, date as date_type
import calendar
import math


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
        return -0.005
    return 0.0


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
def agent_bonuses_page(request: Request):
    return templates.TemplateResponse("agent_bonuses.html", {"request": request})


@router.get("/api/agent-bonuses/retention")
def agent_bonuses_retention_api(date_from: str, date_to: str):
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
            COALESCE(u.office, '')                             AS office,
            COALESCE(tgt.monthly_target_net, 0)::float        AS target_net,
            COALESCE(net.net_usd, 0)::float                   AS net_usd,
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

            cur.execute(sql, {
                "date_from":    date_from,
                "date_to_excl": date_to_exclusive,
                "date_to":      date_to,
                "last_day":     last_day,
            })
            rows = cur.fetchall()

        today               = datetime.utcnow().date()
        month_end           = last_day_of_month(dt_from)
        working_days        = count_working_days(dt_from, month_end, holidays)
        working_days_passed = count_working_days(dt_from, min(dt_to, today), holidays)
        working_days_left   = working_days - working_days_passed

        wd_total  = working_days
        wd_passed = working_days_passed

        data = []
        for r in rows:
            office_name     = r[0]
            dept_name       = r[1]
            agent_name      = r[2]
            office          = r[3]
            target_net      = round(float(r[4]), 2)
            net_usd         = round(float(r[5]), 2)
            open_volume_usd = round(float(r[6]), 2)

            # Target volume prorated to working days passed
            target_vol_base = target_net * 1650
            if wd_total > 0:
                target_vol = math.ceil(target_vol_base / wd_total * wd_passed)
            else:
                target_vol = target_vol_base

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
            })

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
