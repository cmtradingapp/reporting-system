from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from datetime import date

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/ftc-date", response_class=HTMLResponse)
async def ftc_date_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse("ftc_date.html", {
        "request": request,
        "current_user": user,
    })


@router.get("/api/ftc-date")
async def ftc_date_api(request: Request, end_date: str = None):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    if not end_date:
        end_date = date.today().strftime("%Y-%m-%d")

    sql = """
        WITH ftc_groups AS (
            SELECT
                a.accountid,
                a.client_qualification_date::date AS qual_date,
                (%(end_date)s::date - a.client_qualification_date::date) AS days_diff
            FROM accounts a
            WHERE a.client_qualification_date IS NOT NULL
              AND a.client_qualification_date::date <= %(end_date)s::date
              AND a.is_test_account = 0
              AND (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 0 AND 120
        ),
        tx_per_account AS (
            SELECT
                t.vtigeraccountid AS accountid,
                SUM(CASE WHEN t.transactiontype = 'Deposit'             THEN t.usdamount ELSE 0 END)
              - SUM(CASE WHEN t.transactiontype = 'Deposit Cancelled'   THEN t.usdamount ELSE 0 END)
                    AS deposit_usd,
                SUM(CASE WHEN t.transactiontype = 'Withdrawal'          THEN t.usdamount ELSE 0 END)
              - SUM(CASE WHEN t.transactiontype = 'Withdrawal Cancelled' THEN t.usdamount ELSE 0 END)
                    AS withdrawal_usd
            FROM transactions t
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.confirmation_time::date <= %(end_date)s::date
            GROUP BY t.vtigeraccountid
        ),
        rdp AS (
            SELECT DISTINCT t.vtigeraccountid AS accountid
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactiontype = 'Deposit'
              AND t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND a.client_qualification_date IS NOT NULL
              AND t.confirmation_time::date > a.client_qualification_date::date
              AND t.confirmation_time::date <= %(end_date)s::date
              AND a.is_test_account = 0
        ),
        withdrawalers AS (
            SELECT DISTINCT t.vtigeraccountid AS accountid
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.transactiontype = 'Withdrawal'
              AND t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.confirmation_time::date <= %(end_date)s::date
              AND a.is_test_account = 0
        ),
        grouped AS (
            SELECT
                CASE
                    WHEN fg.days_diff BETWEEN 0   AND 7   THEN 1
                    WHEN fg.days_diff BETWEEN 8   AND 14  THEN 2
                    WHEN fg.days_diff BETWEEN 15  AND 30  THEN 3
                    WHEN fg.days_diff BETWEEN 31  AND 60  THEN 4
                    WHEN fg.days_diff BETWEEN 61  AND 90  THEN 5
                    WHEN fg.days_diff BETWEEN 91  AND 120 THEN 6
                END AS group_order,
                CASE
                    WHEN fg.days_diff BETWEEN 0   AND 7   THEN '0 - 7 days'
                    WHEN fg.days_diff BETWEEN 8   AND 14  THEN '8 - 14 days'
                    WHEN fg.days_diff BETWEEN 15  AND 30  THEN '15 - 30 days'
                    WHEN fg.days_diff BETWEEN 31  AND 60  THEN '31 - 60 days'
                    WHEN fg.days_diff BETWEEN 61  AND 90  THEN '61 - 90 days'
                    WHEN fg.days_diff BETWEEN 91  AND 120 THEN '91 - 120 days'
                END AS day_group,
                fg.accountid,
                COALESCE(tx.deposit_usd,    0) AS deposit_usd,
                COALESCE(tx.withdrawal_usd, 0) AS withdrawal_usd,
                CASE WHEN rdp.accountid IS NOT NULL THEN 1 ELSE 0 END AS is_rdp,
                CASE WHEN wd.accountid  IS NOT NULL THEN 1 ELSE 0 END AS is_withdrawaler
            FROM ftc_groups fg
            LEFT JOIN tx_per_account tx ON tx.accountid = fg.accountid
            LEFT JOIN rdp              ON rdp.accountid = fg.accountid
            LEFT JOIN withdrawalers wd ON wd.accountid  = fg.accountid
        )
        SELECT
            group_order,
            day_group,
            COUNT(DISTINCT accountid)        AS ftc_count,
            SUM(is_rdp)                      AS rdp_count,
            COALESCE(SUM(deposit_usd),    0) AS deposit_usd,
            COALESCE(SUM(withdrawal_usd), 0) AS withdrawal_usd,
            SUM(is_withdrawaler)             AS wd_count
        FROM grouped
        WHERE group_order IS NOT NULL
        GROUP BY group_order, day_group
        ORDER BY group_order
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"end_date": end_date})
            rows = cur.fetchall()

        data = []
        total_ftc = total_rdp = total_dep = total_wd = total_wdcount = 0

        for r in rows:
            group_order, day_group, ftc, rdp_cnt, dep, wd, wdcount = r
            ftc = int(ftc or 0)
            rdp_cnt = int(rdp_cnt or 0)
            dep = float(dep or 0)
            wd = float(wd or 0)
            wdcount = int(wdcount or 0)
            net_dep = dep - wd
            data.append({
                "day_group":      day_group,
                "ftc":            ftc,
                "rdp":            rdp_cnt,
                "deposit":        round(dep),
                "withdrawal":     round(wd),
                "net_deposit":    round(net_dep),
                "ltv":            round(net_dep / ftc, 2) if ftc > 0 else 0,
                "pct_std":        round(rdp_cnt / ftc * 100) if ftc > 0 else 0,
                "wd_count":       wdcount,
                "pct_wd_clients": round(wdcount / ftc * 100) if ftc > 0 else 0,
                "pct_wd_usd":     round(wd / dep * 100) if dep > 0 else 0,
            })
            total_ftc    += ftc
            total_rdp    += rdp_cnt
            total_dep    += dep
            total_wd     += wd
            total_wdcount += wdcount

        total_net = total_dep - total_wd
        grand = {
            "day_group":      "Grand Total",
            "ftc":            total_ftc,
            "rdp":            total_rdp,
            "deposit":        round(total_dep),
            "withdrawal":     round(total_wd),
            "net_deposit":    round(total_net),
            "ltv":            round(total_net / total_ftc, 2) if total_ftc > 0 else 0,
            "pct_std":        round(total_rdp / total_ftc * 100) if total_ftc > 0 else 0,
            "wd_count":       total_wdcount,
            "pct_wd_clients": round(total_wdcount / total_ftc * 100) if total_ftc > 0 else 0,
            "pct_wd_usd":     round(total_wd / total_dep * 100) if total_dep > 0 else 0,
        }

        return JSONResponse(content={"rows": data, "grand_total": grand, "end_date": end_date})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
