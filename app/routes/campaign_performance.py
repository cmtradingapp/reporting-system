from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app import cache
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

VALID_GROUPS = {
    "campaign_legacy_id":   "COALESCE(c.campaign_legacy_id, '(Unassigned)')",
    "campaign_name":        "COALESCE(c.campaign_name, '(Unassigned)')",
    "campaign_channel":     "COALESCE(c.campaign_channel, '(Unassigned)')",
    "campaign_sub_channel": "COALESCE(c.campaign_sub_channel, '(Unassigned)')",
    "original_affiliate":   "COALESCE(a.original_affiliate, '(Unassigned)')",
}
GROUP_LABELS = {
    "campaign_legacy_id":   "Campaign Legacy ID",
    "campaign_name":        "Campaign Name",
    "campaign_channel":     "Campaign Channel",
    "campaign_sub_channel": "Campaign Sub-channel",
    "original_affiliate":   "Original Affiliate",
    "none":                 "",
}


@router.get("/campaign-performance", response_class=HTMLResponse)
async def campaign_performance_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse("campaign_performance.html", {
        "request": request,
        "current_user": user,
    })


@router.get("/api/campaign-performance")
async def campaign_performance_api(request: Request, date_from: str, date_to: str):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    _ck = f"camp_perf_v1:{date_from}:{date_to}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Leads + Live Accounts (always today / MTD from mv_account_stats)
            cur.execute("""
                SELECT new_leads_today, new_leads_month, new_live_today, new_live_month
                FROM mv_account_stats LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                leads_today, leads_mtd, live_today, live_mtd = (
                    int(row[0] or 0), int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)
                )
            else:
                leads_today = leads_mtd = live_today = live_mtd = 0

            # Deposits, Withdrawals, Net, FTD daily + MTD (tx_date axis)
            cur.execute("""
                SELECT
                    COALESCE(SUM(deposit_usd),    0)                                                AS deposits,
                    COALESCE(SUM(withdrawal_usd), 0)                                                AS withdrawals,
                    COALESCE(SUM(net_usd),        0)                                                AS net_deposits,
                    COALESCE(SUM(ftd_count),      0)                                                AS ftd_mtd,
                    COALESCE(SUM(CASE WHEN tx_date = %(date_to)s THEN ftd_count ELSE 0 END), 0)     AS ftd_daily
                FROM mv_daily_kpis
                WHERE tx_date >= %(date_from)s AND tx_date < %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive, "date_to": date_to})
            row = cur.fetchone()
            if row:
                deposits_total    = float(row[0] or 0)
                withdrawals_total = float(row[1] or 0)
                net_total         = float(row[2] or 0)
                ftd_mtd           = int(row[3] or 0)
                ftd_daily         = int(row[4] or 0)
            else:
                deposits_total = withdrawals_total = net_total = ftd_mtd = ftd_daily = 0

            # FTC daily + MTD (qual_date axis)
            cur.execute("""
                SELECT
                    COALESCE(SUM(ftc_count), 0)                                                     AS ftc_mtd,
                    COALESCE(SUM(CASE WHEN qual_date = %(date_to)s THEN ftc_count ELSE 0 END), 0)   AS ftc_daily
                FROM mv_daily_kpis
                WHERE qual_date >= %(date_from)s AND qual_date < %(date_to_excl)s
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive, "date_to": date_to})
            row = cur.fetchone()
            ftc_mtd   = int(row[0] or 0) if row else 0
            ftc_daily = int(row[1] or 0) if row else 0

            # Number of Traders — distinct non-test accounts with approved transactions
            cur.execute("""
                SELECT COUNT(DISTINCT t.vtigeraccountid)
                FROM transactions t
                JOIN accounts a  ON a.accountid = t.vtigeraccountid
                JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                  AND t.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
                  AND t.confirmation_time::date >= %(date_from)s
                  AND t.confirmation_time::date <  %(date_to_excl)s
                  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
            """, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            row = cur.fetchone()
            traders_count = int(row[0] or 0) if row else 0

        _result = {
            "leads":         {"daily": leads_today, "mtd": leads_mtd},
            "live_accounts": {"daily": live_today,  "mtd": live_mtd},
            "ftd":           {"daily": ftd_daily,   "mtd": ftd_mtd},
            "ftc":           {"daily": ftc_daily,   "mtd": ftc_mtd},
            "deposits":      round(deposits_total, 2),
            "withdrawals":   round(withdrawals_total, 2),
            "net_deposits":  round(net_total, 2),
            "traders_count": traders_count,
            "date_from":     date_from,
            "date_to":       date_to,
        }
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


@router.get("/api/campaign-performance/table")
async def campaign_performance_table_api(
    request: Request, date_from: str, date_to: str,
    group1: str = "none", group2: str = "none",
):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    if group1 not in VALID_GROUPS and group1 != "none":
        return JSONResponse(status_code=400, content={"detail": "Invalid group1"})
    if group2 not in VALID_GROUPS and group2 != "none":
        return JSONResponse(status_code=400, content={"detail": "Invalid group2"})

    _ck = f"camp_tbl_v1:{date_from}:{date_to}:{group1}:{group2}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()
        date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    has_g1 = group1 != "none"
    has_g2 = group2 != "none" and has_g1
    g1_sql = VALID_GROUPS.get(group1, "")
    g2_sql = VALID_GROUPS.get(group2, "")

    conn = get_connection()
    try:
        with conn.cursor() as cur:

            # ── Accounts query (leads, live_accounts, ftc) ──────────────────
            acct_sel, acct_grp = [], []
            p = 1
            if has_g1: acct_sel.append(f"{g1_sql} AS g1"); acct_grp.append(str(p)); p += 1
            if has_g2: acct_sel.append(f"{g2_sql} AS g2"); acct_grp.append(str(p)); p += 1
            acct_sel += [
                "COUNT(*) FILTER (WHERE a.createdtime IS NOT NULL"
                " AND a.createdtime::date >= %(date_from)s"
                " AND a.createdtime::date < %(date_to_excl)s) AS leads",
                "COUNT(*) FILTER (WHERE a.createdtime IS NOT NULL"
                " AND a.createdtime::date >= %(date_from)s"
                " AND a.createdtime::date < %(date_to_excl)s"
                " AND a.birth_date IS NOT NULL) AS live_accounts",
                "COUNT(*) FILTER (WHERE a.client_qualification_date IS NOT NULL"
                " AND a.client_qualification_date::date >= %(date_from)s"
                " AND a.client_qualification_date::date < %(date_to_excl)s) AS ftc",
            ]
            acct_sql = (
                f"SELECT {', '.join(acct_sel)}"
                " FROM accounts a LEFT JOIN campaigns c ON SPLIT_PART(a.campaign, '.', 1) = c.crmid"
                " WHERE a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)"
            )
            if acct_grp:
                acct_sql += f" GROUP BY {', '.join(acct_grp)}"

            cur.execute(acct_sql, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            acct_rows = cur.fetchall()

            # ── Transactions query (ftd, deposits, withdrawals, traders) ────
            txn_sel, txn_grp = [], []
            p = 1
            if has_g1: txn_sel.append(f"{g1_sql} AS g1"); txn_grp.append(str(p)); p += 1
            if has_g2: txn_sel.append(f"{g2_sql} AS g2"); txn_grp.append(str(p)); p += 1
            txn_sel += [
                "COALESCE(SUM(CASE WHEN t.ftd = 1 AND t.transactiontype = 'Deposit'"
                " THEN 1 ELSE 0 END), 0) AS ftd",
                "COALESCE(SUM(CASE WHEN t.transactiontype IN ('Deposit', 'Withdrawal Cancelled')"
                " THEN t.usdamount ELSE 0 END), 0) AS deposits",
                "COALESCE(SUM(CASE WHEN t.transactiontype IN ('Withdrawal', 'Deposit Cancelled')"
                " THEN t.usdamount ELSE 0 END), 0) AS withdrawals",
                "COUNT(DISTINCT t.vtigeraccountid) AS traders",
            ]
            txn_sql = (
                f"SELECT {', '.join(txn_sel)}"
                " FROM transactions t"
                " JOIN accounts a ON a.accountid = t.vtigeraccountid"
                " LEFT JOIN campaigns c ON SPLIT_PART(a.campaign, '.', 1) = c.crmid"
                " JOIN crm_users u ON u.id = t.original_deposit_owner"
                " WHERE t.transactionapproval = 'Approved'"
                "   AND (t.deleted = 0 OR t.deleted IS NULL)"
                "   AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')"
                "   AND t.vtigeraccountid IS NOT NULL"
                "   AND a.is_test_account = 0"
                "   AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'"
                "   AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'"
                "   AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'"
                "   AND t.confirmation_time::date >= %(date_from)s"
                "   AND t.confirmation_time::date < %(date_to_excl)s"
                "   AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'"
            )
            if txn_grp:
                txn_sql += f" GROUP BY {', '.join(txn_grp)}"

            cur.execute(txn_sql, {"date_from": date_from, "date_to_excl": date_to_exclusive})
            txn_rows = cur.fetchall()

        # ── Merge rows ───────────────────────────────────────────────────────
        def _parse_acct(r):
            off = 0
            g1 = r[off] if has_g1 else None; off += int(has_g1)
            g2 = r[off] if has_g2 else None; off += int(has_g2)
            return g1, g2, int(r[off] or 0), int(r[off + 1] or 0), int(r[off + 2] or 0)

        def _parse_txn(r):
            off = 0
            g1 = r[off] if has_g1 else None; off += int(has_g1)
            g2 = r[off] if has_g2 else None; off += int(has_g2)
            return g1, g2, int(r[off] or 0), float(r[off + 1] or 0), float(r[off + 2] or 0), int(r[off + 3] or 0)

        merged: dict = {}
        for r in acct_rows:
            g1, g2, leads, live, ftc = _parse_acct(r)
            k = (g1, g2)
            merged[k] = {"g1": g1, "g2": g2, "leads": leads, "live_accounts": live, "ftc": ftc,
                         "ftd": 0, "deposits": 0.0, "withdrawals": 0.0, "net_deposits": 0.0, "traders": 0}

        for r in txn_rows:
            g1, g2, ftd, deps, wds, traders = _parse_txn(r)
            k = (g1, g2)
            if k not in merged:
                merged[k] = {"g1": g1, "g2": g2, "leads": 0, "live_accounts": 0, "ftc": 0,
                              "ftd": 0, "deposits": 0.0, "withdrawals": 0.0, "net_deposits": 0.0, "traders": 0}
            merged[k].update({
                "ftd": ftd,
                "deposits": round(deps, 2),
                "withdrawals": round(wds, 2),
                "net_deposits": round(deps - wds, 2),
                "traders": traders,
            })

        rows = sorted(merged.values(), key=lambda r: r["deposits"], reverse=True)

        totals = {
            "leads":         sum(r["leads"] for r in rows),
            "live_accounts": sum(r["live_accounts"] for r in rows),
            "ftd":           sum(r["ftd"] for r in rows),
            "ftc":           sum(r["ftc"] for r in rows),
            "deposits":      round(sum(r["deposits"] for r in rows), 2),
            "withdrawals":   round(sum(r["withdrawals"] for r in rows), 2),
            "net_deposits":  round(sum(r["net_deposits"] for r in rows), 2),
            "traders":       sum(r["traders"] for r in rows),
        }

        _result = {
            "rows":         rows,
            "totals":       totals,
            "group1_label": GROUP_LABELS.get(group1, ""),
            "group2_label": GROUP_LABELS.get(group2, ""),
            "date_from":    date_from,
            "date_to":      date_to,
        }
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
