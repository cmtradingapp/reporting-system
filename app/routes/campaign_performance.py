from fastapi import APIRouter, Request, Query
from typing import List, Optional
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

VALID_PERIODS = {"day", "month", "year"}
PERIOD_LABELS = {"day": "Day", "month": "Month", "year": "Year", "none": ""}

# Base filters applied to all accounts-level queries
_ACCT_FILTERS = (
    " AND a.accountid IS NOT NULL AND TRIM(a.accountid::text) != ''"
    " AND a.createdtime::date >= '2024-01-01'"
    " AND (a.assigned_to IS NULL OR a.assigned_to NOT IN ("
    "  SELECT id FROM crm_users"
    "  WHERE TRIM(COALESCE(agent_name, full_name, '')) ILIKE 'test%%'"
    "     OR TRIM(COALESCE(agent_name, full_name, '')) ILIKE 'duplicated%%'))"
)
# Same but without the assigned_to check (for transaction queries where agent is already filtered via u)
_TXN_ACCT_FILTERS = (
    " AND a.accountid IS NOT NULL AND TRIM(a.accountid::text) != ''"
)

VALID_GROUPS = {
    "marketing_group":      "COALESCE(c.marketing_group, '(Unassigned)')",
    "campaign_legacy_id":   "COALESCE(c.campaign_legacy_id, '(Unassigned)')",
    "campaign_name":        "COALESCE(c.campaign_name, '(Unassigned)')",
    "campaign_channel":     "COALESCE(c.campaign_channel, '(Unassigned)')",
    "campaign_sub_channel": "COALESCE(c.campaign_sub_channel, '(Unassigned)')",
    "original_affiliate":   "COALESCE(a.original_affiliate, '(Unassigned)')",
    "office_name":          "COALESCE(cu.office_name, '(Unassigned)')",
    "agent_name":           "COALESCE(cu.agent_name, '(Unassigned)')",
    "agent_team":           "COALESCE(cu.team, '(Unassigned)')",
    "country":              "COALESCE(a.country_iso, '(Unassigned)')",
    "client_classification": "COALESCE(a.classification_int::text, '(Unassigned)')",
    "segmentation":         "COALESCE(CASE a.segmentation WHEN '1' THEN '-A' WHEN '2' THEN 'B' WHEN '3' THEN 'C' WHEN '4' THEN '+A' END, '(Unassigned)')",
}
GROUP_LABELS = {
    "marketing_group":      "Marketing Group",
    "campaign_legacy_id":   "Campaign Legacy ID",
    "campaign_name":        "Campaign Name",
    "campaign_channel":     "Campaign Channel",
    "campaign_sub_channel": "Campaign Sub-channel",
    "original_affiliate":   "Original Affiliate",
    "office_name":          "Office",
    "agent_name":           "Agent",
    "agent_team":           "Agent Team",
    "country":              "Country",
    "client_classification": "Client Classification Category",
    "segmentation":         "Segmentation",
    "none":                 "",
}

FTC_GROUP_RANGES = {
    "0 - 7 days":   "(%(date_to)s::date - a.client_qualification_date::date) BETWEEN 0 AND 7",
    "8 - 14 days":  "(%(date_to)s::date - a.client_qualification_date::date) BETWEEN 8 AND 14",
    "15 - 30 days": "(%(date_to)s::date - a.client_qualification_date::date) BETWEEN 15 AND 30",
    "31 - 60 days": "(%(date_to)s::date - a.client_qualification_date::date) BETWEEN 31 AND 60",
    "61 - 90 days": "(%(date_to)s::date - a.client_qualification_date::date) BETWEEN 61 AND 90",
    "91 - 120 days":"(%(date_to)s::date - a.client_qualification_date::date) BETWEEN 91 AND 120",
    "120+ days":    "(%(date_to)s::date - a.client_qualification_date::date) > 120",
}

# Lazy-loaded country map (iso2 → full name)
_country_map_cache = None

def _get_country_map() -> dict:
    global _country_map_cache
    if _country_map_cache:
        return _country_map_cache
    try:
        from app.db.mssql_conn import get_country_map
        result = get_country_map()
        if result:
            _country_map_cache = result
        return result or {}
    except Exception:
        return {}


@router.get("/campaign-performance", response_class=HTMLResponse)
async def campaign_performance_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    ap = user.get("allowed_pages_list")
    if ap is not None and "marketing" not in ap:
        return RedirectResponse(url="/performance", status_code=302)
    if ap is None and user.get("role") not in ("admin", "marketing", "general"):
        return RedirectResponse(url="/performance", status_code=302)
    return templates.TemplateResponse("campaign_performance.html", {
        "request": request,
        "current_user": user,
    })


# ── Filter options ────────────────────────────────────────────────────────────

def _camp_filter_options_calc() -> dict:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT marketing_group
                FROM campaigns
                WHERE marketing_group IS NOT NULL AND marketing_group <> ''
                ORDER BY marketing_group
            """)
            marketing_groups = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT campaign_legacy_id
                FROM campaigns
                WHERE campaign_legacy_id IS NOT NULL AND campaign_legacy_id <> ''
                ORDER BY campaign_legacy_id
            """)
            legacy_ids = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT campaign_name
                FROM campaigns
                WHERE campaign_name IS NOT NULL AND campaign_name <> ''
                ORDER BY campaign_name
            """)
            campaign_names = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT campaign_channel
                FROM campaigns
                WHERE campaign_channel IS NOT NULL AND campaign_channel <> ''
                ORDER BY campaign_channel
            """)
            channels = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT campaign_sub_channel
                FROM campaigns
                WHERE campaign_sub_channel IS NOT NULL AND campaign_sub_channel <> ''
                ORDER BY campaign_sub_channel
            """)
            sub_channels = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT original_affiliate
                FROM accounts
                WHERE original_affiliate IS NOT NULL AND original_affiliate <> ''
                  AND is_test_account = 0
                ORDER BY original_affiliate
                LIMIT 2000
            """)
            affiliates = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT country_iso
                FROM accounts
                WHERE country_iso IS NOT NULL AND country_iso <> ''
                  AND is_test_account = 0
            """)
            _cmap = _get_country_map()
            countries = sorted({
                _cmap.get(r[0].upper(), r[0])
                for r in cur.fetchall()
                if r[0]
            })

            cur.execute("""
                SELECT DISTINCT office_name
                FROM crm_users
                WHERE office_name IS NOT NULL AND office_name <> ''
                ORDER BY office_name
            """)
            offices = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT agent_name
                FROM crm_users
                WHERE agent_name IS NOT NULL AND agent_name <> ''
                  AND TRIM(agent_name) NOT ILIKE 'test%'
                  AND TRIM(agent_name) NOT ILIKE 'duplicated%'
                ORDER BY agent_name
            """)
            agents = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT desk_name
                FROM crm_users
                WHERE desk_name IS NOT NULL AND desk_name <> ''
                ORDER BY desk_name
            """)
            teams = [r[0] for r in cur.fetchall()]

        return {
            "marketing_groups":      marketing_groups,
            "campaign_legacy_ids":   legacy_ids,
            "campaign_names":        campaign_names,
            "campaign_channels":     channels,
            "campaign_sub_channels": sub_channels,
            "original_affiliates":   affiliates,
            "countries":             countries,
            "offices":               offices,
            "agents":                agents,
            "teams":                 teams,
        }
    finally:
        conn.close()


@router.get("/api/campaign-performance/filter-options")
async def campaign_filter_options(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    _ck = "camp_filter_opts_v3"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        result = _camp_filter_options_calc()
        cache.set(_ck, result)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


# ── KPI cards ─────────────────────────────────────────────────────────────────

def _camp_kpi_calc(date_from: str, date_to: str, f_classification: str = None,
                   q_date_from: str = None, q_date_to: str = None,
                   f_mkt_group=None, f_legacy_id=None, f_campaign_name=None,
                   f_channel=None, f_sub_channel=None, f_affiliate=None,
                   f_country=None, f_office=None, f_agent=None, f_team=None,
                   f_segmentation: str = None) -> dict:
    dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()
    date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")

    if f_classification == "High Quality":
        class_where = "AND a.classification_int BETWEEN 6 AND 10"
    elif f_classification == "Low Quality":
        class_where = "AND a.classification_int BETWEEN 1 AND 5"
    elif f_classification == "No segmentation":
        class_where = "AND (a.classification_int IS NULL OR a.classification_int NOT BETWEEN 1 AND 10)"
    else:
        class_where = ""

    qual_where  = ""
    qual_params: dict = {}
    ftc_date_from    = date_from
    ftc_date_to_excl = date_to_exclusive
    ftc_date_daily   = date_to
    if q_date_from and q_date_to:
        q_dt_to = datetime.strptime(q_date_to, "%Y-%m-%d").date()
        q_date_to_excl = (q_dt_to + timedelta(days=1)).strftime("%Y-%m-%d")
        qual_where = ("AND a.client_qualification_date IS NOT NULL"
                      " AND a.client_qualification_date::date >= %(q_date_from)s"
                      " AND a.client_qualification_date::date < %(q_date_to_excl)s")
        qual_params = {"q_date_from": q_date_from, "q_date_to_excl": q_date_to_excl}
        ftc_date_from    = q_date_from
        ftc_date_to_excl = q_date_to_excl
        ftc_date_daily   = q_date_to

    extra_where = f"{class_where} {qual_where}".strip()

    # Marketing / campaign filters
    camp_filter_params: dict = {}
    camp_filter_where, _, kpi_needs_cu = _build_filter_clauses(
        f_mkt_group, f_legacy_id, f_campaign_name, f_channel, f_sub_channel,
        f_affiliate, None, None, date_to, camp_filter_params,
        f_country=f_country, f_office=f_office, f_agent=f_agent, f_team=f_team,
        f_segmentation=f_segmentation
    )
    camp_filter_where = camp_filter_where.strip()
    needs_camp_join = bool(f_mkt_group or f_legacy_id or f_campaign_name or f_channel or f_sub_channel)
    camp_join = "LEFT JOIN campaigns c ON SPLIT_PART(a.campaign, '.', 1) = c.crmid" if needs_camp_join else ""
    kpi_cu_join = "LEFT JOIN crm_users cu ON cu.id = a.assigned_to" if kpi_needs_cu else ""
    all_extra = f"{extra_where} {camp_filter_where}".strip()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if not all_extra:
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
            else:
                base_p = {"date_from": date_from, "date_to_excl": date_to_exclusive, "date_to": date_to,
                          **qual_params, **camp_filter_params}
                cur.execute(f"""
                    SELECT
                        COUNT(*) FILTER (WHERE a.createdtime::date = %(date_to)s)                    AS leads_today,
                        COUNT(*) FILTER (WHERE a.createdtime::date >= %(date_from)s
                                           AND a.createdtime::date < %(date_to_excl)s)               AS leads_mtd,
                        COUNT(*) FILTER (WHERE a.createdtime::date = %(date_to)s AND a.birth_date IS NOT NULL) AS live_today,
                        COUNT(*) FILTER (WHERE a.birth_date IS NOT NULL
                                           AND a.createdtime::date >= %(date_from)s
                                           AND a.createdtime::date < %(date_to_excl)s)               AS live_mtd
                    FROM accounts a
                    {camp_join}
                    {kpi_cu_join}
                    WHERE a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)
                    {_ACCT_FILTERS}
                    {extra_where}
                    {camp_filter_where}
                """, base_p)
                row = cur.fetchone()
                leads_today, leads_mtd, live_today, live_mtd = (
                    int(row[0] or 0), int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)
                ) if row else (0, 0, 0, 0)

            if not all_extra:
                # FTD + deposits from MV (join crm_users to exclude duplicated% agents, same as table)
                cur.execute("""
                    SELECT
                        COALESCE(SUM(k.deposit_usd),    0)                                                      AS deposits,
                        COALESCE(SUM(k.withdrawal_usd), 0)                                                      AS withdrawals,
                        COALESCE(SUM(k.net_usd),        0)                                                      AS net_deposits,
                        COALESCE(SUM(k.ftd_count),      0)                                                      AS ftd_mtd,
                        COALESCE(SUM(CASE WHEN k.tx_date = %(date_to)s THEN k.ftd_count ELSE 0 END), 0)         AS ftd_daily
                    FROM mv_daily_kpis k
                    LEFT JOIN crm_users u ON u.id = k.agent_id
                    WHERE k.tx_date >= %(date_from)s AND k.tx_date < %(date_to_excl)s
                      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
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

                # FTC from accounts table directly — same logic as the performance table
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE a.client_qualification_date::date >= %(date_from)s
                                           AND a.client_qualification_date::date < %(date_to_excl)s) AS ftc_mtd,
                        COUNT(*) FILTER (WHERE a.client_qualification_date::date = %(date_to)s)      AS ftc_daily
                    FROM accounts a
                    WHERE a.client_qualification_date IS NOT NULL
                      AND a.is_test_account = 0
                      AND (a.is_demo = 0 OR a.is_demo IS NULL)
                """ + _ACCT_FILTERS, {"date_from": date_from, "date_to_excl": date_to_exclusive, "date_to": date_to})
                row = cur.fetchone()
                ftc_mtd   = int(row[0] or 0) if row else 0
                ftc_daily = int(row[1] or 0) if row else 0
            else:
                base_p = {"date_from": date_from, "date_to_excl": date_to_exclusive, "date_to": date_to,
                          "ftc_date_from": ftc_date_from, "ftc_date_to_excl": ftc_date_to_excl, "ftc_date_daily": ftc_date_daily,
                          **qual_params, **camp_filter_params}
                cur.execute(f"""
                    SELECT
                        COALESCE(SUM(CASE WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN t.usdamount ELSE 0 END), 0) AS deposits,
                        COALESCE(SUM(CASE WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled') THEN t.usdamount ELSE 0 END), 0) AS withdrawals,
                        COALESCE(SUM(CASE WHEN t.ftd = 1 AND t.transactiontype = 'Deposit' THEN 1 ELSE 0 END), 0)                       AS ftd_mtd,
                        COALESCE(SUM(CASE WHEN t.ftd = 1 AND t.transactiontype = 'Deposit'
                                          AND t.confirmation_time::date = %(date_to)s THEN 1 ELSE 0 END), 0)                            AS ftd_daily
                    FROM transactions t
                    JOIN accounts a ON a.accountid = t.vtigeraccountid
                    {camp_join}
                    LEFT JOIN crm_users u ON u.id = t.original_deposit_owner
                    WHERE t.transactionapproval = 'Approved'
                      AND (t.deleted = 0 OR t.deleted IS NULL)
                      AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
                      AND t.vtigeraccountid IS NOT NULL
                      AND a.is_test_account = 0
                      AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                      AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                      AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
                      AND t.confirmation_time::date >= %(date_from)s
                      AND t.confirmation_time::date <  %(date_to_excl)s
                    {extra_where}
                    {camp_filter_where}
                """, base_p)
                row = cur.fetchone()
                if row:
                    deposits_total    = float(row[0] or 0)
                    withdrawals_total = float(row[1] or 0)
                    net_total         = deposits_total - withdrawals_total
                    ftd_mtd           = int(row[2] or 0)
                    ftd_daily         = int(row[3] or 0)
                else:
                    deposits_total = withdrawals_total = net_total = ftd_mtd = ftd_daily = 0

                cur.execute(f"""
                    SELECT
                        COUNT(*) FILTER (WHERE a.client_qualification_date::date >= %(ftc_date_from)s
                                           AND a.client_qualification_date::date < %(ftc_date_to_excl)s) AS ftc_mtd,
                        COUNT(*) FILTER (WHERE a.client_qualification_date::date = %(ftc_date_daily)s)   AS ftc_daily
                    FROM accounts a
                    {camp_join}
                    {kpi_cu_join}
                    WHERE a.client_qualification_date IS NOT NULL
                      AND a.is_test_account = 0
                      AND (a.is_demo = 0 OR a.is_demo IS NULL)
                    {_ACCT_FILTERS}
                    {class_where}
                    {camp_filter_where}
                """, base_p)
                row = cur.fetchone()
                ftc_mtd   = int(row[0] or 0) if row else 0
                ftc_daily = int(row[1] or 0) if row else 0

            base_p = {"date_from": date_from, "date_to_excl": date_to_exclusive,
                      **qual_params, **camp_filter_params}
            cur.execute(f"""
                SELECT COUNT(DISTINCT t.vtigeraccountid)
                FROM transactions t
                JOIN accounts a  ON a.accountid = t.vtigeraccountid
                {camp_join}
                LEFT JOIN crm_users u ON u.id = t.original_deposit_owner
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')
                  AND t.vtigeraccountid IS NOT NULL
                  AND a.is_test_account = 0
                  {_TXN_ACCT_FILTERS}
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'
                  AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'
                  AND t.confirmation_time::date >= %(date_from)s
                  AND t.confirmation_time::date <  %(date_to_excl)s
                  AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
                {extra_where}
                {camp_filter_where}
            """, base_p)
            row = cur.fetchone()
            traders_count = int(row[0] or 0) if row else 0

        return {
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
    finally:
        conn.close()


@router.get("/api/campaign-performance")
async def campaign_performance_api(
    request: Request, date_from: str, date_to: str,
    f_classification: str = None, q_date_from: str = None, q_date_to: str = None,
    f_mkt_group: Optional[List[str]] = Query(default=None),
    f_legacy_id: Optional[List[str]] = Query(default=None),
    f_campaign_name: Optional[List[str]] = Query(default=None),
    f_channel: Optional[List[str]] = Query(default=None),
    f_sub_channel: Optional[List[str]] = Query(default=None),
    f_affiliate: Optional[List[str]] = Query(default=None),
    f_country: Optional[List[str]] = Query(default=None),
    f_office: Optional[List[str]] = Query(default=None),
    f_agent: Optional[List[str]] = Query(default=None),
    f_team: Optional[List[str]] = Query(default=None),
    f_segmentation: str = None,
):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    def _ck_part(v): return ','.join(sorted(v)) if v else ''
    _ck = (f"camp_perf_v9:{date_from}:{date_to}:{f_classification}:{q_date_from}:{q_date_to}"
           f":{_ck_part(f_mkt_group)}:{_ck_part(f_legacy_id)}:{_ck_part(f_campaign_name)}"
           f":{_ck_part(f_channel)}:{_ck_part(f_sub_channel)}:{_ck_part(f_affiliate)}"
           f":{_ck_part(f_country)}:{_ck_part(f_office)}:{_ck_part(f_agent)}:{_ck_part(f_team)}"
           f":{f_segmentation}")
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    try:
        _result = _camp_kpi_calc(
            date_from, date_to, f_classification, q_date_from, q_date_to,
            f_mkt_group, f_legacy_id, f_campaign_name, f_channel, f_sub_channel, f_affiliate,
            f_country, f_office, f_agent, f_team, f_segmentation=f_segmentation
        )
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


# ── Table ─────────────────────────────────────────────────────────────────────

def _build_filter_clauses(
    f_mkt_group, f_legacy_id, f_campaign_name, f_channel, f_sub_channel,
    f_affiliate, f_classification, ftc_groups_list, date_to, params,
    q_date_from=None, q_date_to_excl=None,
    f_country=None, f_office=None, f_agent=None, f_team=None,
    f_segmentation=None
):
    """Returns (extra_where_str, needs_cc_join, needs_cu_join).
    Appends needed params to the `params` dict in-place."""
    clauses = []

    if f_mkt_group:
        vals = list(f_mkt_group) if not isinstance(f_mkt_group, str) else [f_mkt_group]
        clauses.append("AND c.marketing_group = ANY(%(f_mkt_group)s)")
        params["f_mkt_group"] = vals
    if f_legacy_id:
        vals = list(f_legacy_id) if not isinstance(f_legacy_id, str) else [f_legacy_id]
        clauses.append("AND c.campaign_legacy_id = ANY(%(f_legacy_id)s)")
        params["f_legacy_id"] = vals
    if f_campaign_name:
        vals = list(f_campaign_name) if not isinstance(f_campaign_name, str) else [f_campaign_name]
        clauses.append("AND c.campaign_name = ANY(%(f_campaign_name)s)")
        params["f_campaign_name"] = vals
    if f_channel:
        vals = list(f_channel) if not isinstance(f_channel, str) else [f_channel]
        clauses.append("AND c.campaign_channel = ANY(%(f_channel)s)")
        params["f_channel"] = vals
    if f_sub_channel:
        vals = list(f_sub_channel) if not isinstance(f_sub_channel, str) else [f_sub_channel]
        clauses.append("AND c.campaign_sub_channel = ANY(%(f_sub_channel)s)")
        params["f_sub_channel"] = vals
    if f_affiliate:
        vals = list(f_affiliate) if not isinstance(f_affiliate, str) else [f_affiliate]
        clauses.append("AND a.original_affiliate = ANY(%(f_affiliate)s)")
        params["f_affiliate"] = vals
    if f_country:
        names = list(f_country) if not isinstance(f_country, str) else [f_country]
        _cmap = _get_country_map()
        _rev  = {v.upper(): k for k, v in _cmap.items()}
        iso_vals = [_rev.get(n.upper(), n) for n in names]
        clauses.append("AND a.country_iso = ANY(%(f_country)s)")
        params["f_country"] = iso_vals

    needs_cu_join = False
    if f_office:
        vals = list(f_office) if not isinstance(f_office, str) else [f_office]
        clauses.append("AND cu.office_name = ANY(%(f_office)s)")
        params["f_office"] = vals
        needs_cu_join = True
    if f_agent:
        vals = list(f_agent) if not isinstance(f_agent, str) else [f_agent]
        clauses.append("AND cu.agent_name = ANY(%(f_agent)s)")
        params["f_agent"] = vals
        needs_cu_join = True
    if f_team:
        vals = list(f_team) if not isinstance(f_team, str) else [f_team]
        clauses.append("AND cu.desk_name = ANY(%(f_team)s)")
        params["f_team"] = vals
        needs_cu_join = True

    needs_cc_join = False
    if f_classification:
        if f_classification == "High Quality":
            clauses.append("AND a.classification_int BETWEEN 6 AND 10")
        elif f_classification == "Low Quality":
            clauses.append("AND a.classification_int BETWEEN 1 AND 5")
        else:  # No segmentation
            clauses.append("AND (a.classification_int IS NULL OR a.classification_int NOT BETWEEN 1 AND 10)")

    if ftc_groups_list:
        ftc_conds = [FTC_GROUP_RANGES[g] for g in ftc_groups_list if g in FTC_GROUP_RANGES]
        if ftc_conds:
            params["date_to"] = date_to
            clauses.append("AND a.client_qualification_date IS NOT NULL")
            clauses.append("AND (" + " OR ".join(ftc_conds) + ")")

    if q_date_from and q_date_to_excl:
        clauses.append("AND a.client_qualification_date IS NOT NULL")
        clauses.append("AND a.client_qualification_date::date >= %(q_date_from)s")
        clauses.append("AND a.client_qualification_date::date < %(q_date_to_excl)s")
        params["q_date_from"]    = q_date_from
        params["q_date_to_excl"] = q_date_to_excl

    if f_segmentation:
        _seg_map = {'-A': '1', 'B': '2', 'C': '3', '+A': '4'}
        db_val = _seg_map.get(f_segmentation)
        if db_val:
            clauses.append("AND a.segmentation = %(f_segmentation)s")
            params["f_segmentation"] = db_val

    return "\n".join(clauses), needs_cc_join, needs_cu_join


def _camp_table_calc(
    date_from: str, date_to: str,
    group1: str = "none", group2: str = "none", period: str = "none",
    f_mkt_group=None, f_legacy_id=None, f_campaign_name=None,
    f_channel=None, f_sub_channel=None, f_affiliate=None,
    f_classification: str = None, ftc_groups: str = None,
    q_date_from: str = None, q_date_to: str = None,
    f_country=None, f_office=None, f_agent=None, f_team=None,
    f_segmentation: str = None,
) -> dict:
    dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()
    date_to_exclusive = (dt_to + timedelta(days=1)).strftime("%Y-%m-%d")

    q_date_to_excl = None
    if q_date_from and q_date_to:
        q_dt_to = datetime.strptime(q_date_to, "%Y-%m-%d").date()
        q_date_to_excl = (q_dt_to + timedelta(days=1)).strftime("%Y-%m-%d")

    ftc_groups_list = [g.strip() for g in ftc_groups.split(",")] if ftc_groups else None

    has_period = period != "none"
    has_g1 = group1 != "none"
    has_g2 = group2 != "none" and has_g1
    g1_sql = VALID_GROUPS.get(group1, "")
    g2_sql = VALID_GROUPS.get(group2, "")

    # Determine which extra JOINs are needed
    groups_needing_cu = {"office_name", "agent_name"}
    groups_needing_cc = set()
    needs_cu_join_for_group = group1 in groups_needing_cu or group2 in groups_needing_cu
    needs_cc_join_for_group = group1 in groups_needing_cc or group2 in groups_needing_cc

    # Period SQL
    if period == "day":
        acct_period_sql = "a.createdtime::date"
        txn_period_sql  = "t.confirmation_time::date"
        ftc_period_sql  = "a.client_qualification_date::date"
    elif period == "month":
        acct_period_sql = "date_trunc('month', a.createdtime)::date"
        txn_period_sql  = "date_trunc('month', t.confirmation_time)::date"
        ftc_period_sql  = "date_trunc('month', a.client_qualification_date)::date"
    elif period == "year":
        acct_period_sql = "date_trunc('year', a.createdtime)::date"
        txn_period_sql  = "date_trunc('year', t.confirmation_time)::date"
        ftc_period_sql  = "date_trunc('year', a.client_qualification_date)::date"
    else:
        acct_period_sql = txn_period_sql = ftc_period_sql = ""

    # Build filter clauses
    ftc_from    = q_date_from    if q_date_from    else date_from
    ftc_to_excl = q_date_to_excl if q_date_to_excl else date_to_exclusive
    acct_params = {"date_from": date_from, "date_to_excl": date_to_exclusive, "ftc_from": ftc_from, "ftc_to_excl": ftc_to_excl}
    filter_where, needs_cc_join_for_filter, needs_cu_join_for_filter = _build_filter_clauses(
        f_mkt_group, f_legacy_id, f_campaign_name, f_channel, f_sub_channel,
        f_affiliate, f_classification, ftc_groups_list, date_to, acct_params,
        q_date_from=q_date_from, q_date_to_excl=q_date_to_excl,
        f_country=f_country, f_office=f_office, f_agent=f_agent, f_team=f_team,
        f_segmentation=f_segmentation
    )
    needs_cu_join = needs_cu_join_for_group or needs_cu_join_for_filter
    needs_cc_join = needs_cc_join_for_group or needs_cc_join_for_filter

    # Extra JOINs
    extra_joins = ""
    if needs_cu_join:
        extra_joins += " LEFT JOIN crm_users cu ON cu.id = a.assigned_to"
    if needs_cc_join:
        extra_joins += " LEFT JOIN client_classification cc ON cc.accountid = a.accountid::BIGINT"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # ── Accounts query ─────────────────────────────────────────────
            acct_sel, acct_grp = [], []
            p = 1
            if has_period: acct_sel.append(f"{acct_period_sql} AS period"); acct_grp.append(str(p)); p += 1
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
                # FTC handled in separate query (grouped by qualification date, not creation date)
            ]
            acct_date_filter = (
                " AND a.createdtime::date >= %(date_from)s AND a.createdtime::date < %(date_to_excl)s"
                if has_period else ""
            )
            acct_sql = (
                f"SELECT {', '.join(acct_sel)}"
                " FROM accounts a LEFT JOIN campaigns c ON SPLIT_PART(a.campaign, '.', 1) = c.crmid"
                f"{extra_joins}"
                " WHERE a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)"
                + _ACCT_FILTERS +
                f"{acct_date_filter}"
                f"\n{filter_where}"
            )
            if acct_grp:
                acct_sql += f" GROUP BY {', '.join(acct_grp)}"

            cur.execute(acct_sql, acct_params)
            acct_rows = cur.fetchall()

            # ── Transactions query ─────────────────────────────────────────
            txn_params = {"date_from": date_from, "date_to_excl": date_to_exclusive}
            txn_filter_where, _, _ = _build_filter_clauses(
                f_mkt_group, f_legacy_id, f_campaign_name, f_channel, f_sub_channel,
                f_affiliate, f_classification, ftc_groups_list, date_to, txn_params,
                q_date_from=q_date_from, q_date_to_excl=q_date_to_excl,
                f_country=f_country, f_office=f_office, f_agent=f_agent, f_team=f_team,
                f_segmentation=f_segmentation
            )

            txn_sel, txn_grp = [], []
            p = 1
            if has_period: txn_sel.append(f"{txn_period_sql} AS period"); txn_grp.append(str(p)); p += 1
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
                " LEFT JOIN crm_users u ON u.id = t.original_deposit_owner"
                f"{extra_joins}"
                " WHERE t.transactionapproval = 'Approved'"
                "   AND (t.deleted = 0 OR t.deleted IS NULL)"
                "   AND t.transactiontype IN ('Deposit', 'Withdrawal Cancelled', 'Withdrawal', 'Deposit Cancelled')"
                "   AND t.vtigeraccountid IS NOT NULL"
                "   AND a.is_test_account = 0"
                + _TXN_ACCT_FILTERS +
                "   AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'"
                "   AND TRIM(COALESCE(u.full_name, '')) NOT ILIKE 'test%%'"
                "   AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'duplicated%%'"
                "   AND t.confirmation_time::date >= %(date_from)s"
                "   AND t.confirmation_time::date < %(date_to_excl)s"
                "   AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'"
                f"\n{txn_filter_where}"
            )
            if txn_grp:
                txn_sql += f" GROUP BY {', '.join(txn_grp)}"

            cur.execute(txn_sql, txn_params)
            txn_rows = cur.fetchall()

            # ── FTC query (grouped by qualification date, not creation date) ─
            ftc_params = {"ftc_from": ftc_from, "ftc_to_excl": ftc_to_excl}
            ftc_filter_where, _, _ = _build_filter_clauses(
                f_mkt_group, f_legacy_id, f_campaign_name, f_channel, f_sub_channel,
                f_affiliate, f_classification, ftc_groups_list, date_to, ftc_params,
                q_date_from=q_date_from, q_date_to_excl=q_date_to_excl,
                f_country=f_country, f_office=f_office, f_agent=f_agent, f_team=f_team,
                f_segmentation=f_segmentation
            )
            ftc_sel, ftc_grp = [], []
            p = 1
            if has_period: ftc_sel.append(f"{ftc_period_sql} AS period"); ftc_grp.append(str(p)); p += 1
            if has_g1: ftc_sel.append(f"{g1_sql} AS g1"); ftc_grp.append(str(p)); p += 1
            if has_g2: ftc_sel.append(f"{g2_sql} AS g2"); ftc_grp.append(str(p)); p += 1
            ftc_sel.append("COUNT(*) AS ftc")
            ftc_sql = (
                f"SELECT {', '.join(ftc_sel)}"
                " FROM accounts a LEFT JOIN campaigns c ON SPLIT_PART(a.campaign, '.', 1) = c.crmid"
                f"{extra_joins}"
                " WHERE a.is_test_account = 0 AND (a.is_demo = 0 OR a.is_demo IS NULL)"
                " AND a.client_qualification_date IS NOT NULL"
                " AND a.client_qualification_date::date >= %(ftc_from)s"
                " AND a.client_qualification_date::date < %(ftc_to_excl)s"
                + _ACCT_FILTERS +
                f"\n{ftc_filter_where}"
            )
            if ftc_grp:
                ftc_sql += f" GROUP BY {', '.join(ftc_grp)}"
            cur.execute(ftc_sql, ftc_params)
            ftc_rows = cur.fetchall()

        # ── Merge rows ────────────────────────────────────────────────────
        def _parse_acct(r):
            off = 0
            per = str(r[off]) if has_period else None; off += int(has_period)
            g1  = r[off] if has_g1 else None;          off += int(has_g1)
            g2  = r[off] if has_g2 else None;          off += int(has_g2)
            return per, g1, g2, int(r[off] or 0), int(r[off + 1] or 0)

        def _parse_ftc(r):
            off = 0
            per = str(r[off]) if has_period else None; off += int(has_period)
            g1  = r[off] if has_g1 else None;          off += int(has_g1)
            g2  = r[off] if has_g2 else None;          off += int(has_g2)
            return per, g1, g2, int(r[off] or 0)

        def _parse_txn(r):
            off = 0
            per = str(r[off]) if has_period else None; off += int(has_period)
            g1  = r[off] if has_g1 else None;          off += int(has_g1)
            g2  = r[off] if has_g2 else None;          off += int(has_g2)
            return per, g1, g2, int(r[off] or 0), float(r[off + 1] or 0), float(r[off + 2] or 0), int(r[off + 3] or 0)

        merged: dict = {}
        for r in acct_rows:
            per, g1, g2, leads, live = _parse_acct(r)
            k = (per, g1, g2)
            merged[k] = {"period": per, "g1": g1, "g2": g2, "leads": leads, "live_accounts": live, "ftc": 0,
                         "ftd": 0, "deposits": 0.0, "withdrawals": 0.0, "net_deposits": 0.0, "traders": 0}

        for r in ftc_rows:
            per, g1, g2, ftc = _parse_ftc(r)
            k = (per, g1, g2)
            if k not in merged:
                merged[k] = {"period": per, "g1": g1, "g2": g2, "leads": 0, "live_accounts": 0, "ftc": 0,
                              "ftd": 0, "deposits": 0.0, "withdrawals": 0.0, "net_deposits": 0.0, "traders": 0}
            merged[k]["ftc"] = ftc

        for r in txn_rows:
            per, g1, g2, ftd, deps, wds, traders = _parse_txn(r)
            k = (per, g1, g2)
            if k not in merged:
                merged[k] = {"period": per, "g1": g1, "g2": g2, "leads": 0, "live_accounts": 0, "ftc": 0,
                              "ftd": 0, "deposits": 0.0, "withdrawals": 0.0, "net_deposits": 0.0, "traders": 0}
            merged[k].update({
                "ftd": ftd,
                "deposits": round(deps, 2),
                "withdrawals": round(wds, 2),
                "net_deposits": round(deps - wds, 2),
                "traders": traders,
            })

        # Translate country ISO codes to full names
        if group1 == "country" or group2 == "country":
            cmap = _get_country_map()
            for row in merged.values():
                if group1 == "country" and row.get("g1") and row["g1"] != "(Unassigned)":
                    row["g1"] = cmap.get(row["g1"].strip().upper(), row["g1"])
                if group2 == "country" and row.get("g2") and row["g2"] != "(Unassigned)":
                    row["g2"] = cmap.get(row["g2"].strip().upper(), row["g2"])

        def _cr(num, den):
            return round(num / den * 100, 2) if den else 0.0

        def _add_cr(row):
            row["cr_lead_to_live"] = _cr(row["live_accounts"], row["leads"])
            row["cr_live_to_ftd"]  = _cr(row["ftd"],           row["live_accounts"])
            row["cr_lead_to_ftc"]  = _cr(row["ftc"],           row["leads"])
            row["cr_live_to_ftc"]  = _cr(row["ftc"],           row["live_accounts"])
            row["cr_ftd_to_ftc"]   = _cr(row["ftc"],           row["ftd"])
            row["ltv"]             = round(row["net_deposits"] / row["ftc"], 2) if row["ftc"] else 0.0
            return row

        for row in merged.values():
            _add_cr(row)

        if has_period:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
            rows = sorted(
                (r for r in merged.values()
                 if r.get("period") and datetime.strptime(r["period"][:10], "%Y-%m-%d").date() >= dt_from
                 and datetime.strptime(r["period"][:10], "%Y-%m-%d").date() <= dt_to),
                key=lambda r: (r["g1"] or "", r["g2"] or "", r["period"] or "")
            )
        else:
            rows = sorted(merged.values(), key=lambda r: r["deposits"], reverse=True)

        totals = _add_cr({
            "leads":         sum(r["leads"] for r in rows),
            "live_accounts": sum(r["live_accounts"] for r in rows),
            "ftd":           sum(r["ftd"] for r in rows),
            "ftc":           sum(r["ftc"] for r in rows),
            "deposits":      round(sum(r["deposits"] for r in rows), 2),
            "withdrawals":   round(sum(r["withdrawals"] for r in rows), 2),
            "net_deposits":  round(sum(r["net_deposits"] for r in rows), 2),
            "traders":       sum(r["traders"] for r in rows),
        })

        return {
            "rows":         rows,
            "totals":       totals,
            "group1_label": GROUP_LABELS.get(group1, ""),
            "group2_label": GROUP_LABELS.get(group2, ""),
            "period":       period,
            "period_label": PERIOD_LABELS.get(period, ""),
            "date_from":    date_from,
            "date_to":      date_to,
        }
    finally:
        conn.close()


@router.get("/api/campaign-performance/table")
async def campaign_performance_table_api(
    request: Request, date_from: str, date_to: str,
    group1: str = "none", group2: str = "none", period: str = "none",
    f_mkt_group: Optional[List[str]] = Query(default=None),
    f_legacy_id: Optional[List[str]] = Query(default=None),
    f_campaign_name: Optional[List[str]] = Query(default=None),
    f_channel: Optional[List[str]] = Query(default=None),
    f_sub_channel: Optional[List[str]] = Query(default=None),
    f_affiliate: Optional[List[str]] = Query(default=None),
    f_classification: str = None, ftc_groups: str = None,
    q_date_from: str = None, q_date_to: str = None,
    f_country: Optional[List[str]] = Query(default=None),
    f_office: Optional[List[str]] = Query(default=None),
    f_agent: Optional[List[str]] = Query(default=None),
    f_team: Optional[List[str]] = Query(default=None),
    f_segmentation: str = None,
):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    if group1 not in VALID_GROUPS and group1 != "none":
        return JSONResponse(status_code=400, content={"detail": "Invalid group1"})
    if group2 not in VALID_GROUPS and group2 != "none":
        return JSONResponse(status_code=400, content={"detail": "Invalid group2"})
    if period not in VALID_PERIODS and period != "none":
        return JSONResponse(status_code=400, content={"detail": "Invalid period"})

    def _ck_part(v): return ','.join(sorted(v)) if v else ''
    _ck = (f"camp_tbl_v10:{date_from}:{date_to}:{group1}:{group2}:{period}"
           f":{_ck_part(f_mkt_group)}:{_ck_part(f_legacy_id)}:{_ck_part(f_campaign_name)}:{_ck_part(f_channel)}"
           f":{_ck_part(f_sub_channel)}:{_ck_part(f_affiliate)}:{f_classification}:{ftc_groups}"
           f":{q_date_from}:{q_date_to}:{_ck_part(f_country)}:{_ck_part(f_office)}:{_ck_part(f_agent)}:{_ck_part(f_team)}"
           f":{f_segmentation}")
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    try:
        datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"detail": "Invalid date format"})

    try:
        _result = _camp_table_calc(
            date_from, date_to, group1, group2, period,
            f_mkt_group, f_legacy_id, f_campaign_name, f_channel,
            f_sub_channel, f_affiliate, f_classification, ftc_groups,
            q_date_from, q_date_to,
            f_country=f_country, f_office=f_office, f_agent=f_agent, f_team=f_team,
            f_segmentation=f_segmentation,
        )
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
