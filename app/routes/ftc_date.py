from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app import cache
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

_country_map_cache    = None
_region_map_cache     = None
_ret_status_cache     = None
_sales_status_cache   = None

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

def _get_region_map() -> dict:
    global _region_map_cache
    if _region_map_cache:
        return _region_map_cache
    try:
        from app.db.mssql_conn import get_country_region_map
        result = get_country_region_map()
        if result:
            _region_map_cache = result
        return result or {}
    except Exception:
        return {}

def _get_ret_status_map() -> dict:
    global _ret_status_cache
    if _ret_status_cache:
        return _ret_status_cache
    try:
        from app.db.mssql_conn import get_ret_status_map
        result = get_ret_status_map()
        if result:
            _ret_status_cache = result
        return result or {}
    except Exception:
        return {}

def _get_sales_status_map() -> dict:
    global _sales_status_cache
    if _sales_status_cache:
        return _sales_status_cache
    try:
        from app.db.mssql_conn import get_sales_status_map
        result = get_sales_status_map()
        if result:
            _sales_status_cache = result
        return result or {}
    except Exception:
        return {}

# ── Group dimension SQL fragments ─────────────────────────────────────────────

_DAYS_FTC_VAL = (
    "CASE"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 0   AND 7   THEN '0 - 7 days'"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 8   AND 14  THEN '8 - 14 days'"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 15  AND 30  THEN '15 - 30 days'"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 31  AND 60  THEN '31 - 60 days'"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 61  AND 90  THEN '61 - 90 days'"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 91  AND 120 THEN '91 - 120 days'"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) > 120               THEN '120+ days'"
    " ELSE '(Unknown)' END"
)
_DAYS_FTC_SORT = (
    "CASE"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 0   AND 7   THEN 1"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 8   AND 14  THEN 2"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 15  AND 30  THEN 3"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 31  AND 60  THEN 4"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 61  AND 90  THEN 5"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) BETWEEN 91  AND 120 THEN 6"
    " WHEN (%(end_date)s::date - a.client_qualification_date::date) > 120               THEN 7"
    " ELSE 99 END"
)
_AGE_GROUP_VAL = (
    "CASE"
    " WHEN a.birth_date IS NULL THEN '(Unknown)'"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 18 AND 24 THEN '18-24'"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 25 AND 29 THEN '25-29'"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 30 AND 34 THEN '30-34'"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 35 AND 39 THEN '35-39'"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 40 AND 44 THEN '40-44'"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 45 AND 49 THEN '45-49'"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) >= 50             THEN '50+'"
    " ELSE '(Unknown)' END"
)
_AGE_GROUP_SORT = (
    "CASE"
    " WHEN a.birth_date IS NULL THEN 99"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 18 AND 24 THEN 1"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 25 AND 29 THEN 2"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 30 AND 34 THEN 3"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 35 AND 39 THEN 4"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 40 AND 44 THEN 5"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) BETWEEN 45 AND 49 THEN 6"
    " WHEN DATE_PART('year', AGE(%(end_date)s::date, a.birth_date)) >= 50             THEN 7"
    " ELSE 99 END"
)

_DIMS = {
    "days_from_ftc":          {"val": _DAYS_FTC_VAL,                                              "sort": _DAYS_FTC_SORT,  "cu": False, "camp": False},
    "marketing_group":        {"val": "COALESCE(c.marketing_group,        '(Unassigned)')",       "sort": "NULL::int",     "cu": False, "camp": True },
    "campaign_legacy":        {"val": "COALESCE(c.campaign_legacy_id,     '(Unassigned)')",       "sort": "NULL::int",     "cu": False, "camp": True },
    "office_name":            {"val": "COALESCE(u.office_name,            '(Unassigned)')",       "sort": "NULL::int",     "cu": True,  "camp": False},
    "team":                   {"val": "COALESCE(u.department,             '(Unassigned)')",       "sort": "NULL::int",     "cu": True,  "camp": False},
    "agent":                  {"val": "COALESCE(u.agent_name,             '(Unassigned)')",       "sort": "NULL::int",     "cu": True,  "camp": False},
    "age_group":              {"val": _AGE_GROUP_VAL,                                             "sort": _AGE_GROUP_SORT, "cu": False, "camp": False},
    "sales_client_potential": {"val": "COALESCE(ROUND(a.sales_client_potential::numeric)::int::text, '(Unassigned)')", "sort": "ROUND(a.sales_client_potential::numeric)::int", "cu": False, "camp": False},
    "country_name":           {"val": "COALESCE(a.country_iso,            '(Unassigned)')",       "sort": "NULL::int",     "cu": False, "camp": False},
    "region":                 {"val": "COALESCE(a.country_iso,            '(Unassigned)')",       "sort": "NULL::int",     "cu": False, "camp": False},
    "segmentation":           {"val": "COALESCE(CASE a.segmentation WHEN '1' THEN '-A' WHEN '2' THEN 'B' WHEN '3' THEN 'C' WHEN '4' THEN '+A' END, '(Unassigned)')", "sort": "COALESCE(a.segmentation::int, 99)", "cu": False, "camp": False},
    "retention_status":       {"val": "COALESCE(a.retention_status::text, '(Unassigned)')",       "sort": "COALESCE(a.retention_status::int, 999)", "cu": False, "camp": False},
    "sales_status":           {"val": "COALESCE(a.sales_status::text,     '(Unassigned)')",       "sort": "COALESCE(a.sales_status::int, 999)",     "cu": False, "camp": False},
}


@router.get("/ftc-date", response_class=HTMLResponse)
async def ftc_date_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    ap = user.get("allowed_pages_list")
    if user.get("role") != "admin" and not (ap is not None and "ftc_date" in ap):
        return RedirectResponse(url="/performance")
    return templates.TemplateResponse("ftc_date.html", {
        "request": request,
        "current_user": user,
    })


@router.get("/api/ftc-date/options")
async def ftc_date_options(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    sql = """
        SELECT DISTINCT u.id, u.agent_name, u.office_name, u.department
        FROM crm_users u
        WHERE u.id IN (SELECT DISTINCT assigned_to FROM accounts WHERE assigned_to IS NOT NULL)
          AND u.agent_name IS NOT NULL
        ORDER BY u.agent_name
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        agents  = [{"id": r[0], "name": r[1]} for r in rows]
        offices = sorted(set(r[2] for r in rows if r[2]))
        teams   = sorted(set(r[3] for r in rows if r[3]))
        return JSONResponse(content={"agents": agents, "offices": offices, "teams": teams})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


_ALL_FTC_GROUPS = {"0 - 7 days", "8 - 14 days", "15 - 30 days", "31 - 60 days", "61 - 90 days", "91 - 120 days", "120+ days"}

_GROUP_DAY_SQL = {
    "0 - 7 days":    "(%(end_date)s::date - a.client_qualification_date::date) BETWEEN 0 AND 7",
    "8 - 14 days":   "(%(end_date)s::date - a.client_qualification_date::date) BETWEEN 8 AND 14",
    "15 - 30 days":  "(%(end_date)s::date - a.client_qualification_date::date) BETWEEN 15 AND 30",
    "31 - 60 days":  "(%(end_date)s::date - a.client_qualification_date::date) BETWEEN 31 AND 60",
    "61 - 90 days":  "(%(end_date)s::date - a.client_qualification_date::date) BETWEEN 61 AND 90",
    "91 - 120 days": "(%(end_date)s::date - a.client_qualification_date::date) BETWEEN 91 AND 120",
    "120+ days":     "(%(end_date)s::date - a.client_qualification_date::date) > 120",
}


@router.get("/api/ftc-date")
async def ftc_date_api(
    request: Request,
    end_date: str = None,
    group1: str = "days_from_ftc",
    group2: str = "none",
    agent_id: int = None,
    office: str = None,
    team: str = None,
    classification: str = None,
    ftc_groups: str = None,   # comma-separated checked group names
):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    if not end_date:
        end_date = datetime.now(_TZ).date().strftime("%Y-%m-%d")

    if group1 not in _DIMS:
        group1 = "days_from_ftc"
    if group2 not in _DIMS:
        group2 = "none"
    has_g2 = group2 != "none"

    # Parse checked FTC groups — empty / all-7 = no filter
    ftc_groups_list = [g.strip() for g in ftc_groups.split(",")] if ftc_groups else []
    ftc_groups_set  = set(ftc_groups_list)
    apply_group_filter = bool(ftc_groups_set) and ftc_groups_set != _ALL_FTC_GROUPS

    _ck = f"ftc_v7:{end_date}:{group1}:{group2}:{agent_id}:{office}:{team}:{classification}:{ftc_groups}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    d1 = _DIMS[group1]
    d2 = _DIMS.get(group2)

    needs_cu   = d1["cu"] or (d2 and d2["cu"]) or bool(agent_id or office or team)
    needs_camp = d1["camp"] or (d2 and d2["camp"])
    age_filter = "AND a.birth_date IS NOT NULL\n" if (group1 == 'age_group' or group2 == 'age_group') else ""

    cu_join   = "LEFT JOIN crm_users u ON u.id = a.assigned_to" if needs_cu   else ""
    camp_join = "LEFT JOIN campaigns c ON SPLIT_PART(a.campaign, '.', 1) = c.crmid" if needs_camp else ""

    params = {"end_date": end_date}
    filter_parts = []
    if agent_id:
        filter_parts.append("AND u.id = %(agent_id)s")
        params["agent_id"] = agent_id
    if office:
        filter_parts.append("AND u.office_name = %(office)s")
        params["office"] = office
    if team:
        filter_parts.append("AND u.department = %(team)s")
        params["team"] = team
    if classification == "Low Quality":
        filter_parts.append("AND a.classification_int BETWEEN 1 AND 5")
    elif classification == "High Quality":
        filter_parts.append("AND a.classification_int BETWEEN 6 AND 10")
    elif classification == "No segmentation":
        filter_parts.append("AND (a.classification_int IS NULL OR a.classification_int NOT BETWEEN 1 AND 10)")
    if apply_group_filter:
        or_clauses = [_GROUP_DAY_SQL[g] for g in ftc_groups_list if g in _GROUP_DAY_SQL]
        if or_clauses:
            filter_parts.append("AND (" + " OR ".join(or_clauses) + ")")
    filter_sql = "\n      ".join(filter_parts)

    g2_sel    = (", (" + d2["val"] + ") AS g2, (" + d2["sort"] + ") AS g2_sort") if has_g2 else ""
    g2_joined = ", b.g2, b.g2_sort" if has_g2 else ""
    g2_grp    = ", g2, g2_sort" if has_g2 else ""
    g2_ord    = ", COALESCE(g2_sort, 0), g2" if has_g2 else ""

    sql = (
        "WITH base_accounts AS (\n"
        "    SELECT a.accountid,\n"
        "           (" + d1["val"]  + ") AS g1,\n"
        "           (" + d1["sort"] + ") AS g1_sort"
        + (",\n           " + g2_sel.lstrip(", ") if has_g2 else "") + "\n"
        "    FROM accounts a\n"
        "    LEFT JOIN client_classification cc ON cc.accountid = a.accountid::BIGINT\n"
        + ("    " + cu_join   + "\n" if cu_join   else "")
        + ("    " + camp_join + "\n" if camp_join else "")
        + "    WHERE a.client_qualification_date IS NOT NULL\n"
        "      AND a.client_qualification_date::date >= '2024-01-01'\n"
        "      AND a.client_qualification_date::date <= %(end_date)s::date\n"
        "      AND a.is_test_account = 0\n"
        + ("      " + age_filter if age_filter else "")
        + (("      " + filter_sql + "\n") if filter_sql else "")
        + "),\n"
        "tx_per_account AS (\n"
        "    SELECT t.vtigeraccountid AS accountid,\n"
        "           SUM(CASE WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN t.usdamount ELSE 0 END) AS deposit_usd,\n"
        "           SUM(CASE WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled') THEN t.usdamount ELSE 0 END) AS withdrawal_usd\n"
        "    FROM transactions t\n"
        "    WHERE t.transactionapproval = 'Approved'\n"
        "      AND (t.deleted = 0 OR t.deleted IS NULL)\n"
        "      AND LOWER(COALESCE(t.comment,'')) NOT LIKE '%%bonus%%'\n"
        "      AND COALESCE(t.confirmation_time, t.created_time)::date >= '2024-01-01'\n"
        "      AND COALESCE(t.confirmation_time, t.created_time)::date <= %(end_date)s::date\n"
        "    GROUP BY t.vtigeraccountid\n"
        "),\n"
        "rdp AS (\n"
        "    SELECT DISTINCT accountid FROM mv_std_clients\n"
        "    WHERE has_second_deposit = 1\n"
        "      AND second_deposit_date::date <= %(end_date)s::date\n"
        "),\n"
        "withdrawalers AS (\n"
        "    SELECT DISTINCT t.vtigeraccountid AS accountid\n"
        "    FROM transactions t\n"
        "    JOIN accounts a ON a.accountid = t.vtigeraccountid\n"
        "    WHERE t.transactiontype = 'Withdrawal'\n"
        "      AND t.transactionapproval = 'Approved'\n"
        "      AND (t.deleted = 0 OR t.deleted IS NULL)\n"
        "      AND COALESCE(t.confirmation_time, t.created_time)::date <= %(end_date)s::date\n"
        "      AND a.is_test_account = 0\n"
        "),\n"
        "traders AS (\n"
        "    SELECT DISTINCT ta.vtigeraccountid AS accountid\n"
        "    FROM dealio_trades_mt4 d\n"
        "    JOIN trading_accounts ta ON ta.login::bigint = d.login::bigint\n"
        "    JOIN accounts a ON a.accountid = ta.vtigeraccountid\n"
        "    WHERE d.notional_value > 0\n"
        "      AND ta.vtigeraccountid IS NOT NULL\n"
        "      AND ta.vtigeraccountid::text != ''\n"
        "      AND d.open_time::date <= %(end_date)s::date\n"
        "      AND a.is_test_account = 0\n"
        "),\n"
        "joined AS (\n"
        "    SELECT b.g1, b.g1_sort" + g2_joined + ",\n"
        "           b.accountid,\n"
        "           COALESCE(tx.deposit_usd,    0) AS deposit_usd,\n"
        "           COALESCE(tx.withdrawal_usd, 0) AS withdrawal_usd,\n"
        "           CASE WHEN rdp.accountid IS NOT NULL THEN 1 ELSE 0 END AS is_rdp,\n"
        "           CASE WHEN wd.accountid  IS NOT NULL THEN 1 ELSE 0 END AS is_withdrawaler,\n"
        "           CASE WHEN tr.accountid  IS NOT NULL THEN 1 ELSE 0 END AS is_trader\n"
        "    FROM base_accounts b\n"
        "    LEFT JOIN tx_per_account tx ON tx.accountid = b.accountid\n"
        "    LEFT JOIN rdp              ON rdp.accountid = b.accountid\n"
        "    LEFT JOIN withdrawalers wd ON wd.accountid  = b.accountid\n"
        "    LEFT JOIN traders tr       ON tr.accountid  = b.accountid\n"
        ")\n"
        "SELECT g1, g1_sort" + g2_grp + ",\n"
        "       COUNT(DISTINCT accountid)        AS ftc_count,\n"
        "       SUM(is_rdp)                      AS rdp_count,\n"
        "       COALESCE(SUM(deposit_usd),    0) AS deposit_usd,\n"
        "       COALESCE(SUM(withdrawal_usd), 0) AS withdrawal_usd,\n"
        "       SUM(is_withdrawaler)             AS wd_count,\n"
        "       SUM(is_trader)                   AS trader_count\n"
        "FROM joined\n"
        "GROUP BY g1, g1_sort" + g2_grp + "\n"
        "ORDER BY COALESCE(g1_sort, 0), g1" + g2_ord
    )

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            db_rows = cur.fetchall()

        def _build(g1, g2, ftc, rdp_cnt, dep, wd, wdcount, traders):
            ftc = int(ftc or 0); rdp_cnt = int(rdp_cnt or 0)
            dep = float(dep or 0); wd = float(wd or 0)
            wdcount = int(wdcount or 0); traders = int(traders or 0)
            net = dep - wd
            return {
                "g1": g1, "g2": g2,
                "ftc": ftc, "rdp": rdp_cnt,
                "deposit": round(dep), "withdrawal": round(wd),
                "net_deposit": round(net),
                "ltv": round(net / ftc, 2) if ftc > 0 else 0,
                "pct_std": round(rdp_cnt / ftc * 100) if ftc > 0 else 0,
                "wd_count": wdcount,
                "pct_wd_clients": round(wdcount / ftc * 100) if ftc > 0 else 0,
                "pct_wd_usd": round(wd / dep * 100) if dep > 0 else 0,
                "traders": traders,
                "traders_pct": round(traders / ftc * 100) if ftc > 0 else 0,
            }

        data = []
        for r in db_rows:
            if has_g2:
                g1, g1_sort, g2_val, g2_sort, ftc, rdp, dep, wd, wdc, traders = r
            else:
                g1, g1_sort, ftc, rdp, dep, wd, wdc, traders = r
                g2_val = None
            data.append(_build(g1, g2_val, ftc, rdp, dep, wd, wdc, traders))

        if group1 == 'country_name' or group2 == 'country_name':
            cmap = _get_country_map()
            for row in data:
                if group1 == 'country_name' and row.get('g1') and row['g1'] != '(Unassigned)':
                    row['g1'] = cmap.get(row['g1'].strip().upper(), row['g1'])
                if group2 == 'country_name' and row.get('g2') and row['g2'] != '(Unassigned)':
                    row['g2'] = cmap.get(row['g2'].strip().upper(), row['g2'])

        if group1 == 'region' or group2 == 'region':
            rmap = _get_region_map()
            for row in data:
                if group1 == 'region' and row.get('g1') and row['g1'] != '(Unassigned)':
                    row['g1'] = rmap.get(row['g1'].strip().upper(), row['g1'])
                if group2 == 'region' and row.get('g2') and row['g2'] != '(Unassigned)':
                    row['g2'] = rmap.get(row['g2'].strip().upper(), row['g2'])
            # Re-aggregate: many country_iso → same region name
            merged = {}
            for row in data:
                key = (row['g1'], row.get('g2'))
                if key not in merged:
                    merged[key] = dict(row)
                else:
                    m = merged[key]
                    m['ftc']      += row['ftc'];   m['rdp']      += row['rdp']
                    m['deposit']  += row['deposit']; m['withdrawal'] += row['withdrawal']
                    m['wd_count'] += row['wd_count']; m['traders']  += row['traders']
            for row in merged.values():
                ftc = row['ftc']; dep = row['deposit']; wd = row['withdrawal']
                net = dep - wd
                row['net_deposit']    = round(net)
                row['ltv']            = round(net / ftc, 2) if ftc > 0 else 0
                row['pct_std']        = round(row['rdp'] / ftc * 100) if ftc > 0 else 0
                row['pct_wd_clients'] = round(row['wd_count'] / ftc * 100) if ftc > 0 else 0
                row['pct_wd_usd']     = round(wd / dep * 100) if dep > 0 else 0
                row['traders_pct']    = round(row['traders'] / ftc * 100) if ftc > 0 else 0
            data = list(merged.values())

        if group1 == 'retention_status' or group2 == 'retention_status':
            rsmap = _get_ret_status_map()
            for row in data:
                if group1 == 'retention_status' and row.get('g1') and row['g1'] != '(Unassigned)':
                    row['g1'] = rsmap.get(row['g1'], row['g1'])
                if group2 == 'retention_status' and row.get('g2') and row['g2'] != '(Unassigned)':
                    row['g2'] = rsmap.get(row['g2'], row['g2'])

        if group1 == 'sales_status' or group2 == 'sales_status':
            ssmap = _get_sales_status_map()
            for row in data:
                if group1 == 'sales_status' and row.get('g1') and row['g1'] != '(Unassigned)':
                    row['g1'] = ssmap.get(row['g1'], row['g1'])
                if group2 == 'sales_status' and row.get('g2') and row['g2'] != '(Unassigned)':
                    row['g2'] = ssmap.get(row['g2'], row['g2'])

        total_ftc = total_rdp = total_dep = total_wd = total_wdc = total_traders = 0
        for r in data:
            total_ftc     += r["ftc"];      total_rdp     += r["rdp"]
            total_dep     += r["deposit"];  total_wd      += r["withdrawal"]
            total_wdc     += r["wd_count"]; total_traders += r["traders"]
        net = total_dep - total_wd
        grand = {
            "g1": "Grand Total", "g2": None,
            "ftc": total_ftc, "rdp": total_rdp,
            "deposit": round(total_dep), "withdrawal": round(total_wd),
            "net_deposit": round(net),
            "ltv": round(net / total_ftc, 2) if total_ftc > 0 else 0,
            "pct_std": round(total_rdp / total_ftc * 100) if total_ftc > 0 else 0,
            "wd_count": total_wdc,
            "pct_wd_clients": round(total_wdc / total_ftc * 100) if total_ftc > 0 else 0,
            "pct_wd_usd": round(total_wd / total_dep * 100) if total_dep > 0 else 0,
            "traders": total_traders,
            "traders_pct": round(total_traders / total_ftc * 100) if total_ftc > 0 else 0,
        }

        _result = {
            "rows": data, "grand_total": grand,
            "end_date": end_date, "group1": group1, "group2": group2,
        }
        cache.set(_ck, _result)
        return JSONResponse(content=_result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
