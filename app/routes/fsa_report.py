from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app import cache
from datetime import date

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

FSA_COUNTRIES = ('CM','KE','SE','ZM','DK','NL','ES','FI','NO')
# Aruba (AW) is a constituent country of the Kingdom of the Netherlands.
# Included in filters so its clients are counted, but rolled up under NL for display
# (Section 4 uses CASE to normalize AW -> NL; display loop still shows 9 countries).
FSA_COUNTRIES_FILTER = FSA_COUNTRIES + ('AW',)
FSA_COUNTRY_NAMES = {
    'CM': 'Cameroon', 'KE': 'Kenya', 'SE': 'Sweden', 'ZM': 'Zambia',
    'DK': 'Denmark', 'NL': 'Netherlands', 'ES': 'Spain', 'FI': 'Finland', 'NO': 'Norway',
}

def _quarter_dates(year: int, quarter: int):
    q_start_month = (quarter - 1) * 3 + 1
    q_start = date(year, q_start_month, 1)
    q_end_month = q_start_month + 2
    if q_end_month == 3:
        q_end = date(year, 3, 31)
    elif q_end_month == 6:
        q_end = date(year, 6, 30)
    elif q_end_month == 9:
        q_end = date(year, 9, 30)
    else:
        q_end = date(year, 12, 31)
    q_end_excl = date(year + (1 if quarter == 4 else 0),
                      1 if quarter == 4 else q_end_month + 1, 1)
    return q_start, q_end, q_end_excl


@router.get("/fsa-report", response_class=HTMLResponse)
async def fsa_report_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if user.get("role") != "admin" and "fsa_report" not in (user.get("allowed_pages_list") or []):
        return RedirectResponse(url="/performance")
    return templates.TemplateResponse("fsa_report.html", {"request": request, "current_user": user})


@router.get("/api/fsa-report/section3")
async def fsa_report_section3(request: Request, year: int = 2026, quarter: int = 1):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if user.get("role") != "admin" and "fsa_report" not in (user.get("allowed_pages_list") or []):
        return JSONResponse({"error": "forbidden"}, status_code=403)

    q_start, q_end, q_end_excl = _quarter_dates(year, quarter)
    _ck = f"fsa_s3_v1:{year}:{quarter}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(_hit)

    base_filter = """
        funded = 1
        AND is_test_account = 0
        AND (sales_rep_id IS NULL OR sales_rep_id != 3303)
        AND country_iso IN ('CM','KE','SE','ZM','DK','NL','ES','FI','NO','AW')
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Query 1: Active/Inactive counts BOP + EOP
            cur.execute(f"""
                SELECT
                  SUM(CASE WHEN compliance_status IN ('4','9') AND createdtime < %(q_start)s THEN 1 ELSE 0 END) AS active_bop,
                  SUM(CASE WHEN compliance_status NOT IN ('4','9') AND createdtime < %(q_start)s THEN 1 ELSE 0 END) AS inactive_bop,
                  SUM(CASE WHEN compliance_status IN ('4','9') AND createdtime < %(q_end_excl)s THEN 1 ELSE 0 END) AS active_eop,
                  SUM(CASE WHEN compliance_status NOT IN ('4','9') AND createdtime < %(q_end_excl)s THEN 1 ELSE 0 END) AS inactive_eop
                FROM accounts
                WHERE {base_filter}
            """, {"q_start": q_start, "q_end_excl": q_end_excl})
            row = cur.fetchone()
            counts = {
                "active_bop": row[0] or 0,
                "inactive_bop": row[1] or 0,
                "active_eop": row[2] or 0,
                "inactive_eop": row[3] or 0,
            }

            # Query 2: Clients' Funds from daily_equity_zeroed (last day of quarter)
            # Find the latest snapshot day on or before quarter end
            cur.execute(f"""
                SELECT COALESCE(SUM(GREATEST(dez.end_equity_zeroed, 0)), 0)
                FROM daily_equity_zeroed dez
                JOIN trading_accounts ta ON ta.login::bigint = dez.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE dez.day = (
                    SELECT MAX(day) FROM daily_equity_zeroed WHERE day <= %(q_end)s
                )
                  AND a.is_test_account = 0
                  AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
                  AND a.country_iso IN ('CM','KE','SE','ZM','DK','NL','ES','FI','NO','AW')
            """, {"q_end": q_end})
            clients_funds = float(cur.fetchone()[0])

            # Query 3: Age groups
            cur.execute(f"""
                SELECT
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) < 18 THEN 1 ELSE 0 END) AS under_18,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) BETWEEN 36 AND 45 THEN 1 ELSE 0 END) AS age_36_45,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) BETWEEN 46 AND 55 THEN 1 ELSE 0 END) AS age_46_55,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) BETWEEN 56 AND 65 THEN 1 ELSE 0 END) AS age_56_65,
                  SUM(CASE WHEN DATE_PART('year', AGE(%(q_end)s::date, birth_date::date)) > 65 THEN 1 ELSE 0 END) AS age_over_65
                FROM accounts
                WHERE {base_filter}
                  AND compliance_status IN ('4','9')
                  AND createdtime < %(q_end_excl)s
                  AND birth_date IS NOT NULL
            """, {"q_end": q_end, "q_end_excl": q_end_excl})
            age_row = cur.fetchone()
            age_groups = {
                "under_18": age_row[0] or 0,
                "18_25": age_row[1] or 0,
                "26_35": age_row[2] or 0,
                "36_45": age_row[3] or 0,
                "46_55": age_row[4] or 0,
                "56_65": age_row[5] or 0,
                "over_65": age_row[6] or 0,
            }

            # Query 4: Classification of active clients (PEP + total active EOP)
            cur.execute(f"""
                SELECT
                  COUNT(*) AS total_active,
                  SUM(CASE WHEN pep_sanctions = 1 THEN 1 ELSE 0 END) AS pep_count
                FROM accounts
                WHERE {base_filter}
                  AND compliance_status IN ('4','9')
                  AND createdtime < %(q_end_excl)s
            """, {"q_end_excl": q_end_excl})
            cls_row = cur.fetchone()
            classification = {
                "total_active": cls_row[0] or 0,
                "pep": cls_row[1] or 0,
            }

        _result = {
            "counts": counts,
            "clients_funds": clients_funds,
            "age_groups": age_groups,
            "classification": classification,
        }
        cache.set(_ck, _result, ttl=3600)
        return JSONResponse(_result)
    except Exception as e1:
        return JSONResponse({"error": str(e1)}, status_code=500)
    finally:
        conn.close()


@router.get("/api/fsa-report/section4")
async def fsa_report_section4(request: Request, year: int = 2026, quarter: int = 1):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if user.get("role") != "admin" and "fsa_report" not in (user.get("allowed_pages_list") or []):
        return JSONResponse({"error": "forbidden"}, status_code=403)

    q_start, q_end, q_end_excl = _quarter_dates(year, quarter)
    _ck = f"fsa_s4_v1:{year}:{quarter}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(_hit)

    base_filter = """
        a.funded = 1
        AND a.is_test_account = 0
        AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Query 1: Active/Inactive counts per country at EOP
            # Map AW -> NL so Aruba clients are counted under Netherlands
            cur.execute(f"""
                SELECT
                  CASE WHEN a.country_iso = 'AW' THEN 'NL' ELSE a.country_iso END AS country_iso,
                  SUM(CASE WHEN a.compliance_status IN ('4','9') THEN 1 ELSE 0 END) AS active,
                  SUM(CASE WHEN a.compliance_status NOT IN ('4','9') THEN 1 ELSE 0 END) AS inactive
                FROM accounts a
                WHERE {base_filter}
                  AND a.createdtime < %(q_end_excl)s
                  AND a.country_iso IN %(countries)s
                GROUP BY CASE WHEN a.country_iso = 'AW' THEN 'NL' ELSE a.country_iso END
            """, {"q_end_excl": q_end_excl, "countries": FSA_COUNTRIES_FILTER})
            country_counts = {}
            for row in cur.fetchall():
                country_counts[row[0]] = {"active": row[1] or 0, "inactive": row[2] or 0}

            # Query 2: Open volume per country
            # Same as performance report / PBI: dealio_positions + closed trades
            # mapped back to open_time via position_id
            vol_params = {"q_start": q_start, "q_end_excl": q_end_excl,
                          "countries": FSA_COUNTRIES_FILTER}

            cur.execute(f"""
                SELECT country_iso, COALESCE(SUM(notional_usd), 0)
                FROM (
                    -- Open positions by open_time
                    SELECT
                        CASE WHEN a.country_iso = 'AW' THEN 'NL' ELSE a.country_iso END AS country_iso,
                        p.notional_value AS notional_usd
                    FROM dealio_positions p
                    JOIN trading_accounts ta ON ta.login::bigint = p.login
                    JOIN accounts a ON a.accountid = ta.vtigeraccountid
                    WHERE p.open_time >= %(q_start)s AND p.open_time < %(q_end_excl)s
                      AND ta.vtigeraccountid IS NOT NULL
                      AND a.is_test_account = 0
                      AND {base_filter}
                      AND a.country_iso IN %(countries)s

                    UNION ALL

                    -- Closed trades via pre-computed MV (eliminates self-join)
                    SELECT
                        CASE WHEN a.country_iso = 'AW' THEN 'NL' ELSE a.country_iso END AS country_iso,
                        m.notional_value AS notional_usd
                    FROM mv_mt5_resolved m
                    JOIN trading_accounts ta ON ta.login::bigint = m.login
                    JOIN accounts a ON a.accountid = ta.vtigeraccountid
                    WHERE m.open_time >= %(q_start)s AND m.open_time < %(q_end_excl)s
                      AND ta.vtigeraccountid IS NOT NULL
                      AND a.is_test_account = 0
                      AND {base_filter}
                      AND a.country_iso IN %(countries)s
                ) combined
                GROUP BY country_iso
            """, vol_params)
            country_volume = {}
            for row in cur.fetchall():
                country_volume[row[0]] = float(row[1] or 0)

            # Query 3: Close volume per country (entry=1 by close_time)
            cur.execute(f"""
                SELECT
                    CASE WHEN a.country_iso = 'AW' THEN 'NL' ELSE a.country_iso END AS country_iso,
                    COALESCE(SUM(t.notional_value), 0)
                FROM dealio_trades_mt5 t
                JOIN trading_accounts ta ON ta.login::bigint = t.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE t.close_time >= %(q_start)s AND t.close_time < %(q_end_excl)s
                  AND t.entry = 1
                  AND t.close_time > '1971-01-01'
                  AND a.is_test_account = 0
                  AND {base_filter}
                  AND a.country_iso IN %(countries)s
                GROUP BY CASE WHEN a.country_iso = 'AW' THEN 'NL' ELSE a.country_iso END
            """, vol_params)
            for row in cur.fetchall():
                country_volume[row[0]] = country_volume.get(row[0], 0) + float(row[1] or 0)

        # Build response per country
        countries = []
        for iso in FSA_COUNTRIES:
            cc = country_counts.get(iso, {"active": 0, "inactive": 0})
            countries.append({
                "iso": iso,
                "name": FSA_COUNTRY_NAMES.get(iso, iso),
                "active": cc["active"],
                "inactive": cc["inactive"],
                "cfds": country_volume.get(iso, 0),
            })

        _result = {"countries": countries}
        cache.set(_ck, _result, ttl=3600)
        return JSONResponse(_result)
    except Exception as e2:
        return JSONResponse({"error": str(e2)}, status_code=500)
    finally:
        conn.close()


@router.get("/api/fsa-report/section5")
async def fsa_report_section5(request: Request, year: int = 2026, quarter: int = 1):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if user.get("role") != "admin" and "fsa_report" not in (user.get("allowed_pages_list") or []):
        return JSONResponse({"error": "forbidden"}, status_code=403)

    q_start, q_end, q_end_excl = _quarter_dates(year, quarter)
    _ck = f"fsa_s5_v1:{year}:{quarter}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(_hit)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # 6.1 Actual Client's Equity = SUM(GREATEST(convertedequity, 0))
            # at last available date <= quarter end
            cur.execute("""
                SELECT COALESCE(SUM(GREATEST(ddp.convertedequity, 0)), 0)
                FROM dealio_daily_profits ddp
                JOIN trading_accounts ta ON ta.login::bigint = ddp.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE ddp.date = (
                    SELECT MAX(date) FROM dealio_daily_profits WHERE date <= %(q_end)s
                )
                  AND a.funded = 1
                  AND a.is_test_account = 0
                  AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
                  AND a.country_iso IN %(countries)s
            """, {"q_end": q_end, "countries": FSA_COUNTRIES_FILTER})
            actual_equity = float(cur.fetchone()[0])

            # 6.2 Floating PnL = SUM(converteddeltafloatingpnl) for all days in quarter
            cur.execute("""
                SELECT COALESCE(SUM(ddp.converteddeltafloatingpnl), 0)
                FROM dealio_daily_profits ddp
                JOIN trading_accounts ta ON ta.login::bigint = ddp.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE ddp.date >= %(q_start)s AND ddp.date < %(q_end_excl)s
                  AND a.funded = 1
                  AND a.is_test_account = 0
                  AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
                  AND a.country_iso IN %(countries)s
            """, {"q_start": q_start, "q_end_excl": q_end_excl,
                  "countries": FSA_COUNTRIES_FILTER})
            floating_pnl = float(cur.fetchone()[0])

        total_equity = actual_equity + floating_pnl
        equity_ratio = (actual_equity / total_equity * 100) if total_equity else 0

        _result = {
            "actual_equity": actual_equity,
            "floating_pnl": floating_pnl,
            "total_equity": total_equity,
            "equity_ratio": equity_ratio,
        }
        cache.set(_ck, _result, ttl=3600)
        return JSONResponse(_result)
    except Exception as e3:
        return JSONResponse({"error": str(e3)}, status_code=500)
    finally:
        conn.close()


EU_COUNTRIES = {'SE', 'DK', 'NL', 'ES', 'FI', 'NO'}
OTHER_FOREIGN = {'CM', 'KE', 'ZM'}


@router.get("/api/fsa-report/section6")
async def fsa_report_section6(request: Request, year: int = 2026, quarter: int = 1):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if user.get("role") != "admin" and "fsa_report" not in (user.get("allowed_pages_list") or []):
        return JSONResponse({"error": "forbidden"}, status_code=403)

    q_start, q_end, q_end_excl = _quarter_dates(year, quarter)
    _ck = f"fsa_s6_v1:{year}:{quarter}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(_hit)

    base_filter = """
        a.funded = 1
        AND a.is_test_account = 0
        AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
    """

    acct_subquery = """
        SELECT accountid FROM accounts
        WHERE country_iso IN %(countries)s
          AND is_test_account = 0
          AND (sales_rep_id IS NULL OR sales_rep_id != 3303)
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            vol_params = {"q_start": q_start, "q_end_excl": q_end_excl,
                          "countries": FSA_COUNTRIES_FILTER}

            # --- Volume per country (open + close, same as section 4) ---
            cur.execute(f"""
                SELECT country_iso, COALESCE(SUM(notional_usd), 0)
                FROM (
                    SELECT
                        CASE WHEN a.country_iso = 'AW' THEN 'NL' ELSE a.country_iso END AS country_iso,
                        p.notional_value AS notional_usd
                    FROM dealio_positions p
                    JOIN trading_accounts ta ON ta.login::bigint = p.login
                    JOIN accounts a ON a.accountid = ta.vtigeraccountid
                    WHERE p.open_time >= %(q_start)s AND p.open_time < %(q_end_excl)s
                      AND ta.vtigeraccountid IS NOT NULL
                      AND a.is_test_account = 0
                      AND {base_filter}
                      AND a.country_iso IN %(countries)s

                    UNION ALL

                    SELECT
                        CASE WHEN a.country_iso = 'AW' THEN 'NL' ELSE a.country_iso END AS country_iso,
                        m.notional_value AS notional_usd
                    FROM mv_mt5_resolved m
                    JOIN trading_accounts ta ON ta.login::bigint = m.login
                    JOIN accounts a ON a.accountid = ta.vtigeraccountid
                    WHERE m.open_time >= %(q_start)s AND m.open_time < %(q_end_excl)s
                      AND ta.vtigeraccountid IS NOT NULL
                      AND a.is_test_account = 0
                      AND {base_filter}
                      AND a.country_iso IN %(countries)s
                ) combined
                GROUP BY country_iso
            """, vol_params)
            country_volume = {}
            for row in cur.fetchall():
                country_volume[row[0]] = float(row[1] or 0)

            # Close volume per country
            cur.execute(f"""
                SELECT
                    CASE WHEN a.country_iso = 'AW' THEN 'NL' ELSE a.country_iso END AS country_iso,
                    COALESCE(SUM(t.notional_value), 0)
                FROM dealio_trades_mt5 t
                JOIN trading_accounts ta ON ta.login::bigint = t.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE t.close_time >= %(q_start)s AND t.close_time < %(q_end_excl)s
                  AND t.entry = 1
                  AND t.close_time > '1971-01-01'
                  AND a.is_test_account = 0
                  AND {base_filter}
                  AND a.country_iso IN %(countries)s
                GROUP BY CASE WHEN a.country_iso = 'AW' THEN 'NL' ELSE a.country_iso END
            """, vol_params)
            for row in cur.fetchall():
                country_volume[row[0]] = country_volume.get(row[0], 0) + float(row[1] or 0)

            total_volume = sum(country_volume.values())

            # --- end_net_equity (same as section 5 actual equity) ---
            cur.execute("""
                SELECT COALESCE(SUM(GREATEST(ddp.convertedequity, 0)), 0)
                FROM dealio_daily_profits ddp
                JOIN trading_accounts ta ON ta.login::bigint = ddp.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE ddp.date = (
                    SELECT MAX(date) FROM dealio_daily_profits WHERE date <= %(q_end)s
                )
                  AND a.funded = 1
                  AND a.is_test_account = 0
                  AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
                  AND a.country_iso IN %(countries)s
            """, {"q_end": q_end, "countries": FSA_COUNTRIES_FILTER})
            end_net_equity = float(cur.fetchone()[0])

            # --- start_net_equity (last date BEFORE quarter start) ---
            cur.execute("""
                SELECT COALESCE(SUM(GREATEST(ddp.convertedequity, 0)), 0)
                FROM dealio_daily_profits ddp
                JOIN trading_accounts ta ON ta.login::bigint = ddp.login
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE ddp.date = (
                    SELECT MAX(date) FROM dealio_daily_profits WHERE date < %(q_start)s
                )
                  AND a.funded = 1
                  AND a.is_test_account = 0
                  AND (a.sales_rep_id IS NULL OR a.sales_rep_id != 3303)
                  AND a.country_iso IN %(countries)s
            """, {"q_start": q_start, "countries": FSA_COUNTRIES_FILTER})
            start_net_equity = float(cur.fetchone()[0])

            # --- net_usd (standard deposits - standard withdrawals) ---
            cur.execute(f"""
                SELECT COALESCE(SUM(
                  CASE
                    WHEN t.transactiontype = 'Deposit' THEN t.usdamount
                    WHEN t.transactiontype = 'Withdrawal' THEN -t.usdamount
                    ELSE 0
                  END
                ), 0)
                FROM transactions t
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.confirmation_time >= %(q_start)s AND t.confirmation_time < %(q_end_excl)s
                  AND (
                    (t.transactiontype = 'Deposit'
                     AND t.transaction_type_name IN ('Deposit', 'Withdrawal Cancelled'))
                    OR (t.transactiontype = 'Withdrawal'
                        AND t.transaction_type_name IN ('Withdrawal', 'Deposit Cancelled'))
                  )
                  AND t.vtigeraccountid IN ({acct_subquery})
            """, {"q_start": q_start, "q_end_excl": q_end_excl,
                  "countries": FSA_COUNTRIES_FILTER})
            net_usd = float(cur.fetchone()[0])

            # --- dep_bonuses_fees_adj (non-standard deposit transactions) ---
            cur.execute(f"""
                SELECT COALESCE(SUM(t.usdamount), 0)
                FROM transactions t
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.confirmation_time >= %(q_start)s AND t.confirmation_time < %(q_end_excl)s
                  AND (
                    (t.transaction_type_name NOT IN ('Deposit','Withdrawal Cancelled')
                     AND t.transactiontype = 'Deposit')
                    OR t.transaction_type_name = 'Transfer To Account'
                  )
                  AND t.vtigeraccountid IN ({acct_subquery})
            """, {"q_start": q_start, "q_end_excl": q_end_excl,
                  "countries": FSA_COUNTRIES_FILTER})
            dep_bonuses = float(cur.fetchone()[0])

            # --- wd_bonuses_fees_adj (non-standard withdrawal transactions) ---
            cur.execute(f"""
                SELECT COALESCE(SUM(t.usdamount), 0)
                FROM transactions t
                WHERE t.transactionapproval = 'Approved'
                  AND (t.deleted = 0 OR t.deleted IS NULL)
                  AND t.confirmation_time >= %(q_start)s AND t.confirmation_time < %(q_end_excl)s
                  AND (
                    (t.transaction_type_name NOT IN ('Withdrawal','Deposit Cancelled')
                     AND t.transactiontype = 'Withdrawal')
                    OR t.transaction_type_name = 'Transfer From Account'
                  )
                  AND t.vtigeraccountid IN ({acct_subquery})
            """, {"q_start": q_start, "q_end_excl": q_end_excl,
                  "countries": FSA_COUNTRIES_FILTER})
            wd_bonuses = float(cur.fetchone()[0])

        # PnL equity (income generated)
        bonuses_fees_adj = dep_bonuses - wd_bonuses
        pnl_equity = end_net_equity - start_net_equity - net_usd - bonuses_fees_adj

        # Investor type volumes
        eu_vol = sum(country_volume.get(c, 0) for c in EU_COUNTRIES)
        other_vol = sum(country_volume.get(c, 0) for c in OTHER_FOREIGN)

        _result = {
            "total_volume": total_volume,
            "pnl_equity": pnl_equity,
            "investor_types": [
                {"type": "Resident of Seychelles", "volume": 0},
                {"type": "European Union", "volume": eu_vol},
                {"type": "United States of America", "volume": 0},
                {"type": "Other Foreign", "volume": other_vol},
            ],
        }
        cache.set(_ck, _result, ttl=3600)
        return JSONResponse(_result)
    except Exception as e4:
        return JSONResponse({"error": str(e4)}, status_code=500)
    finally:
        conn.close()
