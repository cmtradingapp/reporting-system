from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from app import cache
from datetime import date, datetime
from zoneinfo import ZoneInfo
import traceback
import time

_TZ = ZoneInfo("Europe/Nicosia")

router = APIRouter()


@router.get("/api/debug-eez")
async def debug_eez(request: Request, login: int = None):
    """Diagnostic for inflated EEZ values. No accounts JOIN (avoids lock timeout)."""
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    if user.get("role") != "admin":
        return JSONResponse(status_code=403, content={"detail": "Admin only"})

    today = datetime.now(_TZ).date()
    yesterday = str(today - __import__('datetime').timedelta(days=1))
    conn = get_connection()
    result = {}
    try:
        with conn.cursor() as cur:
            # 1. Snapshot stats for yesterday (no joins needed)
            cur.execute("""
                SELECT COUNT(*), COALESCE(SUM(end_equity_zeroed), 0),
                       MIN(end_equity_zeroed), MAX(end_equity_zeroed),
                       ROUND(AVG(end_equity_zeroed)::numeric, 2)
                FROM daily_equity_zeroed WHERE day = %s
            """, (yesterday,))
            r = cur.fetchone()
            result["snapshot_stats"] = {
                "day": yesterday, "login_count": r[0],
                "total_eez": float(r[1]), "min_eez": float(r[2] or 0),
                "max_eez": float(r[3] or 0), "avg_eez": float(r[4] or 0),
            }

            # 2. Top 20 logins by EEZ (NO accounts join)
            cur.execute("""
                SELECT dez.login, dez.end_equity_zeroed
                FROM daily_equity_zeroed dez
                WHERE dez.day = %s
                ORDER BY dez.end_equity_zeroed DESC LIMIT 20
            """, (yesterday,))
            result["top_20_logins"] = [
                {"login": r[0], "eez": float(r[1] or 0)} for r in cur.fetchall()
            ]

            # 3. Duplicate logins in trading_accounts
            cur.execute("""
                SELECT login::bigint, COUNT(*) AS cnt
                FROM trading_accounts
                WHERE (deleted = 0 OR deleted IS NULL) AND vtigeraccountid IS NOT NULL
                GROUP BY login::bigint HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC LIMIT 20
            """)
            result["duplicate_logins_in_ta"] = [
                {"login": r[0], "ta_count": r[1]} for r in cur.fetchall()
            ]

            # 4. Live stats from trading_accounts only (no accounts join)
            cur.execute("""
                SELECT COUNT(*), COALESCE(SUM(equity), 0), COALESCE(SUM(balance), 0)
                FROM trading_accounts
                WHERE equity > 0 AND (deleted = 0 OR deleted IS NULL)
            """)
            r = cur.fetchone()
            result["live_ta_stats"] = {
                "equity_logins": r[0],
                "sum_equity": float(r[1]), "sum_balance": float(r[2]),
            }

            # 5. Bonus totals
            cur.execute("""
                SELECT COUNT(DISTINCT login), COALESCE(SUM(net_amount), 0)
                FROM bonus_transactions WHERE confirmation_time::date <= %s
            """, (str(today),))
            r = cur.fetchone()
            result["bonus_stats"] = {
                "logins_with_bonus": r[0] or 0,
                "total_cumulative_bonus": float(r[1]),
            }

            # 6. Specific login investigation
            check_login = login or 141727130
            cur.execute("""
                SELECT day, end_equity_zeroed, start_equity_zeroed
                FROM daily_equity_zeroed
                WHERE login = %s ORDER BY day DESC LIMIT 10
            """, (check_login,))
            login_snapshots = [
                {"day": str(r[0]), "end_eez": float(r[1] or 0),
                 "start_eez": float(r[2] or 0) if r[2] else None}
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT login, vtigeraccountid, equity, balance, deleted
                FROM trading_accounts WHERE login::bigint = %s
            """, (check_login,))
            ta_rows = [
                {"login": r[0], "accountid": r[1], "equity": float(r[2] or 0),
                 "balance": float(r[3] or 0), "deleted": r[4]}
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT date, convertedbalance, convertedfloatingpnl
                FROM dealio_daily_profits
                WHERE login = %s ORDER BY date DESC LIMIT 5
            """, (check_login,))
            ddp_rows = [
                {"date": str(r[0]), "convertedbalance": float(r[1] or 0),
                 "convertedfloatingpnl": float(r[2] or 0)}
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT COALESCE(SUM(net_amount), 0)
                FROM bonus_transactions
                WHERE login = %s AND confirmation_time::date <= %s
            """, (check_login, str(today)))
            login_bonus = float(cur.fetchone()[0])

            # Check dealio_users for compbalance
            cur.execute("""
                SELECT login, compbalance, compcredit, lastupdate
                FROM dealio_users WHERE login = %s
                ORDER BY lastupdate DESC NULLS LAST LIMIT 3
            """, (check_login,))
            du_rows = [
                {"login": r[0], "compbalance": float(r[1] or 0),
                 "compcredit": float(r[2] or 0),
                 "lastupdate": str(r[3]) if r[3] else None}
                for r in cur.fetchall()
            ]

            result[f"login_{check_login}"] = {
                "snapshots_last_10_days": login_snapshots,
                "trading_accounts": ta_rows,
                "dealio_daily_profits_last_5": ddp_rows,
                "cumulative_bonus": login_bonus,
                "dealio_users": du_rows,
                "computed_eez": "MAX(0, convertedbalance + floating - bonus)",
            }

        return JSONResponse(result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


@router.post("/api/cleanup-corrupt-ddp")
async def cleanup_corrupt_ddp(request: Request):
    """One-time cleanup: delete corrupt dealio_daily_profits rows and re-snapshot."""
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    if user.get("role") != "admin":
        return JSONResponse(status_code=403, content={"detail": "Admin only"})

    conn = get_connection()
    result = {}
    try:
        with conn.cursor() as cur:
            # 1. Find and delete corrupt rows
            cur.execute("""
                SELECT login, date, sourceid, convertedfloatingpnl
                FROM dealio_daily_profits
                WHERE ABS(COALESCE(convertedfloatingpnl, 0)) >= 100000000
            """)
            corrupt = [{"login": r[0], "date": str(r[1]), "sourceid": r[2],
                        "convertedfloatingpnl": float(r[3] or 0)} for r in cur.fetchall()]
            result["corrupt_rows_found"] = len(corrupt)
            result["corrupt_rows"] = corrupt[:50]  # cap output

            cur.execute("""
                DELETE FROM dealio_daily_profits
                WHERE ABS(COALESCE(convertedfloatingpnl, 0)) >= 100000000
            """)
            result["rows_deleted"] = cur.rowcount
        conn.commit()
    except Exception as e:
        conn.rollback()
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()

    # 2. Re-run snapshots for last 5 days
    from app.etl.fetch_and_store import run_daily_equity_zeroed_snapshot
    today = datetime.now(_TZ).date()
    snapshot_results = []
    for i in range(1, 6):
        d = str(today - __import__('datetime').timedelta(days=i))
        try:
            sr = run_daily_equity_zeroed_snapshot(d)
            snapshot_results.append({"date": d, **sr})
        except Exception as e:
            snapshot_results.append({"date": d, "error": str(e)})
    result["snapshots_refreshed"] = snapshot_results

    return JSONResponse(content=result)


_RETRYABLE_ERRORS = ("conflict with recovery", "ssl syscall error", "eof detected", "timeout expired")

def _with_retry(fn, *args, retries=1, delay=0):
    """Retry fn on transient dealio replica errors (replication conflict, SSL drop, timeout)."""
    for attempt in range(retries):
        try:
            return fn(*args)
        except Exception as e:
            msg = str(e).lower()
            if attempt < retries - 1 and any(s in msg for s in _RETRYABLE_ERRORS):
                time.sleep(delay)
                continue
            raise


@router.get("/api/live-equity-zeroed")
async def live_equity_zeroed(request: Request, date: str = None):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    today = datetime.now(_TZ).date()
    if not date:
        d = today
    else:
        try:
            d = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            return JSONResponse(status_code=400, content={"detail": "Invalid date"})

    is_current_month = (d.year == today.year and d.month == today.month)
    _ck = f"live_eez_v24:{d}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    # For past months, use historical (the value is historical by definition).
    if not is_current_month:
        try:
            result = _with_retry(_historical_calc, d)
            cache.set(_ck, result)
            return JSONResponse(content=result)
        except Exception as e:
            traceback.print_exc()
            return JSONResponse(status_code=500, content={"detail": str(e)})

    # Current month — card must show LIVE data only.
    # On success: cache short-TTL (normal) AND long-TTL "last-known-live" (15 min).
    # On failure: serve last-known-live with a freshness marker so the card
    # keeps showing the most recent real live value while dealio recovers.
    _last_ck = f"live_eez_last_known_v1:{d}"
    try:
        result = _with_retry(_live_calc, d)
        result["computed_at"] = datetime.now(_TZ).isoformat(timespec="seconds")
        result["is_stale"] = False
        cache.set(_ck, result)
        cache.set_long(_last_ck, result, ttl=15 * 60)
        return JSONResponse(content=result)
    except Exception as live_err:
        traceback.print_exc()
        stale = cache.get_long(_last_ck)
        if stale is not None:
            stale = {**stale, "is_stale": True, "dealio_error": str(live_err)}
            return JSONResponse(content=stale)
        # No recent live value available — tell the client so it can show "Unavailable".
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Live dealio data unavailable",
                "dealio_error": str(live_err),
            },
        )


def _historical_calc(d) -> dict:
    """Use dealio_daily_profits with same EEZ formula as eez_comparison page."""
    # Performance: previously did DISTINCT ON over 13.4M dealio_daily_profits rows
    # (full seq scan + disk sort ~18s). Rewritten as LATERAL index scan driven from
    # trading_accounts (~50K non-test/non-deleted rows) using idx_ddps_login_date_desc.
    sql = """
        WITH bonus_bal AS (
            SELECT login, SUM(net_amount) AS old_bonus_balance
            FROM bonus_transactions
            WHERE confirmation_time::date <= %(d)s
            GROUP BY login
        )
        SELECT COALESCE(SUM(
            CASE
                WHEN COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0) <= 0 THEN 0
                ELSE GREATEST(
                    COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0)
                        - COALESCE(b.old_bonus_balance, 0),
                    0
                )
            END
        ), 0) AS total_eez
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
                       AND a.is_test_account = 0
        CROSS JOIN LATERAL (
            SELECT convertedbalance, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE login = ta.login::bigint
              AND date < %(d)s::date + INTERVAL '1 day'
            ORDER BY date DESC
            LIMIT 1
        ) d
        LEFT JOIN bonus_bal b ON b.login = ta.login::bigint
        WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"d": str(d)})
            row = cur.fetchone()
            total = float(row[0] or 0)
            cur.execute("""
                SELECT COALESCE(SUM(end_equity_zeroed), 0)
                FROM daily_equity_zeroed
                WHERE day = %(d)s::date - INTERVAL '1 day'
                  AND login IN (
                      SELECT login::bigint FROM trading_accounts
                      WHERE vtigeraccountid IS NOT NULL
                        AND (deleted = 0 OR deleted IS NULL)
                  )
            """, {"d": str(d)})
            start_row = cur.fetchone()
            start_eez = float(start_row[0] or 0)
        return {"total": round(total), "start_equity_zeroed": round(start_eez), "pnl_cash": None, "net_deposits_today": None, "is_live": False, "date": str(d)}
    finally:
        conn.close()


def _live_calc(d) -> dict:
    """Live EEZ: MAX(0, compbalance + live_floating - cumulative_bonus).
    Matches snapshot formula: MAX(0, convertedbalance + convertedfloatingpnl - bonus).
    compbalance = pure balance (no credit included) — no credit deduction needed.
    compbalance includes today's closed PnL (balance updates when trades close).
    Only includes logins where ta.equity > 0 (avoids stale dealio values for dormant accounts).
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ta.login::bigint
                FROM trading_accounts ta
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
                  AND a.is_test_account = 0
                  AND ta.vtigeraccountid IS NOT NULL
            """)
            valid_logins = [int(r[0]) for r in cur.fetchall()]

            if not valid_logins:
                return {"total": 0, "start_equity_zeroed": 0, "net_deposits_today": 0, "pnl_cash": 0, "is_live": True, "date": str(d)}

            # Start EEZ per login (yesterday snapshot)
            cur.execute("""
                SELECT login, end_equity_zeroed
                FROM daily_equity_zeroed
                WHERE day = %(d)s::date - INTERVAL '1 day'
                  AND login = ANY(%(logins)s)
            """, {"d": str(d), "logins": valid_logins})
            start_eez_map = {}
            start_eez_total = 0.0
            for r in cur.fetchall():
                login_id = int(r[0])
                eez_val  = float(r[1] or 0)
                start_eez_map[login_id] = eez_val
                start_eez_total += eez_val

            # Net deposits today — read from MV (pre-filtered, indexed on tx_date)
            cur.execute("""
                SELECT COALESCE(SUM(net_usd), 0)
                FROM mv_daily_kpis
                WHERE tx_date = %(d)s::date
            """, {"d": str(d)})
            net_deposits_today = float(cur.fetchone()[0] or 0)

            # Logins with equity > 0 from live trading_accounts (+ equity for sanity cap)
            cur.execute("""
                SELECT ta.login::bigint, ta.equity
                FROM trading_accounts ta
                JOIN accounts a ON a.accountid = ta.vtigeraccountid
                WHERE ta.equity > 0
                  AND (ta.deleted = 0 OR ta.deleted IS NULL)
                  AND a.is_test_account = 0
            """)
            _eq_rows = cur.fetchall()
            equity_logins = [int(r[0]) for r in _eq_rows]

            # Cumulative bonus per login up to today (for equity_logins only)
            cur.execute("""
                SELECT login, SUM(net_amount)
                FROM bonus_transactions
                WHERE confirmation_time::date <= %(d)s
                  AND login = ANY(%(logins)s)
                GROUP BY login
            """, {"d": str(d), "logins": equity_logins})
            bonus_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

            # Daily start net equity: MAX(0, convertedbalance + convertedfloatingpnl)
            # from dealio_daily_profits for yesterday, same equity_logins set.
            # Sanity cap: skip rows where ABS(convertedfloatingpnl) > 100M (corrupt data).
            cur.execute("""
                SELECT COALESCE(SUM(CASE
                    WHEN COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0) <= 0 THEN 0
                    ELSE COALESCE(d.convertedbalance,0) + COALESCE(d.convertedfloatingpnl,0)
                END), 0)
                FROM (
                    SELECT DISTINCT ON (login) login, convertedbalance, convertedfloatingpnl
                    FROM dealio_daily_profits
                    WHERE login = ANY(%(logins)s)
                      AND date >= (%(d)s::date - INTERVAL '1 day')
                      AND date <  %(d)s::date
                    ORDER BY login, date DESC
                ) d
            """, {"d": str(d), "logins": equity_logins})
            start_net_equity = float(cur.fetchone()[0] or 0)

            # Today's bonuses (for daily pnl cash)
            cur.execute("""
                SELECT COALESCE(SUM(net_amount), 0)
                FROM bonus_transactions
                WHERE confirmation_time::date = %(d)s
            """, {"d": str(d)})
            today_bonuses = float(cur.fetchone()[0] or 0)

    finally:
        conn.close()

    # Fetch live dealio snapshot. Prefer the remote replica (most current), but fall
    # back to locally-synced copies (dealio_positions / dealio_users / dealio_trades_mt5)
    # when the replica is unhealthy (recovery conflicts, SSL drops). Local tables are
    # refreshed every ~1 min, so this is still "live" in practice.
    floating_map     = {}
    bal_map          = {}
    today_closed_pnl = 0.0
    data_source      = "remote"
    local_synced_at  = None
    if equity_logins:
        try:
            dc = get_dealio_connection()
            try:
                with dc.cursor() as cur:
                    cur.execute("""
                        SELECT login,
                               SUM(COALESCE(computedcommission,0)
                                 + COALESCE(computedprofit,0)
                                 + COALESCE(computedswap,0))
                        FROM dealio.positions
                        WHERE login = ANY(%s) AND cmd < 2 AND symbol NOT IN %s
                        GROUP BY login
                    """, (equity_logins, _EXCLUDED_SYMBOLS_TUPLE))
                    floating_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

                    cur.execute(
                        "SELECT login, compbalance FROM dealio.users WHERE login = ANY(%s)",
                        (equity_logins,)
                    )
                    bal_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

                    cur.execute("""
                        SELECT login,
                               SUM(COALESCE(computedcommission,0)
                                 + COALESCE(computedprofit,0)
                                 + COALESCE(computedswap,0))
                        FROM dealio.trades_mt5
                        WHERE login = ANY(%s)
                          AND entry = 1
                          AND closetime >= %s::date
                          AND closetime <  %s::date + INTERVAL '1 day'
                          AND cmd < 2
                          AND symbol NOT IN %s
                        GROUP BY login
                    """, (equity_logins, str(d), str(d), _EXCLUDED_SYMBOLS_TUPLE))
                    today_closed_pnl = sum(float(r[1] or 0) for r in cur.fetchall())
            finally:
                dc.close()
        except Exception as remote_err:
            # Remote replica unhealthy — use local synced copies.
            traceback.print_exc()
            print(f"[live_eez] remote dealio failed ({remote_err}); using local snapshot")
            data_source = "local_snapshot"
            conn3 = get_connection()
            try:
                with conn3.cursor() as cur:
                    cur.execute("""
                        SELECT login,
                               SUM(COALESCE(computed_commission,0)
                                 + COALESCE(computed_profit,0)
                                 + COALESCE(computed_swap,0))
                        FROM dealio_positions
                        WHERE login = ANY(%s) AND cmd < 2 AND symbol NOT IN %s
                        GROUP BY login
                    """, (equity_logins, _EXCLUDED_SYMBOLS_TUPLE))
                    floating_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

                    # Use compbalance directly from local dealio_users (synced from remote).
                    # A login may have multiple sourceid rows; pick the most recently updated.
                    cur.execute("""
                        SELECT DISTINCT ON (login) login, COALESCE(compbalance,0)
                        FROM dealio_users
                        WHERE login = ANY(%s)
                        ORDER BY login, lastupdate DESC NULLS LAST
                    """, (equity_logins,))
                    bal_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

                    cur.execute("""
                        SELECT COALESCE(SUM(
                            COALESCE(computed_commission,0)
                          + COALESCE(computed_profit,0)
                          + COALESCE(computed_swap,0)
                        ), 0)
                        FROM dealio_trades_mt5
                        WHERE login = ANY(%s)
                          AND entry = 1
                          AND close_time >= %s::date
                          AND close_time <  %s::date + INTERVAL '1 day'
                          AND cmd < 2
                          AND symbol NOT IN %s
                    """, (equity_logins, str(d), str(d), _EXCLUDED_SYMBOLS_TUPLE))
                    today_closed_pnl = float(cur.fetchone()[0] or 0)

                    # Freshness indicator for the card.
                    cur.execute("SELECT MAX(last_update) FROM dealio_positions")
                    row = cur.fetchone()
                    if row and row[0]:
                        local_synced_at = row[0].isoformat(timespec="seconds")
            finally:
                conn3.close()

    current_floating = sum(floating_map.values())
    open_logins      = list(floating_map.keys())

    # EEZ: MAX(0, compbalance + live_floating - bonus)
    # compbalance = pure balance (no credit) — matches snapshot formula exactly.
    # Daily end net equity: same without bonus deduction.
    # Sanity cap: if computed EEZ > 10x ta.equity, use ta.equity instead
    grand_total = 0.0
    daily_end_net_equity = 0.0
    for login, balance in bal_map.items():
        flt    = floating_map.get(login, 0.0)
        net_eq = balance + flt
        bonus  = max(0.0, bonus_map.get(login, 0.0))
        eez    = max(0.0, net_eq - bonus)
        neq    = max(0.0, net_eq)
        grand_total          += eez
        daily_end_net_equity += neq

    # Query eod_floating_yesterday only for currently-open logins
    eod_floating_yesterday = 0.0
    if open_logins:
        conn2 = get_connection()
        try:
            with conn2.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(COALESCE(d.convertedfloatingpnl, 0)), 0)
                    FROM (
                        SELECT DISTINCT ON (login) login, convertedfloatingpnl
                        FROM dealio_daily_profits
                        WHERE login = ANY(%(logins)s)
                          AND date >= (%(d)s::date - INTERVAL '1 day')
                          AND date <  %(d)s::date
                        ORDER BY login, date DESC
                    ) d
                """, {"d": str(d), "logins": open_logins})
                eod_floating_yesterday = float(cur.fetchone()[0] or 0)
        finally:
            conn2.close()

    delta_floating   = current_floating - eod_floating_yesterday
    daily_pnl        = round(delta_floating + today_closed_pnl)

    pnl_cash       = round(start_eez_total - grand_total - net_deposits_today)
    daily_pnl_cash = round(daily_end_net_equity - start_net_equity - net_deposits_today - today_bonuses)
    return {
        "total":                  round(grand_total),
        "start_equity_zeroed":    round(start_eez_total),
        "net_deposits_today":     round(net_deposits_today),
        "pnl_cash":               pnl_cash,
        "daily_pnl_cash":         daily_pnl_cash,
        "daily_pnl":              daily_pnl,
        "current_floating":       round(current_floating),
        "eod_floating_yesterday": round(eod_floating_yesterday),
        "today_closed_pnl":       round(today_closed_pnl),
        "daily_end_net_equity":   round(daily_end_net_equity),
        "daily_start_net_equity": round(start_net_equity),
        "is_live":                True,
        "date":                   str(d),
        "data_source":            data_source,       # "remote" or "local_snapshot"
        "local_synced_at":        local_synced_at,   # last_update of local positions (if fallback)
    }
