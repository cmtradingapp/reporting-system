"""Find the correct daily PnL formula by testing multiple approaches."""
import sys
sys.path.insert(0, '/app')
from datetime import date
from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_floating_pnl_for_logins, get_dealio_closed_pnl_for_logins_date

D    = date(2026, 3, 20)
PREV = date(2026, 3, 19)

conn = get_connection()
with conn.cursor() as cur:

    # Valid login sets
    cur.execute("""
        SELECT ta.login::bigint FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE ta.equity > 0 AND (ta.deleted=0 OR ta.deleted IS NULL) AND a.is_test_account=0
    """)
    equity_logins = [int(r[0]) for r in cur.fetchall()]

    cur.execute("""
        SELECT ta.login::bigint FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE (ta.deleted=0 OR ta.deleted IS NULL) AND a.is_test_account=0
          AND ta.vtigeraccountid IS NOT NULL
    """)
    valid_logins = [int(r[0]) for r in cur.fetchall()]

    print(f"equity_logins: {len(equity_logins)}, valid_logins: {len(valid_logins)}")

    # --- Approach A: SUM(converteddeltafloatingpnl) from today's dealio_daily_profits ---
    cur.execute("""
        SELECT COALESCE(SUM(d.converteddeltafloatingpnl), 0)
        FROM (
            SELECT DISTINCT ON (login) login, converteddeltafloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %s
            ORDER BY login, date DESC
        ) d WHERE d.login = ANY(%s)
    """, (str(D), equity_logins))
    a1 = float(cur.fetchone()[0] or 0)
    print(f"\nA1. SUM(converteddeltafloatingpnl) today, equity_logins: ${a1:,.2f}")

    cur.execute("""
        SELECT COALESCE(SUM(converteddeltafloatingpnl), 0)
        FROM dealio_daily_profits
        WHERE date::date = %s AND login = ANY(%s)
    """, (str(D), equity_logins))
    a2 = float(cur.fetchone()[0] or 0)
    print(f"A2. SUM(converteddeltafloatingpnl) ALL rows today, equity_logins: ${a2:,.2f}")

    # --- Approach B: SUM(convertedclosedpnl) from today's dealio_daily_profits ---
    cur.execute("""
        SELECT COALESCE(SUM(d.convertedclosedpnl), 0)
        FROM (
            SELECT DISTINCT ON (login) login, convertedclosedpnl
            FROM dealio_daily_profits
            WHERE date::date = %s
            ORDER BY login, date DESC
        ) d WHERE d.login = ANY(%s)
    """, (str(D), equity_logins))
    b1 = float(cur.fetchone()[0] or 0)
    print(f"\nB1. SUM(convertedclosedpnl) today latest-per-login, equity_logins: ${b1:,.2f}")

    cur.execute("""
        SELECT COALESCE(SUM(convertedclosedpnl), 0)
        FROM dealio_daily_profits
        WHERE date::date = %s AND login = ANY(%s)
    """, (str(D), equity_logins))
    b2 = float(cur.fetchone()[0] or 0)
    print(f"B2. SUM(convertedclosedpnl) ALL rows today, equity_logins: ${b2:,.2f}")

    # --- Approach C: delta_floating + convertedclosedpnl ---
    cur.execute("""
        SELECT COALESCE(SUM(d.convertedfloatingpnl), 0)
        FROM (
            SELECT DISTINCT ON (login) login, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %s
            ORDER BY login, date DESC
        ) d WHERE d.login = ANY(%s)
    """, (str(D), equity_logins))
    today_floating_snapshot = float(cur.fetchone()[0] or 0)
    print(f"\nC. convertedfloatingpnl latest snapshot TODAY, equity_logins: ${today_floating_snapshot:,.2f}")

    cur.execute("""
        SELECT COALESCE(SUM(d.convertedfloatingpnl), 0)
        FROM (
            SELECT DISTINCT ON (login) login, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %s
            ORDER BY login, date DESC
        ) d WHERE d.login = ANY(%s)
    """, (str(PREV), equity_logins))
    prev_floating_snapshot = float(cur.fetchone()[0] or 0)
    print(f"C. convertedfloatingpnl latest snapshot YESTERDAY, equity_logins: ${prev_floating_snapshot:,.2f}")
    print(f"C. delta (today_snap - yesterday_snap): ${today_floating_snapshot - prev_floating_snapshot:,.2f}")

    # --- How many rows does today have in dealio_daily_profits? ---
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT login) FROM dealio_daily_profits WHERE date::date = %s", (str(D),))
    r = cur.fetchone()
    print(f"\nToday's dealio_daily_profits: {r[0]} rows, {r[1]} distinct logins")

    cur.execute("SELECT MIN(date), MAX(date) FROM dealio_daily_profits WHERE date::date = %s", (str(D),))
    r = cur.fetchone()
    print(f"Today's date range: {r[0]} → {r[1]}")

    cur.execute("SELECT MIN(date), MAX(date) FROM dealio_daily_profits WHERE date::date = %s", (str(PREV),))
    r = cur.fetchone()
    print(f"Yesterday's date range: {r[0]} → {r[1]}")

conn.close()

# --- Live dealio ---
print("\n--- Live dealio ---")
open_rows = get_dealio_floating_pnl_for_logins(equity_logins)
current_floating = sum(float(r[1] or 0) for r in open_rows)
open_logins = [int(r[0]) for r in open_rows]
print(f"current_floating (live): ${current_floating:,.2f}  ({len(open_logins)} logins)")

closed_rows = get_dealio_closed_pnl_for_logins_date(equity_logins, str(D))
today_closed = sum(float(r[1] or 0) for r in closed_rows)
print(f"today_closed_pnl (live): ${today_closed:,.2f}  ({len(closed_rows)} logins)")

print(f"\n--- Summary of approaches ---")
print(f"Our current formula (delta_live_float + closed_live): ${(current_floating - prev_floating_snapshot) + today_closed:,.2f}")
print(f"A2 + today_closed_live (all delta rows + live closed): ${a2 + today_closed:,.2f}")
print(f"B2 (all closed rows from daily_profits today):         ${b2:,.2f}")
print(f"A2 + B2 (all delta_float + all closed from ddp):       ${a2 + b2:,.2f}")
print(f"today_snapshot - yesterday_snapshot + closed_live:     ${(today_floating_snapshot - prev_floating_snapshot) + today_closed:,.2f}")
