"""
compare_pnl_cash.py

Computes our system's daily_pnl_cash using the EXACT same formula as live_equity.py,
then compares it to Dealio's "Daily PnL Cash" per-login.

Run on the server AT THE SAME TIME as downloading the Dealio CSV export:
    docker exec reporting-system-app-1 python compare_pnl_cash.py

Formula:
    daily_pnl_cash = daily_end_net_equity - start_net_equity - net_deposits_today - today_bonuses

Where:
    daily_end_net_equity = SUM per login of MAX(0, compbalance + floating)
    start_net_equity     = SUM per login of MAX(0, convertedbalance + convertedfloatingpnl) from yesterday
    net_deposits_today   = SUM of net_usd from mv_daily_kpis for today
    today_bonuses        = SUM of net_amount from bonus_transactions for today
"""

from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from app.db.postgres_conn import get_connection
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
today = str(datetime.now(_TZ).date())
print(f"today (Cyprus): {today}")
print()

# ── Step 1: get valid logins + start_net_equity (yesterday) ───────────────
pg = get_connection()
with pg.cursor() as cur:

    # All non-test, non-deleted logins
    cur.execute("""
        SELECT ta.login::bigint
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
          AND a.is_test_account = 0
          AND ta.vtigeraccountid IS NOT NULL
    """)
    valid_logins = [int(r[0]) for r in cur.fetchall()]

    # Equity logins (equity > 0 — used for bonus_map and start_net_equity)
    cur.execute("""
        SELECT ta.login::bigint
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE ta.equity > 0
          AND (ta.deleted = 0 OR ta.deleted IS NULL)
          AND a.is_test_account = 0
    """)
    equity_logins = [int(r[0]) for r in cur.fetchall()]

    # start_net_equity: MAX(0, convertedbalance + convertedfloatingpnl) yesterday
    # for equity_logins (same as live_equity.py)
    cur.execute("""
        SELECT login, convertedbalance, convertedfloatingpnl
        FROM (
            SELECT DISTINCT ON (login) login, convertedbalance, convertedfloatingpnl
            FROM dealio_daily_profits
            WHERE date::date = %s::date - INTERVAL '1 day'
            ORDER BY login, date DESC
        ) d
        WHERE d.login = ANY(%s)
    """, (today, equity_logins))
    start_rows = cur.fetchall()
    start_per_login = {int(r[0]): max(0.0, float(r[1] or 0) + float(r[2] or 0)) for r in start_rows}
    start_net_equity = sum(start_per_login.values())

    # Net deposits today
    cur.execute("""
        SELECT COALESCE(SUM(net_usd), 0)
        FROM mv_daily_kpis
        WHERE tx_date = %s::date
    """, (today,))
    net_deposits_today = float(cur.fetchone()[0] or 0)

    # Today's bonuses
    cur.execute("""
        SELECT COALESCE(SUM(net_amount), 0)
        FROM bonus_transactions
        WHERE confirmation_time::date = %s
    """, (today,))
    today_bonuses = float(cur.fetchone()[0] or 0)

    # Cumulative bonus per login for equity_logins
    cur.execute("""
        SELECT login, SUM(net_amount)
        FROM bonus_transactions
        WHERE confirmation_time::date <= %s
          AND login = ANY(%s)
        GROUP BY login
    """, (today, equity_logins))
    bonus_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

pg.close()

print(f"valid_logins:    {len(valid_logins):,}")
print(f"equity_logins:   {len(equity_logins):,}")
print()

# ── Step 2: live data from Dealio ─────────────────────────────────────────
dc = get_dealio_connection()
with dc.cursor() as cur:

    # compbalance per login
    cur.execute(
        "SELECT login, compbalance FROM dealio.users WHERE login = ANY(%s)",
        (equity_logins,)
    )
    bal_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

    # floating per login (open positions)
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

dc.close()

# ── Step 3: compute daily_end_net_equity per login ─────────────────────────
# MAX(0, compbalance + floating) — no bonus deduction for end_net_equity
daily_end_per_login = {}
for login, balance in bal_map.items():
    flt = floating_map.get(login, 0.0)
    daily_end_per_login[login] = max(0.0, balance + flt)

daily_end_net_equity = sum(daily_end_per_login.values())

# ── Step 4: daily_pnl_cash (aggregate, same as live_equity.py) ────────────
daily_pnl_cash = round(daily_end_net_equity - start_net_equity - net_deposits_today - today_bonuses)

print("─" * 60)
print("OUR SYSTEM  daily_pnl_cash  (same formula as live_equity.py)")
print("─" * 60)
print(f"  daily_end_net_equity: ${daily_end_net_equity:>14,.0f}")
print(f"  start_net_equity:     ${start_net_equity:>14,.0f}")
print(f"  net_deposits_today:   ${net_deposits_today:>14,.0f}")
print(f"  today_bonuses:        ${today_bonuses:>14,.0f}")
print(f"  DAILY PNL CASH:       ${daily_pnl_cash:>14,}")
print()

# ── Step 5: Dealio per-login comparison ───────────────────────────────────
# Per login: (daily_end_net_equity_i - start_net_equity_i) — before net deposits/bonuses
# This is our equivalent of what Dealio shows per login
per_login_delta = {}
all_logins_seen = set(daily_end_per_login) | set(start_per_login)
for login in all_logins_seen:
    end_val   = daily_end_per_login.get(login, 0.0)
    start_val = start_per_login.get(login, 0.0)
    delta     = end_val - start_val
    if delta != 0:
        per_login_delta[login] = delta

our_total_delta = sum(per_login_delta.values())

print("─" * 60)
print("PER-LOGIN delta  (end_net_equity - start_net_equity, before dep/bonus)")
print("─" * 60)
print(f"  Logins with non-zero delta: {len(per_login_delta):,}")
print(f"  Sum of per-login deltas:    ${our_total_delta:>14,.2f}")
print(f"  (= daily_pnl_cash + net_deposits_today + today_bonuses)")
print()

# ── Step 6: top movers from our system ────────────────────────────────────
top_ours = sorted(per_login_delta.items(), key=lambda x: abs(x[1]), reverse=True)[:15]
print("─" * 60)
print("TOP 15 LOGINS by |delta| — OUR SYSTEM")
print("─" * 60)
print(f"  {'Login':<15} {'Our delta':>12}  {'start_eq':>12}  {'end_eq':>12}")
for login, delta in top_ours:
    print(f"  {login:<15} {delta:>12,.2f}  {start_per_login.get(login,0):>12,.2f}  {daily_end_per_login.get(login,0):>12,.2f}")
print()
print("Compare the 'Login' and 'Our delta' columns above to the Dealio CSV")
print("Run both at the same time for an accurate comparison.")
