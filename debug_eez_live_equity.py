"""
Test: replace compprevequity with (balance + live_floating) for Live EEZ.
Compares:
  Method A (current):  MAX(0, compprevequity - compcredit - bonus)
  Method B (new):      MAX(0, balance + floating - credit - bonus)
Also checks CEO formula: start_eez + net_deposits + daily_pnl vs Method B.
"""
import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_connection, _EXCLUDED_SYMBOLS_TUPLE
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Nicosia")
today = datetime.now(_TZ).date()
d = str(today)
print(f"Date: {d}\n")

# ── 1. Local postgres ─────────────────────────────────────────────────────────
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

        cur.execute("""
            SELECT login, end_equity_zeroed FROM daily_equity_zeroed
            WHERE day = %s::date - INTERVAL '1 day' AND login = ANY(%s)
        """, (d, valid_logins))
        start_eez = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        cur.execute("""
            SELECT login, SUM(net_amount) FROM bonus_transactions
            WHERE confirmation_time < %s::date + INTERVAL '1 day'
              AND login = ANY(%s)
            GROUP BY login
        """, (d, valid_logins))
        bonus_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        cur.execute("""
            SELECT ta.login::bigint,
                   COALESCE(SUM(CASE
                       WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN  t.usdamount
                       WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled') THEN -t.usdamount
                   END), 0)
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            JOIN trading_accounts ta ON ta.vtigeraccountid = t.vtigeraccountid
            WHERE t.transactionapproval = 'Approved'
              AND (t.deleted = 0 OR t.deleted IS NULL)
              AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
              AND t.confirmation_time >= %s::date
              AND t.confirmation_time <  %s::date + INTERVAL '1 day'
              AND a.is_test_account = 0
              AND (ta.deleted = 0 OR ta.deleted IS NULL)
              AND LOWER(COALESCE(t.comment,'')) NOT LIKE '%%bonus%%'
              AND ta.login::bigint = ANY(%s)
            GROUP BY ta.login::bigint
        """, (d, d, valid_logins))
        net_deps = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        # equity_logins (ta.equity > 0) — same filter as live_equity.py
        cur.execute("""
            SELECT ta.login::bigint
            FROM trading_accounts ta
            JOIN accounts a ON a.accountid = ta.vtigeraccountid
            WHERE ta.equity > 0
              AND (ta.deleted = 0 OR ta.deleted IS NULL)
              AND a.is_test_account = 0
        """)
        equity_logins = [int(r[0]) for r in cur.fetchall()]

        # EOD floating yesterday
        cur.execute("""
            SELECT login, convertedfloatingpnl
            FROM (
                SELECT DISTINCT ON (login) login, convertedfloatingpnl
                FROM dealio_daily_profits
                WHERE date >= %s::date - INTERVAL '1 day'
                  AND date <  %s::date
                ORDER BY login, date DESC
            ) x WHERE x.login = ANY(%s)
        """, (d, d, valid_logins))
        eod_float_yest = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
finally:
    conn.close()

# ── 2. Dealio live data ───────────────────────────────────────────────────────
dc = get_dealio_connection()
try:
    with dc.cursor() as cur:
        # Method A fields (current)
        cur.execute(
            "SELECT login, compprevequity, compcredit FROM dealio.users WHERE login = ANY(%s)",
            (equity_logins,)
        )
        method_a = {int(r[0]): (float(r[1] or 0), float(r[2] or 0)) for r in cur.fetchall()}

        # Method B fields (new) — compbalance (USD-converted) + compcredit from live dealio.users
        cur.execute(
            "SELECT login, compbalance, compcredit FROM dealio.users WHERE login = ANY(%s)",
            (equity_logins,)
        )
        live_bal_cr = {int(r[0]): (float(r[1] or 0), float(r[2] or 0)) for r in cur.fetchall()}

        # Floating PnL from dealio.positions (same for both methods)
        cur.execute("""
            SELECT login,
                   SUM(COALESCE(computedcommission,0)+COALESCE(computedprofit,0)+COALESCE(computedswap,0))
            FROM dealio.positions
            WHERE login = ANY(%s) AND cmd < 2 AND symbol NOT IN %s
            GROUP BY login
        """, (equity_logins, _EXCLUDED_SYMBOLS_TUPLE))
        cur_float = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        # Today's closed PnL
        cur.execute("""
            SELECT login,
                   SUM(COALESCE(computed_commission,0)+COALESCE(computed_profit,0)+COALESCE(computed_swap,0))
            FROM dealio.trades_mt4
            WHERE login = ANY(%s)
              AND close_time >= %s::date AND close_time < %s::date + INTERVAL '1 day'
              AND cmd < 2 AND symbol NOT IN %s
            GROUP BY login
        """, (equity_logins, d, d, _EXCLUDED_SYMBOLS_TUPLE))
        closed_pnl = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
finally:
    dc.close()

# ── 3. Compute totals ─────────────────────────────────────────────────────────
total_a = 0.0
total_b = 0.0
rows = []

for login in equity_logins:
    bonus = max(0.0, bonus_map.get(login, 0.0))

    # Method A
    eq_a, cr_a = method_a.get(login, (0.0, 0.0))
    eez_a = max(0.0, eq_a - cr_a - bonus)

    # Method B
    bal, cr_b = live_bal_cr.get(login, (0.0, 0.0))
    flt = cur_float.get(login, 0.0)
    live_equity = bal + flt
    eez_b = max(0.0, live_equity - cr_b - bonus)

    total_a += eez_a
    total_b += eez_b

    # CEO formula for this login
    s_eez = start_eez.get(login, 0.0)
    nd    = net_deps.get(login, 0.0)
    yf    = eod_float_yest.get(login, 0.0)
    cp    = closed_pnl.get(login, 0.0)
    dpnl  = (flt - yf) + cp
    formula = s_eez + nd + dpnl

    diff_ab = eez_b - eez_a
    gap_b   = eez_b - formula

    if abs(diff_ab) > 100 or abs(gap_b) > 100:
        rows.append({
            "login": login,
            "bal": bal, "flt": flt, "cr_b": cr_b,
            "eq_a": eq_a, "cr_a": cr_a,
            "bonus": bonus,
            "eez_a": eez_a, "eez_b": eez_b,
            "start_eez": s_eez, "net_dep": nd, "dpnl": dpnl,
            "formula": formula,
            "diff_ab": diff_ab, "gap_b": gap_b,
        })

# ── 4. Summary ────────────────────────────────────────────────────────────────
start_total = sum(start_eez.get(l, 0.0) for l in equity_logins)
nd_total    = sum(net_deps.get(l, 0.0)   for l in equity_logins)

print(f"{'─'*65}")
print(f"  Start EEZ (yesterday):          {start_total:>12,.0f}")
print(f"  Net Deposits today:             {nd_total:>12,.0f}")
print(f"  Method A — compprevequity:      {total_a:>12,.0f}")
print(f"  Method B — balance + floating:  {total_b:>12,.0f}")
print(f"  Difference (B - A):             {total_b - total_a:>12,.0f}")
print(f"{'─'*65}")

# CEO formula check for Method B
all_dpnl = sum(
    (cur_float.get(l,0) - eod_float_yest.get(l,0)) + closed_pnl.get(l,0)
    for l in equity_logins
)
formula_total = start_total + nd_total + all_dpnl
print(f"\n  CEO formula (start+nd+pnl):     {formula_total:>12,.0f}")
print(f"  Method B (live EEZ):            {total_b:>12,.0f}")
print(f"  Gap (B - formula):              {total_b - formula_total:>12,.0f}")

# ── 5. Top differences between methods ───────────────────────────────────────
rows.sort(key=lambda r: abs(r["diff_ab"]), reverse=True)
print(f"\nTop 15 logins where Method B ≠ Method A (|diff| > $100):")
print(f"{'Login':>12} {'Bal':>12} {'Float':>10} {'Cr':>10} {'EEZ_A':>10} {'EEZ_B':>10} {'Diff(B-A)':>12}")
print("─" * 80)
for r in rows[:15]:
    print(f"{r['login']:>12} {r['bal']:>12,.0f} {r['flt']:>10,.0f} {r['cr_b']:>10,.0f} "
          f"{r['eez_a']:>10,.0f} {r['eez_b']:>10,.0f} {r['diff_ab']:>12,.0f}")

# ── 6. CEO formula gap for Method B ──────────────────────────────────────────
rows.sort(key=lambda r: abs(r["gap_b"]), reverse=True)
print(f"\nTop 15 logins: gap between Method B and CEO formula:")
print(f"{'Login':>12} {'StartEEZ':>10} {'NetDep':>8} {'DailyPnL':>10} {'Formula':>10} {'EEZ_B':>10} {'Gap':>10}")
print("─" * 75)
for r in rows[:15]:
    print(f"{r['login']:>12} {r['start_eez']:>10,.0f} {r['net_dep']:>8,.0f} {r['dpnl']:>10,.0f} "
          f"{r['formula']:>10,.0f} {r['eez_b']:>10,.0f} {r['gap_b']:>10,.0f}")
