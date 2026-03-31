"""
Per-login comparison:
  CEO formula:  start_eez + net_deposits + daily_pnl
  Live EEZ:     MAX(0, equity - credit - MAX(0, bonus))
Shows where the gap comes from.
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

# ── 1. Load everything from local postgres ────────────────────────────────────
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
        print(f"Valid logins: {len(valid_logins)}")

        # Start EEZ (yesterday)
        cur.execute("""
            SELECT login, end_equity_zeroed FROM daily_equity_zeroed
            WHERE day = %s::date - INTERVAL '1 day' AND login = ANY(%s)
        """, (d, valid_logins))
        start_eez = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        # Net deposits today
        cur.execute("""
            SELECT ta.login::bigint,
                   COALESCE(SUM(CASE
                       WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN  t.usdamount
                       WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled') THEN -t.usdamount
                   END), 0)
            FROM transactions t
            JOIN accounts a  ON a.accountid  = t.vtigeraccountid
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

        # Cumulative bonus per login
        cur.execute("""
            SELECT login, SUM(net_amount) FROM bonus_transactions
            WHERE confirmation_time < %s::date + INTERVAL '1 day'
              AND login = ANY(%s)
            GROUP BY login
        """, (d, valid_logins))
        bonus_map = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        # EOD floating yesterday (for delta calc)
        cur.execute("""
            SELECT login, convertedfloatingpnl
            FROM (
                SELECT DISTINCT ON (login) login, convertedfloatingpnl
                FROM dealio_daily_profits
                WHERE date >= %s::date - INTERVAL '1 day'
                  AND date <  %s::date
                ORDER BY login, date DESC
            ) x
            WHERE x.login = ANY(%s)
        """, (d, d, valid_logins))
        eod_float_yest = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

finally:
    conn.close()

# ── 2. Load live data from dealio ─────────────────────────────────────────────
dc = get_dealio_connection()
try:
    with dc.cursor() as cur:
        cur.execute(
            "SELECT login, compprevequity, compcredit FROM dealio.users WHERE login = ANY(%s)",
            (valid_logins,)
        )
        live_eq = {int(r[0]): (float(r[1] or 0), float(r[2] or 0)) for r in cur.fetchall()}

        cur.execute("""
            SELECT login,
                   SUM(COALESCE(computedcommission,0)+COALESCE(computedprofit,0)+COALESCE(computedswap,0))
            FROM dealio.positions
            WHERE login = ANY(%s) AND cmd < 2 AND symbol NOT IN %s
            GROUP BY login
        """, (valid_logins, _EXCLUDED_SYMBOLS_TUPLE))
        cur_float = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}

        cur.execute("""
            SELECT login,
                   SUM(COALESCE(computed_commission,0)+COALESCE(computed_profit,0)+COALESCE(computed_swap,0))
            FROM dealio.trades_mt4
            WHERE login = ANY(%s) AND close_time >= %s::date AND close_time < %s::date + INTERVAL '1 day'
              AND cmd < 2 AND symbol NOT IN %s
            GROUP BY login
        """, (valid_logins, d, d, _EXCLUDED_SYMBOLS_TUPLE))
        closed_pnl = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
finally:
    dc.close()

# ── 3. Per-login comparison ───────────────────────────────────────────────────
all_logins = set(valid_logins)
rows = []
for login in all_logins:
    s_eez   = start_eez.get(login, 0.0)
    nd      = net_deps.get(login, 0.0)
    bonus   = max(0.0, bonus_map.get(login, 0.0))
    eq, cr  = live_eq.get(login, (0.0, 0.0))

    cf      = cur_float.get(login, 0.0)
    yf      = eod_float_yest.get(login, 0.0)
    cp      = closed_pnl.get(login, 0.0)
    delta_f = cf - yf
    dpnl    = delta_f + cp

    live_eez_val    = max(0.0, eq - cr - bonus)
    formula_val     = s_eez + nd + dpnl
    gap             = live_eez_val - formula_val

    rows.append({
        "login": login, "start_eez": s_eez, "net_dep": nd,
        "daily_pnl": dpnl, "formula": formula_val,
        "live_eez": live_eez_val, "gap": gap,
        "equity": eq, "credit": cr, "bonus": bonus,
    })

total_formula  = sum(r["formula"]   for r in rows)
total_live_eez = sum(r["live_eez"]  for r in rows)
total_gap      = sum(r["gap"]       for r in rows)

print(f"\n{'─'*60}")
print(f"  Total Formula (Start+Net+PnL): {total_formula:>12,.0f}")
print(f"  Total Live EEZ:                {total_live_eez:>12,.0f}")
print(f"  Total Gap (Live - Formula):    {total_gap:>12,.0f}")
print(f"{'─'*60}")

# ── 4. Category breakdown ─────────────────────────────────────────────────────
cats = {
    "A: start>0, live>0  (normal)":       [r for r in rows if r["start_eez"] > 0 and r["live_eez"] > 0],
    "B: start=0, live>0  (crossed floor)": [r for r in rows if r["start_eez"] == 0 and r["live_eez"] > 0],
    "C: start>0, live=0  (went below)":    [r for r in rows if r["start_eez"] > 0 and r["live_eez"] == 0],
    "D: start=0, live=0  (both zero)":     [r for r in rows if r["start_eez"] == 0 and r["live_eez"] == 0],
}
print("\nCategory breakdown:")
for name, grp in cats.items():
    g_formula  = sum(r["formula"]  for r in grp)
    g_live     = sum(r["live_eez"] for r in grp)
    g_gap      = sum(r["gap"]      for r in grp)
    g_start    = sum(r["start_eez"] for r in grp)
    g_nd       = sum(r["net_dep"]  for r in grp)
    g_pnl      = sum(r["daily_pnl"] for r in grp)
    print(f"\n  {name}  ({len(grp)} logins)")
    print(f"    Start EEZ:  {g_start:>12,.0f}   Net Dep: {g_nd:>10,.0f}   Daily PnL: {g_pnl:>12,.0f}")
    print(f"    Formula:    {g_formula:>12,.0f}   Live EEZ:{g_live:>10,.0f}   Gap:       {g_gap:>12,.0f}")

# ── 5. Top 20 largest gap contributors ───────────────────────────────────────
rows_with_gap = [r for r in rows if abs(r["gap"]) > 100]
rows_with_gap.sort(key=lambda r: abs(r["gap"]), reverse=True)
print(f"\nTop 20 largest gap contributors (|gap| > $100):")
print(f"{'Login':>12} {'StartEEZ':>12} {'NetDep':>10} {'DailyPnL':>12} {'Formula':>12} {'LiveEEZ':>12} {'Gap':>12}")
print("─" * 86)
for r in rows_with_gap[:20]:
    print(f"{r['login']:>12} {r['start_eez']:>12,.0f} {r['net_dep']:>10,.0f} {r['daily_pnl']:>12,.0f} {r['formula']:>12,.0f} {r['live_eez']:>12,.0f} {r['gap']:>12,.0f}")
