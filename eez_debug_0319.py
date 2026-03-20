"""
EEZ Debug: Compare per-login snapshot vs live-formula reconstruction for 2026-03-19.

Run on server:
  cd /opt/reporting-system/reporting-system
  PGPASSWORD=8PpVuUasBVR85T7WuAec python3 eez_debug_0319.py
"""
import psycopg2

TARGET_DATE = "2026-03-19"
PREV_DATE   = "2026-03-18"

conn = psycopg2.connect(
    host="127.0.0.1", port=5432,
    user="postgres", password="8PpVuUasBVR85T7WuAec",
    dbname="datawarehouse"
)

with conn.cursor() as cur:

    # ── 1. Snapshot values for 2026-03-19 ────────────────────────────────────
    cur.execute("""
        SELECT login, end_equity_zeroed
        FROM daily_equity_zeroed
        WHERE day = %s
        ORDER BY login
    """, (TARGET_DATE,))
    snapshot = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
    print(f"[1] Snapshot rows for {TARGET_DATE}: {len(snapshot)}")

    # ── 2. Start EEZ per login (from 2026-03-18 snapshot) ────────────────────
    cur.execute("""
        SELECT login, end_equity_zeroed
        FROM daily_equity_zeroed
        WHERE day = %s
        ORDER BY login
    """, (PREV_DATE,))
    start_eez = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
    print(f"[2] Start EEZ rows ({PREV_DATE}): {len(start_eez)}")

    # ── 3. Net deposits per login on 2026-03-19 ───────────────────────────────
    cur.execute("""
        SELECT ta.login::bigint,
               COALESCE(SUM(CASE
                   WHEN t.transactiontype IN ('Deposit','Withdrawal Cancelled') THEN  t.usdamount
                   WHEN t.transactiontype IN ('Withdrawal','Deposit Cancelled') THEN -t.usdamount
               END), 0) AS net_dep
        FROM transactions t
        JOIN accounts a           ON a.accountid = t.vtigeraccountid
        JOIN trading_accounts ta  ON ta.vtigeraccountid = t.vtigeraccountid
        JOIN crm_users u          ON u.id = t.original_deposit_owner
        WHERE t.transactionapproval = 'Approved'
          AND (t.deleted = 0 OR t.deleted IS NULL)
          AND t.transactiontype IN ('Deposit','Withdrawal Cancelled','Withdrawal','Deposit Cancelled')
          AND t.confirmation_time::date = %s
          AND EXTRACT(YEAR FROM t.confirmation_time) >= 2024
          AND t.vtigeraccountid IS NOT NULL
          AND a.is_test_account = 0
          AND (ta.deleted = 0 OR ta.deleted IS NULL)
          AND TRIM(COALESCE(u.agent_name, u.full_name, '')) NOT ILIKE 'test%%'
          AND LOWER(COALESCE(t.comment, '')) NOT LIKE '%%bonus%%'
        GROUP BY ta.login::bigint
    """, (TARGET_DATE,))
    net_deposits = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
    print(f"[3] Logins with net deposits on {TARGET_DATE}: {len(net_deposits)}")

    # ── 4. Floating PnL per login from dealio_daily_profits for 2026-03-19 ───
    #    (convertedbalance + convertedfloatingpnl = equity proxy for that date)
    cur.execute("""
        SELECT DISTINCT ON (login)
            login,
            convertedbalance,
            convertedfloatingpnl
        FROM dealio_daily_profits
        WHERE date::date = %s
        ORDER BY login, date DESC
    """, (TARGET_DATE,))
    raw_equity = {}
    for r in cur.fetchall():
        login = int(r[0])
        bal   = float(r[1] or 0)
        fpnl  = float(r[2] or 0)
        raw_equity[login] = {"balance": bal, "fpnl": fpnl}
    print(f"[4] dealio_daily_profits rows for {TARGET_DATE}: {len(raw_equity)}")

    # ── 5. Cumulative bonus per login up to 2026-03-19 ────────────────────────
    cur.execute("""
        SELECT login, SUM(net_amount)
        FROM bonus_transactions
        WHERE confirmation_time::date <= %s
        GROUP BY login
    """, (TARGET_DATE,))
    bonus = {int(r[0]): float(r[1] or 0) for r in cur.fetchall()}
    print(f"[5] Logins with bonus up to {TARGET_DATE}: {len(bonus)}")

    # ── 6. Valid logins (non-test, non-deleted, has vtigeraccountid) ──────────
    cur.execute("""
        SELECT ta.login::bigint
        FROM trading_accounts ta
        JOIN accounts a ON a.accountid = ta.vtigeraccountid
        WHERE (ta.deleted = 0 OR ta.deleted IS NULL)
          AND a.is_test_account = 0
          AND ta.vtigeraccountid IS NOT NULL
    """)
    valid_logins = {int(r[0]) for r in cur.fetchall()}
    print(f"[6] Valid logins: {len(valid_logins)}")

conn.close()

# ── Reconstruct live formula for each login ───────────────────────────────────
# live  = max(0, start_eez + net_deposits + floating_pnl - cumulative_bonus)
# Note: floating_pnl here = convertedfloatingpnl from dealio_daily_profits for that day
#       (proxy — real live would be from dealio replica at time of calculation)

all_logins = valid_logins & (set(snapshot.keys()) | set(start_eez.keys()) | set(raw_equity.keys()))

rows = []
for login in sorted(all_logins):
    snap    = snapshot.get(login, None)   # what's stored
    start   = start_eez.get(login, 0.0)
    dep     = net_deposits.get(login, 0.0)
    eq_info = raw_equity.get(login)
    fpnl    = eq_info["fpnl"]   if eq_info else 0.0
    bal     = eq_info["balance"] if eq_info else 0.0
    bon     = max(0.0, bonus.get(login, 0.0))   # clamp
    equity  = bal + fpnl

    # Live formula (same as _live_calc)
    live_val = max(0.0, start + dep + fpnl - bon)

    # Historical formula (same as _historical_calc / snapshot formula)
    if equity <= 0:
        hist_val = 0.0
    else:
        hist_val = max(0.0, equity - bon)

    diff = live_val - (snap if snap is not None else 0.0)

    rows.append({
        "login":    login,
        "snapshot": snap,
        "start_eez": start,
        "net_dep":  dep,
        "balance":  bal,
        "fpnl":     fpnl,
        "equity":   equity,
        "bonus":    bon,
        "live_val": round(live_val, 2),
        "hist_val": round(hist_val, 2),
        "diff_live_vs_snap": round(diff, 2),
    })

# ── Print rows with meaningful differences ────────────────────────────────────
THRESHOLD = 10.0  # only show logins where live vs snapshot differs by >$10

print(f"\n{'='*130}")
print(f"Per-login comparison: live formula vs stored snapshot ({TARGET_DATE})")
print(f"Only showing logins where |live - snapshot| > ${THRESHOLD}")
print(f"{'='*130}")
header = f"{'LOGIN':>12}  {'SNAPSHOT':>12}  {'LIVE_VAL':>12}  {'HIST_VAL':>12}  {'DIFF':>12}  {'START_EEZ':>12}  {'NET_DEP':>10}  {'BALANCE':>12}  {'FPNL':>12}  {'BONUS':>12}"
print(header)
print("-"*130)

filtered = [r for r in rows if abs(r["diff_live_vs_snap"]) > THRESHOLD]
filtered.sort(key=lambda x: abs(x["diff_live_vs_snap"]), reverse=True)

for r in filtered:
    snap_str = f"{r['snapshot']:,.2f}" if r['snapshot'] is not None else "MISSING"
    print(
        f"{r['login']:>12}  {snap_str:>12}  {r['live_val']:>12,.2f}  "
        f"{r['hist_val']:>12,.2f}  {r['diff_live_vs_snap']:>12,.2f}  "
        f"{r['start_eez']:>12,.2f}  {r['net_dep']:>10,.2f}  "
        f"{r['balance']:>12,.2f}  {r['fpnl']:>12,.2f}  {r['bonus']:>12,.2f}"
    )

print(f"\nTotal logins compared: {len(rows)}")
print(f"Logins with |diff| > ${THRESHOLD}: {len(filtered)}")

# Totals
snap_total = sum(r["snapshot"] for r in rows if r["snapshot"] is not None)
live_total = sum(r["live_val"] for r in rows)
hist_total = sum(r["hist_val"] for r in rows)
print(f"\nTOTALS:")
print(f"  Stored snapshot total : ${snap_total:>15,.2f}")
print(f"  Live formula total    : ${live_total:>15,.2f}")
print(f"  Hist formula total    : ${hist_total:>15,.2f}")
print(f"  Live - Snapshot diff  : ${live_total - snap_total:>15,.2f}")
print(f"  Hist - Snapshot diff  : ${hist_total - snap_total:>15,.2f}")

# Also: logins in snapshot but NOT in valid_logins (orphaned)
orphaned = set(snapshot.keys()) - valid_logins
print(f"\nLogins in snapshot but NOT in valid_logins (orphaned/test/deleted): {len(orphaned)}")
orphaned_sum = sum(snapshot[l] for l in orphaned)
print(f"  Orphaned snapshot value: ${orphaned_sum:,.2f}")
