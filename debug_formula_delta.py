"""
debug_formula_delta.py

Tests whether using the pre-computed delta columns in dealio.daily_profits
matches Dealio's known daily_pnl_cash values.

Also checks for duplicate login rows per day and checks convertedequity.

Known Dealio daily_pnl_cash:
  2026-03-23: -285,438    2026-03-18: -654,361
  2026-03-22:  -14,425    2026-03-17: -175,504
  2026-03-21:  -44,155    2026-03-16: -191,888
  2026-03-20: -270,655    2026-03-15:   -1,527
  2026-03-19: -422,665    2026-03-14:  -57,923

Run:
    docker exec reporting-system-app-1 python debug_formula_delta.py
"""

from app.db.dealio_conn import get_dealio_connection
from datetime import date, timedelta

KNOWN = {
    date(2026,3,23): -285438,
    date(2026,3,22):  -14425,
    date(2026,3,21):  -44155,
    date(2026,3,20): -270655,
    date(2026,3,19): -422665,
    date(2026,3,18): -654361,
    date(2026,3,17): -175504,
    date(2026,3,16): -191888,
    date(2026,3,15):   -1527,
    date(2026,3,14):  -57923,
}

DATES = sorted(KNOWN.keys(), reverse=True)

dc = get_dealio_connection()
with dc.cursor() as cur:

    # ── Check for duplicate logins per day ───────────────────────────────────
    print("Duplicate login check (23/3):")
    cur.execute("""
        SELECT COUNT(*) total_rows, COUNT(DISTINCT login) unique_logins
        FROM dealio.daily_profits
        WHERE date::date = '2026-03-23'
    """)
    r = cur.fetchone()
    print(f"  total rows: {r[0]:,}   unique logins: {r[1]:,}")
    print()

    # ── Test formula variations using delta columns ──────────────────────────
    print("Testing delta-based formulas:")
    print()
    header = f"{'Date':<12} {'Dealio':>10} {'F10 delta':>12} {'F11 delta-nd':>14} {'F12 eq-diff':>12}  gap F10  gap F11  gap F12"
    print(header)
    print("─" * len(header))

    for d in DATES:
        d_prev = d - timedelta(days=1)

        # F10: SUM(convertedclosedpnl + converteddeltafloatingpnl)  — all logins, no net deposit
        # F11: SUM(convertedclosedpnl + converteddeltafloatingpnl - convertednetdeposit) — all logins
        # F12: SUM(convertedequity today) - SUM(convertedequity yesterday) - net_dep
        cur.execute("""
            SELECT
                COALESCE(SUM(COALESCE(convertedclosedpnl,0) + COALESCE(converteddeltafloatingpnl,0)), 0)                          AS f10,
                COALESCE(SUM(COALESCE(convertedclosedpnl,0) + COALESCE(converteddeltafloatingpnl,0) - COALESCE(convertednetdeposit,0)), 0) AS f11,
                COALESCE(SUM(COALESCE(convertedequity,0)), 0)                                                                      AS eq_today,
                COALESCE(SUM(COALESCE(convertednetdeposit,0)), 0)                                                                  AS net_dep,
                COUNT(*) AS cnt
            FROM (
                SELECT DISTINCT ON (login)
                    convertedclosedpnl, converteddeltafloatingpnl,
                    convertednetdeposit, convertedequity
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d),))
        row = cur.fetchone()
        f10_raw  = float(row[0])
        f11_raw  = float(row[1])
        eq_today = float(row[2])
        net_dep  = float(row[3])

        # Yesterday's convertedequity for F12 SOD
        cur.execute("""
            SELECT COALESCE(SUM(COALESCE(convertedequity,0)), 0)
            FROM (
                SELECT DISTINCT ON (login) convertedequity
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d_prev),))
        eq_yesterday = float(cur.fetchone()[0])

        f10 = round(f10_raw)
        f11 = round(f11_raw)
        f12 = round(eq_today - eq_yesterday - net_dep)

        known = KNOWN[d]
        print(f"{str(d):<12} {known:>10,} {f10:>12,} {f11:>14,} {f12:>12,}  {known-f10:>+7,}  {known-f11:>+7,}  {known-f12:>+7,}")

    print()
    print("F10 = SUM(closedpnl + deltafloating)              no net deposits")
    print("F11 = SUM(closedpnl + deltafloating - netdeposit) net deposits per login")
    print("F12 = SUM(convertedequity) today - yesterday - net_dep")

dc.close()
