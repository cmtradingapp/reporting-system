"""
debug_table_sync.py

1. Compares our Postgres dealio_daily_profits vs Dealio replica dealio.daily_profits
   to verify the sync is exact.

2. For each date with a known Dealio value, tries to reverse-engineer the exact
   formula by testing variations (no clip, different net deposit sign, etc.)

Run:
    docker exec reporting-system-app-1 python debug_table_sync.py
"""

from app.db.dealio_conn import get_dealio_connection
from app.db.postgres_conn import get_connection
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

# ── Part 1: Table sync check ─────────────────────────────────────────────────
print("=" * 80)
print("PART 1: Our dealio_daily_profits (Postgres) vs dealio.daily_profits (replica)")
print("=" * 80)
print(f"{'Date':<12} {'PG rows':>8} {'DC rows':>8} {'PG SUM(cb)':>14} {'DC SUM(cb)':>14} {'match?':>7}")
print("─" * 70)

pg = get_connection()
dc = get_dealio_connection()

for d in DATES:
    with pg.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(convertedbalance),0), COALESCE(SUM(convertedfloatingpnl),0), COALESCE(SUM(convertednetdeposit),0)
            FROM dealio_daily_profits
            WHERE date::date = %s
        """, (str(d),))
        pg_row = cur.fetchone()
        pg_cnt, pg_cb, pg_cf, pg_nd = int(pg_row[0]), float(pg_row[1]), float(pg_row[2]), float(pg_row[3])

    with dc.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(convertedbalance),0), COALESCE(SUM(convertedfloatingpnl),0), COALESCE(SUM(convertednetdeposit),0)
            FROM dealio.daily_profits
            WHERE date::date = %s
        """, (str(d),))
        dc_row = cur.fetchone()
        dc_cnt, dc_cb, dc_cf, dc_nd = int(dc_row[0]), float(dc_row[1]), float(dc_row[2]), float(dc_row[3])

    match = "OK" if abs(pg_cb - dc_cb) < 1 and pg_cnt == dc_cnt else "DIFF"
    print(f"{str(d):<12} {pg_cnt:>8,} {dc_cnt:>8,} {pg_cb:>14,.0f} {dc_cb:>14,.0f} {match:>7}")

print()

# ── Part 2: Formula reverse-engineering from Dealio replica ──────────────────
print("=" * 80)
print("PART 2: Formula variations vs known Dealio values (all logins, replica data)")
print("=" * 80)
print()
print("Testing: daily_pnl_cash = EOD - SOD - net_dep (various EOD/SOD definitions)")
print()

header = f"{'Date':<12} {'Dealio':>10} {'F6 clip':>10} {'F7 noclip':>10} {'F8 no-MAX-SOD':>14} {'F9 noMAX both':>14}"
print(header)
print("─" * len(header))

for d in DATES:
    d_prev = d - timedelta(days=1)

    with dc.cursor() as cur:
        # EOD aggregates for day d (ALL logins, no filter)
        cur.execute("""
            SELECT
                -- clipped per login
                COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0),
                -- no clip
                COALESCE(SUM(cb+cf), 0),
                -- net deposits
                COALESCE(SUM(nd), 0),
                -- count
                COUNT(*)
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf,
                    COALESCE(convertednetdeposit,  0) AS nd
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d),))
        row = cur.fetchone()
        eod_clip, eod_raw, net_dep, eod_cnt = float(row[0]), float(row[1]), float(row[2]), int(row[3])

        # SOD aggregates for day d_prev (ALL logins, no filter)
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN cb+cf > 0 THEN cb+cf ELSE 0 END), 0),
                COALESCE(SUM(cb+cf), 0)
            FROM (
                SELECT DISTINCT ON (login)
                    COALESCE(convertedbalance,     0) AS cb,
                    COALESCE(convertedfloatingpnl, 0) AS cf
                FROM dealio.daily_profits
                WHERE date::date = %s
                ORDER BY login, date DESC
            ) x
        """, (str(d_prev),))
        row = cur.fetchone()
        sod_clip, sod_raw = float(row[0]), float(row[1])

    f6 = round(eod_clip - sod_clip - net_dep)   # clip both SOD and EOD
    f7 = round(eod_raw  - sod_raw  - net_dep)   # no clip at all
    f8 = round(eod_clip - sod_raw  - net_dep)   # clip EOD, raw SOD
    f9 = round(eod_raw  - sod_clip - net_dep)   # raw EOD, clip SOD

    known = KNOWN[d]
    print(f"{str(d):<12} {known:>10,} {f6:>10,} {f7:>10,} {f8:>14,} {f9:>14,}")

print()
print("F6 = MAX(0,cb+cf) EOD  - MAX(0,cb+cf) SOD  - net_dep  [clip both]")
print("F7 = (cb+cf) EOD       - (cb+cf) SOD        - net_dep  [no clip at all]")
print("F8 = MAX(0,cb+cf) EOD  - (cb+cf) SOD        - net_dep  [clip EOD, raw SOD]")
print("F9 = (cb+cf) EOD       - MAX(0,cb+cf) SOD   - net_dep  [raw EOD, clip SOD]")

pg.close()
dc.close()
