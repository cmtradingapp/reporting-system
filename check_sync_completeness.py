import sys
sys.path.insert(0, '/app')
from app.db.dealio_conn import get_dealio_connection
from app.db.postgres_conn import get_connection

pg_conn   = get_connection()
src_conn  = get_dealio_connection()

try:
    with pg_conn.cursor() as cur, src_conn.cursor() as src:

        # ── dealio_daily_profits ──────────────────────────────────────────────
        cur.execute("SELECT COUNT(*), MIN(date::date), MAX(date::date) FROM dealio_daily_profits")
        local_ddp = cur.fetchone()

        src.execute("SELECT COUNT(*), MIN(date::date), MAX(date::date) FROM dealio.daily_profits")
        src_ddp = src.fetchone()

        print("=== dealio_daily_profits ===")
        print(f"  Local : {local_ddp[0]:>10,} rows | {local_ddp[1]} → {local_ddp[2]}")
        print(f"  Source: {src_ddp[0]:>10,} rows | {src_ddp[1]} → {src_ddp[2]}")
        diff = src_ddp[0] - local_ddp[0]
        print(f"  Gap   : {diff:>10,} rows {'⚠ MISSING' if diff > 0 else '✓ OK'}")

        # ── dealio_trades_mt4 ─────────────────────────────────────────────────
        cur.execute("SELECT COUNT(*), MIN(ticket), MAX(ticket) FROM dealio_trades_mt4")
        local_dtm = cur.fetchone()

        src.execute("""
            SELECT COUNT(*), MIN(ticket), MAX(ticket)
            FROM dealio.trades_mt4
            WHERE cmd IN (0,1)
        """)
        src_dtm = src.fetchone()

        print("\n=== dealio_trades_mt4 (cmd 0/1 only) ===")
        print(f"  Local : {local_dtm[0]:>10,} rows | ticket {local_dtm[1]} → {local_dtm[2]}")
        print(f"  Source: {src_dtm[0]:>10,} rows | ticket {src_dtm[1]} → {src_dtm[2]}")
        diff2 = src_dtm[0] - local_dtm[0]
        print(f"  Gap   : {diff2:>10,} rows {'⚠ MISSING' if diff2 > 0 else '✓ OK'}")

        # ── dealio_users ──────────────────────────────────────────────────────
        cur.execute("SELECT COUNT(*) FROM dealio_users")
        local_du = cur.fetchone()

        src.execute("SELECT COUNT(*) FROM dealio.users")
        src_du = src.fetchone()

        print("\n=== dealio_users ===")
        print(f"  Local : {local_du[0]:>10,} rows")
        print(f"  Source: {src_du[0]:>10,} rows")
        diff3 = src_du[0] - local_du[0]
        print(f"  Gap   : {diff3:>10,} rows {'⚠ MISSING' if diff3 > 0 else '✓ OK'}")

        # ── Check daily_profits coverage per month ────────────────────────────
        print("\n=== dealio_daily_profits rows per month (local vs source) ===")
        cur.execute("""
            SELECT TO_CHAR(date::date, 'YYYY-MM') AS month, COUNT(*) AS cnt
            FROM dealio_daily_profits
            GROUP BY 1 ORDER BY 1 DESC LIMIT 12
        """)
        local_monthly = {r[0]: r[1] for r in cur.fetchall()}

        src.execute("""
            SELECT TO_CHAR(date::date, 'YYYY-MM') AS month, COUNT(*) AS cnt
            FROM dealio.daily_profits
            GROUP BY 1 ORDER BY 1 DESC LIMIT 12
        """)
        src_monthly = {r[0]: r[1] for r in src.fetchall()}

        all_months = sorted(set(local_monthly) | set(src_monthly), reverse=True)
        print(f"  {'Month':<10} {'Local':>12} {'Source':>12} {'Gap':>10}")
        print(f"  {'-'*48}")
        for m in all_months:
            l = local_monthly.get(m, 0)
            s = src_monthly.get(m, 0)
            flag = ' ⚠' if (s - l) > 0 else ''
            print(f"  {m:<10} {l:>12,} {s:>12,} {s-l:>10,}{flag}")

finally:
    pg_conn.close()
    src_conn.close()
