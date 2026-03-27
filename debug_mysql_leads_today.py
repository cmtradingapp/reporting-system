"""Run on server: python debug_mysql_leads_today.py"""
from app.db.mysql_conn import _get_connection

conn = _get_connection()
try:
    with conn.cursor() as cur:

        cur.execute("SELECT UTC_TIMESTAMP(), NOW(), @@global.time_zone, @@session.time_zone")
        row = cur.fetchone()
        print(f"MySQL UTC_TIMESTAMP() : {row[0]}")
        print(f"MySQL NOW()           : {row[1]}")
        print(f"MySQL global TZ       : {row[2]}")
        print(f"MySQL session TZ      : {row[3]}")
        print()

        cur.execute("SELECT MAX(creation_time), MIN(creation_time) FROM users WHERE is_test = 0")
        row = cur.fetchone()
        print(f"Latest creation_time  : {row[0]}")
        print(f"Oldest creation_time  : {row[1]}")
        print()

        cur.execute("""
            SELECT DATE(creation_time) AS day, COUNT(*)
            FROM users
            WHERE is_test = 0
              AND creation_time >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -7 DAY)
            GROUP BY 1 ORDER BY 1 DESC
        """)
        rows = cur.fetchall()
        print("MySQL accounts per day (last 7 days):")
        for r in rows:
            print(f"  {r[0]}  ->  {r[1]}")
        print()

        cur.execute("""
            SELECT COUNT(*)
            FROM users
            WHERE is_test = 0
              AND DATE(creation_time) = DATE(UTC_TIMESTAMP())
        """)
        print(f"MySQL accounts where DATE(creation_time) = today (UTC): {cur.fetchone()[0]}")

        cur.execute("""
            SELECT COUNT(*)
            FROM users
            WHERE is_test = 0
              AND creation_time >= DATE_ADD(UTC_TIMESTAMP(), INTERVAL -24 HOUR)
        """)
        print(f"MySQL accounts in last 24h                             : {cur.fetchone()[0]}")

        cur.execute("""
            SELECT COUNT(*)
            FROM users u
            LEFT JOIN user_account_info_records uair ON uair.user_id = u.id
            WHERE u.is_test = 0
              AND DATE(u.creation_time) = DATE(UTC_TIMESTAMP())
              AND u.last_update_time < DATE_ADD(UTC_TIMESTAMP(), INTERVAL -6 HOUR)
        """)
        print(f"Today's accounts NOT touched in last 6h (missed by sync): {cur.fetchone()[0]}")

finally:
    conn.close()
