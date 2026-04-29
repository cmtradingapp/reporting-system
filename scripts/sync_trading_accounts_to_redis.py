#!/usr/bin/env python3
"""
Sync trading_accounts from MySQL (CRM) → Redis.

Redis key structure:
  trading_account:{login}       — JSON with account details
  trading_accounts:logins       — SET of all logins
  trading_accounts:last_update  — ISO timestamp

Runs on the MT5Node server (109.199.112.72) via cron.
MySQL: cmtrading-replica-db (AWS RDS read replica)
Redis: localhost:6379
"""
import json
import time
import redis
import pymysql
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
MYSQL_HOST = "cmtrading-replica-db.cllx9icdmhvp.eu-west-1.rds.amazonaws.com"
MYSQL_PORT = 3306
MYSQL_USER = "db_readonly"
MYSQL_PASSWORD = "wmFZBKH4E5j9m8Ax"
MYSQL_DB = "crmdb"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0

BATCH_SIZE = 5000

# ── MySQL query ───────────────────────────────────────────────────────────────
QUERY = """
SELECT
    bu.id                                       AS trading_account_id,
    CONCAT(bu.first_name, ' ', bu.last_name)    AS trading_account_name,
    bu.user_id                                  AS vtigeraccountid,
    bu.trade_group_string                       AS trade_group,
    bu.last_update_time                         AS last_update,
    bu.equity / 100                             AS equity,
    bu.open_pnl                                 AS open_pnl,
    bu.close_pnl                                AS total_pnl,
    bu.total_commission                         AS commission,
    (0 = bu.is_deleted)                         AS enable,
    (0 = bu.is_trading_active)                  AS enable_read_only,
    bu.external_id                              AS login,
    bu.currency                                 AS currency,
    IF(bu.is_demo, 1, 2)                        AS serverid,
    CASE
        WHEN u.acquisition_status = 0 AND u.sales_rep != 0     THEN u.sales_rep
        WHEN u.acquisition_status = 0 AND u.sales_rep = 0      THEN u.sales_desk_id
        WHEN u.acquisition_status = 1 AND u.retention_rep != 0 THEN u.retention_rep
        ELSE u.retention_desk_id
    END                                         AS assigned_to,
    bu.balance / 100                            AS balance,
    bu.total_swap                               AS swaps,
    bu.leverage                                 AS leverage,
    bu.margin                                   AS margin,
    bu.free_margin                              AS margin_free,
    bu.creation_time                            AS created_time,
    bu.is_deleted                               AS deleted
FROM v_ant_broker_user bu
LEFT JOIN v_ant_users u ON u.id = bu.user_id
WHERE bu.trade_group_string NOT LIKE '%test%'
  AND bu.is_demo != 1
  AND bu.is_deleted = 0
"""


def sync():
    start = time.time()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting trading_accounts sync...")

    # Connect MySQL
    mysql_conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DB, charset="utf8mb4",
        connect_timeout=30, read_timeout=120,
        cursorclass=pymysql.cursors.DictCursor,
    )

    # Connect Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    r.ping()

    try:
        with mysql_conn.cursor() as cur:
            cur.execute(QUERY)
            rows = cur.fetchall()

        print(f"  Fetched {len(rows)} rows from MySQL in {time.time()-start:.1f}s")

        # Build new login set
        new_logins = set()
        pipe = r.pipeline(transaction=False)
        count = 0

        for row in rows:
            login = str(row.get("login", ""))
            if not login:
                continue

            # Convert non-serializable types
            from decimal import Decimal
            clean = {}
            for k, v in row.items():
                if isinstance(v, datetime):
                    clean[k] = v.isoformat()
                elif isinstance(v, Decimal):
                    clean[k] = float(v)
                elif v is None:
                    clean[k] = None
                else:
                    clean[k] = v

            pipe.set(f"trading_account:{login}", json.dumps(clean))
            new_logins.add(login)
            count += 1

            if count % BATCH_SIZE == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)

        # Flush remaining
        if count % BATCH_SIZE != 0:
            pipe.execute()

        # Update the index set — remove stale logins, add new ones
        existing = r.smembers("trading_accounts:logins")
        stale = existing - new_logins
        if stale:
            pipe = r.pipeline(transaction=False)
            for login in stale:
                pipe.delete(f"trading_account:{login}")
                pipe.srem("trading_accounts:logins", login)
            pipe.execute()
            print(f"  Removed {len(stale)} stale logins")

        # Overwrite the set
        if new_logins:
            pipe = r.pipeline(transaction=False)
            pipe.delete("trading_accounts:logins")
            for i in range(0, len(new_logins), BATCH_SIZE):
                batch = list(new_logins)[i:i+BATCH_SIZE]
                pipe.sadd("trading_accounts:logins", *batch)
            pipe.execute()

        # Timestamp
        r.set("trading_accounts:last_update", datetime.now(timezone.utc).isoformat())

        elapsed = time.time() - start
        print(f"  Synced {count} trading accounts to Redis in {elapsed:.1f}s")
        print(f"  Keys: trading_account:{{login}}, trading_accounts:logins ({len(new_logins)}), trading_accounts:last_update")

    finally:
        mysql_conn.close()


if __name__ == "__main__":
    sync()
