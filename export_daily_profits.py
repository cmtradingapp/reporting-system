"""
Export dealio.daily_profits (latest row per login) to Excel.
Run from /opt/reporting-system/reporting-system on the server:
    python export_daily_profits.py
"""
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DEALIO_PG_HOST", "cmtrading-replicadb.dealio.ai"),
    port=int(os.getenv("DEALIO_PG_PORT", 5106)),
    user=os.getenv("DEALIO_PG_USER"),
    password=os.getenv("DEALIO_PG_PASSWORD"),
    dbname=os.getenv("DEALIO_PG_DB", "dealio"),
    connect_timeout=30,
    options="-c statement_timeout=120000",
    sslmode="require",
    sslcert=os.getenv("DEALIO_PG_SSLCERT", "/root/.postgresql/client.crt"),
    sslkey=os.getenv("DEALIO_PG_SSLKEY", "/root/.postgresql/client.key"),
    sslrootcert=os.getenv("DEALIO_PG_SSLROOTCERT", "/root/.postgresql/ca.crt"),
    client_encoding="utf8",
)

SQL = """
SELECT t.*
FROM dealio.daily_profits t
INNER JOIN (
    SELECT login, MAX(date) AS max_date
    FROM dealio.daily_profits
    GROUP BY login
) latest
ON t.login = latest.login AND t.date = latest.max_date
ORDER BY t.login
"""

print("Querying dealio.daily_profits...")
df = pd.read_sql(SQL, conn)
conn.close()

print(f"Fetched {len(df):,} rows, {df['login'].nunique():,} unique logins")

out = "daily_profit_latest.xlsx"
df.to_excel(out, index=False)
print(f"Saved: {out}")
