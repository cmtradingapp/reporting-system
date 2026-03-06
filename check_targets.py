from app.db.postgres_conn import get_connection
conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT date, agent_id, net FROM targets WHERE date >= '2026-03-01' AND date <= '2026-03-31' ORDER BY agent_id, date LIMIT 20")
for r in cur.fetchall():
    print(r)
conn.close()
