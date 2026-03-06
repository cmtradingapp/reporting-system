from app.db.postgres_conn import get_connection
conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT * FROM crm_users")
cols = [d[0] for d in cur.description]
print(",".join(cols))
for row in cur.fetchall():
    print(",".join(str(v).replace(",", "|") if v is not None else "" for v in row))
conn.close()
