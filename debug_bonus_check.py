import sys
sys.path.insert(0, '/app')
from app.db.postgres_conn import get_connection
from app.db.dealio_conn import get_dealio_connection

logins = [141635360, 141319759, 141932329]

conn = get_connection()
with conn.cursor() as cur:
    cur.execute(
        'SELECT login, SUM(net_amount) FROM bonus_transactions WHERE login = ANY(%s) GROUP BY login',
        (logins,)
    )
    print('bonus_transactions:', cur.fetchall())
conn.close()

dc = get_dealio_connection()
with dc.cursor() as cur:
    cur.execute(
        'SELECT login, compcredit FROM dealio.users WHERE login = ANY(%s)',
        (logins,)
    )
    print('compcredit (dealio):  ', cur.fetchall())
dc.close()
