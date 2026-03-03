import pymysql
import pandas as pd
from app.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB


def get_operators() -> pd.DataFrame:
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB if MYSQL_DB else None,
        connect_timeout=10,
        ssl={"ssl": True},
    )
    try:
        query = "SELECT o.id, o.full_name FROM v_ant_operators o"
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()
