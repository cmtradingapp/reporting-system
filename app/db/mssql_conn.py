import pymssql
import pandas as pd
from app.config import MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DB


def get_targets() -> pd.DataFrame:
    conn = pymssql.connect(
        server=MSSQL_HOST,
        port=str(MSSQL_PORT),
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=MSSQL_DB,
        tds_version="7.4",
        conn_properties="",
    )
    try:
        query = """
            SELECT date, agent_id, ftc, net
            FROM report.target
            WHERE YEAR(date) = YEAR(GETDATE())
              AND MONTH(date) = MONTH(GETDATE())
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()
