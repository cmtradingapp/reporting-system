import os
from dotenv import load_dotenv

load_dotenv()

# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "cmtrading-replica-db.cllx9icdmhvp.eu-west-1.rds.amazonaws.com")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "db_readonly")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "crmdb")

# MSSQL
MSSQL_HOST = os.getenv("MSSQL_HOST", "cmtmainserver.database.windows.net")
MSSQL_PORT = int(os.getenv("MSSQL_PORT", 1433))
MSSQL_USER = os.getenv("MSSQL_USER", "clawreadonly")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "")
MSSQL_DB = os.getenv("MSSQL_DB", "cmt_main")

# PostgreSQL (local)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.getenv("POSTGRES_DB", "datawarehouse")

# Dealio PostgreSQL (replica source)
DEALIO_PG_HOST     = os.getenv("DEALIO_PG_HOST", "cmtrading-replicadb.dealio.ai")
DEALIO_PG_PORT     = int(os.getenv("DEALIO_PG_PORT", 5432))
DEALIO_PG_USER     = os.getenv("DEALIO_PG_USER", "")
DEALIO_PG_PASSWORD = os.getenv("DEALIO_PG_PASSWORD", "")
DEALIO_PG_DB       = os.getenv("DEALIO_PG_DB", "dealio")

# JWT / Auth
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 8
