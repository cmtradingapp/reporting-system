from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse

from app.etl.fetch_and_store import run_mssql_dealio_mt5trades_full_etl

router = APIRouter()


@router.post("/sync/mssql-dealio-mt5trades/full")
def sync_mssql_dealio_mt5trades_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_mssql_dealio_mt5trades_full_etl)
    return JSONResponse(content={"status": "started", "info": "syncing all rows from MSSQL report.dealio_mt5trades"})
