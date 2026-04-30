from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse

from app.etl.fetch_and_store import (
    run_dealio_trades_mt5_etl,
    run_dealio_trades_mt5_full_etl,
    run_dealio_trades_mt5_missing_etl,
    run_dealio_trades_mt5_rebuild_etl,
)

router = APIRouter()


@router.post("/sync/dealio-mt5trades")
def sync_dealio_mt5trades():
    try:
        result = run_dealio_trades_mt5_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/sync/dealio-mt5trades/full")
def sync_dealio_mt5trades_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dealio_trades_mt5_full_etl)
    return JSONResponse(content={"status": "started"})


@router.post("/sync/dealio-mt5trades/missing")
def sync_dealio_mt5trades_missing(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dealio_trades_mt5_missing_etl)
    return JSONResponse(content={"status": "started"})


@router.post("/sync/dealio-mt5trades/rebuild")
def sync_dealio_mt5trades_rebuild(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dealio_trades_mt5_rebuild_etl)
    return JSONResponse(content={"status": "started", "info": "truncating and re-syncing all MT5 trades"})
