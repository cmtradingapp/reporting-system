from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse

from app.etl.fetch_and_store import (
    run_dealio_mt4trades_etl,
    run_dealio_mt4trades_full_etl,
    run_dealio_trades_mt4_missing_etl,
    run_dealio_trades_mt4_rebuild_etl,
    run_dealio_trades_mt4_refresh_notional_etl,
)

router = APIRouter()


@router.post("/sync/dealio-mt4trades")
def sync_dealio_mt4trades():
    try:
        result = run_dealio_mt4trades_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/sync/dealio-mt4trades/full")
def sync_dealio_mt4trades_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dealio_mt4trades_full_etl)
    return JSONResponse(content={"status": "started"})


@router.post("/sync/dealio-trades-mt4/missing")
def sync_dealio_trades_mt4_missing(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dealio_trades_mt4_missing_etl)
    return JSONResponse(content={"status": "started"})


@router.post("/sync/dealio-trades-mt4/refresh-notional")
def sync_dealio_trades_mt4_refresh_notional(background_tasks: BackgroundTasks, hours: int = 2160):
    background_tasks.add_task(run_dealio_trades_mt4_refresh_notional_etl, hours)
    return JSONResponse(content={"status": "started", "lookback_hours": hours})


@router.post("/sync/dealio-trades-mt4/rebuild")
def sync_dealio_trades_mt4_rebuild(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dealio_trades_mt4_rebuild_etl)
    return JSONResponse(
        content={"status": "started", "info": "truncating and re-syncing all rows, incremental scheduler paused"}
    )
