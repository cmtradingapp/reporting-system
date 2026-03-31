from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse
from app.etl.fetch_and_store import (
    run_dealio_users_etl, run_dealio_users_full_etl,
    run_dealio_trades_mt4_etl, run_dealio_trades_mt4_full_etl,
    run_dealio_trades_mt4_by_open_time_etl,
)

router = APIRouter()


@router.post("/sync/dealio-users")
def sync_dealio_users():
    try:
        result = run_dealio_users_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/sync/dealio-users/full")
def sync_dealio_users_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dealio_users_full_etl)
    return JSONResponse(content={"status": "started"})


@router.post("/sync/dealio-trades-mt4")
def sync_dealio_trades_mt4():
    try:
        result = run_dealio_trades_mt4_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/sync/dealio-trades-mt4/full")
def sync_dealio_trades_mt4_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dealio_trades_mt4_full_etl)
    return JSONResponse(content={"status": "started"})


@router.post("/sync/dealio-trades-mt4/by-open-time")
def sync_dealio_trades_mt4_by_open_time(background_tasks: BackgroundTasks, from_date: str = "2026-01-01"):
    background_tasks.add_task(run_dealio_trades_mt4_by_open_time_etl, from_date)
    return JSONResponse(content={"status": "started", "from_date": from_date})
