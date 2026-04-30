from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse

from app.etl.fetch_and_store import run_dealio_daily_profits_etl, run_dealio_daily_profits_full_etl

router = APIRouter()


@router.post("/sync/dealio-daily-profits")
def sync_dealio_daily_profits():
    try:
        result = run_dealio_daily_profits_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/sync/dealio-daily-profits/full")
def sync_dealio_daily_profits_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dealio_daily_profits_full_etl)
    return JSONResponse(content={"status": "started"})
