from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse
from app.etl.fetch_and_store import run_dealio_daily_profit_etl, run_dealio_daily_profit_full_etl

router = APIRouter()


@router.post("/sync/dealio-daily-profit")
def sync_dealio_daily_profit():
    try:
        result = run_dealio_daily_profit_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/sync/dealio-daily-profit/full")
def sync_dealio_daily_profit_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dealio_daily_profit_full_etl)
    return JSONResponse(content={"status": "started"})
