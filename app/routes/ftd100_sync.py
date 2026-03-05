from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse
from app.etl.fetch_and_store import run_ftd100_etl

router = APIRouter()


@router.post("/sync/ftd100")
def sync_ftd100():
    try:
        result = run_ftd100_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/sync/ftd100/full")
def sync_ftd100_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_ftd100_etl)
    return JSONResponse(content={"status": "started"})
