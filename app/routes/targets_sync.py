from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.etl.fetch_and_store import run_targets_etl

router = APIRouter()


@router.post("/sync/targets")
def sync_targets():
    try:
        result = run_targets_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/sync/targets/full")
def sync_targets_full():
    try:
        result = run_targets_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
