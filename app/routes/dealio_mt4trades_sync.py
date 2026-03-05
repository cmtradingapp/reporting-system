from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.etl.fetch_and_store import run_dealio_mt4trades_etl, run_dealio_mt4trades_full_etl

router = APIRouter()


@router.post("/sync/dealio-mt4trades")
async def sync_dealio_mt4trades():
    try:
        result = run_dealio_mt4trades_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/sync/dealio-mt4trades/full")
async def sync_dealio_mt4trades_full():
    try:
        result = run_dealio_mt4trades_full_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
