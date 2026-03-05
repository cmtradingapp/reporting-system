from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse
from app.etl.fetch_and_store import run_trading_accounts_etl, run_trading_accounts_full_etl

router = APIRouter()


@router.post("/sync/trading-accounts")
def sync_trading_accounts():
    try:
        result = run_trading_accounts_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/sync/trading-accounts/full")
def sync_trading_accounts_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_trading_accounts_full_etl)
    return JSONResponse(content={"status": "started"})
