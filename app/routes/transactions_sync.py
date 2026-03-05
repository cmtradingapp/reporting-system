from fastapi import APIRouter
from app.etl.fetch_and_store import run_transactions_etl, run_transactions_full_etl

router = APIRouter()


@router.post("/sync/transactions")
def sync_transactions(hours: int = 24):
    return run_transactions_etl(hours=hours)


@router.post("/sync/transactions/full")
def sync_transactions_full():
    return run_transactions_full_etl()
