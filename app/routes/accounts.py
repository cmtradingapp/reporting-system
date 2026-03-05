from fastapi import APIRouter
from app.etl.fetch_and_store import run_accounts_etl, run_accounts_full_etl

router = APIRouter()


@router.post("/sync/accounts")
def sync_accounts(hours: int = 24):
    return run_accounts_etl(hours=hours)


@router.post("/sync/accounts/full")
def sync_accounts_full():
    return run_accounts_full_etl()
