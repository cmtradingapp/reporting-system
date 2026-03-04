from fastapi import APIRouter
from app.etl.fetch_and_store import run_accounts_etl

router = APIRouter()


@router.post("/sync/accounts")
async def sync_accounts(hours: int = 24):
    result = run_accounts_etl(hours=hours)
    return result
