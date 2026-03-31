from fastapi import APIRouter, BackgroundTasks
from app.etl.fetch_and_store import run_accounts_etl, run_accounts_full_etl, run_accounts_by_qual_date_etl, run_accounts_by_created_date_etl

router = APIRouter()


@router.post("/sync/accounts")
def sync_accounts(hours: int = 24):
    return run_accounts_etl(hours=hours)


@router.post("/sync/accounts/full")
def sync_accounts_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_accounts_full_etl)
    return {"status": "started"}


@router.post("/sync/accounts/by-qual-date")
def sync_accounts_by_qual_date(from_date: str = "2026-01-01"):
    return run_accounts_by_qual_date_etl(from_date=from_date)


@router.post("/sync/accounts/by-created-date")
def sync_accounts_by_created_date(background_tasks: BackgroundTasks, from_date: str = "2026-01-01"):
    background_tasks.add_task(run_accounts_by_created_date_etl, from_date)
    return {"status": "started", "from_date": from_date}
