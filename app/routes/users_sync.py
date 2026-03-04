from fastapi import APIRouter
from app.etl.fetch_and_store import run_users_etl, run_users_full_etl

router = APIRouter()


@router.post("/sync/users")
async def sync_users(hours: int = 24):
    return run_users_etl(hours=hours)


@router.post("/sync/users/full")
async def sync_users_full():
    return run_users_full_etl()
