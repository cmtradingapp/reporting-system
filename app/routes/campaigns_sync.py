from fastapi import APIRouter

from app.etl.fetch_and_store import run_campaigns_etl

router = APIRouter()


@router.post("/sync/campaigns")
def sync_campaigns():
    return run_campaigns_etl()
