from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.etl.fetch_and_store import run_client_classification_etl

router = APIRouter()


@router.post("/sync/client-classification")
def sync_client_classification():
    try:
        result = run_client_classification_etl()
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
