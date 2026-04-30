from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_last_sync_times, get_mv_status

router = APIRouter()


@router.get("/api/last-sync")
async def api_last_sync(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return JSONResponse(content=get_last_sync_times())


@router.get("/api/mv-status")
async def api_mv_status(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    if user.get("role") != "admin":
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})
    return JSONResponse(content=get_mv_status())
