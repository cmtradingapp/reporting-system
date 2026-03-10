from fastapi import Request, HTTPException
from fastapi.responses import RedirectResponse
from app.auth.auth import decode_access_token
from app.db.postgres_conn import get_auth_user_by_id


async def get_current_user(request: Request):
    token = request.cookies.get('access_token')
    if not token:
        return RedirectResponse(url='/login', status_code=302)
    user_id = decode_access_token(token)
    if user_id is None:
        r = RedirectResponse(url='/login', status_code=302)
        r.delete_cookie('access_token')
        return r
    user = get_auth_user_by_id(user_id)
    if user is None or user['is_active'] != 1:
        r = RedirectResponse(url='/login', status_code=302)
        r.delete_cookie('access_token')
        return r
    return user


def require_admin(user: dict):
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    return user
