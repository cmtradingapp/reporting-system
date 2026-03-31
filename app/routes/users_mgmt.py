import secrets
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user, require_admin
from app.auth.auth import hash_password
from app.db.postgres_conn import (
    list_auth_users, create_auth_user, update_auth_user,
    update_auth_user_password, deactivate_auth_user, get_connection,
)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/admin/users", response_class=HTMLResponse)
async def users_mgmt_page(request: Request):
    user = await get_current_user(request)
    from fastapi.responses import RedirectResponse
    if isinstance(user, RedirectResponse):
        return user
    require_admin(user)
    users = list_auth_users()
    return templates.TemplateResponse("users_mgmt.html", {
        "request": request,
        "current_user": user,
        "users": users,
    })


@router.post("/api/admin/users")
async def api_create_user(request: Request):
    user = await get_current_user(request)
    from fastapi.responses import RedirectResponse
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    require_admin(user)
    body = await request.json()
    import json as _json
    email = body.get('email', '').strip().lower()
    full_name = body.get('full_name', '').strip()
    role = body.get('role', 'agent')
    crm_user_id = body.get('crm_user_id') or None
    ap_list = body.get('allowed_pages') or None  # list or None
    allowed_pages = _json.dumps(ap_list) if ap_list else None
    if not email or not full_name:
        return JSONResponse(status_code=400, content={"detail": "email and full_name are required"})
    pw_hash = hash_password('Welcome1!')
    try:
        new_id = create_auth_user(email, full_name, pw_hash, role, crm_user_id, allowed_pages)
        return JSONResponse(content={"id": new_id, "temp_password": "Welcome1!"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.put("/api/admin/users/{user_id}")
async def api_update_user(user_id: int, request: Request):
    user = await get_current_user(request)
    from fastapi.responses import RedirectResponse
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    require_admin(user)
    import json as _json
    body = await request.json()
    ap_list = body.get('allowed_pages') or None
    allowed_pages = _json.dumps(ap_list) if ap_list else None
    try:
        update_auth_user(
            user_id,
            body.get('full_name', ''),
            body.get('email', '').strip().lower(),
            body.get('role', 'agent'),
            int(body.get('is_active', 1)),
            body.get('crm_user_id') or None,
            allowed_pages,
        )
        return JSONResponse(content={"status": "ok"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/api/admin/users/{user_id}/reset-password")
async def api_reset_password(user_id: int, request: Request):
    user = await get_current_user(request)
    from fastapi.responses import RedirectResponse
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    require_admin(user)
    temp_password = secrets.token_urlsafe(9)
    pw_hash = hash_password(temp_password)
    try:
        update_auth_user_password(user_id, pw_hash, force_change=1)
        return JSONResponse(content={"temp_password": temp_password})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/api/admin/users/{user_id}/deactivate")
async def api_deactivate_user(user_id: int, request: Request):
    user = await get_current_user(request)
    from fastapi.responses import RedirectResponse
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    require_admin(user)
    try:
        deactivate_auth_user(user_id)
        return JSONResponse(content={"status": "ok"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.get("/api/admin/crm-users/search")
async def api_search_crm_users(request: Request, q: str = ""):
    user = await get_current_user(request)
    from fastapi.responses import RedirectResponse
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    require_admin(user)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, COALESCE(agent_name, full_name, id::text) AS name, email
                FROM crm_users
                WHERE (agent_name ILIKE %s OR full_name ILIKE %s OR email ILIKE %s)
                  AND status = 'Active'
                LIMIT 20
            """, (f'%{q}%', f'%{q}%', f'%{q}%'))
            rows = cur.fetchall()
        return JSONResponse(content=[{"id": r[0], "name": r[1], "email": r[2] or ""} for r in rows])
    finally:
        conn.close()
