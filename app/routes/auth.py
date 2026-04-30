from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth.auth import create_access_token, decode_access_token, verify_password
from app.db.postgres_conn import get_auth_user_by_email, update_auth_user_last_login

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    token = request.cookies.get("access_token")
    if token:
        from app.db.postgres_conn import get_auth_user_by_id

        uid = decode_access_token(token)
        if uid:
            user = get_auth_user_by_id(uid)
            if user and user["is_active"] == 1:
                return RedirectResponse(url="/performance", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login")
async def login_submit(request: Request, email: str = Form(...), password: str = Form(...)):
    user = get_auth_user_by_email(email.strip().lower())
    if user is None or not verify_password(password, user["password_hash"]):
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Invalid email or password.",
            },
            status_code=401,
        )
    if user["is_active"] != 1:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Your account has been deactivated. Please contact your administrator.",
            },
            status_code=401,
        )

    token = create_access_token(user["id"])
    update_auth_user_last_login(user["id"])
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=8 * 3600,
    )
    return response


@router.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("access_token")
    return response
