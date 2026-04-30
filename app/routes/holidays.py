from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/holidays", response_class=HTMLResponse)
async def holidays_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if user.get("role") != "admin" and "holidays" not in (user.get("allowed_pages_list") or []):
        return RedirectResponse(url="/performance")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT holiday_date, description FROM public_holidays ORDER BY holiday_date")
            rows = cur.fetchall()
        holidays = [{"date": str(r[0]), "description": r[1] or ""} for r in rows]
        return templates.TemplateResponse(
            "holidays.html", {"request": request, "current_user": user, "holidays": holidays}
        )
    finally:
        conn.close()


@router.post("/api/holidays")
def add_holiday(payload: dict):
    date_str = payload.get("date", "").strip()
    description = payload.get("description", "").strip()
    if not date_str:
        return JSONResponse(status_code=400, content={"detail": "date is required"})
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public_holidays (holiday_date, description)
                VALUES (%s, %s)
                ON CONFLICT (holiday_date) DO UPDATE SET description = EXCLUDED.description
                """,
                (date_str, description or None),
            )
        conn.commit()
        return JSONResponse(content={"status": "ok"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()


@router.delete("/api/holidays/{date_str}")
def delete_holiday(date_str: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public_holidays WHERE holiday_date = %s", (date_str,))
        conn.commit()
        return JSONResponse(content={"status": "ok"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
