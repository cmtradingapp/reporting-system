from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.db.postgres_conn import fetch_users_with_targets, fetch_last_sync
from datetime import datetime
import pandas as pd

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/users", response_class=HTMLResponse)
async def users_page(request: Request):
    df = fetch_users_with_targets()
    last_sync = fetch_last_sync()
    current_month = datetime.now().strftime("%B %Y")

    # Format datetime columns as strings to avoid NaT issues in Jinja2
    for col in ["last_logon_time", "last_update_time"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%d") if pd.notnull(x) else None)

    df = df.where(pd.notnull(df), None)
    users = df.to_dict(orient="records")
    total_active = len(df)
    total_ftc = float(df["total_ftc"].sum()) if not df.empty else 0
    total_net = float(df["total_net"].sum()) if not df.empty else 0

    return templates.TemplateResponse("users.html", {
        "request": request,
        "current_month": current_month,
        "last_sync": last_sync,
        "users": users,
        "total_active": total_active,
        "total_ftc": total_ftc,
        "total_net": total_net,
    })
