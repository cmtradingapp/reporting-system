from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.db.postgres_conn import fetch_report_data, fetch_last_sync
from app.etl.fetch_and_store import run_etl
from datetime import datetime

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    df = fetch_report_data()
    last_sync = fetch_last_sync()
    current_month = datetime.now().strftime("%B %Y")

    agents = df.to_dict(orient="records")
    total_ftc = float(df["total_ftc"].sum()) if not df.empty else 0
    total_net = float(df["total_net"].sum()) if not df.empty else 0
    total_agents = len(df)

    chart_labels = [a["full_name"] or a["agent_id"] for a in agents[:20]]
    chart_net = [float(a["total_net"]) for a in agents[:20]]
    chart_ftc = [float(a["total_ftc"]) for a in agents[:20]]

    return templates.TemplateResponse("report.html", {
        "request": request,
        "current_month": current_month,
        "last_sync": last_sync,
        "agents": agents,
        "total_ftc": total_ftc,
        "total_net": total_net,
        "total_agents": total_agents,
        "chart_labels": chart_labels,
        "chart_net": chart_net,
        "chart_ftc": chart_ftc,
    })


@router.post("/sync")
async def sync_data():
    result = run_etl()
    return result
