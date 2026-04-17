from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from app.auth.dependencies import get_current_user
from app.db.postgres_conn import get_connection

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/transactions-report", response_class=HTMLResponse)
async def transactions_report_page(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return user
    if user.get("role") != "admin" and "transactions_report" not in (user.get("allowed_pages_list") or []):
        return RedirectResponse(url="/performance", status_code=302)
    return templates.TemplateResponse("transactions_report.html", {
        "request": request,
        "current_user": user,
    })


@router.get("/api/transactions-report")
async def transactions_report_api(request: Request):
    user = await get_current_user(request)
    if isinstance(user, RedirectResponse):
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    if user.get("role") != "admin" and "transactions_report" not in (user.get("allowed_pages_list") or []):
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                t.mttransactionsid,
                t.login,
                t.usdamount,
                t.transaction_type_name,
                t.original_deposit_owner,
                t.confirmation_time
            FROM transactions t
            JOIN accounts a ON a.accountid = t.vtigeraccountid
            WHERE t.confirmation_time::date >= '2026-03-01'
              AND t.confirmation_time::date <  '2026-04-01'
              AND t.transactionapproval = 'Approved'
              AND a.is_test_account = 0
            ORDER BY t.confirmation_time DESC
        """)
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    data = []
    for r in rows:
        data.append({
            "mttransactionsid":      r[0],
            "login":                 r[1],
            "usdamount":             float(r[2]) if r[2] is not None else None,
            "transaction_type_name": r[3],
            "original_deposit_owner": r[4],
            "confirmation_time":     r[5].isoformat() if r[5] is not None else None,
        })

    return JSONResponse(content={"rows": data, "total": len(data)})
