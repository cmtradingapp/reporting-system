from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse
from app.etl.fetch_and_store import run_transactions_etl, run_transactions_full_etl, run_bonus_transactions_etl, run_bonus_transactions_full_etl
from app.db.mssql_conn import _get_mssql_connection

router = APIRouter()


@router.post("/sync/transactions")
def sync_transactions(hours: int = 24):
    return run_transactions_etl(hours=hours)


@router.post("/sync/transactions/full")
def sync_transactions_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_transactions_full_etl)
    return {"status": "started"}


@router.post("/sync/bonus-transactions")
def sync_bonus_transactions(hours: int = 24):
    return run_bonus_transactions_etl(hours=hours)


@router.post("/sync/bonus-transactions/full")
def sync_bonus_transactions_full(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_bonus_transactions_full_etl)
    return {"status": "started"}


@router.get("/api/debug-bonus-sample")
def debug_bonus_sample():
    conn = _get_mssql_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT TOP 5
                    mttransactionsid, login, transactiontype, transaction_type_name,
                    transactionapproval, usdamount, deleted, server_id, confirmation_time
                FROM report.vtiger_mttransactions
                WHERE transaction_type_name IN ('FRF Commission', 'Bonus', 'FRF Commission Cancelled', 'BonusCancelled')
                ORDER BY mttransactionsid DESC
            """)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, [str(v) for v in r])) for r in cur.fetchall()]
            cur.execute("""
                SELECT COUNT(*) FROM report.vtiger_mttransactions
                WHERE transaction_type_name IN ('FRF Commission', 'Bonus', 'FRF Commission Cancelled', 'BonusCancelled')
            """)
            total = cur.fetchone()[0]
        return JSONResponse(content={"total_matching": total, "sample": rows})
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        conn.close()
