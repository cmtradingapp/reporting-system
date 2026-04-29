from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from app.db.redis_conn import (
    is_redis_healthy, get_last_update,
    get_all_open_positions_stats, get_all_account_stats,
    get_closed_volume_for_period, get_rates,
)
from app import cache
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import threading
import time as _time
import calendar

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

_TZ = ZoneInfo("Europe/Nicosia")

# Background cache for closed positions (expensive 7M key scan)
_closed_cache = {"data": None, "computed_at": None, "computing": False, "params": None}
_closed_lock = threading.Lock()


def _compute_closed_background(period_start_ts, period_end_ts):
    """Compute closed volume + today's PnL in background thread."""
    with _closed_lock:
        if _closed_cache["computing"]:
            return
        _closed_cache["computing"] = True
    try:
        result = get_closed_volume_for_period(period_start_ts, period_end_ts)
        result["computed_at"] = datetime.now(_TZ).isoformat(timespec="seconds")
        _closed_cache["data"] = result
        _closed_cache["computed_at"] = _time.time()
        _closed_cache["params"] = (period_start_ts, period_end_ts)
    except Exception as e:
        print(f"[redis-perf] closed stats error: {e}")
    finally:
        _closed_cache["computing"] = False


@router.get("/redis-perf", response_class=HTMLResponse)
async def redis_perf_page(request: Request):
    return templates.TemplateResponse("redis_perf.html", {"request": request})


@router.get("/api/redis-perf")
async def redis_perf_api(request: Request,
                         month: int = Query(None, ge=1, le=12),
                         year: int = Query(None, ge=2019, le=2030)):

    # Default to current month
    now = datetime.now(_TZ)
    if not year:
        year = now.year
    if not month:
        month = now.month

    # Period timestamps (UTC)
    period_start = datetime(year, month, 1, tzinfo=timezone.utc)
    last_day = calendar.monthrange(year, month)[1]
    period_end = datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc)
    period_start_ts = int(period_start.timestamp())
    period_end_ts = int(period_end.timestamp()) + 1  # exclusive

    _ck = f"redis_perf_v2:{year}:{month}"
    _hit = cache.get(_ck)
    if _hit is not None:
        return JSONResponse(content=_hit)

    if not is_redis_healthy():
        return JSONResponse(status_code=503, content={"detail": "Redis unavailable"})

    # Open positions (fast: ~25K keys) — volume filtered by period
    pos_stats = get_all_open_positions_stats(
        period_start_ts=period_start_ts,
        period_end_ts=period_end_ts,
    )
    login_floating = pos_stats.pop("_login_floating", {})

    # Account stats (fast: pipeline for position logins only)
    acct_stats = get_all_account_stats(login_floating=login_floating)

    # Closed positions (slow: 7M keys — use background cache)
    closed = _closed_cache.get("data")
    closed_age = None
    cached_params = _closed_cache.get("params")
    params_match = cached_params == (period_start_ts, period_end_ts)

    if closed and _closed_cache.get("computed_at"):
        closed_age = round(_time.time() - _closed_cache["computed_at"])

    if closed is None or not params_match or (closed_age and closed_age > 120):
        # Trigger background computation if stale, missing, or different period
        if not _closed_cache.get("computing"):
            t = threading.Thread(target=_compute_closed_background,
                                 args=(period_start_ts, period_end_ts), daemon=True)
            t.start()

    if closed and params_match:
        closed_result = dict(closed)
        closed_result["cache_age_seconds"] = closed_age
    else:
        closed_result = {"closed_volume": None, "closed_volume_count": None,
                         "total_pnl": None, "trade_count": None,
                         "total_commission": None, "total_swap": None,
                         "status": "computing" if _closed_cache.get("computing") else "unavailable"}

    # Combined open volume = open positions (in period) + closed positions (opened in period)
    open_vol = pos_stats.get("total_volume", 0) or 0
    closed_vol = closed_result.get("closed_volume") or 0
    combined_volume = round(open_vol + closed_vol, 2) if closed_result.get("closed_volume") is not None else None

    # Rates (fast: 13 keys)
    rates = get_rates()

    # Freshness
    freshness = get_last_update()

    result = {
        "period": {"year": year, "month": month, "label": f"{calendar.month_abbr[month]} {year}"},
        "open_positions": pos_stats,
        "accounts": acct_stats,
        "closed_today": closed_result,
        "combined_volume": combined_volume,
        "rates": rates,
        "data_freshness": freshness,
        "computed_at": datetime.now(_TZ).isoformat(timespec="seconds"),
    }

    cache.set(_ck, result, ttl=30)
    return JSONResponse(content=result)
