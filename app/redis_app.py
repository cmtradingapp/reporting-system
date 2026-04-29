"""Standalone FastAPI app for Redis Performance page — no PostgreSQL required."""
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from app.routes.redis_perf import router as redis_perf_router

app = FastAPI(title="Redis Performance")
app.add_middleware(GZipMiddleware, minimum_size=500)
app.include_router(redis_perf_router)
