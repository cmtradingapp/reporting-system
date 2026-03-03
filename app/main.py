from fastapi import FastAPI
from app.routes.report import router

app = FastAPI(title="Agent Performance Report")

app.include_router(router)
