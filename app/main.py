from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.routes.report import router
from app.routes.users import router as users_router
from app.db.postgres_conn import ensure_table


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_table()
    yield


app = FastAPI(title="Agent Performance Report", lifespan=lifespan)

app.include_router(router)
app.include_router(users_router)
