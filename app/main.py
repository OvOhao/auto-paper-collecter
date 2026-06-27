"""FastAPI entrypoint. Serves the API + the static dashboard, starts the scheduler."""
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from sqlalchemy import text

from .db import Base, engine
from . import models  # noqa: F401  (register tables)
from .api import router
from .scheduler import start_scheduler

Base.metadata.create_all(bind=engine)


def _migrate():
    """Add columns introduced after a DB was first created (SQLite-safe, idempotent)."""
    cols = [("saved_items", "feedback", "VARCHAR DEFAULT ''")]
    with engine.begin() as conn:
        for table, col, decl in cols:
            try:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {decl}"))
            except Exception:
                pass  # column already exists


_migrate()

app = FastAPI(title="ScholarPulse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

app.include_router(router)


@app.on_event("startup")
def _startup():
    start_scheduler()


@app.get("/health")
def health():
    return {"ok": True}


# serve the dashboard (frontend/) at root, if present
_static = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.isdir(_static):
    app.mount("/", StaticFiles(directory=_static, html=True), name="static")
