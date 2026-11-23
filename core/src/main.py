from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text

from container import Container, build_container
from shared.config.settings import get_settings

log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    container = build_container(settings)
    app.state.container = container
    await container.start()
    try:
        yield
    finally:
        await container.stop()
        app.state.container = None


app = FastAPI(title="PythonEE Core", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
async def ready() -> JSONResponse:
    container: Container | None = getattr(app.state, "container", None)
    if container is None:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "reason": "container not initialized"},
        )
    try:
        async with container.session_factory() as session:
            await session.execute(text("SELECT 1"))
    except Exception as exc:
        log.warning("readiness_db_check_failed", error=str(exc))
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "reason": "database"},
        )
    return JSONResponse(status_code=200, content={"status": "ready"})
