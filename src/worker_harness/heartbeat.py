"""FastAPI-based HTTP server for worker heartbeats."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, status
from pydantic import ValidationError

from .db import Database
from .models import WorkerRegistration

log = logging.getLogger("heartbeat-server")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: already connected by the caller
    yield
    # Shutdown: caller handles db.close()


def create_app(db: Database) -> FastAPI:
    app = FastAPI(title="Worker Harness Heartbeat API", lifespan=lifespan)

    @app.post("/register")
    async def register(reg: WorkerRegistration):
        """
        Full registration or heartbeat from a worker.
        Workers send this on startup and every N seconds thereafter.
        """
        try:
            worker = await db.upsert_worker(reg)
            log.info(
                f"Worker registered/updated: {worker.name} "
                f"(id={worker.id}, ip={worker.zerotier_ip}, gpus={worker.gpu_count})"
            )
            return {"status": "ok", "worker_id": worker.id}
        except ValidationError as e:
            log.error(f"Invalid registration payload: {e}")
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=str(e))
        except Exception as e:
            log.error(f"Registration failed: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=str(e))

    @app.get("/health")
    async def health():
        return {"status": "healthy", "ts": datetime.now(timezone.utc).isoformat()}

    return app


async def run_heartbeat_server(
    db: Database,
    host: str = "0.0.0.0",
    port: int = 12888,
) -> None:
    """Run the heartbeat HTTP server using uvicorn."""
    import uvicorn
    app = create_app(db)
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()
