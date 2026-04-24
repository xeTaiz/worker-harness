"""Configuration loading for the orchestrator."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel


class SSHConfig(BaseModel):
    key_path: str = os.path.expanduser("~/.ssh/id_rsa")
    user: str = "root"
    connect_timeout: int = 10


class HeartbeatConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 12888
    offline_cutoff_seconds: int = 180  # mark worker offline if no heartbeat in 3min


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"


class Config(BaseModel):
    db_path: Path = Path("~/.config/worker-harness/db.sqlite").expanduser()
    ssh: SSHConfig = SSHConfig()
    heartbeat: HeartbeatConfig = HeartbeatConfig()
    logging: LoggingConfig = LoggingConfig()

    @classmethod
    def load(cls) -> Config:
        """Load config from environment variables and defaults."""
        return cls(
            db_path=Path(os.environ.get("WH_DB_PATH", "~/.config/worker-harness/db.sqlite")).expanduser(),
            ssh=SSHConfig(
                key_path=os.environ.get("WH_SSH_KEY", os.path.expanduser("~/.ssh/id_rsa")),
                user=os.environ.get("WH_SSH_USER", "root"),
            ),
            heartbeat=HeartbeatConfig(
                host=os.environ.get("WH_HB_HOST", "0.0.0.0"),
                port=int(os.environ.get("WH_HB_PORT", "12888")),
                offline_cutoff_seconds=int(os.environ.get("WH_OFFLINE_CUTOFF", "180")),
            ),
            logging=LoggingConfig(
                level=os.environ.get("WH_LOG_LEVEL", "INFO"),
            ),
        )
