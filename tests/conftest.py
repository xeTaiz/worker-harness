"""Hermetic defaults: tests never read the operator's fleet configuration."""

import os
import tempfile
from pathlib import Path

# Deliberately nonexistent: a missing inventory disables fleet orchestration.
_ABSENT = Path(tempfile.gettempdir()) / "wh-tests-absent-config"
os.environ.setdefault("WH_WEB_ORIGIN", "http://127.0.0.1:12889")
os.environ.setdefault("WH_MACHINES_PATH", str(_ABSENT / "machines.toml"))
os.environ.setdefault("WH_PROJECTS_PATH", str(_ABSENT / "projects.toml"))
