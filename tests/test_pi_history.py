from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from worker_harness import pi_history


def test_pi_package_root_requires_importable_package_layout(tmp_path):
    root = tmp_path / "package"
    (root / "dist").mkdir(parents=True)
    (root / "package.json").write_text("{}")
    (root / "dist" / "index.js").write_text("")
    cli = root / "dist" / "cli.js"
    cli.write_text("")
    assert pi_history._pi_package_root(cli) == root
    with pytest.raises(RuntimeError, match="cannot provide SessionManager"):
        pi_history._pi_package_root(tmp_path / "pi")


def test_list_session_history_runs_bounded_helper_and_validates_shape(monkeypatch, tmp_path):
    cwd = tmp_path / "repo"
    cwd.mkdir()
    package = tmp_path / "package"
    (package / "dist").mkdir(parents=True)
    (package / "package.json").write_text("{}")
    (package / "dist" / "index.js").write_text("")
    cli = package / "dist" / "cli.js"
    cli.write_text("")
    cli.chmod(0o755)
    bun = tmp_path / "bun"
    bun.write_text("")
    bun.chmod(0o755)
    captured = {}

    def executable(name, environment):
        return bun if name == "bun" else cli

    def run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0, json.dumps([{
            "id": "uuid", "cwd": str(cwd), "name": "Repo",
        }]), "")

    monkeypatch.setattr(pi_history, "_executable", executable)
    monkeypatch.setattr(pi_history, "_host_runtime", lambda: None)
    monkeypatch.setattr(pi_history.subprocess, "run", run)
    rows = pi_history.list_session_history(str(cwd))
    assert rows[0]["id"] == "uuid"
    assert captured["command"][2:5] == ["list", str(package), str(cwd)]
    assert captured["kwargs"]["timeout"] == 20


def test_resolve_requires_exact_helper_identity(monkeypatch):
    monkeypatch.setattr(pi_history, "_run_helper", lambda cwd, session_id: {"id": "other"})
    with pytest.raises(RuntimeError, match="exact session ID"):
        pi_history.resolve_session_history("/tmp", "wanted")


def test_helper_failure_is_bounded(monkeypatch, tmp_path):
    cwd = tmp_path / "repo"
    cwd.mkdir()
    monkeypatch.setattr(pi_history, "_executable", lambda *args: tmp_path / "executable")
    monkeypatch.setattr(pi_history, "_pi_package_root", lambda path: tmp_path)
    monkeypatch.setattr(pi_history, "_host_runtime", lambda: None)
    monkeypatch.setattr(
        pi_history.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 1, "", "x" * 3000),
    )
    with pytest.raises(RuntimeError) as error:
        pi_history.list_session_history(str(cwd))
    assert len(str(error.value)) < 2100
