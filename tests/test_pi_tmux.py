from __future__ import annotations

import contextlib
import os
import shlex
import subprocess
from pathlib import Path

import pytest

from worker_harness import pi_tmux


def completed(stdout: str = "", *, returncode: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess(["tmux"], returncode, stdout, stderr)


def test_is_immediate_tmux_requires_both_environment_values(monkeypatch):
    monkeypatch.delenv("TMUX", raising=False)
    monkeypatch.delenv("TMUX_PANE", raising=False)
    assert not pi_tmux.is_immediate_tmux()
    monkeypatch.setenv("TMUX", "/tmp/tmux,1,0")
    assert not pi_tmux.is_immediate_tmux()
    monkeypatch.setenv("TMUX_PANE", "%1")
    assert pi_tmux.is_immediate_tmux()


@pytest.mark.parametrize("session", ["work", "1", "$x", "$1\n"])
def test_picker_rejects_non_authoritative_tmux_session(monkeypatch, session):
    monkeypatch.setenv("TMUX", "/tmp/tmux,1,0")
    with pytest.raises(RuntimeError, match="tmux target session"):
        pi_tmux.open_or_focus_attachment_window({"id": "pi-id"}, session, "/dev/pts/1")


@pytest.mark.parametrize("client", ["", "-t", "bad\nclient", "bad\0client"])
def test_picker_rejects_invalid_client(monkeypatch, client):
    monkeypatch.setenv("TMUX", "/tmp/tmux,1,0")
    with pytest.raises(RuntimeError, match="tmux target client"):
        pi_tmux.open_or_focus_attachment_window({"id": "pi-id"}, "$1", client)


def test_find_reusable_window_requires_exact_owned_live_pane(monkeypatch):
    rows = "\n".join(
        (
            "%1\t@1\tpi-prefix\t1\t0\t101",
            "%2\t@2\tpi-id\t0\t0\t102",
            "%3\t@3\tpi-id\t1\t1\t103",
            "malformed",
            "%4\t@4\tpi-id\t1\t0\tnot-a-pid",
            "%5\t@5\tpi-id\t1\t0\t105",
        )
    )
    monkeypatch.setattr(pi_tmux, "_run_tmux", lambda *args, **kwargs: completed(rows))
    monkeypatch.setattr(pi_tmux, "_pid_is_live", lambda pid: pid == 105)
    assert pi_tmux._find_reusable_window("$1", "pi-id") == "@5"


def test_reuse_focuses_only_exact_invoking_client(monkeypatch):
    calls = []
    monkeypatch.setenv("TMUX", "/tmp/tmux,1,0")
    monkeypatch.setattr(pi_tmux, "_attachment_lock", lambda *args: contextlib.nullcontext())
    monkeypatch.setattr(pi_tmux, "_find_reusable_window", lambda *args: "@7")
    state_updates = []
    monkeypatch.setattr(
        pi_tmux,
        "_set_window_state",
        lambda *args: state_updates.append(args),
    )
    monkeypatch.setattr(pi_tmux, "_run_tmux", lambda args, **kwargs: calls.append(args) or completed())

    assert pi_tmux.open_or_focus_attachment_window(
        {"id": "exact-uuid", "name": "One"}, "$3", "/dev/pts/9"
    ) == "@7"
    assert state_updates == [("@7", "One", "disconnected")]
    assert calls == [["switch-client", "-c", "/dev/pts/9", "-t", "@7"]]


def test_new_window_quotes_child_and_sets_ownership_state(monkeypatch, tmp_path):
    calls = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TMUX", "/tmp/tmux,1,0")
    monkeypatch.setattr(pi_tmux, "_attachment_lock", lambda *args: contextlib.nullcontext())
    monkeypatch.setattr(pi_tmux, "_find_reusable_window", lambda *args: None)
    monkeypatch.setattr(pi_tmux.shutil, "which", lambda name: "/opt/bin/wh" if name == "wh" else None)

    def run(args, **kwargs):
        calls.append(args)
        if args[0] == "new-window":
            return completed("@8\t%12\n")
        return completed()

    monkeypatch.setattr(pi_tmux, "_run_tmux", run)
    window = pi_tmux.open_or_focus_attachment_window(
        {"id": "uuid-1", "name": "name with ' quote", "state": "working"},
        "$2",
        "client-1",
    )

    assert window == "@8"
    create = calls[0]
    assert create[:7] == ["new-window", "-d", "-P", "-F", "#{window_id}\t#{pane_id}", "-t", "$2:"]
    assert create[create.index("-c") + 1] == str(tmp_path)
    gated = shlex.split(create[-1])
    assert gated[:4] == [
        "sh",
        "-c",
        'tmux wait-for "$1"; shift; exec "$@"',
        "wh-pi-child",
    ]
    assert gated[4].startswith("wh-pi-attach-")
    assert gated[5:] == [
        "/opt/bin/wh",
        "pi",
        "attach",
        "--tmux-child",
        "--session-name=name with ' quote",
        "--session-state=working",
        "uuid-1",
    ]
    assert ["set-option", "-p", "-t", "%12", "@wh_pi_owned", "1"] in calls
    assert ["set-option", "-p", "-t", "%12", "@wh_pi_attach_session", "uuid-1"] in calls
    assert ["set-option", "-w", "-t", "@8", "@wh_pi_color", pi_tmux.WORKING_COLOR] in calls
    assert ["rename-window", "-t", "@8", "π ● name with ' quote"] in calls
    assert calls[-2] == ["wait-for", "-S", gated[4]]
    assert calls[-1] == ["switch-client", "-c", "client-1", "-t", "@8"]


def test_picker_rejects_option_like_pi_id(monkeypatch):
    monkeypatch.setenv("TMUX", "/tmp/tmux,1,0")
    with pytest.raises(RuntimeError, match="Pi session ID"):
        pi_tmux.open_or_focus_attachment_window({"id": "--bad"}, "$1", "client")


def test_invalid_new_window_locator_is_rejected(monkeypatch):
    monkeypatch.setenv("TMUX", "/tmp/tmux,1,0")
    monkeypatch.setattr(pi_tmux, "_attachment_lock", lambda *args: contextlib.nullcontext())
    monkeypatch.setattr(pi_tmux, "_find_reusable_window", lambda *args: None)
    monkeypatch.setattr(pi_tmux, "_run_tmux", lambda *args, **kwargs: completed("bad\n"))
    with pytest.raises(RuntimeError, match="invalid window locator"):
        pi_tmux.open_or_focus_attachment_window({"id": "uuid"}, "$1", "client")


def test_current_attachment_window_requires_owned_marker(monkeypatch):
    monkeypatch.setenv("TMUX_PANE", "%4")
    monkeypatch.setattr(pi_tmux, "_run_tmux", lambda *args, **kwargs: completed("@9\t1\n"))
    assert pi_tmux.current_attachment_window() == "@9"
    monkeypatch.setattr(pi_tmux, "_run_tmux", lambda *args, **kwargs: completed("@9\t\n"))
    assert pi_tmux.current_attachment_window() is None


def test_update_attachment_window_sets_error_title_and_color(monkeypatch):
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        if args[0] == "show-options":
            return completed("1\n")
        return completed()

    monkeypatch.setattr(pi_tmux, "_run_tmux", run)
    assert pi_tmux.update_attachment_window("Broken\nPi", "runtime_error", "@2")
    assert ["set-option", "-w", "-t", "@2", "@wh_pi_state", "error"] in calls
    assert ["set-option", "-w", "-t", "@2", "@wh_pi_color", pi_tmux.ERROR_COLOR] in calls
    assert ["rename-window", "-t", "@2", "π ! Broken Pi"] in calls


def test_update_attachment_window_refuses_unowned_window(monkeypatch):
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        return completed(returncode=1)

    monkeypatch.setattr(pi_tmux, "_run_tmux", run)
    assert not pi_tmux.update_attachment_window("Do not rename", "idle", "@3")
    assert calls == [["show-options", "-wv", "-t", "@3", "@wh_pi_owned"]]


def test_runtime_lock_directory_and_file_are_private(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("TMUX", "/tmp/tmux,1,0")
    with pi_tmux._attachment_lock("$1", "uuid"):
        locks = list((tmp_path / "worker-harness" / "tmux-attachments" / "locks").iterdir())
        assert len(locks) == 1
        assert (locks[0].stat().st_mode & 0o777) == 0o600
    assert (tmp_path / "worker-harness" / "tmux-attachments").stat().st_mode & 0o777 == 0o700
