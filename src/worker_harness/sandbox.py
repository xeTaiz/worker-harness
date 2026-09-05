"""Launch lines for sandboxed fleet agents.

Every agent the harness starts runs under ``~/dotfiles/agent-sandbox``: no SSH
key, no agent forwarding, no tailscaled socket, no git credentials. The command
below is typed into a herdr pane by the harness, so it must be a valid shell
line and must reference the sandbox wrapper by absolute path — ``which -a omp``
resolves the raw mise install first, and `herdr agent start` would bypass the
sandbox entirely.
"""

from __future__ import annotations

import shlex
from typing import Literal

from .machines import Machine

Role = Literal["orchestrator", "pm", "task"]
ROLES: tuple[Role, ...] = ("orchestrator", "pm", "task")

# One path set for all three roles: task agents run fleet experiments, read
# shared data, and query the vault, so narrowing them further breaks the work.
# The pane's cwd is bound read-write unconditionally by the wrapper and must
# never be listed here.
ROLE_PATHS: tuple[str, ...] = (
    "~/Work",
    "~/mnt",
    "~/.omp",
    "~/.config/local-vault-production",
    "~/.config/pi/local-vault.token",
)

# The orchestrator never edits, runs commands, or spawns subagents; it routes.
# `write` survives any allowlist (xd:// devices ride on it) and is constrained
# by its system prompt instead.
ORCHESTRATOR_TOOLS = "read,grep,glob,todo,hub,web_search"

MISE_TOOL = "github:can1357/oh-my-pi"


def paths_file_content(role: Role, *, home: str) -> str:
    """Render the ``AGENT_SANDBOX_PATHS`` file for one role."""
    if role not in ROLES:
        raise ValueError(f"unknown role: {role}")
    home = home.rstrip("/")
    lines = [f"# worker-harness {role} sandbox paths"]
    lines += [path.replace("~", home, 1) if path.startswith("~/") else path for path in ROLE_PATHS]
    return "\n".join(lines) + "\n"


def launch_command(
    *,
    machine: Machine,
    role: Role,
    paths_file: str,
    session_id: str,
    token: str,
    base_url: str,
    prompt_file: str,
    add_dirs: list[str] | None = None,
) -> str:
    """Compose the shell line that starts one sandboxed agent in a pane."""
    if role not in ROLES:
        raise ValueError(f"unknown role: {role}")

    env = {
        "AGENT_SANDBOX_NO_GIT_CREDENTIALS": "1",
        "AGENT_SANDBOX_PATHS": paths_file,
        "AGENT_SANDBOX_SESSION_DIR": machine.session_dir(session_id),
        "WH_ORCHESTRATOR_URL": base_url.rstrip("/"),
        "WH_SESSION_ID": session_id,
        "WH_SESSION_TOKEN": token,
        "WH_SESSION_ROLE": role,
    }
    # Always override a task's inherited readonly setting when launching a PM.
    # Plugin registration, not the built-in tool allowlist, enforces this flag.
    env["LOCAL_VAULT_READONLY"] = "1" if role == "task" else "0"

    # Replacing the default prompt would strip the coding-agent workflow that pm
    # and task both depend on; only the orchestrator, which never opens a file,
    # gets a clean slate.
    prompt_flag = "--system-prompt" if role == "orchestrator" else "--append-system-prompt"

    argv = [
        f"{machine.home.rstrip('/')}/.local/bin/omp",
        prompt_flag,
        prompt_file,
    ]
    if role == "orchestrator":
        argv += ["--tools", ORCHESTRATOR_TOOLS]
    for directory in add_dirs or []:
        argv += ["--add-dir", directory]

    assignments = " ".join(f"{key}={shlex.quote(value)}" for key, value in env.items())
    return (
        f"exec env -u WH_PI_SESSION_ID -u WH_OPERATOR_TOKEN -u WH_OPERATOR_TOKEN_FILE "
        f"{assignments} {shlex.join(argv)}"
    )


__all__ = [
    "MISE_TOOL",
    "ORCHESTRATOR_TOOLS",
    "ROLES",
    "ROLE_PATHS",
    "Role",
    "launch_command",
    "paths_file_content",
]
