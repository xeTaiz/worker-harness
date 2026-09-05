"""Lifecycle management for the orchestrator, project managers, and task agents."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
import logging
import os
import secrets
import re
import shlex
import time
from pathlib import Path, PurePosixPath
from typing import Any, AsyncIterator, Literal
from uuid import uuid4

from . import herdr_host, sandbox
from .herdr_host import HerdrError, HerdrUnavailable, ShimError
from .lanes import LaneTimeout
from .machines import Machine
from .models import PiSession, PiSessionCommand, PiSessionEvent, PiSessionState, PiSessionType
from .projects import Project

log = logging.getLogger(__name__)

FLEET_SOURCE = "worker-harness:fleet"
_DEFAULT_FLEET_BASE_URL = "http://orchestrator.hs.d0me.xyz:12889"
_DEFAULT_ORCHESTRATOR_REPO = "~/Work/agent-orchestrator"
_REMOTE_ERRORS = (HerdrError, HerdrUnavailable, ShimError, LaneTimeout, OSError)
_STARTING_STATE = PiSessionState.STARTING
_TERMINAL_PM_STATES = {PiSessionState.STOPPED, PiSessionState.FAILED}

@dataclass(frozen=True)
class _LaunchSpec:
    role: Literal["orchestrator", "pm", "task"]
    machine: Machine
    cwd: str
    project_name: str
    repo: str
    remote: str
    base_branch: str
    name: str
    label: str
    session_type: PiSessionType
    prompt_path: Path
    add_dirs: tuple[str, ...]


def orchestrator_repo() -> Path:
    """Return the service-local prompt/config source, mounted into its container."""
    override = os.environ.get("WH_ORCHESTRATOR_REPO", "").strip()
    return Path(override or _DEFAULT_ORCHESTRATOR_REPO).expanduser()


def orchestrator_machine_name() -> str:
    """Return the machine key used to host the global orchestrator."""
    return os.environ.get("WH_ORCHESTRATOR_MACHINE", "").strip() or "desktop"


class WorktreeError(Exception):
    """A task worktree could not be created or safely removed."""


def herdr_agent_name(name: str) -> str:
    """Coerce a fleet session name into herdr's agent-name grammar.

    herdr requires ``^[a-z][a-z0-9_-]{0,31}$``; fleet names like
    ``task-worker-harness-feat-workers-list-json`` are longer than that. The
    label is cosmetic, so trim rather than fail the launch.
    """
    cleaned = re.sub(r"[^a-z0-9_-]+", "-", name.lower()).strip("-")
    if not cleaned or not cleaned[0].isalpha():
        cleaned = f"a{cleaned}"
    return cleaned[:32].rstrip("-_") or "agent"


def fleet_base_url() -> str:
    """Return the URL embedded in launched fleet-agent environments."""
    return os.environ.get("WH_FLEET_BASE_URL", _DEFAULT_FLEET_BASE_URL).strip() or _DEFAULT_FLEET_BASE_URL


async def create_worktree(
    machine: Machine,
    repo: str,
    branch: str,
    base: str = "main",
) -> tuple[str, str]:
    """Create a uniquely named branch and worktree, trying at most five names."""
    repo_name = repo.rstrip("/").rsplit("/", 1)[-1]
    last_error: ShimError | None = None
    for attempt in range(1, 6):
        actual_branch = branch if attempt == 1 else f"{branch}-{attempt}"
        branch_slug = actual_branch.replace("/", "-")
        path = machine.worktree_dir(repo_name, branch_slug)
        try:
            await herdr_host.shim(
                machine,
                "worktree-add",
                repo,
                actual_branch,
                base,
                path,
            )
        except ShimError as exc:
            last_error = exc
            log.info("Could not create branch %s on %s: %s", actual_branch, machine.name, exc)
            continue
        return path, actual_branch

    raise WorktreeError(f"could not create worktree for {branch!r} after 5 attempts") from last_error


async def destroy_worktree(machine: Machine, repo: str, path: str, branch: str) -> None:
    """Remove a worktree and then its now-unchecked-out local branch."""
    try:
        await herdr_host.shim(machine, "worktree-remove", repo, path)
    except ShimError as exc:
        raise WorktreeError(f"could not remove worktree {path}: {exc}") from exc

    try:
        await herdr_host.shim(machine, "branch-delete", repo, branch)
    except ShimError as exc:
        # The checkout is already gone. A leftover branch is harmless and can be
        # cleaned manually; failing teardown here would misrepresent the result.
        log.error("Could not delete branch %s on %s: %s", branch, machine.name, exc)


class Orchestrator:
    """Own durable fleet session state and project/task resource lifecycles."""

    def __init__(
        self,
        db: Any,
        machines: dict[str, Machine],
        projects: dict[str, Project],
        *,
        base_url: str | None = None,
    ) -> None:
        self.db = db
        self.machines = machines
        self.projects = projects
        self.base_url = (base_url or fleet_base_url()).rstrip("/")
        self._orchestrator_lock = asyncio.Lock()
        self._pm_locks = {name: asyncio.Lock() for name in projects}

    def _launch_spec(
        self,
        *,
        role: Literal["orchestrator", "pm", "task"],
        machine: Machine,
        cwd: str,
        project_name: str = "",
        repo: str = "",
        remote: str = "",
        base_branch: str = "main",
        branch: str = "",
    ) -> _LaunchSpec:
        if role == "orchestrator":
            return _LaunchSpec(
                role=role,
                machine=machine,
                cwd=cwd,
                project_name="",
                repo=cwd,
                remote="",
                base_branch="",
                name="orchestrator",
                label="orchestrator",
                session_type=PiSessionType.GLOBAL_ROUTER,
                prompt_path=self._prompt_path(role),
                add_dirs=(),
            )
        if not project_name or not repo:
            raise ValueError(f"{role} launch requires project name and repository")

        branch_label = branch.replace("/", "-")
        name = f"pm-{project_name}" if role == "pm" else f"task-{project_name}-{branch_label}"
        return _LaunchSpec(
            role=role,
            machine=machine,
            cwd=cwd,
            project_name=project_name,
            repo=repo,
            remote=remote,
            base_branch=base_branch,
            name=name,
            label=name,
            session_type=PiSessionType.INTERACTIVE,
            prompt_path=self._prompt_path(role),
            add_dirs=(repo,) if role == "task" else (),
        )

    async def ensure_orchestrator(self) -> PiSession:
        async with self._orchestrator_lock:
            return await self._ensure_orchestrator()

    async def _ensure_orchestrator(self) -> PiSession:
        sessions = await self.db.list_pi_sessions_by_role("orchestrator")
        for session in sessions:
            if session.state not in _TERMINAL_PM_STATES:
                if session.state == _STARTING_STATE:
                    return await self._wait_for_bridge(self._session_machine(session), session)
                return session

        resume_path = next(
            (
                str(session.meta.get("resume_path"))
                for session in sessions
                if session.meta.get("resume_path")
            ),
            None,
        )
        machine_name = orchestrator_machine_name()
        try:
            machine = self.machines[machine_name]
        except KeyError as exc:
            raise KeyError(f"orchestrator machine {machine_name!r} is not configured") from exc
        repo = os.environ.get("WH_ORCHESTRATOR_CWD", "").strip()
        if not repo:
            repo = f"{machine.home}/Work/agent-orchestrator"
        if not repo.startswith("/"):
            raise ValueError("WH_ORCHESTRATOR_CWD must be an absolute remote path")
        return await self._launch(
            role="orchestrator",
            machine=machine,
            cwd=repo,
            project_name="",
            repo=repo,
            remote="",
            base_branch="",
            resume_path=resume_path,
        )

    async def ensure_pm(self, project_name: str) -> PiSession:
        async with self.pm_session(project_name) as manager:
            return manager

    @asynccontextmanager
    async def pm_session(self, project_name: str) -> AsyncIterator[PiSession]:
        """Keep dispatch and idle retirement serialized for one manager."""
        self._project(project_name)
        async with self._pm_locks[project_name]:
            yield await self._ensure_pm(project_name)

    async def _ensure_pm(self, project_name: str) -> PiSession:
        project = self._project(project_name)
        sessions = await self.db.list_pi_sessions_by_role("pm")
        project_sessions = [
            session for session in sessions if session.meta.get("project") == project_name
        ]
        for session in project_sessions:
            if session.state not in _TERMINAL_PM_STATES:
                if session.state == _STARTING_STATE:
                    return await self._wait_for_bridge(self._session_machine(session), session)
                return session

        resume_path = next(
            (
                str(session.meta.get("resume_path"))
                for session in project_sessions
                if session.meta.get("resume_path")
            ),
            None,
        )
        manager = await self._launch(
            role="pm",
            machine=self._machine(project),
            cwd=project.repo,
            project_name=project.name,
            repo=project.repo,
            remote=project.remote,
            base_branch=project.base_branch,
            resume_path=resume_path,
        )
        for previous in project_sessions:
            await self.db.reparent_pi_session_children(previous.id, manager.id)
        return manager

    async def launch_pm(self, project: Project) -> PiSession:
        return await self.ensure_pm(project.name)

    async def launch_task(
        self,
        project: Project,
        *,
        branch: str,
        briefing: str,
        parent_session_id: str,
    ) -> PiSession:
        async with self._pm_locks[project.name]:
            return await self._launch_task(
                project, branch=branch, briefing=briefing, parent_session_id=parent_session_id,
            )

    async def _launch_task(
        self,
        project: Project,
        *,
        branch: str,
        briefing: str,
        parent_session_id: str,
    ) -> PiSession:
        parent = await self.db.get_pi_session(parent_session_id)
        if (
            parent is None
            or parent.role != "pm"
            or parent.meta.get("project") != project.name
            or parent.state in _TERMINAL_PM_STATES
        ):
            raise WorktreeError("task parent must be the active manager of its project")
        return await self._launch(
            role="task",
            machine=self._machine(project),
            cwd=project.repo,
            project_name=project.name,
            repo=project.repo,
            remote=project.remote,
            base_branch=project.base_branch,
            branch=branch,
            briefing=briefing,
            parent_session_id=parent_session_id,
        )

    async def _launch(
        self,
        *,
        role: Literal["orchestrator", "pm", "task"],
        machine: Machine,
        cwd: str,
        project_name: str,
        repo: str,
        remote: str,
        base_branch: str = "main",
        branch: str = "",
        briefing: str = "",
        parent_session_id: str | None = None,
        resume_path: str | None = None,
    ) -> PiSession:
        if resume_path:
            transcript = PurePosixPath(resume_path)
            root = PurePosixPath(machine.home) / ".omp/agent/sessions"
            if (
                not transcript.is_relative_to(root)
                or ".." in transcript.parts
                or transcript.suffix != ".jsonl"
            ):
                raise ValueError("resume path must be a transcript under the target machine's agent sessions")
        spec = self._launch_spec(
            role=role,
            machine=machine,
            cwd=cwd,
            project_name=project_name,
            repo=repo,
            remote=remote,
            base_branch=base_branch,
            branch=branch,
        )
        if role == "orchestrator":
            parent_session_id = None
            briefing = ""
        session_id = str(uuid4())
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        worktree = ""
        actual_branch = ""

        if role == "task":
            try:
                worktree, actual_branch = await create_worktree(
                    machine, repo, branch, base_branch,
                )
            except WorktreeError as exc:
                session = self._new_session(
                    spec=spec,
                    session_id=session_id,
                    token_hash=token_hash,
                    parent_session_id=parent_session_id,
                    briefing=briefing,
                    worktree="",
                    branch=branch,
                    workspace=None,
                )
                await self.db.insert_pi_session(session)
                await self._mark_failed(session, exc, event_type="launch-failed")
                raise
            spec = self._launch_spec(
                role=role,
                machine=machine,
                cwd=worktree,
                project_name=project_name,
                repo=repo,
                remote=remote,
                base_branch=base_branch,
                branch=actual_branch,
            )

        try:
            workspace = await herdr_host.workspace_create(
                machine, cwd=spec.cwd, label=spec.label,
            )
        except _REMOTE_ERRORS as exc:
            session = self._new_session(
                spec=spec,
                session_id=session_id,
                token_hash=token_hash,
                parent_session_id=parent_session_id,
                briefing=briefing,
                worktree=worktree,
                branch=actual_branch,
                workspace=None,
            )
            await self.db.insert_pi_session(session)
            await self._mark_failed(session, exc, event_type="launch-failed")
            if worktree:
                await self._cleanup_failed_worktree(session)
            raise

        session = self._new_session(
            spec=spec,
            session_id=session_id,
            token_hash=token_hash,
            parent_session_id=parent_session_id,
            briefing=briefing,
            worktree=worktree,
            branch=actual_branch,
            workspace=workspace,
            resume_path=resume_path,
        )
        await self.db.insert_pi_session(session)

        try:
            prompt = spec.prompt_path.read_text()
        except (OSError, UnicodeError) as exc:
            error = RuntimeError(f"cannot read required {role} prompt: {spec.prompt_path}")
            await self._mark_failed(session, error, event_type="launch-failed")
            await self._close_after_failed_launch(machine, session)
            if worktree:
                await self._cleanup_failed_worktree(session)
            raise error from exc
        if not prompt.strip():
            error = RuntimeError(f"required {role} prompt is empty: {spec.prompt_path}")
            await self._mark_failed(session, error, event_type="launch-failed")
            await self._close_after_failed_launch(machine, session)
            if worktree:
                await self._cleanup_failed_worktree(session)
            raise error

        paths_content = sandbox.paths_file_content(role, home=machine.home)
        command = sandbox.launch_command(
            machine=machine,
            role=role,
            paths_file=session.meta["paths_file"],
            session_id=session.id,
            token=token,
            base_url=self.base_url,
            prompt_file=session.meta["prompt_file"],
            add_dirs=list(spec.add_dirs) or None,
        )
        if resume_path:
            command += f" --resume {shlex.quote(resume_path)}"

        launch_attempted = False
        try:
            await herdr_host.write_file(machine, session.meta["paths_file"], paths_content)
            await herdr_host.write_file(machine, session.meta["prompt_file"], prompt)
            launch_file = f"{session.meta['cache_dir']}/launch.sh"
            await herdr_host.write_file(machine, launch_file, command + "\n")
            # A transport failure can occur after execution: from this point a
            # checkout may contain agent work and must never be auto-destroyed.
            launch_attempted = True
            await herdr_host.pane_run(
                machine, session.meta["pane_id"], f"exec /bin/sh {shlex.quote(launch_file)}",
            )
            if role == "task":
                await self._enqueue_briefing(session, briefing)
            # Cosmetic only: herdr's sidebar identity. wh's report_agent
            # projection is the authoritative state, and a sandboxed agent is
            # frequently undetectable to herdr in the first place, so no rename
            # failure may abort a launch.
            try:
                await herdr_host.agent_rename(
                    machine, session.meta["pane_id"], herdr_agent_name(session.name),
                )
            except HerdrError as exc:
                log.warning(
                    "herdr rejected the agent name for session %s (%s); continuing by pane id",
                    session.id, exc.code,
                )
                await self._record_event(
                    session.id,
                    "herdr-error",
                    {"operation": "agent-rename", "code": exc.code, "detail": str(exc)},
                )
            await self.report_state(session, "working")
            await herdr_host.report_metadata(
                machine,
                session.meta["pane_id"],
                source=FLEET_SOURCE,
                agent=herdr_agent_name(session.name),
                display_agent=f"π {session.name}",
            )
        except (Exception, asyncio.CancelledError) as exc:
            await self._mark_failed(session, exc, event_type="launch-failed")
            await self._close_after_failed_launch(machine, session)
            if worktree and not launch_attempted:
                await self._cleanup_failed_worktree(session)
            raise
        try:
            return await self._wait_for_bridge(machine, session)
        except asyncio.CancelledError as exc:
            await self._mark_failed(session, exc, event_type="launch-failed")
            await self._close_after_failed_launch(machine, session)
            raise


    def _new_session(
        self,
        *,
        spec: _LaunchSpec,
        session_id: str,
        token_hash: str,
        parent_session_id: str | None,
        briefing: str,
        worktree: str,
        branch: str,
        workspace: dict[str, Any] | None,
        resume_path: str | None = None,
    ) -> PiSession:
        cache_dir = spec.machine.session_dir(session_id)
        meta = {
            "machine": spec.machine.name,
            "workspace_id": (workspace or {}).get("workspace_id") or "",
            "pane_id": (workspace or {}).get("pane_id") or "",
            "project": spec.project_name,
            "repo": spec.repo,
            "remote": spec.remote,
            "worktree": worktree,
            "branch": branch,
            "resume_path": resume_path or "",
            "pr_url": "",
            "cache_dir": cache_dir,
            "paths_file": f"{cache_dir}/sandbox-paths",
            "prompt_file": f"{cache_dir}/{spec.role}-prompt.md",
            "pane_sequence": 0,
        }
        now = int(time.time())
        return PiSession(
            id=session_id,
            parent_session_id=parent_session_id,
            session_type=spec.session_type,
            role=spec.role,
            state=_STARTING_STATE,
            task=briefing,
            name=spec.name,
            cwd=spec.cwd,
            host=spec.machine.name,
            agent="omp",
            token_hash=token_hash,
            meta=meta,
            created_at=now,
            updated_at=now,
        )

    async def _wait_for_bridge(self, machine: Machine, session: PiSession) -> PiSession:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 60.0
        while loop.time() < deadline:
            current = await self.db.get_pi_session(session.id)
            if current is not None and current.state in _TERMINAL_PM_STATES:
                error = RuntimeError(current.detail or f"agent session {session.id} stopped during launch")
                await self._close_after_failed_launch(machine, current)
                raise error
            if current is not None and current.bridge_incarnation:
                return current
            await asyncio.sleep(0.5)

        pane_output = ""
        try:
            pane_output = await herdr_host.pane_read(
                machine,
                session.meta["pane_id"],
                source="recent-unwrapped",
                lines=120,
            )
        except _REMOTE_ERRORS as exc:
            log.error("Could not read failed launch pane for %s: %s", session.id, exc)
            pane_output = f"<pane read failed: {exc}>"
            await self._record_event(
                session.id,
                "herdr-error",
                {"operation": "pane-read", "detail": str(exc)},
            )

        timeout_error = TimeoutError(f"agent session {session.id} did not register within 60 seconds")
        session.state = PiSessionState.FAILED
        session.detail = str(timeout_error)
        session.updated_at = int(time.time())
        await self.db.finish_pi_session(session.id, session.state, session.detail)
        await self._close_after_failed_launch(machine, session)
        await self._record_event(
            session.id,
            "launch-failed",
            {"detail": str(timeout_error), "pane_output": pane_output},
        )
        raise timeout_error

    async def _enqueue_briefing(self, session: PiSession, briefing: str) -> None:
        now = int(time.time())
        command = PiSessionCommand(
            session_id=session.id,
            kind="prompt",
            message=briefing,
            deliver_as="steer",
            created_at=now,
        )
        await self.db.enqueue_pi_session_command(command)
        await self._record_event(
            session.id,
            "prompt-queued",
            {"command_id": command.id, "deliver_as": "steer"},
        )

    async def teardown_task(self, session: PiSession, *, force: bool = False) -> None:
        if session.role != "task":
            raise WorktreeError(f"session {session.id} is not a task session")
        if not session.meta.get("pr_url") and not force:
            raise WorktreeError(
                f"refusing to destroy task session {session.id} without a PR; pass force=True to override"
            )

        machine = self._session_machine(session)
        pane_id = str(session.meta.get("pane_id") or "")
        if pane_id:
            try:
                await herdr_host.pane_close(machine, pane_id)
            except _REMOTE_ERRORS as exc:
                await self._remote_cleanup_error(session, "pane-close", exc)
                raise WorktreeError("refusing to remove a worktree while its pane may still run") from exc
            try:
                sequence = await self._next_sequence(session)
                await herdr_host.release_agent(
                    machine,
                    pane_id,
                    source=FLEET_SOURCE,
                    agent=herdr_agent_name(session.name),
                    sequence=sequence,
                )
            except _REMOTE_ERRORS as exc:
                await self._remote_cleanup_error(session, "release-agent", exc)

        try:
            await destroy_worktree(
                machine,
                str(session.meta["repo"]),
                str(session.meta["worktree"]),
                str(session.meta["branch"]),
            )
        except WorktreeError as exc:
            await self._mark_failed(session, exc, event_type="teardown-failed")
            raise

        try:
            await herdr_host.remove_file(machine, str(session.meta["cache_dir"]))
        except _REMOTE_ERRORS as exc:
            await self._remote_cleanup_error(session, "remove-cache", exc)

        session.state = PiSessionState.STOPPED
        session.detail = ""
        session.updated_at = int(time.time())
        await self.db.finish_pi_session(session.id, session.state, session.detail)
        await self._record_event(session.id, "stopped", {"force": force})

    async def stop_session(self, session: PiSession) -> None:
        machine = self._session_machine(session)
        pane_id = str(session.meta.get("pane_id") or "")
        if pane_id:
            try:
                sequence = await self._next_sequence(session)
                await herdr_host.release_agent(
                    machine,
                    pane_id,
                    source=FLEET_SOURCE,
                    agent=herdr_agent_name(session.name),
                    sequence=sequence,
                )
            except _REMOTE_ERRORS as exc:
                await self._remote_cleanup_error(session, "release-agent", exc)
            try:
                await herdr_host.pane_close(machine, pane_id)
            except _REMOTE_ERRORS as exc:
                await self._remote_cleanup_error(session, "pane-close", exc)
                raise
        try:
            await herdr_host.remove_file(machine, str(session.meta.get("cache_dir") or ""))
        except _REMOTE_ERRORS as exc:
            await self._remote_cleanup_error(session, "remove-cache", exc)

        session.state = PiSessionState.STOPPED
        session.updated_at = int(time.time())
        await self.db.finish_pi_session(session.id, session.state, "")
        await self._record_event(session.id, "stopped", {})

    async def poll(self) -> None:
        """Project durable bridge state and retire idle managers without live children."""
        now = int(time.time())
        idle_seconds = int(os.environ.get("WH_PM_IDLE_SECONDS", "1800"))
        tasks = await self.db.list_pi_sessions_by_role("task")
        sessions = [
            *await self.db.list_pi_sessions_by_role("orchestrator"),
            *await self.db.list_pi_sessions_by_role("pm"),
            *tasks,
        ]
        for session in sessions:
            if session.state in _TERMINAL_PM_STATES or not session.meta.get("pane_id"):
                continue
            try:
                if (
                    session.role == "pm"
                    and session.state == PiSessionState.IDLE
                    and idle_seconds > 0
                    and now - session.updated_at >= idle_seconds
                    and not session.has_pending_messages
                    and not any(
                        task.parent_session_id == session.id
                        and task.state not in _TERMINAL_PM_STATES
                        for task in tasks
                    )
                ):
                    async with self._pm_locks[str(session.meta["project"])]:
                        current = await self.db.get_pi_session(session.id)
                        current_tasks = await self.db.list_pi_sessions_by_role("task")
                        if (
                            current is not None
                            and current.state == PiSessionState.IDLE
                            and not current.has_pending_messages
                            and now - current.updated_at >= idle_seconds
                            and not any(
                                task.parent_session_id == session.id
                                and task.state not in _TERMINAL_PM_STATES
                                for task in current_tasks
                            )
                        ):
                            await self.stop_session(current)
                    continue
                state = "idle" if session.state == PiSessionState.IDLE else "working"
                await self.report_state(session, state, message=session.question or session.detail)
            except Exception:
                log.exception("Could not poll fleet session %s", session.id)


    async def report_state(self, session: PiSession, state: str, *, message: str = "") -> None:
        machine = self._session_machine(session)
        sequence = await self._next_sequence(session)
        try:
            await herdr_host.report_agent(
                machine,
                str(session.meta["pane_id"]),
                state,
                source=FLEET_SOURCE,
                agent=herdr_agent_name(session.name),
                sequence=sequence,
                message=message,
                agent_session_path=str(session.meta.get("resume_path") or ""),
            )
        except _REMOTE_ERRORS as exc:
            log.error("Could not report %s state for session %s: %s", state, session.id, exc)
            await self._record_event(
                session.id,
                "herdr-error",
                {"operation": "report-state", "state": state, "detail": str(exc)},
            )
            raise

    async def _next_sequence(self, session: PiSession) -> int:
        return await self.db.next_pi_session_pane_sequence(session.id)

    async def _mark_failed(
        self,
        session: PiSession,
        error: BaseException,
        *,
        event_type: str,
    ) -> None:
        log.error("Session %s %s: %s", session.id, event_type, error)
        session.state = PiSessionState.FAILED
        session.detail = str(error)
        session.updated_at = int(time.time())
        await self.db.finish_pi_session(session.id, session.state, session.detail)
        await self._record_event(
            session.id,
            event_type,
            {"detail": str(error), "error_type": type(error).__name__},
        )

    async def _record_event(self, session_id: str, event_type: str, payload: dict[str, Any]) -> None:
        await self.db.insert_pi_session_event(
            PiSessionEvent(
                session_id=session_id,
                event_type=event_type,
                payload=payload,
                created_at=int(time.time()),
            )
        )

    async def _remote_cleanup_error(
        self,
        session: PiSession,
        operation: str,
        error: BaseException,
    ) -> None:
        log.error("Remote %s failed for session %s: %s", operation, session.id, error)
        await self._record_event(
            session.id,
            "herdr-error",
            {"operation": operation, "detail": str(error)},
        )

    async def _close_after_failed_launch(self, machine: Machine, session: PiSession) -> None:
        pane_id = str(session.meta.get("pane_id") or "")
        if pane_id:
            try:
                await herdr_host.pane_close(machine, pane_id)
            except _REMOTE_ERRORS as exc:
                await self._remote_cleanup_error(session, "pane-close", exc)
        try:
            await herdr_host.remove_file(machine, str(session.meta.get("cache_dir") or ""))
        except _REMOTE_ERRORS as exc:
            await self._remote_cleanup_error(session, "remove-cache", exc)

    async def _cleanup_failed_worktree(self, session: PiSession) -> None:
        worktree = str(session.meta.get("worktree") or "")
        branch = str(session.meta.get("branch") or "")
        if not worktree or not branch:
            return
        machine = self._session_machine(session)
        try:
            await destroy_worktree(machine, str(session.meta["repo"]), worktree, branch)
        except WorktreeError as exc:
            log.error("Could not clean failed task worktree for %s: %s", session.id, exc)
            await self._record_event(
                session.id,
                "teardown-failed",
                {"operation": "destroy-worktree", "detail": str(exc)},
            )

    def _project(self, project_name: str) -> Project:
        try:
            return self.projects[project_name]
        except KeyError as exc:
            raise KeyError(f"unknown project: {project_name}") from exc

    def _machine(self, project: Project) -> Machine:
        try:
            return self.machines[project.machine]
        except KeyError as exc:
            raise KeyError(
                f"project {project.name!r} references unknown machine {project.machine!r}"
            ) from exc

    def _session_machine(self, session: PiSession) -> Machine:
        machine_name = str(session.meta.get("machine") or session.host)
        try:
            return self.machines[machine_name]
        except KeyError as exc:
            raise KeyError(f"session {session.id} references unknown machine {machine_name!r}") from exc

    @staticmethod
    def _prompt_path(role: Literal["orchestrator", "pm", "task"]) -> Path:
        filename = "orchestrator-system.md" if role == "orchestrator" else f"{role}.md"
        return orchestrator_repo() / "prompts" / filename


__all__ = [
    "herdr_agent_name",
    "FLEET_SOURCE",
    "Orchestrator",
    "WorktreeError",
    "create_worktree",
    "destroy_worktree",
    "fleet_base_url",
    "orchestrator_machine_name",
    "orchestrator_repo",
]
