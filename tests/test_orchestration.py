"""Regression coverage for managed session ownership and failure cleanup."""

from __future__ import annotations

import asyncio
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from worker_harness.db import Database
from worker_harness.herdr_host import HerdrError
from worker_harness.machines import Machine, load_machines
from worker_harness.models import PiSession, PiSessionState
from worker_harness.orchestration import Orchestrator, WorktreeError
from worker_harness.pr import PR_SECTIONS, open_pr
from worker_harness.projects import Project, load_projects, projects_path
from worker_harness.sandbox import launch_command


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = Database(self.root / "sessions.sqlite")
        await self.db.connect()
        self.machine = Machine("desktop", "user@desktop", "/home/user")
        self.project = Project("one", "desktop", "/home/user/Work/one", "owner/one")
        self.orch = Orchestrator(self.db, {"desktop": self.machine}, {"one": self.project})

    async def asyncTearDown(self):
        await self.db.close()
        self.tmp.cleanup()

    async def session(self, role="pm", **kwargs):
        session = PiSession(
            role=role,
            state=kwargs.pop("state", PiSessionState.IDLE),
            host="desktop",
            updated_at=1,
            meta={"project": "one", "pane_id": "pane", "cache_dir": "/home/user/.cache/worker-harness/test"},
            **kwargs,
        )
        await self.db.insert_pi_session(session)
        return session

    async def test_concurrent_lazy_pm_launch_has_one_owner(self):
        async def launch(**kwargs):
            await asyncio.sleep(0)
            return await self.session()

        with patch.object(self.orch, "_launch", side_effect=launch):
            first, second = await asyncio.gather(self.orch.ensure_pm("one"), self.orch.ensure_pm("one"))
        self.assertEqual(first.id, second.id)
        self.assertEqual([s.id for s in await self.db.list_pi_sessions_by_role("pm")], [first.id])

    async def test_restarted_pm_inherits_existing_task_ownership_and_resume(self):
        old = await self.session(state=PiSessionState.STOPPED)
        await self.db.update_pi_session_meta(old.id, {"resume_path": "/home/user/.omp/conversation.jsonl"})
        child = await self.session("task", parent_session_id=old.id)
        seen = {}

        async def launch(**kwargs):
            seen.update(kwargs)
            return await self.session()

        with patch.object(self.orch, "_launch", side_effect=launch):
            manager = await self.orch.ensure_pm("one")
        self.assertEqual((await self.db.get_pi_session(child.id)).parent_session_id, manager.id)
        self.assertEqual(seen["resume_path"], "/home/user/.omp/conversation.jsonl")

    async def test_idle_poll_preserves_busy_children_and_pending_dispatch(self):
        manager = await self.session()
        child = await self.session("task", parent_session_id=manager.id, state=PiSessionState.BLOCKED)
        with patch.object(self.orch, "report_state", new=AsyncMock()), patch.object(
            self.orch, "stop_session", new=AsyncMock()
        ) as stop, patch.dict(os.environ, {"WH_PM_IDLE_SECONDS": "1"}):
            await self.orch.poll()
            stop.assert_not_awaited()
            await self.db.finish_pi_session(child.id, PiSessionState.STOPPED, "")
            manager.has_pending_messages = True
            await self.db.update_pi_session(manager)
            await self.orch.poll()
            stop.assert_not_awaited()
            manager.has_pending_messages = False
            await self.db.update_pi_session(manager)
            await self.orch.poll()
            self.assertEqual(stop.await_count, 1)

    async def test_pm_dispatch_context_prevents_idle_retirement(self):
        manager = await self.session()
        with patch.object(self.orch, "report_state", new=AsyncMock()), patch.object(
            self.orch, "stop_session", new=AsyncMock()
        ) as stop, patch.dict(os.environ, {"WH_PM_IDLE_SECONDS": "1"}):
            async with self.orch.pm_session("one"):
                poll = asyncio.create_task(self.orch.poll())
                await asyncio.sleep(0)
                manager.has_pending_messages = True
                await self.db.update_pi_session(manager)
                stop.assert_not_awaited()
            await poll
            stop.assert_not_awaited()

    async def test_timeout_removes_launch_cache_but_preserves_task_worktree(self):
        session = await self.session("task", state=PiSessionState.STARTING)
        with patch("worker_harness.orchestration.asyncio.get_running_loop") as loop, patch(
            "worker_harness.orchestration.herdr_host.pane_read", new=AsyncMock(return_value="failed")
        ), patch("worker_harness.orchestration.herdr_host.pane_close", new=AsyncMock()), patch(
            "worker_harness.orchestration.herdr_host.remove_file", new=AsyncMock()
        ) as remove, patch("worker_harness.orchestration.destroy_worktree", new=AsyncMock()) as destroy:
            loop.return_value.time.side_effect = [0, 61]
            with self.assertRaises(TimeoutError):
                await self.orch._wait_for_bridge(self.machine, session)
        remove.assert_awaited_once_with(self.machine, session.meta["cache_dir"])
        destroy.assert_not_awaited()
        self.assertEqual((await self.db.get_pi_session(session.id)).state, PiSessionState.FAILED)

    async def test_post_run_failure_preserves_checkout_and_hides_token_from_pane(self):
        parent = await self.session()
        prompt = self.root / "prompt"
        prompt.write_text("Task role instructions")
        written = {}

        async def write_file(machine, path, content):
            written[path] = content

        with patch.object(self.orch, "_prompt_path", return_value=prompt), patch(
            "worker_harness.orchestration.create_worktree", new=AsyncMock(return_value=("/home/user/task", "task"))
        ), patch("worker_harness.orchestration.destroy_worktree", new=AsyncMock()) as destroy, patch(
            "worker_harness.orchestration.herdr_host.workspace_create",
            new=AsyncMock(return_value={"workspace_id": "workspace", "pane_id": "pane"}),
        ), patch("worker_harness.orchestration.herdr_host.write_file", side_effect=write_file), patch(
            "worker_harness.orchestration.herdr_host.pane_run", new=AsyncMock()
        ) as run, patch("worker_harness.orchestration.herdr_host.agent_rename", new=AsyncMock()), patch.object(
            self.orch, "report_state", new=AsyncMock(side_effect=HerdrError("failed", "projection unavailable"))
        ), patch("worker_harness.orchestration.herdr_host.pane_close", new=AsyncMock()), patch(
            "worker_harness.orchestration.herdr_host.remove_file", new=AsyncMock()
        ) as remove:
            with self.assertRaises(HerdrError):
                await self.orch.launch_task(self.project, branch="task", briefing="Do work", parent_session_id=parent.id)
        destroy.assert_not_awaited()
        remove.assert_awaited_once()
        self.assertNotIn("WH_SESSION_TOKEN", run.await_args.args[2])
        script = next(content for path, content in written.items() if path.endswith("launch.sh"))
        self.assertIn("WH_SESSION_TOKEN=", script)

    async def test_teardown_requires_pr_and_confirmed_pane_close(self):
        session = await self.session("task")
        with patch("worker_harness.orchestration.destroy_worktree", new=AsyncMock()) as destroy:
            with self.assertRaises(WorktreeError):
                await self.orch.teardown_task(session)
            session.meta["pr_url"] = "https://github.com/owner/one/pull/1"
            with patch("worker_harness.orchestration.herdr_host.pane_close", new=AsyncMock(
                side_effect=HerdrError("unavailable", "unknown pane state")
            )):
                with self.assertRaises(WorktreeError):
                    await self.orch.teardown_task(session)
        destroy.assert_not_awaited()

    async def test_pr_creation_rejects_unrelated_url(self):
        summary = "\n\n".join(f"{section}\nReviewed change" for section in PR_SECTIONS)
        with patch("worker_harness.pr.write_file", new=AsyncMock()), patch(
            "worker_harness.pr.shim", new=AsyncMock(return_value="https://github.com/other/repo/pull/1")
        ):
            with self.assertRaises(RuntimeError):
                await open_pr(self.machine, self.project, branch="task", worktree="/home/user/task", summary=summary, session_id="task")


class LaunchEnvironmentTests(unittest.TestCase):
    def test_launch_overrides_inherited_role_and_operator_credentials(self):
        with tempfile.TemporaryDirectory() as home:
            wrapper = Path(home) / ".local/bin/omp"
            wrapper.parent.mkdir(parents=True)
            wrapper.write_text("#!/bin/sh\nprintf '%s\\n' \"$LOCAL_VAULT_READONLY\" \"${WH_OPERATOR_TOKEN-unset}\" \"${WH_OPERATOR_TOKEN_FILE-unset}\" \"${WH_PI_SESSION_ID-unset}\" \"$AGENT_SANDBOX_SESSION_DIR\" \"$@\"\n")
            wrapper.chmod(0o755)
            command = launch_command(machine=Machine("test", "test", home), role="pm", paths_file="/paths", session_id="session", token="token", base_url="http://localhost", prompt_file="/prompt with spaces")
            result = subprocess.run(["/bin/sh", "-c", command], env={**os.environ, "LOCAL_VAULT_READONLY": "1", "WH_OPERATOR_TOKEN": "secret", "WH_OPERATOR_TOKEN_FILE": "/secret", "WH_PI_SESSION_ID": "legacy"}, text=True, capture_output=True, check=True)
            self.assertEqual(result.stdout.splitlines(), ["0", "unset", "unset", "unset", f"{home}/.cache/worker-harness/session", "--append-system-prompt", "/prompt with spaces"])

    def test_invalid_top_level_catalog_tables_disable_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text('machine = "bad"\nproject = "bad"\n')
            self.assertEqual(load_machines(path), {})
            self.assertEqual(load_projects(path), {})

    def test_container_prompt_source_controls_default_project_catalog(self):
        with patch.dict(os.environ, {"WH_ORCHESTRATOR_REPO": "/opt/agent-orchestrator", "WH_PROJECTS_PATH": ""}):
            self.assertEqual(projects_path(), Path("/opt/agent-orchestrator/projects.toml"))

    def test_machine_environment_defaults_respect_explicit_overrides(self):
        with tempfile.TemporaryDirectory() as directory:
            configured = Path(directory) / "machines.toml"
            explicit = Path(directory) / "explicit.toml"
            configured.write_text('[machine.desktop]\nssh_target = "user@desktop"\nhome = "/home/user"\n')
            explicit.write_text('[machine.other]\nssh_target = "user@other"\nhome = "/home/user"\nkey_path = "/etc/other-key"\n')
            with patch.dict(os.environ, {"WH_MACHINES_PATH": str(configured), "WH_HERDR_KEY": "/etc/service-key"}):
                self.assertEqual(load_machines()["desktop"].key_path, "/etc/service-key")
                self.assertEqual(load_machines(explicit)["other"].key_path, "/etc/other-key")
