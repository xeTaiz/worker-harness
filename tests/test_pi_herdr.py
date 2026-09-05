"""Herdr lifecycle projection for streamed Worker Harness attachments."""

from __future__ import annotations

import unittest

from worker_harness.pi_herdr import HerdrAttachmentReporter


class RecordingReporter(HerdrAttachmentReporter):
    def __init__(self, environment: dict[str, str]) -> None:
        super().__init__(environment.get("HERDR_BIN_PATH"), environment.get("HERDR_PANE_ID"), environment)
        self.calls: list[tuple[str, ...]] = []

    async def _run_cli(self, *arguments: str) -> None:
        self.calls.append(arguments)


class HerdrAttachmentReporterTests(unittest.IsolatedAsyncioTestCase):
    environment = {
        "HERDR_ENV": "1",
        "HERDR_BIN_PATH": "/usr/bin/herdr",
        "HERDR_PANE_ID": "w1:p2",
        "HERDR_SOCKET_PATH": "/tmp/herdr.sock",
    }

    def test_requires_explicit_herdr_pane_environment(self):
        self.assertFalse(HerdrAttachmentReporter.from_environment({}).enabled)
        self.assertFalse(HerdrAttachmentReporter.from_environment({
            "HERDR_ENV": "1",
            "HERDR_BIN_PATH": "/usr/bin/herdr",
            "HERDR_PANE_ID": "w1:p2",
        }).enabled)
        self.assertTrue(HerdrAttachmentReporter.from_environment(self.environment).enabled)

    async def test_reports_bound_session_state_and_sidebar_metadata(self):
        reporter = RecordingReporter(self.environment)
        await reporter.bind({
            "id": "session-1",
            "agent": "omp",
            "state": "working",
            "name": "Remote review",
            "host": "gpu-host",
        })

        self.assertEqual(reporter.calls[0][:8], (
            "pane",
            "report-agent",
            "w1:p2",
            "--source",
            "worker-harness:attachment",
            "--agent",
            "omp",
            "--state",
        ))
        self.assertIn("working", reporter.calls[0])
        self.assertEqual(reporter.calls[1][:5], (
            "pane",
            "report-metadata",
            "w1:p2",
            "--source",
            "worker-harness:attachment-display",
        ))
        self.assertIn("omp Remote review", reporter.calls[1])
        self.assertIn("wh_session_id=session-1", reporter.calls[1])
        self.assertIn("wh_host=gpu-host", reporter.calls[1])

        await reporter.update_state("working")
        self.assertEqual(len(reporter.calls), 2)
        await reporter.update_state("error")
        self.assertEqual(len(reporter.calls), 3)
        self.assertIn("unknown", reporter.calls[2])

    async def test_release_clears_metadata_before_lifecycle_authority(self):
        reporter = RecordingReporter(self.environment)
        await reporter.bind({
            "id": "session-1",
            "agent": "pi",
            "state": "idle",
            "name": "Research",
            "host": "laptop",
        })
        reporter.calls.clear()

        await reporter.release()

        self.assertEqual(reporter.calls[0][1], "report-metadata")
        self.assertIn("--clear-display-agent", reporter.calls[0])
        self.assertIn("wh_session_id", reporter.calls[0])
        self.assertIn("wh_host", reporter.calls[0])
        self.assertEqual(reporter.calls[1][1], "release-agent")
        self.assertIn("pi", reporter.calls[1])

        await reporter.release()
        self.assertEqual(len(reporter.calls), 2)

    async def test_rebinding_releases_old_agent_before_reporting_new_one(self):
        reporter = RecordingReporter(self.environment)
        await reporter.bind({"id": "first", "agent": "pi", "state": "idle", "name": "First"})
        reporter.calls.clear()

        await reporter.bind({"id": "second", "agent": "omp", "state": "working", "name": "Second"})

        self.assertEqual([call[1] for call in reporter.calls[:3]], [
            "report-metadata",
            "release-agent",
            "report-agent",
        ])
        self.assertIn("omp", reporter.calls[2])
        self.assertIn("wh_session_id=second", reporter.calls[3])


if __name__ == "__main__":
    unittest.main()
