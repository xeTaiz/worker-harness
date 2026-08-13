from pathlib import Path
import subprocess
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
ENTRYPOINT = REPO_ROOT / "worker_container" / "entrypoint.sh"


class WorkerEntrypointTests(unittest.TestCase):
    def test_optional_pi_environment_defaults_under_nounset(self) -> None:
        script = f"""
set -eu
unset WH_PI_INGEST_BASE_URL WH_PI_RELAY_PORT WH_PI_JOB_SOCKET WH_PI_COMMAND
WH_DIR=/tmp/worker-harness-test
HARNESS_DIR="$WH_DIR/harness"
{self._pi_defaults_block()}
printf '%s\n' "$WH_PI_INGEST_BASE_URL" "$WH_PI_RELAY_PORT" "$WH_PI_JOB_SOCKET" "$WH_PI_COMMAND"
"""
        result = subprocess.run(
            ["bash", "-c", script],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            result.stdout.splitlines(),
            ["", "27888", "/tmp/worker-harness-test/harness/pi-job/socket", ""],
        )

    @staticmethod
    def _pi_defaults_block() -> str:
        lines = ENTRYPOINT.read_text().splitlines()
        names = (
            "WH_PI_INGEST_BASE_URL=",
            "WH_PI_RELAY_PORT=",
            "WH_PI_JOB_SOCKET=",
            "WH_PI_COMMAND=",
        )
        selected = [line for line in lines if line.startswith(names)]
        if len(selected) != len(names):
            raise AssertionError("entrypoint optional Pi defaults are incomplete")
        return "\n".join(selected)


if __name__ == "__main__":
    unittest.main()
