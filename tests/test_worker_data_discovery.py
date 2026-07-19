"""Tests for the worker-side shallow shared-data discovery contract."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


DAEMON_PATH = Path(__file__).parents[1] / "worker_container" / "worker_daemon.py"


def load_daemon_module():
    spec = importlib.util.spec_from_file_location("worker_daemon_for_test", DAEMON_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class WorkerDataDiscoveryTests(unittest.TestCase):
    def test_advertises_only_immediate_non_symlink_directories(self):
        daemon = load_daemon_module()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            bind_root = tmp_path / "data"
            (bind_root / "ds1" / "nested").mkdir(parents=True)
            (bind_root / "ds2").mkdir()
            (bind_root / "not-a-directory.txt").write_text("x", encoding="utf-8")
            (bind_root / "linked-ds").symlink_to(bind_root / "ds1", target_is_directory=True)

            wh_dir = tmp_path / "wh"
            manifest = wh_dir / "data" / "bind-paths.json"
            manifest.parent.mkdir(parents=True)
            manifest.write_text(json.dumps({"paths": [str(bind_root)]}), encoding="utf-8")
            daemon.WH_DIR = wh_dir

            self.assertEqual(
                daemon.get_data_paths(),
                [str(bind_root / "ds1"), str(bind_root / "ds2")],
            )

    def test_missing_or_invalid_bind_roots_are_not_advertised(self):
        daemon = load_daemon_module()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            wh_dir = tmp_path / "wh"
            manifest = wh_dir / "data" / "bind-paths.json"
            manifest.parent.mkdir(parents=True)
            manifest.write_text(
                json.dumps({"paths": ["relative", "/", "/does/not/exist", "/data/../secret"]}),
                encoding="utf-8",
            )
            daemon.WH_DIR = wh_dir

            self.assertEqual(daemon.get_data_paths(), [])


if __name__ == "__main__":
    unittest.main()
