from __future__ import annotations

import asyncio
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from xml.sax.saxutils import escape

from worker_harness.db import Database
from worker_harness.models import GPUInfo, WorkerRegistration


DAEMON_PATH = Path(__file__).parents[1] / "worker_container" / "worker_daemon.py"


def load_daemon_module():
    spec = importlib.util.spec_from_file_location("worker_daemon_for_gpu_test", DAEMON_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def gpu_xml(
    index: int = 0, *, used_mb: int = 700, total_mb: int = 16384,
    utilization: str = "0 %", processes: tuple[tuple[str, str, str], ...] = (),
) -> str:
    inventory = "".join(
        f"<process_info><pid>{1000 + offset}</pid><type>{escape(kind)}</type>"
        f"<process_name>{escape(name)}</process_name><used_memory>{escape(memory)}</used_memory>"
        "</process_info>"
        for offset, (name, kind, memory) in enumerate(processes)
    )
    return (
        f'<gpu id="00000000:{index + 1:02x}:00.0"><product_name>NVIDIA Tesla V100</product_name>'
        f"<uuid>GPU-{index}</uuid><minor_number>{index}</minor_number>"
        f"<fb_memory_usage><total>{total_mb} MiB</total><used>{used_mb} MiB</used>"
        f"<free>{total_mb - used_mb} MiB</free></fb_memory_usage>"
        f"<utilization><gpu_util>{utilization}</gpu_util><memory_util>0 %</memory_util></utilization>"
        "<mig_mode><current_mig>Disabled</current_mig><pending_mig>Disabled</pending_mig></mig_mode>"
        f"<processes>{inventory}</processes></gpu>"
    )


def smi_xml(*gpus: str) -> bytes:
    return (
        '<?xml version="1.0" ?><!DOCTYPE nvidia_smi_log SYSTEM "nvsmi_device_v12.dtd">'
        "<nvidia_smi_log><timestamp>Sun Oct 25 07:00:00 2026</timestamp>"
        f"<driver_version>580.82.09</driver_version><attached_gpus>{len(gpus)}</attached_gpus>"
        + "".join(gpus) + "</nvidia_smi_log>"
    ).encode()


class GPUAvailabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.daemon = load_daemon_module()

    def test_ignores_small_and_display_processes_but_requires_free_vram(self):
        output = smi_xml(
            gpu_xml(0, total_mb=49140, used_mb=1024, processes=(("python", "C", "1024 MiB"),)),
            gpu_xml(1, total_mb=49140, used_mb=700, processes=(("python", "C", "200 MiB"),)),
            gpu_xml(2, used_mb=5000),
            gpu_xml(3, used_mb=2500, processes=(("/usr/lib/Xorg", "G", "2048 MiB"),)),
        )
        with patch.object(self.daemon.subprocess, "check_output", return_value=output):
            result = self.daemon.get_gpu_info()

        self.assertEqual([gpu["busy"] for gpu in result["gpus"]], [True, False, True, False])

    def sample(self, xml: str) -> dict:
        with patch.object(self.daemon.subprocess, "check_output", return_value=smi_xml(xml)) as query:
            result = self.daemon.get_gpu_info()
        self.assertEqual(query.call_args.args[0], ["nvidia-smi", "-q", "-x"])
        self.assertEqual(result["gpu_count"], 1)
        return result["gpus"][0]

    def test_complete_empty_inventory_is_valid(self):
        gpu = self.sample(gpu_xml(used_mb=0))
        self.assertTrue(gpu["activity_valid"])
        self.assertEqual(gpu["uuid"], "GPU-0")
        self.assertEqual(gpu["index"], 0)
        self.assertEqual(gpu["memory_used_bytes"], 0)
        self.assertEqual(gpu["process_count"], 0)
        self.assertEqual(gpu["workload_process_count"], 0)
        self.assertEqual(gpu["workload_memory_bytes"], 0)
        self.assertFalse(gpu["display_only"])
        self.assertEqual(gpu["vram_total_gb"], 16.0)

    def test_each_display_graphics_process_is_exempt(self):
        for executable in ("Xorg", "Xwayland", "gnome-shell", "kwin_wayland"):
            with self.subTest(executable=executable):
                gpu = self.sample(gpu_xml(
                    used_mb=2048, utilization="35 %",
                    processes=((f"/usr/lib/{executable}", "G", "2048 MiB"),),
                ))
                self.assertTrue(gpu["activity_valid"])
                self.assertTrue(gpu["display_only"])
                self.assertEqual(gpu["utilization_percent"], 35)
                self.assertEqual(gpu["process_count"], 1)
                self.assertEqual(gpu["memory_used_bytes"], 2048 * 1024**2)
                self.assertEqual(gpu["workload_process_count"], 0)
                self.assertEqual(gpu["workload_memory_bytes"], 0)

    def test_small_compute_counts_even_alongside_display(self):
        gpu = self.sample(gpu_xml(processes=(
            ("/usr/lib/Xorg", "G", "500 MiB"), ("python", "C", "12 MiB"),
        )))
        self.assertTrue(gpu["activity_valid"])
        self.assertFalse(gpu["display_only"])
        self.assertFalse(gpu["busy"])  # Availability's small-process exception is unchanged.
        self.assertEqual(gpu["process_count"], 2)
        self.assertEqual(gpu["workload_process_count"], 1)
        self.assertEqual(gpu["workload_memory_bytes"], 12 * 1024**2)

    def test_only_exact_display_names_and_graphics_only_types_are_exempt(self):
        for executable, kind in (
            ("blender", "G"), ("Xorg", "C"), ("Xorg", "C+G"),
            ("unknown-app", "G"), ("Xorg", "?"), ("Xorg-helper", "G"),
        ):
            with self.subTest(executable=executable, kind=kind):
                gpu = self.sample(gpu_xml(processes=((executable, kind, "0 MiB"),)))
                self.assertTrue(gpu["activity_valid"])
                self.assertFalse(gpu["display_only"])
                self.assertEqual(gpu["workload_process_count"], 1)
                self.assertEqual(gpu["workload_memory_bytes"], 0)

    def test_missing_or_unsupported_fields_fail_closed(self):
        baseline = gpu_xml(processes=(("python", "C", "32 MiB"),))
        for original, replacement in (
            ("<processes>", "<unavailable>"),
            ("<uuid>GPU-0</uuid>", ""),
            ("<gpu_util>0 %</gpu_util>", "<gpu_util>N/A</gpu_util>"),
            ("<used>700 MiB</used>", "<used>[Not Supported]</used>"),
            ("<used_memory>32 MiB</used_memory>", "<used_memory>N/A</used_memory>"),
            ("<process_name>python</process_name>", "<process_name>[Not Supported]</process_name>"),
            ("<process_name>python</process_name>", "<process_name>Insufficient Permissions</process_name>"),
            ("<pid>1000</pid>", "<pid>N/A</pid>"),
            ("<type>C</type>", "<type>N/A</type>"),
            ("<process_info>", "<unexpected>"),
            ("<current_mig>Disabled</current_mig>", "<current_mig>Enabled</current_mig>"),
            ("<current_mig>Disabled</current_mig>", "<current_mig></current_mig>"),
        ):
            with self.subTest(original=original, replacement=replacement):
                xml = baseline.replace(original, replacement)
                if original in {"<processes>", "<process_info>"}:
                    xml = xml.replace(original.replace("<", "</"), replacement.replace("<", "</"))
                gpu = self.sample(xml)
                self.assertFalse(gpu["activity_valid"])
                self.assertFalse(gpu["display_only"])
        for inventory in ("N/A", "[Not Supported]", "<process_info/>"):
            with self.subTest(inventory=inventory):
                xml = gpu_xml().replace("<processes></processes>", f"<processes>{inventory}</processes>")
                self.assertFalse(self.sample(xml)["activity_valid"])

    def test_gpu_without_mig_support_reports_valid_whole_gpu_telemetry(self):
        xml = gpu_xml().replace("<current_mig>Disabled</current_mig>", "<current_mig>N/A</current_mig>")
        gpu = self.sample(xml)
        self.assertTrue(gpu["activity_valid"])
        self.assertEqual(gpu["workload_process_count"], 0)

    def test_incomplete_or_duplicate_device_inventory_is_invalid(self):
        outputs = (
            smi_xml(gpu_xml()).replace(b"<attached_gpus>1</attached_gpus>", b"<attached_gpus>2</attached_gpus>"),
            smi_xml(gpu_xml(), gpu_xml()),
            smi_xml(gpu_xml(), gpu_xml(1).replace("<uuid>GPU-1</uuid>", "<uuid>GPU-0</uuid>")),
        )
        for output in outputs:
            with self.subTest(output=output):
                with patch.object(self.daemon.subprocess, "check_output", return_value=output):
                    result = self.daemon.get_gpu_info()
                self.assertTrue(result["gpus"])
                self.assertTrue(all(not gpu["activity_valid"] for gpu in result["gpus"]))

    def test_failed_or_malformed_query_never_produces_idle_gpus(self):
        for failure in (
            subprocess.CalledProcessError(1, ["nvidia-smi"]),
            subprocess.TimeoutExpired(["nvidia-smi"], 5), FileNotFoundError(),
        ):
            with self.subTest(failure=failure):
                with patch.object(self.daemon.subprocess, "check_output", side_effect=failure):
                    self.assertEqual(self.daemon.get_gpu_info(), {"gpu_count": 0, "gpus": []})
        with patch.object(self.daemon.subprocess, "check_output", return_value=b"<broken"):
            self.assertEqual(self.daemon.get_gpu_info(), {"gpu_count": 0, "gpus": []})

    def test_payload_samples_have_new_ids_and_stable_daemon_identity(self):
        with (
            patch.object(self.daemon, "get_gpu_info", return_value={"gpu_count": 0, "gpus": []}),
            patch.object(self.daemon, "get_system_info", return_value={}),
            patch.object(self.daemon, "get_active_jobs", return_value=[]),
            patch.object(self.daemon, "get_data_paths", return_value=[]),
            patch.object(self.daemon.time, "monotonic_ns", side_effect=[100, 200]),
            patch.object(self.daemon.Path, "read_text", return_value="boot-id\n"),
        ):
            first = self.daemon.build_payload("worker", "100.64.0.1", "")
            second = self.daemon.build_payload("worker", "100.64.0.1", "")
        self.assertNotEqual(first["gpu_sample_id"], second["gpu_sample_id"])
        self.assertEqual(first["daemon_instance_id"], second["daemon_instance_id"])
        self.assertTrue(first["daemon_instance_id"])
        self.assertEqual(first["worker_boot_id"], "boot-id")
        self.assertEqual([first["gpu_sample_monotonic_ns"], second["gpu_sample_monotonic_ns"]], [100, 200])

    def test_persists_reported_busy_state(self):
        async def check() -> None:
            with tempfile.TemporaryDirectory() as tmp:
                db = Database(Path(tmp) / "worker-harness.db")
                await db.connect()
                try:
                    await db.upsert_worker(WorkerRegistration(
                        worker_id="worker-1",
                        name="worker-1",
                        worker_ip="100.64.0.1",
                        gpu_count=2,
                        gpus=[
                            GPUInfo(index=0, name="V100", vram_total_gb=16, vram_used_gb=1, busy=False),
                            GPUInfo(index=1, name="V100", vram_total_gb=16, vram_used_gb=12, busy=True),
                        ],
                    ))
                    worker = await db.get_worker("worker-1")
                    self.assertIsNotNone(worker)
                    self.assertEqual(worker.gpu_busy, [False, True])
                finally:
                    await db.close()

        asyncio.run(check())


if __name__ == "__main__":
    unittest.main()
