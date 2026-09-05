"""agent subcommand group — machine-friendly JSON output for AI agents."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console

console = Console()


def _config():
    from worker_harness.config import Config
    return Config.load()


def _db():
    from worker_harness.db import Database
    return Database(_config().db_path)


async def _db_connect():
    db = _db()
    await db.connect()
    return db


def _agent_workers_impl():
    async def run():
        db = await _db_connect()
        async with db:
            from worker_harness.models import (
                AgentWorkersResponse, WorkerSummary,
            )

            workers = await db.list_workers()
            summaries: list[WorkerSummary] = []
            for w in workers:
                running_count = await db.get_running_job_count_for_worker(w.id)
                summaries.append(WorkerSummary(
                    id=w.id,
                    name=w.name,
                    worker_ip=w.worker_ip,
                    dns_name=w.dns_name,
                    ssh_user=w.ssh_user,
                    harness_dir=w.harness_dir,
                    gpu_count=w.gpu_count,
                    gpu_names=w.gpu_names,
                    cpu_cores=w.cpu_cores,
                    total_ram_gb=w.total_ram_gb,
                    used_ram_gb=w.used_ram_gb,
                    total_disk_gb=w.total_disk_gb,
                    used_disk_gb=w.used_disk_gb,
                    status=w.status,
                    last_heartbeat_ts=w.last_heartbeat_ts,
                    running_job_count=running_count,
                ))

            response = AgentWorkersResponse(
                workers=summaries,
                total_online=sum(1 for s in summaries if s.status.value == "online"),
                total_offline=sum(1 for s in summaries if s.status.value == "offline"),
            )
            console.print(response.model_dump_json(indent=2))
    asyncio.run(run())


def _agent_free_gpus_impl():
    async def run():
        db = await _db_connect()
        async with db:
            from worker_harness.models import AgentFreeGPUsResponse, FreeGPUEntry

            workers = await db.list_workers()
            gpus: list[FreeGPUEntry] = []
            for w in workers:
                if w.gpu_count == 0:
                    continue
                for i in range(w.gpu_count):
                    gpu_name = w.gpu_names[i] if i < len(w.gpu_names) else "Unknown"
                    vram_total = w.gpu_vram_gb[i] if i < len(w.gpu_vram_gb) else 0.0
                    gpus.append(FreeGPUEntry(
                        worker_id=w.id,
                        worker_name=w.name,
                        gpu_index=i,
                        gpu_name=gpu_name,
                        vram_total_gb=vram_total,
                        vram_used_gb=0.0,  # TODO: wire up from heartbeat payload
                    ))

            total_free = sum(e.vram_total_gb for e in gpus)
            response = AgentFreeGPUsResponse(gpus=gpus, total_free_gb=total_free)
            console.print(response.model_dump_json(indent=2))
    asyncio.run(run())


# Build the typer app
app = typer.Typer(name="agent", help="Machine-friendly commands for AI agents.")
app.command(name="workers")(_agent_workers_impl)
app.command(name="free-gpus")(_agent_free_gpus_impl)
