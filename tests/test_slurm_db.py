"""Allocation identity, ordinary queue and transactional event persistence."""

import asyncio
import hashlib
import tempfile
import time
import unittest
from pathlib import Path

from pydantic import ValidationError

from worker_harness.db import Database, SlurmError
from worker_harness.models import (
    GPUInfo, Job, JobResultReport, JobStatus, RunnerResult, SlurmHeartbeat,
    SlurmLaunchPolicy, SlurmObservation, SlurmReservationRequest,
    WorkerRegistration, WorkerStatus,
)


class SlurmDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "db.sqlite"
        self.db = Database(self.path)
        await self.db.connect()

    async def asyncTearDown(self):
        await self.db.close()
        self.tmp.cleanup()

    @staticmethod
    def reservation(key="request", *, cpu=True, recipient="recipient"):
        return SlurmReservationRequest(
            request_id=key, name=f"allocation-{key}", gpu_count=0 if cpu else 2,
            recipient_id=recipient,
            registration_token_hash=hashlib.sha256(b"scoped-token").hexdigest(),
            slurm=SlurmLaunchPolicy(
                cluster_id="test-cpu" if cpu else "test-gpu", storage_id="test",
                compute_kind="cpu" if cpu else "gpu", cpus=4, memory_mb=8192,
                walltime_seconds=600, failure_grace_seconds=60,
                warning_seconds=30, termination_grace_seconds=10,
            ),
        )

    @staticmethod
    def registration(worker, **kwargs):
        return WorkerRegistration(
            worker_id=worker.id, name=worker.name, worker_ip="100.64.0.2",
            gpu_count=worker.gpu_count, cpu_cores=4, total_ram_gb=8,
            gpus=[GPUInfo(index=i, name="test", uuid=f"GPU-{i}",
                          vram_total_gb=40, vram_used_gb=0)
                  for i in range(worker.gpu_count)],
            **kwargs,
        )

    async def queued(self, worker, key=None, **kwargs):
        return await self.db.insert_queued_job(Job(
            worker_id=worker.id, command="true", gpu_count=worker.gpu_count,
            submission_key=key, **kwargs,
        ))

    @staticmethod
    def result(job, *, exit_code=0, finished_at=1000.125, finished_monotonic=100.25,
               started_at=None, started_monotonic=None, boot_id="boot-id"):
        return JobResultReport(job_id=job.id, result=RunnerResult(
            exit_code=exit_code, descendants_exited=True,
            finished_at=finished_at, finished_monotonic=finished_monotonic, boot_id=boot_id,
            started_at=started_at, started_monotonic=started_monotonic,
        ))

    async def test_reservation_is_offline_idempotent_and_capacity_immutable(self):
        request = self.reservation()
        worker = await self.db.reserve_slurm_worker(request)
        repeated = await self.db.reserve_slurm_worker(request.model_dump())
        self.assertEqual(worker.id, repeated.id)
        self.assertEqual(worker.status, WorkerStatus.OFFLINE)
        self.assertEqual((worker.gpu_count, worker.cpu_cores, worker.total_ram_gb), (0, 4, 8))
        self.assertNotIn("registration_token_hash", worker.model_dump()["slurm"])
        with self.assertRaises(SlurmError) as error:
            await self.db.reserve_slurm_worker(request.model_copy(update={"name": "changed"}))
        self.assertEqual(error.exception.code, "IDEMPOTENCY_CONFLICT")
        self.assertEqual(len(await self.db.list_workers()), 1)

    async def test_reservation_serializes_across_connections(self):
        other = Database(self.path)
        await other.connect()
        try:
            workers = await asyncio.gather(
                self.db.reserve_slurm_worker(self.reservation()),
                other.reserve_slurm_worker(self.reservation()),
            )
            self.assertEqual(workers[0].id, workers[1].id)
        finally:
            await other.close()

    async def test_capacity_policy_validation(self):
        body = self.reservation().model_dump()
        body["gpu_count"] = 1
        with self.assertRaises(ValidationError):
            SlurmReservationRequest.model_validate(body)
        body = self.reservation(cpu=False).model_dump()
        body["gpu_count"] = 0
        with self.assertRaises(ValidationError):
            SlurmReservationRequest.model_validate(body)

    async def test_scoped_authentication_and_terminal_revocation(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        self.assertTrue(await self.db.authenticate_slurm_worker(worker.id, "scoped-token"))
        self.assertFalse(await self.db.authenticate_slurm_worker(worker.id, "incorrect"))
        self.assertFalse(await self.db.authenticate_slurm_worker("missing", "scoped-token"))
        await self.db.observe_slurm_worker(worker.id, SlurmObservation(
            observed_at=1000, scheduler_state="CANCELLED"))
        self.assertFalse(await self.db.authenticate_slurm_worker(worker.id, "scoped-token"))

    async def test_registration_requires_exact_identity_and_ordered_gpu_mapping(self):
        worker = await self.db.reserve_slurm_worker(self.reservation(cpu=False))
        reg = self.registration(worker)
        with self.assertRaises(SlurmError):
            await self.db.register_slurm_worker(reg.model_copy(update={"gpu_count": 1}), ready=True)
        missing_uuid = reg.model_copy(deep=True)
        missing_uuid.gpus[0].uuid = None
        with self.assertRaises(SlurmError):
            await self.db.register_slurm_worker(missing_uuid, ready=True)
        wrong_cpu = reg.model_copy(update={"cpu_cores": 8})
        with self.assertRaises(SlurmError):
            await self.db.register_slurm_worker(wrong_cpu, ready=True)
        unready = await self.db.register_slurm_worker(reg, ready=False)
        self.assertEqual(unready.status, WorkerStatus.OFFLINE)
        ready = await self.db.register_slurm_worker(reg, ready=True)
        self.assertEqual(ready.gpu_device_ids, ["GPU-0", "GPU-1"])
        swapped = reg.model_copy(deep=True)
        swapped.gpus[0].uuid, swapped.gpus[1].uuid = "GPU-1", "GPU-0"
        with self.assertRaises(SlurmError):
            await self.db.register_slurm_worker(swapped, ready=True)
        self.assertEqual([event.event_type for event in await self.db.list_job_events("recipient")], ["worker_ready"])

    async def test_gpu_mapping_startup_failure_records_error_without_claiming_readiness(self):
        worker = await self.db.reserve_slurm_worker(self.reservation(cpu=False))
        failure = self.registration(worker, slurm=SlurmHeartbeat(startup_error="ambiguous GPU UUID mapping"))
        failure.gpus = []
        for ready in (False, True):
            observed = await self.db.register_slurm_worker(failure, ready=ready)
            self.assertEqual(observed.status, WorkerStatus.OFFLINE)
            self.assertEqual(observed.gpu_count, 2)
            self.assertEqual(observed.gpu_device_ids, [])
            self.assertIsNone(observed.slurm.started_at)
        with self.assertRaises(SlurmError):
            await self.db.register_slurm_worker(failure.model_copy(update={"gpu_count": 0}), ready=False)
        self.assertEqual([event.event_type for event in await self.db.list_job_events("recipient")],
                         ["worker_start_failed"])
        valid = self.registration(worker)
        await self.db.register_slurm_worker(valid, ready=False)
        observed = await self.db.register_slurm_worker(failure, ready=True)
        self.assertEqual(observed.status, WorkerStatus.OFFLINE)
        self.assertEqual(observed.gpu_device_ids, ["GPU-0", "GPU-1"])
        self.assertEqual(observed.gpu_names, ["test", "test"])
        self.assertIsNone(observed.slurm.started_at)

    async def test_legacy_registration_and_reserved_prefix_cannot_change_identity(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        with self.assertRaises(PermissionError):
            await self.db.upsert_worker(self.registration(worker))
        with self.assertRaises(ValueError):
            await self.db.upsert_worker(WorkerRegistration(
                worker_id="cluster:invalid", name="invalid", worker_ip="127.0.0.1"))
        self.assertEqual((await self.db.get_worker(worker.id)).status, WorkerStatus.OFFLINE)

    async def test_initial_job_id_and_idempotent_queue_survive_later_mutations(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        first = await self.queued(worker, "request")
        self.assertEqual((await self.db.get_worker(worker.id)).slurm.initial_job_id, first.id)
        self.assertEqual(first.notify_recipient_id, "recipient")
        await self.db.update_pending_job(first.id, worker_id=None, position=None,
                                         name="renamed", expected_seconds=50, gpu_count=None)
        await self.db.observe_slurm_worker(worker.id, SlurmObservation(
            observed_at=1000, scheduler_state="COMPLETED"))
        repeated = await self.queued(worker, "request")
        self.assertEqual(repeated.id, first.id)
        self.assertEqual(repeated.status, JobStatus.FAILED)
        with self.assertRaises(SlurmError) as error:
            await self.db.insert_queued_job(Job(worker_id=worker.id, command="false",
                                                gpu_count=0, submission_key="request"))
        self.assertEqual(error.exception.code, "IDEMPOTENCY_CONFLICT")
        self.assertEqual(len(await self.db.list_jobs(worker.id)), 1)

    async def test_explicit_recipient_overrides_inheritance(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        job = await self.queued(worker, notify_recipient_id="explicit")
        job.status = JobStatus.STARTING
        await self.db.update_job(job)
        job.status = JobStatus.FAILED
        job.finished_at = 1000.125
        job.exit_code = 7
        await self.db.update_job(job)
        self.assertEqual(await self.db.list_job_events("recipient"), [])
        events = await self.db.list_job_events("explicit")
        self.assertEqual([event.event_type for event in events], ["job_failed"])
        self.assertEqual(events[0].details["failure_deadline"], 1060.125)
        self.assertEqual(await self.db.list_job_events("explicit", after=events[0].sequence), [])

    async def test_cpu_claim_waits_for_registration_and_serializes_across_connections(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        first = await self.queued(worker)
        second = await self.queued(worker)
        self.assertIsNone(await self.db.claim_queued_job(first.id, [], 1, worker_id=worker.id))
        await self.db.register_slurm_worker(self.registration(worker), ready=True)
        other = Database(self.path)
        await other.connect()
        try:
            claims = await asyncio.gather(
                self.db.claim_queued_job(first.id, [], 2, worker_id=worker.id),
                other.claim_queued_job(first.id, [], 2, worker_id=worker.id),
            )
            self.assertEqual(sum(claim is not None for claim in claims), 1)
            self.assertIsNone(await other.claim_queued_job(second.id, [], 3, worker_id=worker.id))
            first = await self.db.get_job(first.id)
            first.status = JobStatus.DONE
            first.finished_at = 4
            first.exit_code = 0
            await self.db.update_job(first)
            self.assertIsNotNone(await other.claim_queued_job(second.id, [], 5, worker_id=worker.id))
        finally:
            await other.close()

    async def test_zero_gpu_queue_is_cpu_allocation_only(self):
        gpu = await self.db.reserve_slurm_worker(self.reservation("gpu", cpu=False))
        cpu = await self.db.reserve_slurm_worker(self.reservation("cpu"))
        fleet = await self.db.upsert_worker(WorkerRegistration(
            worker_id="fleet", name="fleet", worker_ip="100.64.0.3", gpu_count=2))
        for worker, count in ((gpu, 0), (cpu, 1), (fleet, 0)):
            with self.assertRaises(ValueError):
                await self.db.insert_queued_job(Job(worker_id=worker.id, command="true", gpu_count=count))
        self.assertEqual(await self.db.list_jobs(), [])

    async def test_recovered_results_precede_terminal_reconciliation_and_are_absorbing(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        success = await self.queued(worker)
        unresolved = await self.queued(worker)
        observation = SlurmObservation(observed_at=1100, scheduler_state="COMPLETED",
                                       recovered_results=[self.result(success)])
        await self.db.observe_slurm_worker(worker.id, observation)
        await self.db.observe_slurm_worker(worker.id, observation)
        self.assertEqual((await self.db.get_job(success.id)).status, JobStatus.DONE)
        self.assertEqual((await self.db.get_job(success.id)).finished_at, 1000.125)
        self.assertEqual((await self.db.get_job(unresolved.id)).status, JobStatus.FAILED)
        unresolved.status = JobStatus.DONE
        unresolved.exit_code = 0
        await self.db.update_job(unresolved)
        self.assertEqual((await self.db.get_job(unresolved.id)).status, JobStatus.FAILED)
        types = [event.event_type for event in await self.db.list_job_events("recipient")]
        self.assertEqual(types.count("job_finished"), 1)
        self.assertEqual(types.count("job_failed"), 1)
        self.assertEqual(types.count("worker_start_failed"), 1)

    async def test_invalid_recovered_result_rolls_back_complete_batch_and_metadata(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        job = await self.queued(worker)
        other = await self.db.reserve_slurm_worker(self.reservation("other"))
        foreign = await self.queued(other)
        with self.assertRaises(ValueError):
            await self.db.observe_slurm_worker(worker.id, SlurmObservation(
                observed_at=1000, slurm_job_id="123", scheduler_state="COMPLETED",
                recovered_results=[self.result(job), self.result(foreign)]))
        self.assertEqual((await self.db.get_job(job.id)).status, JobStatus.PENDING)
        self.assertIsNone((await self.db.get_worker(worker.id)).slurm.slurm_job_id)
        self.assertEqual(await self.db.list_job_events("recipient"), [])
        with self.assertRaises(ValidationError):
            RunnerResult(exit_code=0, descendants_exited=False, finished_at=1000,
                         finished_monotonic=100, boot_id="boot")

    async def test_ssh_runner_result_shares_heartbeat_evidence_and_event_transaction(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        job = await self.queued(worker)
        report = self.result(job, exit_code=7)
        completed = await self.db.apply_job_runner_result(worker.id, job.id, report.result)
        self.assertEqual(completed.status, JobStatus.FAILED)
        self.assertEqual(completed.finished_at, report.result.finished_at)
        self.assertEqual(completed.runner_result, report.result)
        self.assertEqual((await self.db.get_job(job.id)).runner_result, report.result)
        await self.db.register_slurm_worker(self.registration(worker, slurm=SlurmHeartbeat(
            results=[report])), ready=False)
        self.assertEqual([event.event_type for event in await self.db.list_job_events("recipient")],
                         ["job_failed"])
        self.assertEqual((await self.db.get_worker(worker.id)).slurm.failure_deadline,
                         report.result.finished_at + 60)
        conflicting = self.result(job, exit_code=0)
        repeated = await self.db.apply_job_runner_result(worker.id, job.id, conflicting.result)
        self.assertEqual(repeated.status, JobStatus.FAILED)
        self.assertEqual(repeated.exit_code, 7)
        self.assertEqual(repeated.runner_result, report.result)

    async def test_ssh_runner_result_validates_linkage_and_supports_ordinary_workers(self):
        fleet = await self.db.upsert_worker(WorkerRegistration(
            worker_id="fleet", name="fleet", worker_ip="100.64.0.3"))
        job = Job(worker_id=fleet.id, command="true", notify_recipient_id="fleet-recipient")
        await self.db.insert_job(job)
        other = await self.db.reserve_slurm_worker(self.reservation())
        report = self.result(job)
        with self.assertRaises(ValueError):
            await self.db.apply_job_runner_result(other.id, job.id, report.result)
        with self.assertRaises(KeyError):
            await self.db.apply_job_runner_result("missing", job.id, report.result)
        self.assertEqual((await self.db.get_job(job.id)).status, JobStatus.PENDING)
        self.assertEqual(await self.db.list_job_events("fleet-recipient"), [])
        completed = await self.db.apply_job_runner_result(fleet.id, job.id, report.result)
        self.assertEqual(completed.status, JobStatus.DONE)
        self.assertEqual([event.event_type for event in await self.db.list_job_events("fleet-recipient")],
                         ["job_finished"])

    async def test_scheduler_binding_terminal_and_submission_flags_are_monotonic(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        await self.db.observe_slurm_worker(worker.id, SlurmObservation(
            observed_at=100, slurm_job_id="123", scheduler_state="RUNNING", submission_attempted=True))
        with self.assertRaises(SlurmError):
            await self.db.observe_slurm_worker(worker.id, SlurmObservation(observed_at=101, slurm_job_id="456"))
        await self.db.observe_slurm_worker(worker.id, SlurmObservation(observed_at=110, scheduler_state="TIMEOUT"))
        await self.db.observe_slurm_worker(worker.id, SlurmObservation(
            observed_at=105, scheduler_state="RUNNING", submission_attempted=False))
        await self.db.observe_slurm_worker(worker.id, SlurmObservation(observed_at=120, scheduler_state="PENDING"))
        final = await self.db.get_worker(worker.id)
        self.assertEqual(final.slurm.scheduler_state, "TIMEOUT")
        self.assertEqual(final.slurm.observed_at, 110)
        self.assertTrue(final.slurm.submission_attempted)
        self.assertEqual(final.status, WorkerStatus.OFFLINE)
        with self.assertRaises(SlurmError):
            await self.db.register_slurm_worker(self.registration(worker), ready=True)
        with self.assertRaises(SlurmError):
            await self.queued(worker)

    async def test_definite_submission_failure_fails_pending_work_once(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        job = await self.queued(worker)
        observation = SlurmObservation(observed_at=1000, submission_attempted=True, submission_failed=True)
        await self.db.observe_slurm_worker(worker.id, observation)
        await self.db.observe_slurm_worker(worker.id, observation)
        self.assertEqual((await self.db.get_job(job.id)).status, JobStatus.FAILED)
        self.assertTrue((await self.db.get_worker(worker.id)).slurm.submission_failed)
        self.assertEqual([event.event_type for event in await self.db.list_job_events("recipient")],
                         ["job_failed", "worker_start_failed", "worker_stopped"])

    async def test_initial_allocation_resource_edits_freeze_after_submission(self):
        worker = await self.db.reserve_slurm_worker(self.reservation(cpu=False))
        initial = await self.queued(worker, "request")
        await self.db.observe_slurm_worker(worker.id, SlurmObservation(observed_at=1000, submission_attempted=True))
        with self.assertRaises(SlurmError) as error:
            await self.db.update_pending_job(initial.id, worker_id=None, position=None,
                                             name=None, expected_seconds=None, gpu_count=1)
        self.assertEqual(error.exception.code, "SLURM_IDENTITY_CONFLICT")
        changed = await self.db.update_pending_job(initial.id, worker_id=None, position=None,
                                                  name="renamed", expected_seconds=100, gpu_count=None)
        self.assertEqual(changed.gpu_count, 2)
        self.assertEqual(changed.name, "renamed")

    async def test_drain_closes_admission_and_registration_preserves_it(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        job = await self.queued(worker)
        self.assertFalse(await self.db.drain_slurm_worker(worker.id))
        await self.db.fail_pending_job(job.id, 1000)
        self.assertTrue(await self.db.drain_slurm_worker(worker.id))
        registered = await self.db.register_slurm_worker(self.registration(worker), ready=True)
        self.assertEqual(registered.status, WorkerStatus.DRAINING)
        with self.assertRaises(SlurmError):
            await self.queued(worker)
        with self.assertRaises(SlurmError):
            await self.db.insert_job(Job(worker_id=worker.id, command="diagnostic"))
        await self.db.mark_workers_offline(2**40)
        self.assertEqual((await self.db.get_worker(worker.id)).status, WorkerStatus.DRAINING)

    async def test_concurrent_admission_and_drain_never_strands_pending_work(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        results = await asyncio.gather(self.queued(worker), self.db.drain_slurm_worker(worker.id),
                                       return_exceptions=True)
        queued, drained = results
        if drained:
            self.assertIsInstance(queued, SlurmError)
            self.assertEqual(await self.db.list_queued_jobs(worker.id), [])
        else:
            self.assertIsInstance(queued, Job)
            self.assertNotEqual((await self.db.get_worker(worker.id)).status, WorkerStatus.DRAINING)

    async def test_reserved_and_completed_history_survives_prune_and_delete(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        job = await self.queued(worker)
        self.assertEqual(await self.db.prune_workers(2**40), 0)
        await self.db.observe_slurm_worker(worker.id, SlurmObservation(observed_at=1000, scheduler_state="COMPLETED"))
        self.assertEqual(await self.db.prune_workers(2**40), 0)
        with self.assertRaises(SlurmError):
            await self.db.delete_worker(worker.id)
        self.assertIsNotNone(await self.db.get_worker(worker.id))
        self.assertIsNotNone(await self.db.get_job(job.id))

    async def test_failure_grace_blocks_idle_drain_and_replayed_deadline_cannot_restart_it(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        await self.db.register_slurm_worker(self.registration(worker), ready=True)
        failed = await self.queued(worker)
        finished_at = time.time()
        report = self.result(failed, exit_code=7, finished_at=finished_at)
        heartbeat = SlurmHeartbeat(results=[report], failure_deadline=finished_at + 60, draining=True)
        updated = await self.db.register_slurm_worker(self.registration(worker, slurm=heartbeat), ready=True)
        self.assertEqual(updated.slurm.failure_deadline, finished_at + 60)
        self.assertEqual(updated.status, WorkerStatus.ONLINE)
        self.assertFalse(await self.db.drain_slurm_worker(worker.id))
        diagnostic = Job(worker_id=worker.id, command="diagnose", status=JobStatus.STARTING)
        await self.db.insert_job(diagnostic)
        diagnostic.status = JobStatus.RUNNING
        await self.db.update_job(diagnostic)
        self.assertEqual((await self.db.get_worker(worker.id)).slurm.failure_deadline, finished_at + 60)
        useful = await self.queued(worker)
        useful.status = JobStatus.RUNNING
        await self.db.update_job(useful)
        self.assertIsNone((await self.db.get_worker(worker.id)).slurm.failure_deadline)
        await self.db.register_slurm_worker(self.registration(worker, slurm=heartbeat), ready=True)
        self.assertIsNone((await self.db.get_worker(worker.id)).slurm.failure_deadline)

    async def test_delayed_failure_cannot_restore_grace_after_a_proven_later_useful_start(self):
        for newer_status in (JobStatus.RUNNING, JobStatus.DONE, JobStatus.FAILED):
            with self.subTest(newer_status=newer_status):
                worker = await self.db.reserve_slurm_worker(self.reservation(f"delayed-{newer_status.value}"))
                old = await self.queued(worker)
                newer = await self.queued(worker)
                newer.status = JobStatus.STARTING
                newer.started_at = 2000
                await self.db.update_job(newer)
                if newer_status == JobStatus.RUNNING:
                    newer.status = JobStatus.RUNNING
                    await self.db.update_job(newer)
                    await self.db.register_slurm_worker(self.registration(
                        worker, slurm=SlurmHeartbeat(), active_jobs=[{
                            "job_id": newer.id, "status": "running", "queue_managed": True,
                            "started_at": 2000.125, "started_monotonic": 200.125, "boot_id": "boot-id",
                        }]), ready=False)
                else:
                    await self.db.apply_job_runner_result(worker.id, newer.id, self.result(
                        newer, exit_code=0 if newer_status == JobStatus.DONE else 7,
                        finished_at=3000.125, finished_monotonic=300.125,
                        started_at=2000.125, started_monotonic=200.125).result)
                await self.db.apply_job_runner_result(worker.id, old.id,
                                                     self.result(old, exit_code=7).result)
                deadline = (await self.db.get_worker(worker.id)).slurm.failure_deadline
                self.assertEqual(deadline, 3060.125 if newer_status == JobStatus.FAILED else None)

    async def test_starting_claim_and_diagnostic_do_not_prove_later_useful_execution(self):
        for diagnostic in (False, True):
            with self.subTest(diagnostic=diagnostic):
                worker = await self.db.reserve_slurm_worker(self.reservation(f"unproven-{diagnostic}"))
                old = await self.queued(worker)
                if diagnostic:
                    newer = Job(worker_id=worker.id, command="diagnose", status=JobStatus.RUNNING,
                                started_at=2000)
                    await self.db.insert_job(newer)
                else:
                    newer = await self.queued(worker)
                    newer.status = JobStatus.STARTING
                    newer.started_at = 2000
                    await self.db.update_job(newer)
                await self.db.apply_job_runner_result(worker.id, old.id,
                                                     self.result(old, exit_code=7).result)
                self.assertEqual((await self.db.get_worker(worker.id)).slurm.failure_deadline, 1060.125)

    async def test_precise_terminal_starts_precede_all_results_in_arbitrary_replay_order(self):
        for reverse in (False, True):
            with self.subTest(reverse=reverse):
                worker = await self.db.reserve_slurm_worker(self.reservation(f"same-second-{reverse}"))
                old = await self.queued(worker)
                rerun = await self.queued(worker)
                reports = [
                    self.result(old, exit_code=7, finished_at=1000.4, finished_monotonic=10.4,
                                started_at=1000.1, started_monotonic=10.1),
                    self.result(rerun, finished_at=1000.8, finished_monotonic=10.8,
                                started_at=1000.5, started_monotonic=10.5),
                ]
                if reverse:
                    reports.reverse()
                observed = await self.db.observe_slurm_worker(worker.id, SlurmObservation(
                    observed_at=1001, recovered_results=reports))
                self.assertIsNone(observed.slurm.failure_deadline)
                self.assertEqual(observed.slurm.last_useful_started_at, 1000.5)
                self.assertEqual(observed.slurm.last_useful_started_monotonic, 10.5)
                self.assertEqual(observed.slurm.last_useful_boot_id, "boot-id")
                self.assertEqual((await self.db.get_job(old.id)).status, JobStatus.FAILED)
                self.assertEqual((await self.db.get_job(rerun.id)).status, JobStatus.DONE)
                self.assertEqual((await self.db.get_job(rerun.id)).started_at, 0)
                self.assertIsNone(observed.slurm.started_at)  # Recovery is not live registration.

    async def test_monotonic_start_order_overrides_utc_and_falls_back_across_boots(self):
        cases = [
            # UTC stepped backward, but the same boot proves the rerun is later.
            ("backward", "boot-id", 900.5, 10.5, None),
            # UTC stepped forward, but overlapping useful work started before failure.
            ("forward", "boot-id", 2000.5, 10.2, 1060.4),
            ("other-boot-later", "other-boot", 1000.5, 1.0, None),
            ("other-boot-earlier", "other-boot", 999.5, 1000.0, 1060.4),
        ]
        for name, boot_id, wall, monotonic, expected in cases:
            with self.subTest(name=name):
                worker = await self.db.reserve_slurm_worker(self.reservation(name))
                old = await self.queued(worker)
                newer = await self.queued(worker)
                await self.db.observe_slurm_worker(worker.id, SlurmObservation(
                    observed_at=3000, recovered_results=[
                        self.result(old, exit_code=7, finished_at=1000.4, finished_monotonic=10.4),
                        self.result(newer, finished_at=wall + 1, finished_monotonic=monotonic + 1,
                                    started_at=wall, started_monotonic=monotonic, boot_id=boot_id),
                    ]))
                self.assertEqual((await self.db.get_worker(worker.id)).slurm.failure_deadline, expected)

    async def test_active_start_uses_stored_usefulness_and_clears_grace_by_monotonic_proof(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        old = await self.queued(worker)
        await self.db.apply_job_runner_result(worker.id, old.id, self.result(
            old, exit_code=7, finished_at=2000.4, finished_monotonic=10.4).result)
        diagnostic = Job(worker_id=worker.id, command="diagnose", status=JobStatus.RUNNING)
        await self.db.insert_job(diagnostic)
        proof = {"job_id": diagnostic.id, "status": "running", "queue_managed": True,
                 "started_at": 900.5, "started_monotonic": 10.5, "boot_id": "boot-id"}
        observed = await self.db.register_slurm_worker(self.registration(
            worker, slurm=SlurmHeartbeat(), active_jobs=[proof]), ready=False)
        self.assertEqual(observed.slurm.failure_deadline, 2060.4)
        self.assertIsNone(observed.slurm.last_useful_started_at)
        rerun = await self.queued(worker)
        proof.update(job_id=rerun.id, queue_managed=False)
        observed = await self.db.register_slurm_worker(self.registration(
            worker, slurm=SlurmHeartbeat(), active_jobs=[proof]), ready=False)
        self.assertIsNone(observed.slurm.failure_deadline)
        self.assertEqual(observed.slurm.last_useful_started_monotonic, 10.5)
        # A stale start with a misleading later UTC cannot replace same-boot proof.
        proof.update(started_at=3000.0, started_monotonic=10.2)
        observed = await self.db.register_slurm_worker(self.registration(
            worker, slurm=SlurmHeartbeat(), active_jobs=[proof]), ready=False)
        self.assertEqual(observed.slurm.last_useful_started_at, 900.5)
        self.assertEqual(observed.slurm.last_useful_started_monotonic, 10.5)

    async def test_active_start_proof_precedes_failed_results_in_same_heartbeat(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        old = await self.queued(worker)
        rerun = await self.queued(worker)
        observed = await self.db.register_slurm_worker(self.registration(
            worker, active_jobs=[{
                "job_id": rerun.id, "status": "running", "queue_managed": True,
                "started_at": 1000.5, "started_monotonic": 10.5, "boot_id": "boot-id",
            }], slurm=SlurmHeartbeat(results=[
                self.result(old, exit_code=7, finished_at=1000.4, finished_monotonic=10.4)
            ])), ready=False)
        self.assertIsNone(observed.slurm.failure_deadline)

    async def test_invalid_active_start_rolls_back_all_start_and_result_observations(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        old = await self.queued(worker)
        rerun = await self.queued(worker)
        other = await self.db.reserve_slurm_worker(self.reservation("foreign-start"))
        foreign = await self.queued(other)
        for foreign_id in (foreign.id, "unknown"):
            with self.subTest(foreign_id=foreign_id):
                active = [{
                    "job_id": job_id, "status": "running", "queue_managed": True,
                    "started_at": 1000.5, "started_monotonic": 10.5, "boot_id": "boot-id",
                } for job_id in (rerun.id, foreign_id)]
                with self.assertRaises(ValueError):
                    await self.db.register_slurm_worker(self.registration(
                        worker, active_jobs=active,
                        slurm=SlurmHeartbeat(results=[self.result(old, exit_code=7)])), ready=False)
                self.assertIsNone((await self.db.get_worker(worker.id)).slurm.last_useful_started_at)
                self.assertEqual((await self.db.get_job(old.id)).status, JobStatus.PENDING)

    async def test_first_actual_failure_deadline_is_independent_of_replay_order(self):
        for reverse in (False, True):
            with self.subTest(reverse=reverse):
                worker = await self.db.reserve_slurm_worker(self.reservation(f"failure-order-{reverse}"))
                first = await self.queued(worker)
                second = await self.queued(worker)
                reports = [
                    self.result(first, exit_code=7, finished_at=1000.4, finished_monotonic=10.4),
                    self.result(second, exit_code=7, finished_at=900.4, finished_monotonic=10.8),
                ]
                if reverse:
                    reports.reverse()
                observed = await self.db.observe_slurm_worker(worker.id, SlurmObservation(
                    observed_at=2000, recovered_results=reports))
                self.assertEqual(observed.slurm.failure_deadline, 1060.4)

    async def test_useful_start_proof_survives_database_reconnect(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        old = await self.queued(worker)
        rerun = await self.queued(worker)
        await self.db.apply_job_runner_result(worker.id, rerun.id, self.result(
            rerun, started_at=1000.5, started_monotonic=10.5,
            finished_at=1000.8, finished_monotonic=10.8).result)
        await self.db.close()
        self.db = Database(self.path)
        await self.db.connect()
        await self.db.apply_job_runner_result(worker.id, old.id, self.result(
            old, exit_code=7, finished_at=1000.4, finished_monotonic=10.4).result)
        observed = await self.db.get_worker(worker.id)
        self.assertIsNone(observed.slurm.failure_deadline)
        self.assertEqual(observed.slurm.last_useful_started_monotonic, 10.5)

    async def test_runner_start_pair_validation_allows_utc_reversal_not_monotonic_reversal(self):
        valid = self.result(Job(), started_at=2000, started_monotonic=10,
                            finished_at=1000, finished_monotonic=11).result
        self.assertEqual(valid.started_at, 2000)
        for update in (
            {"started_monotonic": None}, {"started_at": None},
            {"started_monotonic": 12}, {"unexpected_start": True},
        ):
            with self.subTest(update=update), self.assertRaises(ValidationError):
                RunnerResult.model_validate({**valid.model_dump(), **update})

    async def test_checkpoint_observation_closes_admission_and_deduplicates(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        heartbeat = SlurmHeartbeat(checkpoint_requested=True)
        reg = self.registration(worker, slurm=heartbeat)
        await self.db.register_slurm_worker(reg, ready=True)
        await self.db.register_slurm_worker(reg, ready=True)
        self.assertEqual((await self.db.get_worker(worker.id)).status, WorkerStatus.DRAINING)
        self.assertEqual([event.event_type for event in await self.db.list_job_events("recipient")],
                         ["checkpoint_requested"])

    async def test_startup_failure_and_scheduler_terminal_share_one_failure_event(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        reg = self.registration(worker, slurm=SlurmHeartbeat(startup_error="enrollment failed"))
        for _ in range(2):
            await self.db.register_slurm_worker(reg, ready=False)
        observation = SlurmObservation(observed_at=1000, scheduler_state="FAILED")
        for _ in range(2):
            await self.db.observe_slurm_worker(worker.id, observation)
        self.assertEqual([event.event_type for event in await self.db.list_job_events("recipient")],
                         ["worker_start_failed", "worker_stopped"])

    async def test_conditional_cancellation_requires_explicit_cancel_request(self):
        for cancel_requested in (None, False):
            with self.subTest(cancel_requested=cancel_requested):
                with self.assertRaises(ValidationError):
                    SlurmObservation(observed_at=1000, cancel_if_empty=True,
                                     cancel_requested=cancel_requested)
        self.assertFalse(SlurmObservation(observed_at=1000, cancel_requested=True).cancel_if_empty)

    async def test_conditional_cancellation_preserves_all_observations_when_useful_work_remains(self):
        for status in (JobStatus.PENDING, JobStatus.STARTING, JobStatus.RUNNING):
            with self.subTest(status=status):
                worker = await self.db.reserve_slurm_worker(self.reservation(f"guard-{status.value}"))
                job = await self.queued(worker)
                if status != JobStatus.PENDING:
                    job.status = status
                    await self.db.update_job(job)
                before = await self.db.get_worker(worker.id)
                events = await self.db.list_job_events("recipient")
                observed = await self.db.observe_slurm_worker(worker.id, SlurmObservation(
                    observed_at=1000, cancel_requested=True, cancel_if_empty=True,
                    submission_attempted=True, slurm_job_id="123", scheduler_state="CANCELLED",
                    recovered_results=[self.result(job)],
                ))
                self.assertEqual(observed.model_dump(), before.model_dump())
                self.assertFalse(observed.slurm.cancel_requested)
                self.assertEqual((await self.db.get_job(job.id)).status, status)
                self.assertEqual(await self.db.list_job_events("recipient"), events)
                self.assertTrue(await self.db.authenticate_slurm_worker(worker.id, "scoped-token"))

    async def test_conditional_cancellation_ignores_diagnostics_but_closes_fresh_admission(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        initial = await self.queued(worker)
        await self.db.fail_pending_job(initial.id, 1000)
        diagnostic = Job(worker_id=worker.id, command="diagnose", status=JobStatus.RUNNING)
        await self.db.insert_job(diagnostic)
        events = await self.db.list_job_events("recipient")
        cancelled = await self.db.observe_slurm_worker(worker.id, SlurmObservation(
            observed_at=1001, cancel_requested=True, cancel_if_empty=True))
        self.assertTrue(cancelled.slurm.cancel_requested)
        self.assertEqual(cancelled.status, WorkerStatus.DRAINING)
        self.assertEqual((await self.db.get_job(diagnostic.id)).status, JobStatus.RUNNING)
        self.assertEqual(await self.db.list_job_events("recipient"), events)
        self.assertTrue(await self.db.authenticate_slurm_worker(worker.id, "scoped-token"))
        with self.assertRaises(SlurmError):
            await self.queued(worker)

    async def test_conditional_cancellation_does_not_retire_started_or_finished_allocations(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        await self.db.register_slurm_worker(self.registration(worker), ready=True)
        observation = SlurmObservation(observed_at=1000, cancel_requested=True, cancel_if_empty=True)
        before = await self.db.get_worker(worker.id)
        self.assertEqual((await self.db.observe_slurm_worker(worker.id, observation)).model_dump(),
                         before.model_dump())
        # Explicit allocation scope remains unconditional, even after readiness.
        cancelled = await self.db.observe_slurm_worker(worker.id, SlurmObservation(
            observed_at=1000, cancel_requested=True))
        self.assertTrue(cancelled.slurm.cancel_requested)
        self.assertEqual(cancelled.status, WorkerStatus.DRAINING)
        never_started = await self.db.reserve_slurm_worker(self.reservation("finished"))
        await self.db.observe_slurm_worker(never_started.id, SlurmObservation(
            observed_at=1000, submission_failed=True))
        before = await self.db.get_worker(never_started.id)
        self.assertEqual((await self.db.observe_slurm_worker(never_started.id, observation)).model_dump(),
                         before.model_dump())

    async def test_conditional_cancellation_and_admission_honor_both_writer_lock_orders(self):
        for cancellation_first in (False, True):
            with self.subTest(cancellation_first=cancellation_first):
                worker = await self.db.reserve_slurm_worker(self.reservation(f"order-{cancellation_first}"))
                observation = SlurmObservation(observed_at=1000, cancel_requested=True, cancel_if_empty=True)
                async with self.db._queue_lock:
                    if cancellation_first:
                        cancel = asyncio.create_task(self.db.observe_slurm_worker(worker.id, observation))
                        await asyncio.sleep(0)
                        enqueue = asyncio.create_task(self.queued(worker))
                    else:
                        enqueue = asyncio.create_task(self.queued(worker))
                        await asyncio.sleep(0)
                        cancel = asyncio.create_task(self.db.observe_slurm_worker(worker.id, observation))
                    await asyncio.sleep(0)
                queued, cancelled = await asyncio.gather(enqueue, cancel, return_exceptions=True)
                if cancellation_first:
                    self.assertIsInstance(queued, SlurmError)
                    self.assertEqual(queued.code, "SLURM_WORKER_FINISHED")
                    self.assertTrue(cancelled.slurm.cancel_requested)
                    self.assertEqual(await self.db.list_queued_jobs(worker.id), [])
                else:
                    self.assertIsInstance(queued, Job)
                    self.assertFalse(cancelled.slurm.cancel_requested)
                    self.assertEqual((await self.db.get_job(queued.id)).status, JobStatus.PENDING)
                    self.assertEqual(cancelled.status, WorkerStatus.OFFLINE)

    async def test_conditional_cancellation_and_admission_fence_separate_connections(self):
        worker = await self.db.reserve_slurm_worker(self.reservation())
        other = Database(self.path)
        await other.connect()
        try:
            queued, cancelled = await asyncio.gather(
                self.queued(worker),
                other.observe_slurm_worker(worker.id, SlurmObservation(
                    observed_at=1000, cancel_requested=True, cancel_if_empty=True)),
                return_exceptions=True,
            )
            if isinstance(queued, SlurmError):
                self.assertTrue(cancelled.slurm.cancel_requested)
                self.assertEqual(await self.db.list_queued_jobs(worker.id), [])
            else:
                self.assertIsInstance(queued, Job)
                self.assertFalse(cancelled.slurm.cancel_requested)
                self.assertEqual((await self.db.get_job(queued.id)).status, JobStatus.PENDING)
        finally:
            await other.close()

    async def test_ordinary_job_events_need_no_allocation_and_replay_by_recipient(self):
        await self.db.insert_job(Job(id="ordinary", command="true", notify_recipient_id="ordinary-recipient"))
        job = await self.db.get_job("ordinary")
        # Ordinary jobs created without a worker are not executable; attach an
        # ordinary registration before exercising the real status persistence.
        fleet = await self.db.upsert_worker(WorkerRegistration(worker_id="fleet", name="fleet", worker_ip="100.64.0.3"))
        job.worker_id = fleet.id
        job.status = JobStatus.RUNNING
        await self.db.update_job(job)
        await self.db.update_job(job)
        job.status = JobStatus.DONE
        job.exit_code = 0
        job.finished_at = 1000
        await self.db.update_job(job)
        events = await self.db.list_job_events("ordinary-recipient")
        self.assertEqual([event.event_type for event in events], ["job_started", "job_finished"])
        self.assertEqual([event.sequence for event in await self.db.list_job_events("ordinary-recipient", after=events[0].sequence)], [events[1].sequence])
        self.assertEqual(await self.db.list_job_events("someone-else"), [])


if __name__ == "__main__":
    unittest.main()
