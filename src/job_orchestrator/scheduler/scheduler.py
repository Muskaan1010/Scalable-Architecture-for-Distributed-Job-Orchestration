"""
Main scheduler implementation for the Job Orchestrator.

This module provides the Scheduler class that orchestrates job execution,
managing the queue, workers, DAG execution, retry handling, and dead letter queue.
"""

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import UUID
import logging
import threading
import time
import traceback

from ..core.job import Job, JobState, JobPriority
from ..core.dag import DAG
from ..core.state import StateMachine
from ..core.config import OrchestratorConfig
from ..core.exceptions import (
    JobNotFoundError,
    JobAlreadyExistsError,
    QueueFullError,
)
from ..queue.priority_queue import ThreadSafePriorityQueue

from .job_store import JobStore
from .dag_executor import DAGExecutor, DAGStatus, DAGExecution
from .runner import JobRunner, JobResult
from .retry import RetryHandler, RetryPolicy
from .dlq import DeadLetterQueue, DLQEntry, DLQEntryStatus, DLQStats

# 🧠 FIXED: Imported the official Prometheus Gauge metrics client
from prometheus_client import Gauge

logger = logging.getLogger(__name__)

# 🧠 FIXED: Global metric gauge trackers for Prometheus scraping
queue_size_gauge = Gauge(
    'job_orchestrator_queue_total_size', 'Total queue size')
workers_count_gauge = Gauge(
    'job_orchestrator_pool_workers_count', 'Active worker threads')


class Scheduler:
    """Main job scheduler with DAG support."""

    def __init__(self, config: Optional[OrchestratorConfig] = None):
        """Initialize the scheduler."""
        self._config = config or OrchestratorConfig()
        self._lock = threading.RLock()

        # Core components
        self._queue = ThreadSafePriorityQueue(
            max_size=self._config.queue.max_size)
        self._state_machine = StateMachine()
        self._job_store = JobStore()
        self._runner = JobRunner(
            self._state_machine, default_timeout=self._config.job_timeout)
        self._dag_executor = DAGExecutor(
            self,
            on_dag_complete=self._on_dag_complete,
            on_dag_failed=self._on_dag_failed,
        )

        default_retry_policy = RetryPolicy(
            max_retries=self._config.retry.max_retries,
            initial_delay=self._config.retry.base_delay,
            max_delay=self._config.retry.max_delay,
            exponential_base=self._config.retry.exponential_base,
            jitter=self._config.retry.jitter,
        )
        self._retry_handler = RetryHandler(default_policy=default_retry_policy)

        self._dlq = DeadLetterQueue(
            max_size=self._config.dlq.max_size,
            ttl_days=self._config.dlq.auto_cleanup_days,
        )

        self._state_machine.set_job_store(self._job_store)

        # Scheduler thread state
        self._running = False
        self._shutdown_event = threading.Event()
        self._scheduler_thread: Optional[threading.Thread] = None

        # Callbacks arrays
        self._on_job_complete_callbacks: List[Callable[[
            Job, JobResult], None]] = []
        self._on_job_failed_callbacks: List[Callable[[
            Job, JobResult], None]] = []

        self._stats = {
            "jobs_submitted": 0,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "jobs_retried": 0,
            "jobs_sent_to_dlq": 0,
            "dags_submitted": 0,
            "dags_completed": 0,
            "dags_failed": 0,
        }

    def submit(self, job: Job) -> str:
        """Submit a single job for execution."""
        with self._lock:
            if self._job_store.contains(job.id):
                raise JobAlreadyExistsError(job.id)

            if self._config.queue.max_size is not None and len(self._queue) >= self._config.queue.max_size:
                raise QueueFullError(queue_size=len(
                    self._queue), max_size=self._config.queue.max_size)

            self._job_store.add(job)
            self._state_machine.transition(job, JobState.SCHEDULED)

            if not self._queue.push(job):
                job.state = JobState.PENDING
                self._job_store.delete(job.id)
                raise QueueFullError(queue_size=len(
                    self._queue), max_size=self._config.queue.max_size or 0)

            self._stats["jobs_submitted"] += 1
            logger.info(f"Submitted job {job.id}: {job.name}")
            return str(job.id)

    def submit_dag(self, dag: DAG) -> str:
        """Submit a DAG of jobs for execution."""
        with self._lock:
            dag.validate()
            dag.state = JobState.SCHEDULED
            dag.started_at = datetime.utcnow()

            self._dag_executor.start_dag(dag)
            self._stats["dags_submitted"] += 1

            logger.info(
                f"Submitted DAG {dag.id}: {dag.name} with {len(dag.jobs)} jobs")
            return str(dag.id)

    def get_next_job(self, timeout: Optional[float] = None) -> Optional[Job]:
        """Get the next ready job from the queue."""
        if not self._running:
            return None
        job = self._queue.pop(timeout=timeout)
        if job:
            logger.debug(f"Dequeued job {job.id}: {job.name}")
        return job

    def complete_job(self, job_id: str, result: Any = None) -> None:
        """Mark a job as completed and trigger dependents."""
        job_uuid = UUID(job_id)
        with self._lock:
            job = self._job_store.get(job_uuid)
            if not job:
                raise JobNotFoundError(job_uuid)

            if job.state != JobState.COMPLETED:
                job.result = result
                job.completed_at = datetime.utcnow()
                self._state_machine.transition(job, JobState.COMPLETED)

            self._stats["jobs_completed"] += 1
            logger.info(f"Job {job_id} completed successfully")

            for callback in self._on_job_complete_callbacks:
                try:
                    callback(job, JobResult(job_id=job.id, success=True, result=result,
                             started_at=job.started_at, completed_at=job.completed_at))
                except Exception as e:
                    logger.error(
                        f"Error in job success callback execution: {e}")

            dag_id = self._dag_executor.get_dag_for_job(job_uuid)
            if dag_id:
                newly_ready = self._dag_executor.on_job_complete(job_uuid)
                if newly_ready:
                    logger.debug(
                        f"Queued {len(newly_ready)} dependent jobs after completion of {job_id}")

    def fail_job(self, job_id: str, error: Exception) -> None:
        """Mark a job as failed, handle retry or move to DLQ."""
        job_uuid = UUID(job_id)
        with self._lock:
            job = self._job_store.get(job_uuid)
            if not job:
                raise JobNotFoundError(job_uuid)

            error_str = str(error)
            traceback_str = traceback.format_exc()

            if job.state == JobState.RUNNING:
                job.error = error_str
                job.traceback = traceback_str

            if self._retry_handler.should_retry(job, error):
                delay, scheduled_at = self._retry_handler.prepare_for_retry(
                    job)

                if job.state == JobState.FAILED:
                    job.state = JobState.RUNNING

                self._state_machine.transition(job, JobState.RETRYING)
                self._state_machine.transition(job, JobState.SCHEDULED)

                job.error = None
                job.traceback = None
                self._queue.push(job)
                self._stats["jobs_retried"] += 1
                logger.info(
                    f"Job {job_id} scheduled for retry {job.attempt} in {delay:.1f}s")
                return

            if job.state != JobState.FAILED:
                self._state_machine.transition(job, JobState.FAILED)
                job.completed_at = datetime.utcnow()

            entry_id = self._dlq.add(job, error, traceback_str)
            self._stats["jobs_failed"] += 1
            self._stats["jobs_sent_to_dlq"] += 1

            logger.error(
                f"Job {job_id} failed permanently, moved to DLQ (entry: {entry_id})")

            for callback in self._on_job_failed_callbacks:
                try:
                    callback(job, JobResult(job_id=job.id, success=False,
                             error=error_str, traceback=traceback_str))
                except Exception as e:
                    logger.error(f"Error in job failed callback: {e}")

            dag_id = self._dag_executor.get_dag_for_job(job_uuid)
            if dag_id:
                self._dag_executor.on_job_failed(job_uuid, error_str)

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending or scheduled job."""
        job_uuid = UUID(job_id)
        with self._lock:
            job = self._job_store.get(job_uuid)
            if not job:
                raise JobNotFoundError(job_uuid)

            if job.state not in {JobState.PENDING, JobState.SCHEDULED, JobState.RETRYING}:
                logger.warning(
                    f"Cannot cancel job {job_id} in state {job.state.value}")
                return False

            self._queue.remove(job_uuid)
            self._state_machine.transition(job, JobState.CANCELLED)
            job.completed_at = datetime.utcnow()

            logger.info(f"Job {job_id} cancelled")
            return True

    def get_job_status(self, job_id: str) -> JobState:
        """Get the current status of a job."""
        job_uuid = UUID(job_id)
        job = self._job_store.get(job_uuid)
        if not job:
            raise JobNotFoundError(job_uuid)
        return job.state

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by its ID."""
        return self._job_store.get(UUID(job_id))

    def get_dag_status(self, dag_id: str) -> Dict[str, Any]:
        """Get the execution breakdown statistics of a DAG workflow."""
        dag_uuid = UUID(dag_id)
        execution = self._dag_executor.get_status(dag_uuid)
        if not execution:
            return {"dag_id": dag_id, "status": "not_found"}

        jobs_status = []
        for j_id, job in execution.dag.jobs.items():
            jobs_status.append(
                {"job_id": str(j_id), "name": job.name, "state": job.state.value})

        return {
            "dag_id": dag_id,
            "name": execution.dag.name,
            "status": execution.status.value,
            "progress": execution.progress,
            "started_at": execution.started_at.isoformat() if execution.started_at else None,
            "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
            "jobs": jobs_status,
            "completed": len(execution.completed_jobs),
            "failed": len(execution.failed_jobs),
            "running": len(execution.running_jobs),
            "cancelled": len(execution.cancelled_jobs),
            "total": execution.total_jobs,
            "error": execution.error,
        }

    def cancel_dag(self, dag_id: str) -> bool:
        """Cancel all pending jobs in a DAG."""
        return self._dag_executor.cancel_dag(UUID(dag_id))

    def run_job(self, job: Job) -> JobResult:
        """Execute a job synchronously and safely process the outcomes."""
        if not self._job_store.contains(job.id):
            self._job_store.add(job)

        result = self._runner.run(job)
        if result.success:
            self.complete_job(str(job.id), result.result)
        else:
            self.fail_job(str(job.id), Exception(
                result.error or "Synchronous execution crash"))
        return result

    def start(self) -> None:
        """Start the background scheduling loop thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._shutdown_event.clear()
            self._scheduler_thread = threading.Thread(
                target=self._scheduler_loop, name="scheduler-daemon", daemon=True)
            self._scheduler_thread.start()
            logger.info("Scheduler core loop engine active")

    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """Gracefully terminate background daemon activities."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._shutdown_event.set()
            self._queue.shutdown()
            self._dlq.shutdown()

        if wait and self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=timeout)
        logger.info("Scheduler core engine shutdown sequence completed")

    def _scheduler_loop(self) -> None:
        """Internal daemon loop routing scheduled future tasks."""
        while not self._shutdown_event.is_set():
            try:
                now = datetime.utcnow()
                ready_jobs = self._job_store.get_ready_scheduled_jobs(now=now)
                for job in ready_jobs:
                    if job.state == JobState.SCHEDULED and not self._queue.contains(job.id):
                        self._queue.push(job)
                time.sleep(0.5)
            except Exception as e:
                logger.error(
                    f"Error encountered inside internal scheduler loop tracking: {e}")

    def _on_dag_complete(self, dag_id: UUID, status: DAGStatus) -> None:
        """Trigger updates when an execution graph compiles successfully."""
        self._stats["dags_completed"] += 1
        logger.info(
            f"Internal routing confirms graph {dag_id} completed: {status.value}")

    def _on_dag_failed(self, dag_id: UUID, error: str) -> None:
        """Trigger updates when an execution graph fails validation blocks."""
        self._stats["dags_failed"] += 1
        logger.error(
            f"Internal routing confirms graph {dag_id} failed: {error}")

    def get_stats(self) -> Dict[str, Any]:
        """Get live metrics statistics mapping data blocks."""
        with self._lock:
            store_stats = self._job_store.get_stats()
            queue_stats = self._queue.get_stats()
            dlq_stats = self._dlq.get_stats().to_dict()

            # 🧠 FIXED: Feed live metrics data straight to Prometheus Gauge trackers
            q_size = queue_stats.get("total_size", 0)
            queue_size_gauge.set(q_size)

            # Map running jobs currently assigned to pool execution states
            busy_count = len(
                [j for j in self._job_store.get_all() if j.state == JobState.RUNNING])
            workers_count_gauge.set(busy_count)

            return {
                "is_running": self._running,
                **self._stats,
                "store": store_stats,
                "queue": queue_stats,
                "dlq": dlq_stats
            }

    def list_jobs(self, state: Optional[JobState] = None, limit: int = 100, offset: int = 0) -> List[Job]:
        """Retrieve a paginated list of jobs sorted descending by creation time."""
        if state:
            jobs = self._job_store.get_by_state(state)
        else:
            jobs = self._job_store.get_all()
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs[offset:offset+limit]

    def list_dags(self, status: Optional[DAGStatus] = None, limit: int = 100, offset: int = 0) -> List[DAGExecution]:
        """Retrieve a paginated list of DAG executions sorted descending by start time."""
        with self._lock:
            executions = list(self._dag_executor._active_dags.values())
            if status:
                executions = [e for e in executions if e.status == status]
            executions.sort(
                key=lambda e: e.started_at or datetime.min, reverse=True)
            return executions[offset:offset+limit]

    def get_dlq_entries(self, status: Optional[DLQEntryStatus] = None, limit: int = 20, offset: int = 0) -> List[DLQEntry]:
        """List current dead letter queue records with support for offset pagination."""
        return self._dlq.get_all(status=status, limit=limit, offset=offset)

    def on_job_complete(self, callback: Callable[[Job, JobResult], None]) -> None:
        """Register a callback for successful job outcomes."""
        self._on_job_complete_callbacks.append(callback)

    def on_job_failed(self, callback: Callable[[Job, JobResult], None]) -> None:
        """Register a callback for failed job outcomes."""
        self._on_job_failed_callbacks.append(callback)

    @property
    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self._running


__all__ = [
    "Scheduler",
    "RetryHandler",
    "RetryPolicy",
    "DeadLetterQueue",
    "DLQEntry",
    "DLQEntryStatus",
    "DLQStats",
]
