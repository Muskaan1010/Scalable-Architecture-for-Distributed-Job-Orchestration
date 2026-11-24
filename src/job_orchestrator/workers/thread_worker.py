"""
Thread-based Worker implementation for the Job Orchestrator.

This module provides a worker that runs jobs in a separate thread,
suitable for I/O-bound workloads where the GIL is not a bottleneck.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Optional
import logging
import threading
import sys

from .worker import BaseWorker, WorkerState, WorkerType

if TYPE_CHECKING:
    from ..scheduler.scheduler import Scheduler
    from ..core.job import Job

logger = logging.getLogger(__name__)


class ThreadWorker(BaseWorker):
    """Worker that runs jobs in a separate thread."""

    worker_type = WorkerType.THREAD

    def __init__(
        self,
        worker_id: Optional[str] = None,
        scheduler: Optional["Scheduler"] = None,
        poll_timeout: float = 1.0,
        name: Optional[str] = None,
        daemon: bool = True,
    ):
        """Initialize the thread worker."""
        # Support 'name' as alias for 'worker_id' for test compatibility
        if name is not None and worker_id is None:
            worker_id = name

        super().__init__(worker_id=worker_id, scheduler=scheduler)

        self.name = self.worker_id  # Alias for compatibility
        self.daemon = daemon
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._poll_timeout = poll_timeout

        logger.debug(f"ThreadWorker {self.worker_id} created")

    def start(self) -> None:
        """Start the worker thread."""
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError(f"Worker {self.worker_id} is already running")
        if self._scheduler is None:
            raise ValueError(f"Worker {self.worker_id} has no scheduler set")

        self._stop_event.clear()
        self._state = WorkerState.IDLE
        self._started_at = datetime.utcnow()
        self._update_heartbeat()

        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"Worker-{self.worker_id}",
            daemon=self.daemon,
        )
        self._thread.start()
        logger.info(f"ThreadWorker {self.worker_id} started")

    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """Stop the worker thread gracefully."""
        if self._state == WorkerState.STOPPED:
            logger.debug(f"Worker {self.worker_id} already stopped")
            return

        self._state = WorkerState.STOPPING
        self._stop_event.set()

        logger.debug(f"Stop signal sent to worker {self.worker_id}")

        if wait and self._thread is not None:
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                logger.warning(
                    f"Worker {self.worker_id} did not stop within timeout ({timeout}s)")
            else:
                self._state = WorkerState.STOPPED
                logger.info(f"ThreadWorker {self.worker_id} stopped")

    def _run_loop(self) -> None:
        """Main worker loop - fetch and execute jobs."""
        logger.debug(f"Worker {self.worker_id} loop started")

        while not self._stop_event.is_set():
            self._update_heartbeat()
            if self._state == WorkerState.STOPPING:
                break

            try:
                job = self._scheduler.get_next_job(timeout=self._poll_timeout)
                if job is None:
                    continue

                # Execute the job safely with explicit metrics capturing wrapper
                self._run_job_with_monitoring(job)
            except Exception as e:
                logger.error(
                    f"Unexpected error in worker loop {self.worker_id}: {e}", exc_info=True)

        self._state = WorkerState.STOPPED
        logger.debug(f"Worker {self.worker_id} loop ended")

    def _run_job_with_monitoring(self, job: "Job") -> None:
        """
        🧠 FIXED: Explicit state switching wrapper ensuring metrics updates
        and cascading result reporting map clean calculations.
        """
        self._state = WorkerState.BUSY
        self._current_job = job
        self._update_heartbeat()
        start_time = datetime.utcnow()

        try:
            # Leverage scheduler synchronous route configuration safely
            # This triggers runtime compilation and imports automatically
            result = self._scheduler._runner.run(job)
            execution_time = (datetime.utcnow() - start_time).total_seconds()

            self._total_execution_time += execution_time
            if result.success:
                self._jobs_completed += 1
                self._scheduler.complete_job(str(job.id), result.result)
            else:
                self._jobs_failed += 1
                self._scheduler.fail_job(str(job.id), Exception(
                    result.error or "Thread execution failure"))
        except Exception as exc:
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            self._total_execution_time += execution_time
            self._jobs_failed += 1
            self._scheduler.fail_job(str(job.id), exc)
            logger.error(
                f"ThreadWorker crashed processing job {job.id}: {exc}", exc_info=True)
        finally:
            self._state = WorkerState.IDLE
            self._current_job = None
            self._update_heartbeat()

    @property
    def is_alive(self) -> bool:
        """Check if the worker thread is alive."""
        if self._thread is None:
            return False
        return self._thread.is_alive()

    def join(self, timeout: Optional[float] = None) -> None:
        """Wait for the worker thread to terminate."""
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def __repr__(self) -> str:
        return f"ThreadWorker(id={self.worker_id}, state={self._state.value})"


__all__ = [
    "ThreadWorker",
]
