"""
Process-based Worker implementation for the Job Orchestrator.

This module provides a worker that runs jobs in a separate process,
suitable for CPU-bound workloads where true parallelism is needed.
"""

from datetime import datetime
from multiprocessing import Process, Queue, Event
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple
import importlib
import logging
import traceback

from .worker import BaseWorker, WorkerState, WorkerType

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event as EventType
    from ..scheduler.scheduler import Scheduler
    from ..core.job import Job

logger = logging.getLogger(__name__)


def _worker_process_target(
    worker_id: str,
    job_queue: "Queue[Any]",
    result_queue: "Queue[Any]",
    stop_event: "EventType",
    poll_timeout: float,
) -> None:
    """Target function for the worker process."""
    logger.debug(f"Worker process {worker_id} started")

    while not stop_event.is_set():
        try:
            try:
                job_data = job_queue.get(timeout=poll_timeout)
            except Exception:
                continue

            if job_data is None:
                break

            job_id, func_path, args, kwargs, timeout = job_data

            try:
                func = _resolve_function(func_path)
            except Exception as e:
                result_queue.put((
                    job_id,
                    False,
                    None,
                    str(e),
                    traceback.format_exc(),
                ))
                continue

            try:
                start_time = datetime.utcnow()
                result = func(*args, **kwargs)
                execution_time = (datetime.utcnow() -
                                  start_time).total_seconds()

                result_queue.put((
                    job_id,
                    True,
                    result,
                    None,
                    None,
                    execution_time,
                ))
            except Exception as e:
                execution_time = (datetime.utcnow() -
                                  start_time).total_seconds()
                result_queue.put((
                    job_id,
                    False,
                    None,
                    str(e),
                    traceback.format_exc(),
                    execution_time,
                ))
        except Exception as e:
            logger.error(
                f"Unexpected error in worker process {worker_id}: {e}")

    logger.debug(f"Worker process {worker_id} ended")


def _resolve_function(func_path: str) -> Any:
    """Dynamically import and resolve a function from its path."""
    module_path, func_name = func_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


class ProcessWorker(BaseWorker):
    """Worker that runs jobs in a separate process."""

    worker_type = WorkerType.PROCESS

    def __init__(
        self,
        worker_id: Optional[str] = None,
        scheduler: Optional["Scheduler"] = None,
        poll_timeout: float = 1.0,
    ):
        """Initialize the process worker."""
        super().__init__(worker_id=worker_id, scheduler=scheduler)

        self._process: Optional[Process] = None
        self._job_queue: Optional["Queue[Any]"] = None
        self._result_queue: Optional["Queue[Any]"] = None
        self._stop_event: Optional[Any] = None
        self._poll_timeout = poll_timeout

        self._coordinator_thread: Optional[Any] = None
        self._coordinator_stop: Optional[Any] = None

        logger.debug(f"ProcessWorker {self.worker_id} created")

    def start(self) -> None:
        """Start the worker process and coordinator thread."""
        import threading

        if self._process is not None and self._process.is_alive():
            raise RuntimeError(f"Worker {self.worker_id} is already running")
        if self._scheduler is None:
            raise ValueError(f"Worker {self.worker_id} has no scheduler set")

        self._job_queue = Queue()
        self._result_queue = Queue()
        self._stop_event = Event()

        self._state = WorkerState.IDLE
        self._started_at = datetime.utcnow()
        self._update_heartbeat()

        self._process = Process(
            target=_worker_process_target,
            args=(
                self.worker_id,
                self._job_queue,
                self._result_queue,
                self._stop_event,
                self._poll_timeout,
            ),
            name=f"Worker-{self.worker_id}",
            daemon=True,
        )
        self._process.start()

        self._coordinator_stop = threading.Event()
        self._coordinator_thread = threading.Thread(
            target=self._coordinator_loop,
            name=f"WorkerCoordinator-{self.worker_id}",
            daemon=True,
        )
        self._coordinator_thread.start()
        logger.info(
            f"ProcessWorker {self.worker_id} started (PID: {self._process.pid})")

    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """Stop the worker process gracefully."""
        if self._state == WorkerState.STOPPED:
            logger.debug(f"Worker {self.worker_id} already stopped")
            return

        self._state = WorkerState.STOPPING

        if self._coordinator_stop:
            self._coordinator_stop.set()
        if self._coordinator_thread and self._coordinator_thread.is_alive():
            self._coordinator_thread.join(timeout=2.0)

        if self._stop_event:
            self._stop_event.set()
        if self._job_queue:
            try:
                self._job_queue.put(None)
            except Exception:
                pass

        logger.debug(f"Stop signal sent to worker {self.worker_id}")

        if wait and self._process is not None:
            self._process.join(timeout=timeout)
            if self._process.is_alive():
                logger.warning(
                    f"Worker {self.worker_id} did not stop within timeout, terminating...")
                self._process.terminate()
                self._process.join(timeout=1.0)
            self._state = WorkerState.STOPPED
            logger.info(f"ProcessWorker {self.worker_id} stopped")

        self._cleanup_queues()

    def _cleanup_queues(self) -> None:
        """Clean up multiprocessing queues safely without blocking."""
        for q in (self._job_queue, self._result_queue):
            if q:
                try:
                    while not q.empty():
                        q.get_nowait()
                except Exception:
                    pass
                try:
                    q.close()
                    q.join_thread()
                except Exception:
                    pass

    def _coordinator_loop(self) -> None:
        """Coordinator loop bridging scheduler and worker process."""
        from queue import Empty
        logger.debug(f"Coordinator for worker {self.worker_id} started")
        current_job: Optional["Job"] = None

        while not self._coordinator_stop.is_set():
            self._update_heartbeat()
            if self._state == WorkerState.STOPPING:
                break

            try:
                try:
                    result_data = self._result_queue.get_nowait()
                    if result_data:
                        self._handle_result(result_data, current_job)
                        self._state = WorkerState.IDLE
                        current_job = None
                except Exception:
                    pass

                if self._state == WorkerState.IDLE and current_job is None:
                    job = self._scheduler.get_next_job(timeout=0.1)
                    if job is not None:
                        current_job = job
                        self._current_job = job
                        self._state = WorkerState.BUSY

                        job_data = (
                            str(job.id),
                            job.func_path,
                            job.args,
                            job.kwargs,
                            job.timeout,
                        )
                        self._job_queue.put(job_data)
                        logger.debug(
                            f"Sent job {job.id} to worker process {self.worker_id}")
                else:
                    self._coordinator_stop.wait(timeout=0.1)
            except Exception as e:
                logger.error(
                    f"Error in coordinator {self.worker_id}: {e}", exc_info=True)

        logger.debug(f"Coordinator for worker {self.worker_id} ended")

    def _handle_result(self, result_data: Tuple[Any, ...], job: Optional["Job"]) -> None:
        """Handle a result received from the worker process."""
        if len(result_data) == 5:
            job_id, success, result, error, tb = result_data
            execution_time = 0.0
        else:
            job_id, success, result, error, tb, execution_time = result_data

        self._total_execution_time += execution_time

        if success:
            self._jobs_completed += 1
            if job:
                self._scheduler.complete_job(str(job.id), result)
            logger.debug(
                f"Worker {self.worker_id} completed job {job_id} in {execution_time:.2f}s")
        else:
            self._jobs_failed += 1
            if job:
                exc = Exception(error)
                self._scheduler.fail_job(str(job.id), exc)
            logger.debug(
                f"Worker {self.worker_id} failed job {job_id} in {execution_time:.2f}s: {error}")
        self._current_job = None

    def _run_loop(self) -> None:
        """Not used for ProcessWorker - coordination is handled by loop."""
        pass

    @property
    def is_alive(self) -> bool:
        """Check if the worker process is alive."""
        if self._process is None:
            return False
        return self._process.is_alive()

    @property
    def pid(self) -> Optional[int]:
        """Get the process ID of the worker."""
        if self._process is None:
            return None
        return self._process.pid


__all__ = [
    "ProcessWorker",
]
