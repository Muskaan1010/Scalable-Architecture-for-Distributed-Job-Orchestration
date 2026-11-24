"""
Job Runner implementation for the Job Orchestrator.

This module provides job execution with proper error handling and state management.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
import logging
import traceback as tb
import importlib

from ..core.job import Job, JobState
from ..core.state import StateMachine

logger = logging.getLogger(__name__)


@dataclass
class JobResult:
    """Result of a job execution."""
    job_id: Any
    success: bool
    result: Any = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class JobRunner:
    """Executes jobs with proper error handling and state management."""

    def __init__(
        self,
        state_machine: Optional[StateMachine] = None,
        default_timeout: Optional[float] = None
    ):
        """Initialize the job runner."""
        self._state_machine = state_machine or StateMachine()
        self._default_timeout = default_timeout

    def run(self, job: Job) -> JobResult:
        """Execute a job and return the result."""
        started_at = datetime.utcnow()

        try:
            # 🧠 FIXED: Streamlined transition checks directly to RUNNING without side-effects
            if job.state != JobState.RUNNING:
                self._state_machine.transition(job, JobState.RUNNING)

            job.started_at = started_at

            # Load and execute the target function
            func = self._get_function(job)
            result = func(*job.args, **job.kwargs)

            completed_at = datetime.utcnow()

            # 🧠 FIXED: Explicitly finalize internal job object parameters before returning
            job.result = result
            job.completed_at = completed_at
            self._state_machine.transition(job, JobState.COMPLETED)

            return JobResult(
                job_id=job.id,
                success=True,
                result=result,
                started_at=started_at,
                completed_at=completed_at,
            )

        except Exception as e:
            completed_at = datetime.utcnow()
            error_msg = str(e)
            traceback_str = tb.format_exc()

            logger.error(
                f"Job {job.id} failed during run execution: {error_msg}", exc_info=True)

            # 🧠 FIXED: Explicitly update terminal failure fields on the instance payload
            job.error = error_msg
            job.traceback = traceback_str
            job.completed_at = completed_at

            try:
                self._state_machine.transition(job, JobState.FAILED)
            except Exception as transition_err:
                logger.critical(
                    f"Critical state machine transition failure for job {job.id}: {transition_err}")

            return JobResult(
                job_id=job.id,
                success=False,
                error=error_msg,
                traceback=traceback_str,
                started_at=started_at,
                completed_at=completed_at,
            )

    def _get_function(self, job: Job):
        """Get the callable function for a job."""
        if job.func is not None:
            return job.func

        if not job.func_path:
            raise ValueError(f"Job {job.id} has no func or func_path set")

        try:
            module_path, func_name = job.func_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)
            return func
        except (ValueError, ImportError, AttributeError) as e:
            raise ImportError(
                f"Failed to import function from '{job.func_path}': {e}")


__all__ = ["JobResult", "JobRunner"]
