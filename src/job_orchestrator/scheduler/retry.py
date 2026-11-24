"""
Retry handler implementation for the Job Orchestrator.

This module provides the RetryPolicy configuration and RetryHandler class
for managing job retry logic with exponential backoff and jitter.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional, Tuple, Type
import logging
import math
import random

from ..core.job import Job, JobState

if TYPE_CHECKING:
    from .scheduler import Scheduler

logger = logging.getLogger(__name__)


@dataclass
class RetryPolicy:
    """Configuration for job retry behavior."""
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 300.0
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on: Tuple[Type[Exception], ...] = field(
        default_factory=lambda: (Exception,))

    def calculate_delay(self, retry_count: int) -> float:
        """Calculate delay before next retry using exponential backoff."""
        delay = self.initial_delay * (self.exponential_base ** retry_count)
        delay = min(delay, self.max_delay)

        if self.jitter:
            jitter_factor = 0.5 + random.random()
            delay *= jitter_factor
        return delay

    def should_retry(self, retry_count: int, error: Optional[Exception] = None) -> bool:
        """Determine if job should be retried."""
        if retry_count >= self.max_retries:
            return False
        if error is not None:
            return isinstance(error, self.retry_on)
        return True

    def to_dict(self) -> dict:
        """Serialize retry policy to dictionary."""
        return {
            "max_retries": self.max_retries,
            "initial_delay": self.initial_delay,
            "max_delay": self.max_delay,
            "exponential_base": self.exponential_base,
            "jitter": self.jitter,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RetryPolicy":
        """Deserialize retry policy from dictionary."""
        return cls(
            max_retries=data.get("max_retries", 3),
            initial_delay=data.get("initial_delay", 1.0),
            max_delay=data.get("max_delay", 300.0),
            exponential_base=data.get("exponential_base", 2.0),
            jitter=data.get("jitter", True),
        )


class RetryHandler:
    """Handles job retry logic with exponential backoff."""

    def __init__(self, default_policy: Optional[RetryPolicy] = None):
        """Initialize the retry handler."""
        self._default_policy = default_policy or RetryPolicy()

    @property
    def default_policy(self) -> RetryPolicy:
        """Get the default retry policy."""
        return self._default_policy

    def get_policy_for_job(self, job: Job) -> RetryPolicy:
        """Get the retry policy for a specific job."""
        if hasattr(job, 'retry_policy') and job.retry_policy is not None:
            core_policy = job.retry_policy
            return RetryPolicy(
                max_retries=core_policy.max_retries,
                initial_delay=core_policy.base_delay,
                max_delay=core_policy.max_delay,
                exponential_base=core_policy.exponential_base,
                jitter=core_policy.jitter,
                retry_on=core_policy.retry_on,
            )
        return self._default_policy

    def get_retry_delay(self, job: Job) -> float:
        """Calculate delay before next retry for a job."""
        policy = self.get_policy_for_job(job)
        return policy.calculate_delay(job.attempt)

    def should_retry(self, job: Job, error: Optional[Exception] = None) -> bool:
        """Determine if a job should be retried."""
        policy = self.get_policy_for_job(job)
        return policy.should_retry(job.attempt, error)

    def schedule_retry(
        self,
        job: Job,
        scheduler: "Scheduler",
        error: Optional[Exception] = None
    ) -> bool:
        """Schedule a job for retry."""
        policy = self.get_policy_for_job(job)

        if not policy.should_retry(job.attempt, error):
            logger.info(
                f"Job {job.id} exhausted retries ({job.attempt}/{policy.max_retries})")
            return False

        delay = policy.calculate_delay(job.attempt)
        job.attempt += 1
        job.scheduled_at = datetime.utcnow() + timedelta(seconds=delay)

        job.error = None
        job.traceback = None

        logger.info(
            f"Job {job.id} scheduled for retry {job.attempt}/{policy.max_retries} in {delay:.1f}s"
        )
        return True

    def prepare_for_retry(self, job: Job) -> Tuple[float, datetime]:
        """Prepare a job for retry without submitting it."""
        policy = self.get_policy_for_job(job)
        delay = policy.calculate_delay(job.attempt)
        scheduled_at = datetime.utcnow() + timedelta(seconds=delay)

        job.attempt += 1
        job.scheduled_at = scheduled_at
        return delay, scheduled_at

    def get_remaining_retries(self, job: Job) -> int:
        """Get the number of remaining retry attempts for a job."""
        policy = self.get_policy_for_job(job)
        return max(0, policy.max_retries - job.attempt)

    def create_retry_policy(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 300.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retry_on: Optional[Tuple[Type[Exception], ...]] = None,
    ) -> RetryPolicy:
        """Factory method to create custom retry policies."""
        return RetryPolicy(
            max_retries=max_retries,
            initial_delay=initial_delay,
            max_delay=max_delay,
            exponential_base=exponential_base,
            jitter=jitter,
            retry_on=retry_on or (Exception,),
        )


# Pre-defined retry policies for common use cases
AGGRESSIVE_RETRY = RetryPolicy(
    max_retries=10,
    initial_delay=0.5,
    max_delay=60.0,
    exponential_base=1.5,
    jitter=True,
)

CONSERVATIVE_RETRY = RetryPolicy(
    max_retries=3,
    initial_delay=5.0,
    max_delay=600.0,
    exponential_base=3.0,
    jitter=True,
)

NO_RETRY = RetryPolicy(
    max_retries=0,
    initial_delay=0.0,
    max_delay=0.0,
)

LINEAR_BACKOFF = RetryPolicy(
    max_retries=5,
    initial_delay=10.0,
    max_delay=60.0,
    exponential_base=1.0,
    jitter=True,
)


__all__ = [
    "RetryPolicy",
    "RetryHandler",
    "AGGRESSIVE_RETRY",
    "CONSERVATIVE_RETRY",
    "NO_RETRY",
    "LINEAR_BACKOFF",
]
