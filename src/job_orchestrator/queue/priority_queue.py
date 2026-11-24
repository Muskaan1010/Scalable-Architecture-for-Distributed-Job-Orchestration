"""
Thread-safe priority queue implementation for the Job Orchestrator.

This module provides a heap-based priority queue with support for
scheduled jobs, lazy deletion, and O(log n) operations.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Set
from uuid import UUID
import heapq
import threading
import time

from ..core.job import Job, JobPriority


@dataclass(order=True)
class QueueEntry:
    """
    Entry in the priority queue.

    Entries are ordered by (priority, scheduled_time, insertion_order)
    to ensure consistent ordering and FIFO behavior within the same
    priority level.
    """
    priority: int = field(compare=True)
    scheduled_time: float = field(compare=True)
    insertion_order: int = field(compare=True)
    job_id: UUID = field(compare=False)
    job: Job = field(compare=False, repr=False)


class ThreadSafePriorityQueue:
    """
    Thread-safe, memory-efficient priority queue implementation.
    """

    def __init__(
        self,
        max_size: Optional[int] = None,
        *,
        maxsize: Optional[int] = None,
    ):
        """Initialize the priority queue."""
        resolved_max = maxsize if maxsize is not None else max_size
        if resolved_max == 0:
            resolved_max = None
        self._heap: List[QueueEntry] = []
        self._entry_map: Dict[UUID, QueueEntry] = {}
        self._removed: Set[UUID] = set()
        self._counter: int = 0
        self._lock = threading.RLock()
        self._max_size = resolved_max
        self.maxsize = resolved_max if resolved_max is not None else 0
        self._condition = threading.Condition(self._lock)
        self._shutdown = False

    def __len__(self) -> int:
        """Return the number of items in the queue."""
        with self._lock:
            return len(self._entry_map) - len(self._removed)

    @property
    def size(self) -> int:
        """Return the number of items in the queue."""
        return len(self)

    def qsize(self) -> int:
        """Queue-compatible size helper."""
        return self.size

    def empty(self) -> bool:
        """Return True if queue is empty."""
        return self.size == 0

    @property
    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return self.size == 0

    def full(self) -> bool:
        """Return True if queue hit capacity."""
        if self._max_size is None:
            return False
        return self.size >= self._max_size

    @property
    def is_full(self) -> bool:
        """Check if the queue is at max capacity."""
        return self.full()

    def push(self, job: Job) -> bool:
        """Add a job to the queue."""
        if not isinstance(job, Job):
            raise TypeError(
                "ThreadSafePriorityQueue only accepts Job instances")
        with self._lock:
            if self._max_size is not None and len(self) >= self._max_size:
                return False

            if job.id in self._entry_map and job.id not in self._removed:
                self._removed.add(job.id)

            if job.scheduled_at:
                scheduled_time = self._to_timestamp(job.scheduled_at)
            else:
                scheduled_time = self._to_timestamp(job.created_at)

            entry = QueueEntry(
                priority=job.priority.value,
                scheduled_time=scheduled_time,
                insertion_order=self._counter,
                job_id=job.id,
                job=job,
            )
            self._counter += 1

            heapq.heappush(self._heap, entry)
            self._entry_map[job.id] = entry
            self._removed.discard(job.id)

            self._condition.notify_all()
            return True

    def pop(self, timeout: Optional[float] = None) -> Optional[Job]:
        """Remove and return the highest priority ready job."""
        with self._lock:
            deadline = time.time() + timeout if timeout is not None else None

            while not self._shutdown:
                self._cleanup_removed()

                job = self._get_ready_job()
                if job is not None:
                    return job

                if timeout == 0:
                    return None

                if not self._heap:
                    if deadline is not None:
                        remaining = deadline - time.time()
                        if remaining <= 0:
                            return None
                        self._condition.wait(remaining)
                    else:
                        self._condition.wait(1.0)
                else:
                    next_job_time = self._heap[0].scheduled_time
                    wait_time = next_job_time - time.time()

                    if deadline is not None:
                        wait_time = min(wait_time, deadline - time.time())

                    if wait_time > 0:
                        self._condition.wait(wait_time)
                    else:
                        continue

                if deadline is not None and time.time() >= deadline:
                    return None

            return None

    def pop_nowait(self) -> Optional[Job]:
        """Remove and return the highest priority ready job without waiting."""
        return self.pop(timeout=0)

    def pop_if_ready(self, timeout: Optional[float] = None) -> Optional[Job]:
        """Alias used by tests to only return ready jobs."""
        return self.pop(timeout=timeout)

    def _cleanup_removed(self) -> None:
        """Clean up removed entries from the top of the heap."""
        while self._heap and self._heap[0].job_id in self._removed:
            entry = heapq.heappop(self._heap)
            self._removed.discard(entry.job_id)
            if entry.job_id in self._entry_map:
                del self._entry_map[entry.job_id]

    def _get_ready_job(self) -> Optional[Job]:
        """Get and remove the next ready job from the queue."""
        now = time.time()
        self._cleanup_removed()

        # 🧠 OPTIMIZED: Constant time read prevents unready node shuffling bottlenecks
        if self._heap and self._heap[0].scheduled_time <= now:
            entry = heapq.heappop(self._heap)
            if entry.job_id in self._entry_map:
                del self._entry_map[entry.job_id]
            return entry.job

        return None

    @staticmethod
    def _to_timestamp(source_time: Optional[datetime]) -> float:
        reference = source_time
        if reference is None:
            reference = datetime.now(timezone.utc)
        elif reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        else:
            reference = reference.astimezone(timezone.utc)
        return reference.timestamp()

    def peek(self) -> Optional[Job]:
        """Return the highest priority job without removing it."""
        with self._lock:
            self._cleanup_removed()
            return self._heap[0].job if self._heap else None

    def peek_ready(self) -> Optional[Job]:
        """Return the highest priority ready job without removing it."""
        with self._lock:
            self._cleanup_removed()
            if not self._heap:
                return None
            entry = self._heap[0]
            if entry.scheduled_time <= time.time() and entry.job_id not in self._removed:
                return entry.job
            return None

    def remove(self, job_id: UUID) -> bool:
        """Remove a job from the queue by ID (lazy deletion)."""
        with self._lock:
            if job_id in self._entry_map and job_id not in self._removed:
                self._removed.add(job_id)
                self._condition.notify_all()
                return True
            return False

    def delete(self, job_id: UUID) -> bool:
        """Alias for remove()."""
        return self.remove(job_id)

    def get(self, job_id: UUID) -> Optional[Job]:
        """Get a job by ID without removing it."""
        with self._lock:
            if job_id in self._entry_map and job_id not in self._removed:
                return self._entry_map[job_id].job
            return None

    def contains(self, job_id: UUID) -> bool:
        """Check if a job is in the queue."""
        with self._lock:
            return job_id in self._entry_map and job_id not in self._removed

    def update_priority(self, job_id: UUID, new_priority: JobPriority) -> bool:
        """Update a job's priority."""
        with self._lock:
            if job_id not in self._entry_map or job_id in self._removed:
                return False

            job = self._entry_map[job_id].job
            job.priority = new_priority
            self._removed.add(job_id)

            if job.scheduled_at:
                scheduled_time = self._to_timestamp(job.scheduled_at)
            else:
                scheduled_time = self._to_timestamp(job.created_at)

            entry = QueueEntry(
                priority=new_priority.value,
                scheduled_time=scheduled_time,
                insertion_order=self._counter,
                job_id=job.id,
                job=job,
            )
            self._counter += 1

            heapq.heappush(self._heap, entry)
            self._entry_map[job_id] = entry
            self._removed.discard(job_id)
            self._condition.notify_all()
            return True

    def reschedule(self, job_id: UUID, scheduled_at: datetime) -> bool:
        """Reschedule a job to a new time."""
        with self._lock:
            if job_id not in self._entry_map or job_id in self._removed:
                return False

            job = self._entry_map[job_id].job
            job.scheduled_at = scheduled_at
            self._removed.add(job_id)

            entry = QueueEntry(
                priority=job.priority.value,
                scheduled_time=self._to_timestamp(scheduled_at),
                insertion_order=self._counter,
                job_id=job.id,
                job=job,
            )
            self._counter += 1

            heapq.heappush(self._heap, entry)
            self._entry_map[job_id] = entry
            self._removed.discard(job_id)

            self._condition.notify_all()
            return True

    def clear(self) -> int:
        """Remove all jobs from the queue."""
        with self._lock:
            count = len(self)
            self._heap.clear()
            self._entry_map.clear()
            self._removed.clear()
            self._counter = 0
            self._condition.notify_all()
            return count

    def shutdown(self) -> None:
        """Signal shutdown to wake up all waiting threads."""
        with self._lock:
            self._shutdown = True
            self._condition.notify_all()

    def get_stats(self) -> Dict:
        """Get queue statistics."""
        with self._lock:
            now = time.time()
            ready_count = 0
            pending_count = 0

            for entry in self._entry_map.values():
                if entry.job_id not in self._removed:
                    if entry.scheduled_time <= now:
                        ready_count += 1
                    else:
                        pending_count += 1

            priority_counts: Dict[str, int] = {}
            for entry in self._entry_map.values():
                if entry.job_id not in self._removed:
                    priority_name = JobPriority(entry.priority).name
                    priority_counts[priority_name] = priority_counts.get(
                        priority_name, 0) + 1

            return {
                "total_size": len(self),
                "ready": ready_count,
                "scheduled": pending_count,
                "by_priority": priority_counts,
                "heap_size": len(self._heap),
                "removed_count": len(self._removed),
            }

    def get_next_scheduled_time(self) -> Optional[datetime]:
        """Return the nearest scheduled time for any queued job."""
        with self._lock:
            self._cleanup_removed()
            candidates = [
                entry.scheduled_time
                for entry in self._entry_map.values()
                if entry.job_id not in self._removed
            ]
            if not candidates:
                return None
            return datetime.utcfromtimestamp(min(candidates))

    def __iter__(self) -> Iterator[Job]:
        """Iterate over jobs in priority order (non-destructive)."""
        with self._lock:
            sorted_entries = sorted(
                (e for e in self._entry_map.values()
                 if e.job_id not in self._removed)
            )
            return iter(e.job for e in sorted_entries)

    def __contains__(self, job_id: UUID) -> bool:
        """Check if a job ID is in the queue."""
        return self.contains(job_id)


__all__ = [
    "QueueEntry",
    "ThreadSafePriorityQueue",
]
