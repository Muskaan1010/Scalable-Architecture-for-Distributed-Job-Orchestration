"""
In-memory lock manager for single-node deployments.

This module provides a thread-safe lock manager that stores locks in memory.
It's suitable for single-node deployments where all workers are in the same
process or when testing locally.
"""

import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .base import LockInfo, LockManager


class InMemoryLockManager(LockManager):
    """
    Thread-safe in-memory lock manager for single-node deployments.
    """

    def __init__(
        self,
        cleanup_interval: float = 60.0,
        default_ttl: float = 30.0,
        default_timeout: float = 0.0,
    ):
        """Initialize the in-memory lock manager."""
        self._locks: Dict[str, LockInfo] = {}
        self._lock = threading.RLock()
        self._conditions: Dict[str, threading.Condition] = {}
        self._cleanup_interval = cleanup_interval
        self._shutdown_event = threading.Event()
        self._cleanup_thread: Optional[threading.Thread] = None
        self.default_ttl = default_ttl
        self.default_timeout = default_timeout

        if cleanup_interval > 0:
            self._start_cleanup_thread()

    def _start_cleanup_thread(self) -> None:
        """Start the background cleanup thread."""
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
            name="lock-cleanup"
        )
        self._cleanup_thread.start()

    def _cleanup_loop(self) -> None:
        """Background loop that periodically cleans up expired locks."""
        while not self._shutdown_event.is_set():
            self._shutdown_event.wait(timeout=self._cleanup_interval)
            if not self._shutdown_event.is_set():
                self.cleanup_expired()

    def shutdown(self) -> None:
        """Shutdown the lock manager and stop cleanup thread."""
        self._shutdown_event.set()
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5.0)

    def _get_condition(self, resource: str) -> threading.Condition:
        """Get or create a condition variable for a resource."""
        if resource not in self._conditions:
            self._conditions[resource] = threading.Condition(self._lock)
        return self._conditions[resource]

    def _is_expired(self, lock_info: LockInfo) -> bool:
        """Check if a lock has expired."""
        if lock_info.expires_at is None:
            return False
        return datetime.utcnow() > lock_info.expires_at

    def acquire(
        self,
        resource: str,
        owner: Optional[str] = None,
        timeout: Optional[float] = None,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[LockInfo]:
        """Acquire a lock on a resource with optional blocking wait."""
        owner = owner or str(uuid.uuid4())
        effective_timeout = (
            timeout
            if timeout is not None
            else (self.default_timeout if self.default_timeout > 0 else None)
        )
        deadline = (
            time.time() + effective_timeout
            if effective_timeout and effective_timeout > 0
            else None
        )

        while True:
            with self._lock:
                existing = self._locks.get(resource)

                if existing is None or self._is_expired(existing):
                    effective_ttl = ttl if ttl is not None else self.default_ttl
                    lock_info = LockInfo(
                        lock_id=str(uuid.uuid4()),
                        resource=resource,
                        owner=owner,
                        acquired_at=datetime.utcnow(),
                        expires_at=(
                            datetime.utcnow() + timedelta(seconds=effective_ttl)
                            if effective_ttl and effective_ttl > 0
                            else None
                        ),
                        metadata=metadata or {},
                    )
                    self._locks[resource] = lock_info
                    return lock_info

                if existing.owner == owner:
                    effective_ttl = ttl if ttl is not None else self.default_ttl
                    if effective_ttl and effective_ttl > 0:
                        existing.expires_at = datetime.utcnow() + timedelta(seconds=effective_ttl)
                    return existing

                if effective_timeout is None or effective_timeout <= 0:
                    return None

                if deadline and time.time() >= deadline:
                    return None

                condition = self._get_condition(resource)
                remaining = deadline - time.time() if deadline else None
                condition.wait(timeout=min(remaining, 0.1)
                               if remaining else 0.1)

            if deadline and time.time() >= deadline:
                return None

    def release(self, resource: str, owner: Optional[str] = None) -> bool:
        """Release a lock on a resource."""
        with self._lock:
            existing = self._locks.get(resource)
            if existing is None:
                return False
            if owner is not None and existing.owner != owner:
                return False

            del self._locks[resource]
            if resource in self._conditions:
                self._conditions[resource].notify_all()
            return True

    def extend(self, resource: str, owner: str, ttl: float) -> bool:
        """Extend the TTL of an existing lock."""
        with self._lock:
            existing = self._locks.get(resource)
            if existing is None:
                return False
            if existing.owner != owner:
                return False
            if self._is_expired(existing):
                del self._locks[resource]
                return False

            existing.expires_at = datetime.utcnow() + timedelta(seconds=ttl) if ttl > 0 else None
            return True

    def is_locked(self, resource: str) -> bool:
        """Check if a resource is currently locked."""
        with self._lock:
            existing = self._locks.get(resource)
            if existing is None:
                return False
            if self._is_expired(existing):
                del self._locks[resource]
                return False
            return True

    def get_lock_info(self, resource: str) -> Optional[LockInfo]:
        """Get information about a lock."""
        with self._lock:
            existing = self._locks.get(resource)
            if existing is None:
                return None
            if self._is_expired(existing):
                del self._locks[resource]
                return None
            return existing

    def cleanup_expired(self) -> int:
        """Remove all expired locks."""
        with self._lock:
            expired_resources = [
                resource
                for resource, lock_info in self._locks.items()
                if self._is_expired(lock_info)
            ]
            for resource in expired_resources:
                del self._locks[resource]
                if resource in self._conditions:
                    self._conditions[resource].notify_all()
            return len(expired_resources)

    def get_all_locks(self) -> List[LockInfo]:
        """Get all currently active locks."""
        with self._lock:
            self.cleanup_expired()
            return list(self._locks.values())

    def clear(self) -> int:
        """Clear all locks forcibly."""
        with self._lock:
            count = len(self._locks)
            self._locks.clear()
            for condition in self._conditions.values():
                condition.notify_all()
            return count

    @property
    def lock_count(self) -> int:
        """Get the number of currently active locks."""
        with self._lock:
            return sum(
                1 for lock_info in self._locks.values()
                if not self._is_expired(lock_info)
            )

    def __len__(self) -> int:
        return self.lock_count

    def __contains__(self, resource: str) -> bool:
        return self.is_locked(resource)

    def __enter__(self) -> "InMemoryLockManager":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown()


__all__ = [
    "InMemoryLockManager",
]
