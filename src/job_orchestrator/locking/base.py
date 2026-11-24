"""
Abstract base class for lock managers.

This module provides the abstract interface for distributed locking,
which is critical for preventing race conditions when multiple workers
try to process the same job or access shared resources.
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, Optional
import uuid


@dataclass
class LockInfo:
    """
    Information about an acquired lock.

    Attributes:
        lock_id: Unique identifier for this lock acquisition.
        resource: The resource identifier that is locked.
        owner: Identifier of the entity that owns the lock.
        acquired_at: When the lock was acquired.
        expires_at: When the lock will expire (None = no expiration).
        metadata: Additional metadata associated with the lock.
    """
    lock_id: str
    resource: str
    owner: str
    acquired_at: datetime
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        """Check if the lock has expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at

    @property
    def remaining_ttl(self) -> Optional[float]:
        """Get remaining time-to-live in seconds, or None if no expiration."""
        if self.expires_at is None:
            return None
        remaining = (self.expires_at - datetime.utcnow()).total_seconds()
        return max(0.0, remaining)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize lock info to dictionary."""
        return {
            "lock_id": self.lock_id,
            "resource": self.resource,
            "owner": self.owner,
            "acquired_at": self.acquired_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LockInfo":
        """Deserialize lock info from dictionary."""
        return cls(
            lock_id=data["lock_id"],
            resource=data["resource"],
            owner=data["owner"],
            acquired_at=datetime.fromisoformat(data["acquired_at"]),
            expires_at=datetime.fromisoformat(
                data["expires_at"]) if data.get("expires_at") else None,
            metadata=data.get("metadata", {}),
        )


class LockManager(ABC):
    """
    Abstract base class for lock managers.

    Lock managers provide distributed locking functionality to prevent
    race conditions when multiple workers try to access shared resources.
    """

    @abstractmethod
    def acquire(
        self,
        resource: str,
        owner: Optional[str] = None,
        timeout: Optional[float] = None,
        ttl: float = 30.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[LockInfo]:
        """Acquire a lock on a resource."""
        pass

    @abstractmethod
    def release(self, resource: str, owner: Optional[str] = None) -> bool:
        """Release a lock on a resource."""
        pass

    @abstractmethod
    def extend(self, resource: str, owner: str, ttl: float) -> bool:
        """Extend the TTL of an existing lock."""
        pass

    @abstractmethod
    def is_locked(self, resource: str) -> bool:
        """Check if a resource is currently locked."""
        pass

    @abstractmethod
    def get_lock_info(self, resource: str) -> Optional[LockInfo]:
        """Get information about a lock."""
        pass

    @contextmanager
    def lock(
        self,
        resource: str,
        owner: Optional[str] = None,
        timeout: Optional[float] = None,
        ttl: float = 30.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Generator[LockInfo, None, None]:
        """Context manager for automatic lock acquisition and release."""
        from ..core.exceptions import LockAcquisitionError

        if owner is None:
            owner = str(uuid.uuid4())

        lock_info = self.acquire(
            resource=resource,
            owner=owner,
            timeout=timeout,
            ttl=ttl,
            metadata=metadata,
        )

        if lock_info is None:
            raise LockAcquisitionError(
                lock_name=resource,
                timeout=timeout,
                owner=owner,
            )

        try:
            yield lock_info
        finally:
            self.release(resource, lock_info.owner)

    def try_lock(
        self,
        resource: str,
        owner: Optional[str] = None,
        ttl: float = 30.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[LockInfo]:
        """Try to acquire a lock without blocking."""
        return self.acquire(
            resource=resource,
            owner=owner,
            timeout=0,
            ttl=ttl,
            metadata=metadata,
        )

    def force_release(self, resource: str) -> bool:
        """Force release a lock regardless of owner."""
        return self.release(resource, owner=None)


__all__ = [
    "LockInfo",
    "LockManager",
]
