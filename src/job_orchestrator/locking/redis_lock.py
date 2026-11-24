"""
Redis-based distributed lock manager with Redlock algorithm support.

This module provides a distributed lock manager using Redis, implementing
the Redlock algorithm for safety in distributed environments.

See: https://redis.io
"""

import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from .base import LockInfo, LockManager

logger = logging.getLogger(__name__)


# Lua scripts for atomic operations
RELEASE_SCRIPT = """
local value = redis.call("get", KEYS[1])
if value then
    local data = cjson.decode(value)
    if data and data.owner == ARGV[1] then
        return redis.call("del", KEYS[1])
    end
end
return 0
"""

EXTEND_SCRIPT = """
local value = redis.call("get", KEYS[1])
if value then
    local data = cjson.decode(value)
    if data and data.owner == ARGV[1] then
        return redis.call("pexpire", KEYS[1], ARGV[2])
    end
end
return 0
"""

GET_LOCK_SCRIPT = """
local value = redis.call("get", KEYS[1])
if value then
    local ttl = redis.call("pttl", KEYS[1])
    return {value, ttl}
else
    return nil
end
"""


class RedisLockManager(LockManager):
    """
    Distributed lock manager using Redis with Redlock algorithm.
    """

    def __init__(
        self,
        redis_urls: Optional[List[str]] = None,
        redis_client: Any = None,
        key_prefix: str = "lock:",
        retry_count: int = 3,
        retry_delay: float = 0.2,
        clock_drift_factor: float = 0.01,
    ):
        """Initialize the Redis lock manager."""
        self._redis_urls = redis_urls or []
        self._single_client = redis_client
        self._key_prefix = key_prefix
        self._retry_count = retry_count
        self._retry_delay = retry_delay
        self._clock_drift_factor = clock_drift_factor
        self._clients: List[Any] = []
        self._scripts: Dict[str, Any] = {}

        if redis_urls:
            self._init_clients_from_urls()
        elif redis_client:
            self._clients = [redis_client]
            self._register_scripts(redis_client)

    def _init_clients_from_urls(self) -> None:
        """Initialize Redis clients from URLs."""
        try:
            import redis
        except ImportError:
            raise ImportError(
                "redis package is required for RedisLockManager. "
                "Install it with: pip install redis"
            )

        for url in self._redis_urls:
            try:
                client = redis.from_url(url)
                client.ping()
                self._clients.append(client)
                self._register_scripts(client)
            except Exception as e:
                logger.warning(f"Failed to connect to Redis at {url}: {e}")

    def _register_scripts(self, client: Any) -> None:
        """Register Lua scripts with a Redis client."""
        if id(client) not in self._scripts:
            self._scripts[id(client)] = {
                "release": client.register_script(RELEASE_SCRIPT),
                "extend": client.register_script(EXTEND_SCRIPT),
                "get_lock": client.register_script(GET_LOCK_SCRIPT),
            }

    def _get_script(self, client: Any, script_name: str) -> Any:
        """Get a registered script for a client."""
        return self._scripts.get(id(client), {}).get(script_name)

    def _key(self, resource: str) -> str:
        """Get the Redis key for a resource."""
        return f"{self._key_prefix}{resource}"

    def _make_lock_value(self, owner: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Create the lock value stored in Redis."""
        data = {
            "owner": owner,
            "acquired_at": datetime.utcnow().isoformat(),
            "metadata": metadata or {},
        }
        return json.dumps(data)

    def _parse_lock_value(self, value: str) -> Dict[str, Any]:
        """Parse the lock value from Redis."""
        return json.loads(value)

    @property
    def _quorum(self) -> int:
        """Get the quorum size for Redlock."""
        return len(self._clients) // 2 + 1

    def acquire(
        self,
        resource: str,
        owner: Optional[str] = None,
        timeout: Optional[float] = None,
        ttl: float = 30.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[LockInfo]:
        """Acquire a lock using the Redlock algorithm."""
        if not self._clients:
            raise RuntimeError("No Redis clients available")

        owner = owner or str(uuid.uuid4())
        lock_key = self._key(resource)
        ttl_ms = int(ttl * 1000)
        deadline = time.time() + timeout if timeout and timeout > 0 else None

        for attempt in range(self._retry_count):
            start_time = time.time()

            if len(self._clients) == 1:
                acquired = self._acquire_single(
                    lock_key, owner, ttl_ms, metadata)
            else:
                acquired = self._acquire_redlock(
                    lock_key, owner, ttl_ms, metadata)

            if acquired:
                elapsed_ms = (time.time() - start_time) * 1000
                drift = ttl_ms * self._clock_drift_factor + 2
                validity_time_ms = ttl_ms - elapsed_ms - drift

                if validity_time_ms > 0:
                    return LockInfo(
                        lock_id=str(uuid.uuid4()),
                        resource=resource,
                        owner=owner,
                        acquired_at=datetime.utcnow(),
                        expires_at=datetime.utcnow() + timedelta(milliseconds=validity_time_ms),
                        metadata=metadata or {},
                    )
                else:
                    self._release_all(lock_key, owner)

            if deadline and time.time() >= deadline:
                return None

            if attempt < self._retry_count - 1:
                time.sleep(self._retry_delay)

        return None

    def _acquire_single(
        self,
        lock_key: str,
        owner: str,
        ttl_ms: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Acquire lock on a single Redis instance."""
        client = self._clients[0]
        value = self._make_lock_value(owner, metadata)
        try:
            result = client.set(lock_key, value, nx=True, px=ttl_ms)
            return bool(result)
        except Exception as e:
            logger.warning(f"Failed to acquire lock on Redis: {e}")
            return False

    def _acquire_redlock(
        self,
        lock_key: str,
        owner: str,
        ttl_ms: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Acquire lock using Redlock consensus algorithm."""
        value = self._make_lock_value(owner, metadata)
        acquired_count = 0
        for client in self._clients:
            try:
                result = client.set(lock_key, value, nx=True, px=ttl_ms)
                if result:
                    acquired_count += 1
            except Exception as e:
                logger.warning(
                    f"Failed to acquire lock on Redis instance: {e}")
        if acquired_count >= self._quorum:
            return True
        self._release_all(lock_key, owner)
        return False

    def _release_all(self, lock_key: str, owner: str) -> None:
        """Release lock on all Redis instances."""
        for client in self._clients:
            try:
                self._release_on_client(client, lock_key, owner)
            except Exception as e:
                logger.warning(
                    f"Failed to release lock on Redis instance: {e}")

    def _release_on_client(self, client: Any, lock_key: str, owner: str) -> bool:
        """Release lock on a specific Redis client using Lua script."""
        release_script = self._get_script(client, "release")
        if release_script:
            result = release_script(keys=[lock_key], args=[owner])
            return bool(result)
        else:
            value = client.get(lock_key)
            if value:
                try:
                    data = self._parse_lock_value(
                        value.decode() if isinstance(value, bytes) else value)
                    if data.get("owner") == owner:
                        client.delete(lock_key)
                        return True
                except (json.JSONDecodeError, KeyError):
                    pass
            return False

    def release(self, resource: str, owner: Optional[str] = None) -> bool:
        """Release a lock on a resource."""
        if not self._clients:
            return False

        lock_key = self._key(resource)
        released = False
        for client in self._clients:
            try:
                if owner:
                    if self._release_on_client(client, lock_key, owner):
                        released = True
                else:
                    if client.delete(lock_key):
                        released = True
            except Exception as e:
                logger.warning(
                    f"Failed to release lock on Redis instance: {e}")
        return released

    def extend(self, resource: str, owner: str, ttl: float) -> bool:
        """Extend the TTL of an existing lock."""
        if not self._clients:
            return False

        lock_key = self._key(resource)
        ttl_ms = int(ttl * 1000)
        extended_count = 0

        for client in self._clients:
            try:
                extend_script = self._get_script(client, "extend")
                if extend_script:
                    result = extend_script(
                        keys=[lock_key], args=[owner, ttl_ms])
                else:
                    value = client.get(lock_key)
                    if value:
                        data = self._parse_lock_value(
                            value.decode() if isinstance(value, bytes) else value)
                        if data.get("owner") == owner:
                            result = client.pexpire(lock_key, ttl_ms)
                        else:
                            result = 0
                    else:
                        result = 0
                if result:
                    extended_count += 1
            except Exception as e:
                logger.warning(f"Failed to extend lock on Redis instance: {e}")

        if len(self._clients) > 1:
            return extended_count >= self._quorum
        return extended_count > 0

    def is_locked(self, resource: str) -> bool:
        """Check if a resource is currently locked."""
        if not self._clients:
            return False

        lock_key = self._key(resource)
        locked_count = 0
        for client in self._clients:
            try:
                if client.exists(lock_key):
                    locked_count += 1
            except Exception as e:
                logger.warning(f"Failed to check lock on Redis instance: {e}")

        if len(self._clients) > 1:
            return locked_count >= self._quorum
        return locked_count > 0

    def get_lock_info(self, resource: str) -> Optional[LockInfo]:
        """Get information about a lock."""
        if not self._clients:
            return None

        lock_key = self._key(resource)
        for client in self._clients:
            try:
                value = client.get(lock_key)
                if value:
                    ttl_ms = client.pttl(lock_key)
                    data = self._parse_lock_value(
                        value.decode() if isinstance(value, bytes) else value)
                    expires_at = None
                    if ttl_ms > 0:
                        expires_at = datetime.utcnow() + timedelta(milliseconds=ttl_ms)

                    return LockInfo(
                        lock_id=str(uuid.uuid4()),
                        resource=resource,
                        owner=data["owner"],
                        acquired_at=datetime.fromisoformat(
                            data["acquired_at"]),
                        expires_at=expires_at,
                        metadata=data.get("metadata", {}),
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to get lock info from Redis instance: {e}")
                continue
        return None

    def cleanup_expired(self) -> int:
        """Clean up expired locks (handled automatically by Redis)."""
        return 0

    @property
    def is_available(self) -> bool:
        """Check if the Redis lock manager is available and connected."""
        if not self._clients:
            return False

        available_count = 0
        for client in self._clients:
            try:
                client.ping()
                available_count += 1
            except Exception:
                pass

        if len(self._clients) > 1:
            return available_count >= self._quorum
        return available_count > 0


__all__ = [
    "RedisLockManager",
]
