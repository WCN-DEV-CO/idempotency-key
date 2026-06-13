"""idempotency-key — tiny, zero-dependency idempotency helper for Python.

Make any operation safe to retry: the same logical request, identified by an
idempotency key, runs its side effect at most once and replays the stored result
on repeat. Sync + async. In-memory store by default; pluggable backend protocol.

Original implementation. No third-party dependencies. MIT licensed.
"""
from __future__ import annotations

import time
import threading
import hashlib
import functools
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol, Awaitable

__version__ = "0.1.0"
__all__ = ["IdempotencyStore", "InMemoryStore", "IdempotencyManager",
           "idempotent", "make_key", "ConflictError"]


class ConflictError(Exception):
    """Raised when the same key is seen with a different request fingerprint."""


@dataclass
class _Record:
    fingerprint: str
    result: Any = None
    completed: bool = False
    created_at: float = field(default_factory=time.monotonic)


class IdempotencyStore(Protocol):
    """Backend protocol — implement these 3 methods to plug in Redis, a DB, etc."""
    def get(self, key: str) -> Optional[_Record]: ...
    def put(self, key: str, record: _Record) -> None: ...
    def delete(self, key: str) -> None: ...


class InMemoryStore:
    """Thread-safe in-memory store with optional TTL eviction."""

    def __init__(self, ttl_seconds: Optional[float] = None) -> None:
        self._data: dict[str, _Record] = {}
        self._ttl = ttl_seconds
        self._lock = threading.Lock()

    def _expired(self, rec: _Record) -> bool:
        return self._ttl is not None and (time.monotonic() - rec.created_at) > self._ttl

    def get(self, key: str) -> Optional[_Record]:
        with self._lock:
            rec = self._data.get(key)
            if rec is not None and self._expired(rec):
                del self._data[key]
                return None
            return rec

    def put(self, key: str, record: _Record) -> None:
        with self._lock:
            self._data[key] = record

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)


def make_key(*parts: Any) -> str:
    """Build a stable idempotency key from arbitrary parts (deterministic hash)."""
    raw = "\x1f".join(repr(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _fingerprint(args: tuple, kwargs: dict) -> str:
    raw = repr(args) + "\x1f" + repr(sorted(kwargs.items()))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class IdempotencyManager:
    """Coordinates at-most-once execution + result replay for a given store."""

    def __init__(self, store: Optional[IdempotencyStore] = None,
                 strict_fingerprint: bool = True) -> None:
        self.store = store or InMemoryStore()
        self.strict = strict_fingerprint
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _key_lock(self, key: str) -> threading.Lock:
        with self._locks_guard:
            lk = self._locks.get(key)
            if lk is None:
                lk = self._locks[key] = threading.Lock()
            return lk

    def run(self, key: str, fn: Callable[[], Any], fingerprint: str = "") -> Any:
        """Run fn() at most once for `key`; replay the stored result on retry."""
        with self._key_lock(key):
            rec = self.store.get(key)
            if rec is not None:
                if self.strict and fingerprint and rec.fingerprint and rec.fingerprint != fingerprint:
                    raise ConflictError(f"key {key!r} reused with a different request")
                if rec.completed:
                    return rec.result
            rec = _Record(fingerprint=fingerprint or (rec.fingerprint if rec else ""))
            self.store.put(key, rec)
            result = fn()
            rec.result = result
            rec.completed = True
            self.store.put(key, rec)
            return result

    async def run_async(self, key: str, fn: Callable[[], Awaitable[Any]],
                        fingerprint: str = "") -> Any:
        # sync-guarded check/replay; the awaited body runs outside the lock-free path
        rec = self.store.get(key)
        if rec is not None and rec.completed:
            if self.strict and fingerprint and rec.fingerprint and rec.fingerprint != fingerprint:
                raise ConflictError(f"key {key!r} reused with a different request")
            return rec.result
        rec = _Record(fingerprint=fingerprint)
        self.store.put(key, rec)
        result = await fn()
        rec.result = result
        rec.completed = True
        self.store.put(key, rec)
        return result


def idempotent(manager: Optional[IdempotencyManager] = None,
               key: Optional[Callable[..., str]] = None):
    """Decorator: make a function idempotent by argument fingerprint (or custom key)."""
    mgr = manager or IdempotencyManager()

    def deco(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            k = key(*args, **kwargs) if key else make_key(func.__qualname__, args, sorted(kwargs.items()))
            fp = _fingerprint(args, kwargs)
            return mgr.run(k, lambda: func(*args, **kwargs), fingerprint=fp)
        return wrapper
    return deco
