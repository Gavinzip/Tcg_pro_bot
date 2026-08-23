from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
import os
import threading
import time
from typing import Any, Callable


SOURCE_CACHE_TTL_SECONDS = max(
    0,
    int(
        os.getenv(
            "PROFILE_SOURCE_CACHE_TTL_SECONDS",
            os.getenv("PROFILE_API_CACHE_TTL_SECONDS", "300"),
        )
        or "300"
    ),
)
SOURCE_CACHE_MAX_ENTRIES = max(
    1,
    int(os.getenv("PROFILE_SOURCE_CACHE_MAX_ENTRIES", "96") or "96"),
)

_CACHE_LOCK = threading.Lock()
_CACHE: OrderedDict[tuple[str, str, str], tuple[float, Any]] = OrderedDict()
_INFLIGHT: dict[tuple[str, str, str], dict[str, Any]] = {}


def _cache_key(source: str, wallet: str, language: str) -> tuple[str, str, str]:
    return (
        str(source or "").strip().lower(),
        str(wallet or "").strip().lower(),
        str(language or "").strip(),
    )


def _remove_expired_locked(now: float) -> None:
    if SOURCE_CACHE_TTL_SECONDS <= 0:
        _CACHE.clear()
        return
    expired = [
        key
        for key, (cached_at, _value) in _CACHE.items()
        if now - cached_at > SOURCE_CACHE_TTL_SECONDS
    ]
    for key in expired:
        _CACHE.pop(key, None)


def load_profile_source(
    source: str,
    wallet: str,
    language: str,
    loader: Callable[[], Any],
) -> tuple[Any, str]:
    """Load one expensive Profile source with bounded TTL/LRU and single-flight."""
    key = _cache_key(source, wallet, language)
    now = time.time()
    with _CACHE_LOCK:
        _remove_expired_locked(now)
        cached = _CACHE.get(key)
        if cached is not None and SOURCE_CACHE_TTL_SECONDS > 0:
            _CACHE.move_to_end(key)
            return deepcopy(cached[1]), "hit"

        inflight = _INFLIGHT.get(key)
        if inflight is None:
            inflight = {"event": threading.Event(), "value": None, "error": None}
            _INFLIGHT[key] = inflight
            leader = True
        else:
            leader = False

    if not leader:
        inflight["event"].wait()
        error = inflight.get("error")
        if isinstance(error, BaseException):
            raise error
        return deepcopy(inflight.get("value")), "coalesced"

    try:
        value = loader()
        stored = deepcopy(value)
        with _CACHE_LOCK:
            if SOURCE_CACHE_TTL_SECONDS > 0:
                _CACHE[key] = (time.time(), stored)
                _CACHE.move_to_end(key)
                while len(_CACHE) > SOURCE_CACHE_MAX_ENTRIES:
                    _CACHE.popitem(last=False)
            inflight["value"] = stored
        return deepcopy(stored), "miss"
    except BaseException as exc:
        with _CACHE_LOCK:
            inflight["error"] = exc
        raise
    finally:
        with _CACHE_LOCK:
            if _INFLIGHT.get(key) is inflight:
                _INFLIGHT.pop(key, None)
            inflight["event"].set()


def clear_profile_source_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
