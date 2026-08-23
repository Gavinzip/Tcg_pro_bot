from __future__ import annotations

import asyncio
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import threading
import time
from typing import Iterator, TextIO


class ProfileJobLockTimeout(TimeoutError):
    pass


_LOCAL_LOCK = threading.Lock()


def _lock_path() -> Path:
    configured = str(os.getenv("PROFILE_JOB_LOCK_PATH", "") or "").strip()
    if configured:
        return Path(configured).expanduser()
    app_env = str(os.getenv("APP_ENV", "local") or "local").strip().lower()
    default_data_dir = "/data/renaiss_sync" if app_env == "server" else "./data/renaiss_sync"
    data_dir = str(os.getenv("SYNC_DATA_DIR", default_data_dir) or default_data_dir).strip()
    return Path(data_dir) / "state" / "profile_job.lock"


def _wait_seconds() -> float:
    raw = str(os.getenv("PROFILE_JOB_LOCK_WAIT_SECONDS", "180") or "180").strip()
    try:
        return max(0.0, float(raw))
    except ValueError as exc:
        raise ValueError("PROFILE_JOB_LOCK_WAIT_SECONDS must be numeric") from exc


class ProfileJobLease:
    def __init__(self, handle: TextIO) -> None:
        self._handle = handle
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            try:
                self._handle.close()
            finally:
                _LOCAL_LOCK.release()

    def __enter__(self) -> "ProfileJobLease":
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.release()


def acquire_profile_job_lock(wait_seconds: float | None = None) -> ProfileJobLease:
    configured_wait_seconds = _wait_seconds()
    wait_seconds = (
        configured_wait_seconds
        if wait_seconds is None
        else min(configured_wait_seconds, max(0.0, float(wait_seconds)))
    )
    deadline = time.monotonic() + wait_seconds
    if not _LOCAL_LOCK.acquire(timeout=wait_seconds):
        raise ProfileJobLockTimeout("profile job queue wait timed out")

    handle: TextIO | None = None
    try:
        path = _lock_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a+", encoding="utf-8")
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return ProfileJobLease(handle)
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ProfileJobLockTimeout("profile job queue wait timed out")
                time.sleep(min(0.1, max(0.01, deadline - time.monotonic())))
    except Exception:
        if handle is not None:
            handle.close()
        _LOCAL_LOCK.release()
        raise


async def acquire_profile_job_lock_async(wait_seconds: float | None = None) -> ProfileJobLease:
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(None, acquire_profile_job_lock, wait_seconds)
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        lease = await future
        lease.release()
        raise


@contextmanager
def profile_job_lock(wait_seconds: float | None = None) -> Iterator[ProfileJobLease]:
    lease = acquire_profile_job_lock(wait_seconds)
    try:
        yield lease
    finally:
        lease.release()
