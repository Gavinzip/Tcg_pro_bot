"""Foreground scan priority over bounded backup I/O and staged-data upload.

Backup capture yields only at file/page/VM boundaries. Upload processes may be
stopped safely because they read immutable staging files, never live SQLite.
Kernel locks release on crash; no stale PID marker can block future scans.
"""
import asyncio
from contextlib import asynccontextmanager, contextmanager
import fcntl
import os
from pathlib import Path
import signal
import subprocess
import time


POLL_SECONDS = .01


def _try_lock(stream, mode):
    try:
        fcntl.flock(stream.fileno(), mode | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False


class BackupLease:
    def __init__(self, priority, deadline=None):
        self.priority, self.deadline, self.held = priority, deadline, False
        self.stream = priority.open_lock('work')

    def acquire(self):
        while True:
            if self.deadline is not None and time.monotonic() >= self.deadline:
                raise TimeoutError('backup_priority_timeout')
            if _try_lock(self.stream, fcntl.LOCK_EX):
                if not self.priority.scan_pending():
                    self.held = True
                    return
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
            time.sleep(POLL_SECONDS)

    def release(self):
        if self.held:
            fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
            self.held = False

    def checkpoint(self):
        if self.priority.scan_pending():
            self.release()
            self.acquire()

    def close(self):
        self.release()
        self.stream.close()


class JobPriority:
    def __init__(self, source):
        self.directory = Path(source) / '.tcg-drive-backup' / 'priority'
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.directory.is_symlink():
            raise ValueError('priority_directory_symlink')

    def open_lock(self, name):
        # O_NOFOLLOW prevents a lock file being redirected outside our own work.
        fd = os.open(self.directory / (name + '.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        return os.fdopen(fd, 'a+')

    def scan_pending(self):
        with self.open_lock('intent') as intent:
            if not _try_lock(intent, fcntl.LOCK_SH):
                return True
            fcntl.flock(intent.fileno(), fcntl.LOCK_UN)
            return False

    @contextmanager
    def backup_chunk(self, deadline=None):
        lease = BackupLease(self, deadline)
        try:
            lease.acquire()
            yield lease
        finally:
            lease.close()

    @asynccontextmanager
    async def scan(self):
        with self.open_lock('intent') as intent, self.open_lock('work') as work:
            try:
                # Intent stays exclusive while awaiting the current bounded
                # backup chunk, preventing backup from immediately reacquiring.
                while not _try_lock(intent, fcntl.LOCK_EX):
                    await asyncio.sleep(POLL_SECONDS)
                while not _try_lock(work, fcntl.LOCK_EX):
                    await asyncio.sleep(POLL_SECONDS)
                yield
            finally:
                fcntl.flock(work.fileno(), fcntl.LOCK_UN)
                fcntl.flock(intent.fileno(), fcntl.LOCK_UN)


def _stop_group(child, deadline):
    if child.poll() is not None:
        return False
    try:
        os.killpg(child.pid, signal.SIGSTOP)
    except ProcessLookupError:
        return False
    while time.monotonic() < deadline:
        pid, status = os.waitpid(child.pid, os.WUNTRACED | os.WNOHANG)
        if pid:
            if os.WIFSTOPPED(status):
                return True
            child.returncode = os.waitstatus_to_exitcode(status)
            return False
        time.sleep(POLL_SECONDS)
    raise subprocess.TimeoutExpired(child.args, 0)


def communicate_with_priority(child, timeout, lease):
    """Pause the whole uploader group before allowing the scan to take work.

    Original wall-clock timeouts remain bounded, including pauses. Popen keeps
    draining both pipes on every quantum; output is not duplicated on retries.
    """
    priority = lease.priority
    if not lease.held:
        raise ValueError('uploader_started_without_backup_lease')
    deadline, stopped = time.monotonic() + timeout, False
    lease.deadline = deadline
    try:
        # The caller acquired this lease BEFORE starting the uploader.
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(child.args, timeout)
            try:
                return child.communicate(timeout=min(.05, remaining))
            except subprocess.TimeoutExpired:
                if not priority.scan_pending():
                    continue
                stopped = _stop_group(child, deadline)
                if not stopped:
                    return child.communicate(timeout=max(.01, deadline - time.monotonic()))
                lease.release()
                try:
                    lease.acquire()
                except TimeoutError:
                    raise subprocess.TimeoutExpired(child.args, timeout) from None
                os.killpg(child.pid, signal.SIGCONT)
                stopped = False
    finally:
        if stopped:
            try:
                os.killpg(child.pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
