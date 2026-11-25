"""Shared cooldown and stop state for explicit per-second API refusals."""
import threading
import time


class RateLimitRecovery:
    def __init__(self, cooldown_seconds=5):
        self.cooldown_seconds = cooldown_seconds
        self._lock = threading.Lock()
        self._until = 0.
        self._stopped = False

    def deadline(self):
        with self._lock:
            return None if self._stopped else self._until

    def on_refusal(self, *, can_retry):
        """Share a cooldown, or stop new requests after recovery is exhausted."""
        with self._lock:
            if self._stopped:
                return False
            if not can_retry:
                self._stopped = True
                return False
            self._until = max(self._until, time.monotonic() + self.cooldown_seconds)
            return True
