"""Bounded reuse of immutable checkout projections, never of source health.

Only the requested page escapes this module, as an independent copy. Cache keys
include event/revision/snapshot/builder; a new publication cannot reuse old rows.
The byte bound measures serialized payload bytes, not Python heap consumption.
"""
from collections import OrderedDict
from copy import deepcopy
import json
from threading import RLock


class CheckoutPages:
    def __init__(self, *, max_entries=2, max_bytes=128 * 1024 * 1024):
        if type(max_entries) is not int or max_entries < 1 or type(max_bytes) is not int or max_bytes < 1:
            raise ValueError('invalid checkout page cache bounds')
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self._entries = OrderedDict()
        self._bytes = 0
        self._lock = RLock()

    def page(self, key, offset, limit, load):
        # Single flight across HTTP threads. This does not hold a SQLite write
        # transaction, delay publication, or cache a failed projection.
        with self._lock:
            found = self._entries.pop(key, None)
            if found is None:
                rows = load()
                size = len(json.dumps(rows, ensure_ascii=False, separators=(',', ':')).encode())
                found = (rows, size)
                if size <= self.max_bytes:
                    while self._entries and (len(self._entries) >= self.max_entries
                            or self._bytes + size > self.max_bytes):
                        _, (_, removed) = self._entries.popitem(last=False)
                        self._bytes -= removed
                    self._bytes += size
            else:
                rows, size = found
            # Oversized projections use the exact uncached read, not older data.
            if found[1] <= self.max_bytes:
                self._entries[key] = found
            rows = found[0]
            if offset > len(rows):
                raise ValueError('cursor offset outside snapshot')
            return deepcopy(rows[offset:offset + limit]), len(rows)
