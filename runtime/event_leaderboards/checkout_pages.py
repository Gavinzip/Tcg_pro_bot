"""Bounded, rebuildable SQLite projections; never a cache of source health.

Only the requested page is decoded into Python objects. The durable paid
checkout ledger and the public snapshot/cursor protocol remain unchanged.
"""
from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
from threading import RLock


class CheckoutCacheUnavailable(RuntimeError):
    """A projection could not be prepared inside its local storage budget."""


@dataclass
class _Projection:
    path: Path
    db: sqlite3.Connection
    count: int
    size: int


class CheckoutPages:
    def __init__(self, *, max_entries=16, max_bytes=128 * 1024 * 1024,
                 max_disk_bytes=2 * 1024 ** 3, directory=None):
        for value in (max_entries, max_bytes, max_disk_bytes):
            if type(value) is not int or value < 1:
                raise ValueError('invalid checkout page cache bounds')
        self.max_entries = max_entries
        # A memory/write-batch ceiling, not a requirement that all history fit.
        self.max_bytes = max_bytes
        self.max_disk_bytes = max_disk_bytes
        self.directory = directory
        self._entries = OrderedDict()
        self._bytes = 0  # No full serialized projection is held in Python.
        self._disk_bytes = 0
        self._root = None
        self._lock = RLock()
        self.stats = {'hits': 0, 'builds': 0, 'materialized_rows': 0, 'evictions': 0}

    def _evict(self):
        self._discard(next(iter(self._entries)))

    def _discard(self, key):
        entry = self._entries.pop(key)
        self._disk_bytes -= entry.size
        entry.db.close()
        entry.path.unlink(missing_ok=True)
        self.stats['evictions'] += 1

    def _reserve(self, size):
        while self._entries and self._disk_bytes + size > self.max_disk_bytes:
            self._evict()
        if self._disk_bytes + size > self.max_disk_bytes:
            raise CheckoutCacheUnavailable('Checkout projection exceeds its disk budget.')

    def _build(self, load):
        path, db = None, None
        try:
            if self._root is None:
                self._root = tempfile.TemporaryDirectory(prefix='tcg-checkout-pages-', dir=self.directory)
            fd, filename = tempfile.mkstemp(prefix='view-', suffix='.sqlite', dir=self._root.name)
            os.close(fd)
            path = Path(filename)
            db = sqlite3.connect(filename, check_same_thread=False)
            # Disposable cache only. A partial build is never published.
            db.executescript('PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; '
                'PRAGMA cache_size=-1024; PRAGMA temp_store=FILE; PRAGMA mmap_size=0; '
                'CREATE TABLE rows(position INTEGER PRIMARY KEY,payload TEXT NOT NULL);')
            page_size = db.execute('PRAGMA page_size').fetchone()[0]
            db.execute(f'PRAGMA max_page_count={max(1, self.max_disk_bytes // page_size)}')
            count, batch, batch_bytes = 0, [], 0
            batch_limit = min(self.max_bytes, 1024 * 1024)

            def flush():
                nonlocal batch, batch_bytes
                if batch:
                    db.executemany('INSERT INTO rows VALUES(?,?)', batch)
                    self._reserve(db.execute('PRAGMA page_count').fetchone()[0] * page_size)
                    batch, batch_bytes = [], 0

            for row in load():
                payload = json.dumps(row, ensure_ascii=False, separators=(',', ':'))
                size = len(payload.encode('utf-8'))
                if batch and (batch_bytes + size > batch_limit or len(batch) >= 1000):
                    flush()
                batch.append((count, payload))
                batch_bytes += size
                count += 1
            flush()
            self._reserve(db.execute('PRAGMA page_count').fetchone()[0] * page_size)
            db.commit()
            entry = _Projection(path, db, count, path.stat().st_size)
            self.stats['builds'] += 1
            self.stats['materialized_rows'] += count
            return entry
        except BaseException as error:
            if db is not None:
                db.close()
            if path is not None:
                path.unlink(missing_ok=True)
            if isinstance(error, (sqlite3.Error, OSError)):
                raise CheckoutCacheUnavailable('Checkout page cache storage unavailable.') from error
            raise

    def page(self, key, offset, limit, load):
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError('invalid checkout page bounds')
        # Single flight across HTTP threads; no writes to the original ledger.
        began = time.monotonic()
        with self._lock:
            entry = self._entries.pop(key, None)
            built = entry is None
            if entry is None:
                while len(self._entries) >= self.max_entries:
                    self._evict()
                entry = self._build(load)
                self._disk_bytes += entry.size
            else:
                self.stats['hits'] += 1
            self._entries[key] = entry
            if offset > entry.count:
                raise ValueError('cursor offset outside snapshot')
            try:
                rows = [json.loads(record[0]) for record in entry.db.execute(
                    'SELECT payload FROM rows WHERE position>=? AND position<? ORDER BY position',
                    (offset, offset + limit))]
            except (sqlite3.Error, ValueError) as error:
                self._discard(key)
                raise CheckoutCacheUnavailable('Checkout page cache read unavailable.') from error
            elapsed_ms = round((time.monotonic() - began) * 1000)
            if built or elapsed_ms >= 1000:
                print('[checkout-page-cache] ' + json.dumps({
                    'kind': 'prepared' if built else 'slow_page',
                    'scope_ref': hashlib.sha256(repr(key).encode()).hexdigest()[:16],
                    'offset': offset, 'page_rows': len(rows), 'total_rows': entry.count,
                    'elapsed_ms': elapsed_ms, 'disk_bytes': self._disk_bytes,
                    'entries': len(self._entries), 'materialized_rows': self.stats['materialized_rows'],
                }, separators=(',', ':')), flush=True)
            return rows, entry.count

    def close(self):
        with self._lock:
            while self._entries:
                self._evict()
            if self._root is not None:
                self._root.cleanup()
                self._root = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
