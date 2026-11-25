"""Bounded receipt prefetch; the scanner alone commits results in block order."""
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context

from .diagnostics import CURRENT, ScanDiagnostics, phase


class ReceiptPrefetch:
    def __init__(self, transactions, receipts, load, workers):
        self.transactions = iter(dict.fromkeys(tx for tx in transactions if tx not in receipts))
        self.receipts, self.load, self.workers = receipts, load, workers
        self.pending = {}

    def __enter__(self):
        self.executor = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix='checkout-receipt')
        self._fill()
        return self

    def _load(self, tx):
        parent = CURRENT.get()
        # A separate phase stack per task; only closed counters merge to parent.
        with ScanDiagnostics(sink=None, run_id=parent.run_id if parent else None):
            with phase('receipt'):
                return self.load(tx)

    def _fill(self):
        while len(self.pending) < 2 * self.workers:
            tx = next(self.transactions, None)
            if tx is None:
                break
            self.pending[tx] = self.executor.submit(copy_context().run, self._load, tx)

    def resolve(self, tx):
        future = self.pending.get(tx)
        if future is not None:
            # Surface a later failure only when ordered consumption reaches it.
            self.receipts[tx] = future.result()
            del self.pending[tx]
            self._fill()

    def __exit__(self, *error):
        for future in self.pending.values():
            future.cancel()
        self.executor.shutdown(wait=True, cancel_futures=True)
