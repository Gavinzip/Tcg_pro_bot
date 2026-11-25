"""Ordered durable prefixes, bounded work and telemetry under concurrent I/O."""
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.diagnostics import ScanDiagnostics
from runtime.event_leaderboards.scanner import EtherscanProvider, EventLeaderboardScanner, SourceIncomplete
from runtime.event_leaderboards.store import EventLeaderboardStore
from runtime.event_leaderboards.verification import ReceiptPrefetch
from tests.test_event_leaderboard_scanner import Provider, checkout, payment, h, VRF_V3_CONTRACT

ROOT = Path(__file__).resolve().parents[1] / '.scratch' / 'verification-concurrency-tests' / ('run-' + uuid.uuid4().hex)


class ConcurrentVerificationTests(unittest.TestCase):
    def scan(self, provider, name, **options):
        path = ROOT / name / 'ledger.sqlite3'
        if path.exists():
            raise RuntimeError('test_path_already_exists')
        store = EventLeaderboardStore(str(path))
        event = store.register_event(EventDefinition('test', 100, 140, (VRF_V3_CONTRACT,)))
        scanner = EventLeaderboardScanner(store, provider, confirmations=2, verification_workers=3, **options)
        return store, event, scanner

    def test_failure_preserves_only_complete_ordered_blocks_and_resumes(self):
        rows = [checkout(block=b, tx=b) for b in (10, 11, 12)]
        class DelayedFailure(Provider):
            fail = True
            def receipt(self, tx):
                if tx == h(10):
                    time.sleep(.03)  # Failure in a later block completes first.
                if tx == h(11) and self.fail:
                    raise SourceIncomplete('receipt_unavailable')
                return super().receipt(tx)
        provider = DelayedFailure(rows)
        for row in rows:
            provider.add_receipt([row], [payment(row)])
        store, event, scanner = self.scan(provider, 'ordered-failure')
        self.assertEqual(scanner.sync('test', now=150)['state'], 'stale')
        self.assertEqual(store.get_checkpoint('test', source_key(event, VRF_V3_CONTRACT))['next_block'], 11)
        self.assertIsNone(store.read_snapshot('test')[0])
        provider.fail = False
        self.assertEqual(scanner.sync('test', now=150)['state'], 'finalized')
        self.assertEqual(len(store.read_checkout_snapshot('test')[1]), 3)

    def test_failure_in_same_block_never_commits_partial_block(self):
        rows = [checkout(block=10, tx=1), checkout(block=10, tx=2, index=3)]
        provider = Provider(rows)
        for row in rows:
            provider.add_receipt([row], [payment(row)])
        provider.receipts[h(2)]['status'] = '0x0'
        store, event, scanner = self.scan(provider, 'same-block-failure')
        self.assertEqual(scanner.sync('test', now=150)['state'], 'stale')
        self.assertIsNone(store.get_checkpoint('test', source_key(event, VRF_V3_CONTRACT)))

    def test_workers_overlap_io_and_preserve_context_diagnostics(self):
        rows = [checkout(block=b, tx=b) for b in (10, 11, 12)]
        barrier = threading.Barrier(3)
        class Parallel(Provider):
            def receipt(self, tx):
                from runtime.event_leaderboards.diagnostics import record_request
                barrier.wait(timeout=2)
                record_request('eth_getTransactionReceipt', attempted=True, elapsed_ms=10)
                return super().receipt(tx)
        provider = Parallel(rows)
        for row in rows:
            provider.add_receipt([row], [payment(row)])
        store, _, scanner = self.scan(provider, 'context')
        with ScanDiagnostics(sink=None) as diagnostic:
            self.assertEqual(scanner.sync('test', now=150)['state'], 'finalized')
        summary = diagnostic.summary()
        self.assertEqual(summary['totals']['attempts'], 3)
        self.assertEqual(summary['phase_actions']['receipt']['eth_getTransactionReceipt'], 3)
        self.assertEqual(len(store.read_checkout_snapshot('test')[1]), 3)

    def test_one_canonical_block_request_for_simultaneous_receipts(self):
        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'fixture'})
        calls = []
        def request(*args, **kwargs):
            calls.append(kwargs)
            time.sleep(.03)
            return {'number': '0xa', 'hash': h(10), 'timestamp': '0x64'}
        provider.request = request
        with ThreadPoolExecutor(max_workers=4) as executor:
            blocks = list(executor.map(provider.block, [10] * 4))
        self.assertEqual(len(calls), 1)
        self.assertTrue(all(row == blocks[0] for row in blocks))

    def test_prefetch_is_bounded_deduplicated_and_consumed_in_order(self):
        gate = threading.Event()
        lock = threading.Lock()
        calls = []
        def load(tx):
            with lock:
                calls.append(tx)
            gate.wait(timeout=2)
            return tx
        receipts = {}
        with ReceiptPrefetch([i // 2 for i in range(200)], receipts, load, 3) as pending:
            self.assertEqual(len(pending.pending), 6)
            time.sleep(.02)
            self.assertLessEqual(len(calls), 3)
            gate.set()
            for tx in range(100):
                pending.resolve(tx)
                self.assertEqual(receipts[tx], tx)
                self.assertLessEqual(len(pending.pending), 6)
        self.assertEqual(sorted(calls), list(range(100)))

    def test_hard_interrupt_keeps_complete_prefix(self):
        rows = [checkout(block=10, tx=10), checkout(block=11, tx=11)]
        class Interrupted(Provider):
            def receipt(self, tx):
                if tx == h(11):
                    raise KeyboardInterrupt()
                return super().receipt(tx)
        provider = Interrupted(rows)
        for row in rows:
            provider.add_receipt([row], [payment(row)])
        store, event, scanner = self.scan(provider, 'interrupt')
        with self.assertRaises(KeyboardInterrupt):
            scanner.sync('test', now=150)
        self.assertEqual(store.get_checkpoint('test', source_key(event, VRF_V3_CONTRACT))['next_block'], 11)

    def test_reorg_mismatch_in_worker_stops_publication(self):
        row = checkout(block=10, tx=10)
        provider = Provider([row])
        provider.add_receipt([row], [payment(row)])
        provider.changed[10] = h(999)
        store, event, scanner = self.scan(provider, 'reorg')
        result = scanner.sync('test', now=150)
        self.assertEqual(result['error'], 'receipt_reorg_mismatch')
        self.assertIsNone(store.get_checkpoint('test', source_key(event, VRF_V3_CONTRACT)))
        self.assertIsNone(store.read_snapshot('test')[0])


if __name__ == '__main__':
    unittest.main()
