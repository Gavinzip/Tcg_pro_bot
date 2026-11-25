"""Shared rate-limit recovery without real upstream traffic or credentials."""
from concurrent.futures import ThreadPoolExecutor
import threading
from pathlib import Path
import unittest
import uuid
from unittest.mock import patch

from runtime.event_leaderboards.diagnostics import ScanDiagnostics
from runtime.event_leaderboards.scanner import EtherscanProvider, SourceIncomplete
from tests.test_event_provider_retries import Clock
from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.scanner import EventLeaderboardScanner
from runtime.event_leaderboards.store import EventLeaderboardStore
from tests.test_event_leaderboard_scanner import Provider, checkout, payment, h, VRF_V3_CONTRACT

ROOT = Path(__file__).resolve().parents[1] / '.scratch' / 'safe-four-rps-tests' / ('run-' + uuid.uuid4().hex)


class RateRecoveryTests(unittest.TestCase):
    def provider(self):
        return EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'fixture',
                                 'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '4'})

    def test_rate_limit_then_success_keeps_identical_query_and_waits_five_seconds(self):
        provider, clock, calls = self.provider(), Clock(), []
        def read(module, action, params):
            calls.append((clock.now, module, action, dict(params)))
            if len(calls) == 1:
                raise SourceIncomplete('provider_rate_limit')
            return '0xa'
        provider._read_response = read
        with patch('time.monotonic', side_effect=clock.read), patch('time.sleep', side_effect=clock.sleep), ScanDiagnostics(sink=None) as diagnostic:
            self.assertEqual(provider.head(), 10)
        self.assertEqual([c[0] for c in calls], [100., 105.])
        self.assertEqual(calls[0][1:], calls[1][1:])
        self.assertEqual(diagnostic.totals['attempts'], 2)
        self.assertEqual(diagnostic.totals['retries'], 1)
        self.assertEqual(diagnostic.totals['pacing_wait_ms'], 5000)
        self.assertEqual(diagnostic.failures, {'provider_rate_limit': 1})

    def test_repeated_rate_limit_stops_provider_without_extra_network_calls(self):
        provider, clock, calls = self.provider(), Clock(), []
        def read(*args):
            calls.append(clock.now)
            raise SourceIncomplete('provider_rate_limit')
        provider._read_response = read
        with patch('time.monotonic', side_effect=clock.read), patch('time.sleep', side_effect=clock.sleep), ScanDiagnostics(sink=None) as diagnostic:
            for _ in range(4):
                with self.assertRaisesRegex(SourceIncomplete, 'provider_rate_limit'):
                    provider.head()
        self.assertEqual(calls, [100., 105.])
        self.assertEqual(diagnostic.totals['attempts'], 2)
        self.assertEqual(diagnostic.totals['retries'], 1)

    def test_waiting_workers_obey_cooldown_installed_while_one_is_waiting(self):
        provider, clock, starts = self.provider(), Clock(), []
        # A waiter must recheck a newly extended deadline after waking.
        provider._next = 101.
        extended = False
        def sleep(delay):
            nonlocal extended
            if not extended:
                extended = True
                self.assertTrue(provider._rate_recovery.on_refusal(can_retry=True))
            clock.sleep(delay)
        provider._read_response = lambda *args: starts.append(clock.now) or '0xa'
        with patch('time.monotonic', side_effect=clock.read), patch('time.sleep', side_effect=sleep):
            self.assertEqual(provider.head(), 10)
        self.assertEqual(starts, [105.])
        self.assertEqual(clock.delays, [1., 4.])

    def test_all_workers_stop_after_persistent_rate_limit(self):
        provider, clock = self.provider(), Clock()
        entered, release = threading.Event(), threading.Event()
        lock = threading.Lock()
        calls = []
        def read(*args):
            with lock:
                calls.append(clock.now)
                n = len(calls)
            if n == 2:
                entered.set()
                release.wait(timeout=2)
            raise SourceIncomplete('provider_rate_limit')
        provider._read_response = read
        def request():
            try:
                provider.head()
            except SourceIncomplete as error:
                return str(error)
        with patch('time.monotonic', side_effect=clock.read), patch('time.sleep', side_effect=clock.sleep):
            with ThreadPoolExecutor(max_workers=4) as executor:
                first = executor.submit(request)
                self.assertTrue(entered.wait(timeout=2))
                # Mark the repeated refusal before admitting the queued workers.
                release.set()
                self.assertEqual(first.result(), 'provider_rate_limit')
                rest = [executor.submit(request) for _ in range(3)]
                self.assertEqual([f.result() for f in rest], ['provider_rate_limit'] * 3)
        self.assertEqual(len(calls), 2)

    def test_daily_quota_unknown_receipt_and_auth_failures_do_not_retry(self):
        for code in ('provider_quota_exhausted', 'receipt_unavailable', 'provider_missing_result', 'provider_rejected_request'):
            with self.subTest(code=code):
                provider = self.provider()
                with patch.object(provider, '_read_response', side_effect=SourceIncomplete(code)) as read:
                    with self.assertRaisesRegex(SourceIncomplete, code):
                        provider.head()
                self.assertEqual(read.call_count, 1)

    def test_stopped_waiter_does_not_emit_another_request(self):
        provider, clock = self.provider(), Clock()
        provider._next = 101.
        def sleep(delay):
            provider._rate_recovery.on_refusal(can_retry=False)
            clock.sleep(delay)
        with patch.object(provider, '_read_response') as read, patch('time.monotonic', side_effect=clock.read), \
                patch('time.sleep', side_effect=sleep), ScanDiagnostics(sink=None) as diagnostic:
            with self.assertRaisesRegex(SourceIncomplete, 'provider_rate_limit'):
                provider.head()
        read.assert_not_called()
        self.assertEqual(diagnostic.totals['attempts'], 0)

    def test_non_allowlisted_action_does_not_retry_a_rate_refusal(self):
        provider = self.provider()
        with patch.object(provider, '_read_response', side_effect=SourceIncomplete('provider_rate_limit')) as read:
            with self.assertRaisesRegex(SourceIncomplete, 'provider_rate_limit'):
                provider.request('proxy', 'unknown-action')
        self.assertEqual(read.call_count, 1)

    def test_rate_and_timeout_recovery_share_the_three_attempt_cap(self):
        provider, clock, calls = self.provider(), Clock(), []
        def read(*args):
            calls.append(clock.now)
            if len(calls) == 1:
                raise SourceIncomplete('provider_rate_limit')
            clock.now += 15
            raise SourceIncomplete('provider_timeout')
        provider._read_response = read
        with patch('time.monotonic', side_effect=clock.read), patch('time.sleep', side_effect=clock.sleep):
            with self.assertRaisesRegex(SourceIncomplete, 'provider_timeout'):
                provider.head()
        self.assertEqual(calls, [100., 105., 124.])


class ScannerRateRecoveryTests(unittest.TestCase):
    def provider(self, rows, *, persistent=False):
        class Chain(Provider, EtherscanProvider):
            def __init__(self):
                Provider.__init__(self, rows)
                EtherscanProvider.__init__(self, {'EVENT_LEADERBOARD_API_KEY': 'fixture',
                                                  'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '4'})
                self.attempted = []
            def receipt(self, tx):
                return EtherscanProvider.receipt(self, tx)
            def _read_response(self, module, action, params):
                tx = params['txhash']
                self.attempted.append(tx)
                if tx == h(11) and (persistent or self.attempted.count(tx) == 1):
                    raise SourceIncomplete('provider_rate_limit')
                return self.receipts[tx]
        provider = Chain()
        for row in rows:
            provider.add_receipt([row], [payment(row)])
        return provider

    def run_case(self, name, persistent):
        rows = [checkout(block=b, tx=b) for b in (10, 11, 12)]
        provider = self.provider(rows, persistent=persistent)
        store = EventLeaderboardStore(str(ROOT / name / 'ledger.sqlite3'))
        event = store.register_event(EventDefinition('test', 100, 140, (VRF_V3_CONTRACT,)))
        scanner = EventLeaderboardScanner(store, provider, confirmations=2)
        clock = Clock()
        with patch('time.monotonic', side_effect=clock.read), patch('time.sleep', side_effect=clock.sleep), ScanDiagnostics(sink=None):
            result = scanner.sync('test', now=150)
        return rows, provider, store, event, result

    def test_transient_refusal_completes_full_payment_verification(self):
        _, provider, store, _, result = self.run_case('transient', False)
        self.assertEqual(result['state'], 'finalized')
        self.assertEqual(len(store.read_checkout_snapshot('test')[1]), 3)
        self.assertEqual(provider.attempted, [h(10), h(11), h(11), h(12)])

    def test_persistent_refusal_keeps_prefix_and_next_round_resumes(self):
        rows, provider, store, event, result = self.run_case('persistent', True)
        self.assertEqual(result['state'], 'stale')
        self.assertEqual(result['error'], 'provider_rate_limit')
        self.assertEqual(provider.attempted, [h(10), h(11), h(11)])
        self.assertEqual(store.get_checkpoint('test', source_key(event, VRF_V3_CONTRACT))['next_block'], 11)
        self.assertIsNone(store.read_snapshot('test')[0])
        resumed = Provider(rows)
        for row in rows:
            resumed.add_receipt([row], [payment(row)])
        def receipt(tx):
            self.assertNotEqual(tx, h(10), 'completed prefix must reuse verified receipt evidence')
            return resumed.receipts[tx]
        resumed.receipt = receipt
        with ScanDiagnostics(sink=None):
            self.assertEqual(EventLeaderboardScanner(store, resumed, confirmations=2).sync('test', now=150)['state'], 'finalized')
        self.assertEqual(len(store.read_checkout_snapshot('test')[1]), 3)


if __name__ == '__main__':
    unittest.main()
