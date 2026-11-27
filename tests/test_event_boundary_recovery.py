"""Replay the production four-request overlap failure without network access."""
import io
import json
from pathlib import Path
import unittest
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch
import uuid
from bisect import bisect_left
import random

from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.diagnostics import ScanDiagnostics
from runtime.event_leaderboards.scanner import EtherscanProvider, EventLeaderboardScanner, SourceIncomplete, VRF_V3_CONTRACT
from runtime.event_leaderboards.store import EventLeaderboardStore
from tests.test_event_leaderboard_scanner import checkout, payment, h


BASE = 1_790_000_000
RUN_DIR = Path(__file__).resolve().parents[1] / '.scratch' / 'boundary-root-cause-tests' / ('run-' + uuid.uuid4().hex)


class BoundaryRecoveryTests(unittest.TestCase):
    def test_non_numeric_success_hint_cannot_stop_five_complete_overlap_rounds(self):
        root = RUN_DIR / self._testMethodName
        root.mkdir(parents=True, exist_ok=True)
        store = EventLeaderboardStore(str(root / 'ledger.sqlite3'))
        definition = store.register_event(EventDefinition('ongoing', BASE + 10, None,
            (VRF_V3_CONTRACT,), builder=h(7)))
        source = source_key(definition, VRF_V3_CONTRACT)
        store.commit_chunk('ongoing', source, None, 1001, [], block_hash=h(1000))
        original = store.publish_snapshot('ongoing', 1000, BASE + 250)
        late = checkout(block=900, tx=1, count=3)
        new = checkout(block=1100, tx=2, count=2)
        for row in (late, new):
            row['topics'][3] = h(7)
        receipts = {r['transactionHash']: dict(transactionHash=r['transactionHash'],
            status='0x1', blockNumber=r['blockNumber'], blockHash=r['blockHash'],
            logs=[payment(r), r]) for r in (late, new)}
        clock, calls = [0.], []

        def respond(url, timeout):
            q = parse_qs(urlsplit(url).query)
            action = q['action'][0]
            calls.append(action)
            clock[0] += .01
            if action == 'eth_blockNumber':
                result = hex(1200)
            elif action == 'eth_getBlockByNumber':
                height = int(q['tag'][0], 16)
                self.assertTrue(0 <= height <= 1200)
                result = dict(number=hex(height), hash=h(height), timestamp=hex(BASE + height // 4))
            elif action == 'getblocknobytime':
                # Shape reproduces the observed path: HTTP/API success, then
                # integer decoding fails before any hint block is read.
                result = 'not-an-integer'
            elif action == 'getLogs':
                self.assertEqual(q['topic3'], [h(7)])
                low, high = int(q['fromBlock'][0]), int(q['toBlock'][0])
                result = [r for r in (late, new) if low <= int(r['blockNumber'], 16) <= high]
            elif action == 'eth_getTransactionReceipt':
                result = receipts[q['txhash'][0]]
            else:
                self.fail('unexpected action: ' + action)
            return io.BytesIO(json.dumps({'status': '1', 'result': result}).encode())

        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'test-only-no-real-key',
            'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '4'})
        scanner = EventLeaderboardScanner(store, provider, confirmations=64)
        diagnostic_lines = []
        with patch('urllib.request.urlopen', side_effect=respond), \
             patch('time.monotonic', side_effect=lambda: clock[0]), \
             patch('time.sleep', side_effect=lambda delay: clock.__setitem__(0, clock[0] + delay)):
            for _ in range(5):
                with ScanDiagnostics(sink=diagnostic_lines.append) as diagnostic:
                    result = scanner.sync('ongoing', now=BASE + 400)
                self.assertEqual(result['state'], 'active', result.get('error'))
                self.assertEqual(diagnostic.failures, {'provider_invalid_boundary_hint': 1})
                self.assertEqual(diagnostic.summary()['terminal_failures'], {})
                self.assertLessEqual(diagnostic.totals['attempts'], 40)
        observations = [json.loads(line.split(' ', 1)[1]) for line in diagnostic_lines
                        if '"kind":"provider_response"' in line or '"kind": "provider_response"' in line]
        boundary = [row for row in observations if row['observation']['action'] == 'getblocknobytime']
        log_reads = [row for row in observations if row['observation']['action'] == 'getLogs']
        self.assertEqual(len(boundary), 5)
        self.assertTrue(all(row['failure_code'] == 'provider_invalid_boundary_hint' for row in boundary))
        self.assertEqual(len(log_reads), 6)
        self.assertTrue(all(row['failure_code'] == 'ok' for row in log_reads))
        evidence = store.read_checkout_snapshot('ongoing')[1]
        self.assertEqual(len(evidence), 2)
        self.assertEqual(sum(r['draw_count'] for r in evidence), 5)
        self.assertEqual(store.get_checkpoint('ongoing', source)['next_block'], 1137)
        self.assertEqual(store.read_checkout_snapshot('ongoing', original['snapshot_id'])[1], [])

    def test_invalid_success_hints_resolve_exact_plateau_boundaries_with_bounded_reads(self):
        randomizer = random.Random(20261126)
        for case in range(120):
            with self.subTest(case=case):
                stamps, stamp = [], BASE
                for _ in range(randomizer.randint(2, 500)):
                    stamp += randomizer.choice([0, 0, 1, 2, 50])
                    stamps.append(stamp)
                target = randomizer.randint(BASE - 1, stamp + 2)
                malformed = ['not-an-integer', '-1', True, {}, [], 3.5][case % 6]
                reads, calls, clock = [], [], [0.]

                def respond(url, timeout):
                    q = parse_qs(urlsplit(url).query)
                    action = q['action'][0]
                    calls.append(action)
                    if action == 'getblocknobytime':
                        result = malformed
                    else:
                        self.assertEqual(action, 'eth_getBlockByNumber')
                        n = int(q['tag'][0], 16)
                        self.assertTrue(0 <= n < len(stamps))
                        reads.append(n)
                        result = dict(number=hex(n), hash=h(n), timestamp=hex(stamps[n]))
                    return io.BytesIO(json.dumps({'status': '1', 'result': result}).encode())

                provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'fixture',
                    'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '4'})
                with patch('urllib.request.urlopen', side_effect=respond), \
                     patch('time.monotonic', side_effect=lambda: clock[0]), \
                     patch('time.sleep', side_effect=lambda delay: clock.__setitem__(0, clock[0] + delay)), \
                     ScanDiagnostics(sink=None) as diagnostic:
                    if case % 2:
                        provider.block(len(stamps) // 2)
                    self.assertEqual(provider.first_block_at(target, len(stamps) - 1), bisect_left(stamps, target))
                self.assertLessEqual(len(reads), 2 * len(stamps).bit_length() + 6)
                self.assertEqual(calls.count('getblocknobytime'), 1)
                self.assertEqual(diagnostic.failures, {'provider_invalid_boundary_hint': 1})
                self.assertEqual(diagnostic.totals['errors'], 1)
                self.assertEqual(diagnostic.totals['retries'], 0)

    def test_quota_or_auth_rejection_never_uses_canonical_hint_recovery(self):
        for result, code in [('Daily credit quota exhausted', 'provider_quota_exhausted'),
                             ('Invalid API Key', 'provider_rejected_request')]:
            with self.subTest(code=code):
                provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'fixture'})
                with patch('urllib.request.urlopen', return_value=io.BytesIO(json.dumps(
                        {'status': '0', 'message': 'NOTOK', 'result': result}).encode())) as read, \
                     patch.object(provider, 'block', side_effect=AssertionError('must not search past rejection')):
                    with self.assertRaisesRegex(SourceIncomplete, code):
                        provider.first_block_at(BASE, 1000)
                self.assertEqual(read.call_count, 1)

    def test_bad_canonical_block_still_fails_after_hint_recovery(self):
        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'fixture'})
        payloads = [{'status': '1', 'result': 'bad-hint'},
                    {'status': '1', 'result': {'number': '0x64', 'hash': 'bad-hash', 'timestamp': hex(BASE)}}]
        clock = [0.]
        with patch('urllib.request.urlopen', side_effect=lambda *a, **k: io.BytesIO(json.dumps(payloads.pop(0)).encode())), \
             patch('time.monotonic', side_effect=lambda: clock[0]), \
             patch('time.sleep', side_effect=lambda delay: clock.__setitem__(0, clock[0] + delay)):
            with self.assertRaisesRegex(SourceIncomplete, 'invalid_hash'):
                provider.first_block_at(BASE, 100)

    def test_terminal_reorg_code_reaches_job_diagnostics_and_keeps_original_snapshot(self):
        from tests.test_event_leaderboard_scanner import Provider
        root = RUN_DIR / self._testMethodName
        root.mkdir(parents=True, exist_ok=True)
        store = EventLeaderboardStore(str(root / 'ledger.sqlite3'))
        definition = store.register_event(EventDefinition('ongoing', 100, None,
            (VRF_V3_CONTRACT,), builder=h(7)))
        source = source_key(definition, VRF_V3_CONTRACT)
        store.commit_chunk('ongoing', source, None, 1001, [], block_hash=h(1000))
        original = store.publish_snapshot('ongoing', 1000, 10000)
        provider = Provider(head=1200)
        provider.changed[1000] = h(9999)
        with ScanDiagnostics(sink=None) as job:
            result = EventLeaderboardScanner(store, provider, confirmations=64).sync('ongoing', now=20000)
        self.assertEqual(result['error'], 'checkpoint_reorg_mismatch')
        self.assertEqual(job.summary()['terminal_failures'], {'checkpoint_reorg_mismatch': 1})
        self.assertEqual(store.get_checkpoint('ongoing', source)['next_block'], 1001)
        self.assertEqual(store.read_snapshot('ongoing')[0]['snapshot_id'], original['snapshot_id'])


if __name__ == '__main__':
    unittest.main()
