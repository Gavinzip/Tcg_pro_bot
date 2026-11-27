"""Read-only evidence for omissions; never retry, erase, or forgive an incomplete replay."""
import hashlib
import io
import json
from pathlib import Path
import random
import unittest
import uuid
from unittest.mock import patch

from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.diagnostics import ScanDiagnostics, PREFIX, validated_line
from runtime.event_leaderboards.log_window import LogWindow
from runtime.event_leaderboards.scanner import EventLeaderboardScanner, VRF_V3_CONTRACT
from runtime.event_leaderboards.store import EventLeaderboardStore
from runtime.event_leaderboards.overlap_evidence import overlap_comparison, valid_overlap_comparison, SAMPLE_LIMIT
from runtime.event_leaderboards.scanner import EtherscanProvider
from tests.test_event_leaderboard_scanner import checkout, h
from tests.test_event_v3_event_proof import BASE, CODE, PinnedProvider

RUN_DIR = Path(__file__).resolve().parents[1] / '.scratch' / 'overlap-root-cause-tests' / ('run-' + uuid.uuid4().hex)


class OverlapEvidenceTests(unittest.TestCase):
    def test_query_window_and_verification_omissions_are_distinguishable(self):
        key = (h(1), 2)
        known = {key: {'block_number': 10}}
        cases = [
            (set(), set(), set(), (1, 1)),
            ({key}, set(), set(), (0, 1)),
            ({key}, {key}, set(), (0, 0)),
        ]
        for query, window, verified, expected in cases:
            with self.subTest(expected=expected):
                value = overlap_comparison(10, 20, known, window, verified, (10, 30, query))
                self.assertEqual((value['missing_in_query_count'], value['missing_in_window_count']), expected)
                self.assertTrue(valid_overlap_comparison(value))
        self.assertIsNone(overlap_comparison(10, 20, known, {key}, {key}, (10, 30, {key})))
        unknown_query = overlap_comparison(10, 20, known, set(), set())
        self.assertFalse(unknown_query['query_available'])
        self.assertNotIn('missing_in_query_count', unknown_query)
        self.assertTrue(valid_overlap_comparison(unknown_query))

    def test_comparison_is_bounded_and_supervisor_rejects_private_or_malformed_fields(self):
        known = {(h(index + 1), index): {'block_number': 10 + index % 5} for index in range(1000)}
        value = overlap_comparison(10, 20, known, set(), set(), (10, 30, set()))
        self.assertEqual(len(value['samples']), SAMPLE_LIMIT)
        lines = []
        with ScanDiagnostics(sink=lines.append) as diagnostic:
            diagnostic.emit('overlap_comparison', comparison=value)
        record = json.loads(lines[1][len(PREFIX):])
        self.assertLess(len(lines[1].encode()), 4096)
        self.assertIsNotNone(validated_line(lines[1]))
        for change in ({'url': 'https://private/?apikey=secret'}, {'known_count': True},
                       {'missing_count': -1}, {'query_digest': h(1)}, {'samples': value['samples'] * 2}):
            with self.subTest(change=change):
                bad = {**record, 'comparison': {**value, **change}}
                self.assertIsNone(validated_line(PREFIX + json.dumps(bad)))
        bad = json.loads(json.dumps(record))
        bad['comparison']['samples'][0]['wallet'] = 'private-wallet'
        self.assertIsNone(validated_line(PREFIX + json.dumps(bad)))
        bad = json.loads(json.dumps(record))
        bad['comparison']['samples'][0]['identity_ref'] = h(1)
        self.assertIsNone(validated_line(PREFIX + json.dumps(bad)))

    def test_successful_getlogs_records_safe_response_evidence_without_extra_request(self):
        secret = 'never-log-credential'
        payload = {'status': '1', 'message': 'OK', 'result': [{'transactionHash': h(1),
            'wallet': 'never-log-wallet', 'url': 'https://private.invalid/?apikey=' + secret}]}
        raw = json.dumps(payload).encode()
        response = io.BytesIO(raw)
        response.status = 200
        response.headers = {'Content-Type': 'application/json'}
        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': secret})
        lines = []
        with patch('urllib.request.urlopen', return_value=response) as request, ScanDiagnostics(sink=lines.append):
            self.assertEqual(provider.logs_page(VRF_V3_CONTRACT, h(5), 10, 20, 1, builder=h(7)), payload['result'])
        self.assertEqual(request.call_count, 1)
        records = [json.loads(line[len(PREFIX):]) for line in lines]
        observed = [row for row in records if row['kind'] == 'provider_response']
        self.assertEqual(len(observed), 1)
        evidence = observed[0]['observation']
        self.assertEqual(evidence['result_length'], 1)
        self.assertEqual(evidence['body_digest'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(evidence['request'], {'from_block': 10, 'to_block': 20, 'page': 1})
        self.assertEqual(observed[0]['failure_code'], 'ok')
        serialized = '\n'.join(lines)
        for private in [secret, 'never-log-wallet', 'private.invalid', h(1)]:
            self.assertNotIn(private, serialized)

    def test_three_incomplete_responses_then_same_window_recovers_with_97_existing_rows(self):
        root = RUN_DIR / self._testMethodName
        root.mkdir(parents=True)
        store = EventLeaderboardStore(str(root / 'ledger.sqlite3'))
        definition = store.register_event(EventDefinition('proof', BASE * 10, None,
            (VRF_V3_CONTRACT,), builder=h(7)))
        source = source_key(definition, VRF_V3_CONTRACT)
        rows = []
        for index in range(97):
            block = BASE + index % 15
            row = checkout(block=block, tx=index + 100, index=index + 2, count=1)
            row['topics'][3] = h(7)
            row['timeStamp'] = hex(block * 10)
            rows.append(row)
        provider = PinnedProvider(rows)
        scanner = EventLeaderboardScanner(store, provider, confirmations=2,
            v3_event_proof=True, combine_log_queries=True)
        lines = []
        with patch('runtime.event_leaderboards.v3_evidence.AUDITED_CODE_SHA256',
                   hashlib.sha256(bytes.fromhex(CODE[2:])).hexdigest()), ScanDiagnostics(sink=lines.append):
            old = scanner.sync('proof', now=BASE * 10 + 500)['snapshot']['snapshot_id']
            checkpoint = store.get_checkpoint('proof', source)
            provider.rows = rows[1:]
            for iteration in range(3):
                provider.height += 1
                provider.calls.clear()
                result = scanner.sync('proof', now=BASE * 10 + 600 + iteration * 60)
                self.assertEqual(result['error'], 'overlap_missing_known_checkout')
                self.assertEqual(store.get_checkpoint('proof', source), checkpoint)
                self.assertEqual(store.read_snapshot('proof')[0]['snapshot_id'], old)
                self.assertEqual(len(provider.calls), 1, 'diagnostics must not issue another getLogs')
            provider.rows = rows
            provider.height += 1
            result = scanner.sync('proof', now=BASE * 10 + 800)
        self.assertEqual(result['state'], 'active')
        self.assertEqual(result['overlap'][source]['inserted'], 0)
        self.assertEqual(result['overlap'][source]['verified_checkouts'], 97)
        self.assertEqual(len(store.read_checkout_snapshot('proof')[1]), 97)
        self.assertEqual(provider.receipt_calls, [])
        records = [json.loads(line[len(PREFIX):]) for line in lines]
        comparisons = [row for row in records if row['kind'] == 'overlap_comparison']
        self.assertEqual(len(comparisons), 3, 'missing evidence must survive the real scanner boundary')
        for record in comparisons:
            evidence = record['comparison']
            self.assertEqual(evidence['known_count'], 97)
            self.assertEqual(evidence['verified_count'], 96)
            self.assertEqual(evidence['missing_count'], 1)
            self.assertEqual(evidence['missing_in_query_count'], 1)
            self.assertEqual(evidence['missing_in_window_count'], 1)
            self.assertTrue(evidence['query_available'])
            self.assertEqual(evidence['samples'][0]['block_number'], BASE)
            self.assertFalse(evidence['samples'][0]['in_query'])
            self.assertFalse(evidence['samples'][0]['in_window'])
            self.assertIsNotNone(validated_line(PREFIX + json.dumps(record)))
            serialized = json.dumps(evidence)
            self.assertNotIn(rows[0]['transactionHash'], serialized)
            self.assertNotIn(rows[0]['topics'][1], serialized)

    def test_combined_window_filter_preserves_every_boundary_identity(self):
        rng = random.Random(20261127)
        for _ in range(1000):
            start = rng.randrange(0, 50)
            end = start + rng.randrange(0, 25)
            forward_end = end + rng.randrange(1, 25)
            rows = [{'blockNumber': hex(height), 'logIndex': hex(index)}
                    for height in range(start, forward_end + 1)
                    for index in range(rng.randrange(1, 4))]
            calls = []
            def read(low, high):
                calls.append((low, high))
                return [r for r in rows if low <= int(r['blockNumber'], 16) <= high]
            window = LogWindow(read, forward_start=end + 1, forward_end=forward_end, combine=True)
            overlap = window(start, end)
            forward = window(end + 1, forward_end)
            self.assertEqual(overlap + forward, rows)
            self.assertEqual(calls, [(start, forward_end)])


if __name__ == '__main__':
    unittest.main()
