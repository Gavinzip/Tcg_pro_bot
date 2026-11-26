"""Receipt-free V3 must be pinned, range-anchored, and resumable; no HTTP."""
import hashlib
from pathlib import Path
import unittest
import uuid
from unittest.mock import patch

from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.scanner import EventLeaderboardScanner, SourceIncomplete
from runtime.event_leaderboards.store import EventLeaderboardStore
from runtime.event_leaderboards.v3_evidence import AUDITED_IMPLEMENTATION, AUDITED_SINCE, UPGRADED
from tests.test_event_leaderboard_scanner import Provider, checkout, payment, h, data, USDT, VRF_V3_CONTRACT, DEFAULT_PACK_CONTRACTS

RUN_DIR = Path(__file__).resolve().parents[1] / '.scratch' / 'event-v3-proof-tests' / ('run-' + uuid.uuid4().hex)
BASE = AUDITED_SINCE + 100
CODE = '0x6000'


class PinnedProvider(Provider):
    page_size = 1000

    def __init__(self, rows=(), head=BASE + 20):
        super().__init__(rows, head)
        self.proof_calls = []
        self.receipt_calls = []
        self.implementation = AUDITED_IMPLEMENTATION
        self.code = CODE
        self.upgrades = []
        self.block_cache = {}

    def head(self):
        self.block_cache.clear()
        return super().head()

    def block(self, height):
        if height not in self.block_cache:
            self.block_cache[height] = super().block(height)
        return self.block_cache[height]

    def request(self, module, action, **params):
        self.proof_calls.append((action, params))
        if action == 'eth_getStorageAt':
            return '0x' + '0' * 24 + self.implementation[2:]
        if action == 'eth_getCode':
            return self.code
        raise AssertionError(action)

    def logs_page(self, contract, topic, start, end, page, *, builder=None):
        if topic == UPGRADED:
            self.proof_calls.append(('upgrade_logs', {'start': start, 'end': end}))
            return self.upgrades
        return super().logs_page(contract, topic, start, end, page, builder=builder)

    def receipt(self, tx):
        self.receipt_calls.append(tx)
        return super().receipt(tx)


class V3EventProofTests(unittest.TestCase):
    def setUp(self):
        self.root = RUN_DIR / self._testMethodName
        self.root.mkdir(parents=True)
        self.store = EventLeaderboardStore(str(self.root / 'ledger.sqlite3'))
        self.definition = self.store.register_event(EventDefinition('proof', BASE * 10, None,
            (VRF_V3_CONTRACT,), builder=h(7)))
        self.source = source_key(self.definition, VRF_V3_CONTRACT)
        self.pin = patch('runtime.event_leaderboards.v3_evidence.AUDITED_CODE_SHA256',
                         hashlib.sha256(bytes.fromhex(CODE[2:])).hexdigest())
        self.pin.start()
        self.addCleanup(self.pin.stop)

    def row(self, block=BASE, tx=1, **kwargs):
        row = checkout(block=block, tx=tx, **kwargs)
        row['topics'][3] = h(7)
        row['timeStamp'] = hex(block * 10)
        return row

    def scanner(self, provider, **options):
        return EventLeaderboardScanner(self.store, provider, confirmations=2,
            v3_event_proof=True, combine_log_queries=True, **options)

    def test_pinned_event_uses_no_receipt_and_preserves_checkout_payload(self):
        row = self.row()
        p = PinnedProvider([row])  # deliberately no receipt exists
        result = self.scanner(p).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['state'], 'active', result)
        evidence = self.store.read_checkout_snapshot('proof')[1]
        self.assertEqual((len(evidence), evidence[0]['draw_count']), (1, 3))
        self.assertEqual(evidence[0]['timestamp'], BASE * 10)
        self.assertEqual(p.receipt_calls, [])
        self.assertEqual([x[0] for x in p.proof_calls].count('eth_getCode'), 1)
        self.assertEqual([x[0] for x in p.proof_calls].count('eth_getStorageAt'), 1)
        self.assertEqual(result['verification'][VRF_V3_CONTRACT]['mode'], 'audited_v3_event')
        self.assertEqual(result['snapshot']['scan_verification'][VRF_V3_CONTRACT],
                         result['verification'][VRF_V3_CONTRACT])

    def test_only_confirmed_endpoint_state_is_read_and_whole_range_is_upgrade_checked(self):
        p = PinnedProvider([self.row()])
        original = p.request
        def no_old_state(module, action, **params):
            self.assertEqual(params.get('tag'), hex(p.height - 2))
            return original(module, action, **params)
        p.request = no_old_state
        result = self.scanner(p).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['state'], 'active', result)
        queries = [params for action, params in p.proof_calls if action == 'upgrade_logs']
        self.assertEqual(queries, [{'start': BASE, 'end': p.height - 2}])
        self.assertEqual(p.receipt_calls, [])

    def test_version_code_and_in_range_upgrade_each_stop_before_commit(self):
        for case in ('implementation', 'code', 'upgrade', 'malformed_storage'):
            with self.subTest(case=case):
                p = PinnedProvider([self.row()])
                if case == 'implementation': p.implementation = '0x' + 'f' * 40
                if case == 'code': p.code = '0x6001'
                if case == 'upgrade':
                    p.upgrades = [dict(self.row(), topics=[UPGRADED, h(1)], data='0x')]
                if case == 'malformed_storage':
                    p.request = lambda *a, **k: '0x123'
                result = self.scanner(p).sync('proof', now=BASE * 10 + 500)
                self.assertEqual(result['state'], 'stale', result)
                self.assertIsNone(self.store.get_checkpoint('proof', self.source))
                self.assertIsNone(self.store.read_snapshot('proof')[0])
                self.assertEqual(p.receipt_calls, [])

    def test_range_before_audited_implementation_is_rejected(self):
        self.store.register_event(EventDefinition('early', (AUDITED_SINCE - 1) * 10, None,
            (VRF_V3_CONTRACT,), builder=h(7)))
        result = self.scanner(PinnedProvider()).sync('early', now=BASE * 10 + 500)
        self.assertEqual(result['error'], 'v3_unaudited_block_range')

    def test_bad_event_time_keeps_verified_prefix_and_resumes(self):
        a, b = self.row(), self.row(BASE + 1, 2, index=3)
        p = PinnedProvider([a, b])
        b['timeStamp'] = hex((BASE + 99) * 10)
        result = self.scanner(p).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['error'], 'event_timestamp_outside_anchors')
        self.assertEqual(self.store.get_checkpoint('proof', self.source)['next_block'], BASE + 1)
        b['timeStamp'] = hex((BASE + 1) * 10)
        self.assertEqual(self.scanner(p).sync('proof', now=BASE * 10 + 500)['state'], 'active')
        self.assertEqual(len(self.store.read_checkout_snapshot('proof')[1]), 2)
        self.assertEqual(p.receipt_calls, [])

    def test_same_block_failure_does_not_commit_a_partial_block(self):
        a, b = self.row(), self.row(tx=2, index=3)
        b['data'] = data(int(USDT, 16), 96, 10, 2, 30, 32)  # impossible noncontiguous ids
        result = self.scanner(PinnedProvider([a, b])).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['error'], 'invalid_audited_v3_checkout')
        self.assertIsNone(self.store.get_checkpoint('proof', self.source))

    def test_overlap_and_forward_share_one_complete_query_and_repair_late_rows(self):
        a = self.row(BASE + 10)
        p = PinnedProvider([a])
        first = self.scanner(p).sync('proof', now=BASE * 10 + 500)
        pin = first['snapshot']['snapshot_id']
        old_verification = first['snapshot']['scan_verification']
        p.rows.extend([self.row(BASE + 11, 2, index=3), self.row(BASE + 21, 3, index=4)])
        p.height += 10
        p.calls.clear()
        result = self.scanner(p).sync('proof', now=BASE * 10 + 600)
        self.assertEqual(result['state'], 'active', result)
        self.assertEqual(len(p.calls), 1, p.calls)
        self.assertEqual(result['overlap'][self.source]['inserted'], 1)
        self.assertEqual(len(self.store.read_checkout_snapshot('proof')[1]), 3)
        self.assertEqual(len(self.store.read_checkout_snapshot('proof', pin)[1]), 1)
        self.assertEqual(self.store.read_checkout_snapshot('proof', pin)[0]['scan_verification'], old_verification)
        p.calls.clear()
        self.assertEqual(self.scanner(p).sync('proof', now=BASE * 10 + 600)['state'], 'active')
        self.assertEqual(len(self.store.read_checkout_snapshot('proof')[1]), 3)

    def test_overlap_conflict_or_missing_known_row_does_not_advance(self):
        row = self.row()
        p = PinnedProvider([row])
        self.scanner(p).sync('proof', now=BASE * 10 + 500)
        before = self.store.get_checkpoint('proof', self.source)
        row['data'] = self.row(count=4)['data']
        self.assertEqual(self.scanner(p).sync('proof', now=BASE * 10 + 500)['state'], 'stale')
        self.assertEqual(self.store.get_checkpoint('proof', self.source), before)
        p.rows.clear()
        self.assertEqual(self.scanner(p).sync('proof', now=BASE * 10 + 500)['error'], 'overlap_missing_known_checkout')

    def test_default_receipt_mode_remains_available_for_comparison(self):
        row = self.row()
        p = PinnedProvider([row]); p.add_receipt([row], [payment(row)])
        result = EventLeaderboardScanner(self.store, p, confirmations=2).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['state'], 'active', result)
        self.assertEqual(p.receipt_calls, [h(1)])
        self.assertEqual(p.proof_calls, [])

    def test_no_per_event_block_reads_with_three_workers(self):
        p = PinnedProvider([self.row(BASE + i, i + 1, index=2 + i) for i in range(5)])
        result = self.scanner(p, verification_workers=3).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['state'], 'active', result)
        self.assertEqual(set(p.block_calls), {BASE, BASE + 18})
        self.assertEqual(p.receipt_calls, [])

    def test_combined_full_page_splits_without_dropping_rows_or_builder_scope(self):
        p = PinnedProvider([self.row(BASE + i, i + 1, index=2 + i) for i in range(4)])
        p.page_size = 2
        self.assertEqual(self.scanner(p).sync('proof', now=BASE * 10 + 500)['state'], 'active')
        p.rows.extend(self.row(BASE + 20 + i, 10 + i, index=20 + i) for i in range(4))
        p.height += 10
        result = self.scanner(p).sync('proof', now=BASE * 10 + 600)
        self.assertEqual(result['state'], 'active', result)
        self.assertEqual(len(self.store.read_checkout_snapshot('proof')[1]), 8)
        self.assertTrue(all(b == h(7) for b in p.builder_queries))

    def test_known_overlap_does_not_refetch_each_old_block(self):
        p = PinnedProvider([self.row(BASE + i, i + 1, index=2 + i) for i in range(5)])
        self.scanner(p, verification_workers=3).sync('proof', now=BASE * 10 + 500)
        p.block_calls.clear()
        self.assertEqual(self.scanner(p, verification_workers=3).sync('proof', now=BASE * 10 + 500)['state'], 'active')
        self.assertEqual(set(p.block_calls), {BASE, BASE + 18})
        self.assertEqual(p.receipt_calls, [])

    def test_hard_interrupt_in_commit_keeps_only_complete_prefix(self):
        p = PinnedProvider([self.row(), self.row(BASE + 1, 2, index=3)])
        commit = self.store.commit_chunk
        def interrupted(event, source, expected, next_block, *args, **kwargs):
            if next_block == BASE + 2:
                raise KeyboardInterrupt('local simulated deadline')
            return commit(event, source, expected, next_block, *args, **kwargs)
        with patch.object(self.store, 'commit_chunk', side_effect=interrupted), self.assertRaises(KeyboardInterrupt):
            self.scanner(p, verification_workers=3).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(self.store.get_checkpoint('proof', self.source)['next_block'], BASE + 1)
        self.assertIsNone(self.store.read_snapshot('proof')[0])
        self.assertEqual(self.scanner(p, verification_workers=3).sync('proof', now=BASE * 10 + 500)['state'], 'active')
        self.assertEqual(len(self.store.read_checkout_snapshot('proof')[1]), 2)

    def test_duplicate_checkout_ids_or_transaction_in_two_blocks_stops(self):
        a, b = self.row(), self.row(BASE + 1, 2)
        result = self.scanner(PinnedProvider([a, b])).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['error'], 'duplicate_checkout_id')
        b['transactionHash'] = a['transactionHash']
        b['logIndex'] = '0x3'
        result = self.scanner(PinnedProvider([a, b])).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['error'], 'transaction_block_mismatch')

    def test_event_proof_flag_never_bypasses_legacy_receipt_payment(self):
        contract = DEFAULT_PACK_CONTRACTS[1]
        self.store.register_event(EventDefinition('legacy', BASE * 10, None, (contract,)))
        row = checkout(block=BASE, tx=42, contract=contract)
        p = PinnedProvider([row]); p.add_receipt([row], [payment(row, amount=30 * 10**18)])
        self.assertEqual(self.scanner(p).sync('legacy', now=BASE * 10 + 500)['state'], 'active')
        self.assertEqual(p.receipt_calls, [h(42)])
        self.assertEqual(p.proof_calls, [])

    def test_missing_event_timestamp_stops_without_fallback(self):
        row = self.row(); del row['timeStamp']
        p = PinnedProvider([row])
        result = self.scanner(p).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['error'], 'missing_event_timestamp')
        self.assertEqual(p.receipt_calls, [])
        self.assertIsNone(self.store.get_checkpoint('proof', self.source))
        self.assertIsNone(self.store.read_snapshot('proof')[0])

    def test_range_endpoint_hash_and_previous_cursor_still_detect_mismatch(self):
        row = self.row(); row['blockHash'] = h(999)
        p = PinnedProvider([row])
        self.assertEqual(self.scanner(p).sync('proof', now=BASE * 10 + 500)['error'], 'event_block_anchor_mismatch')
        self.assertIsNone(self.store.get_checkpoint('proof', self.source))
        row['blockHash'] = h(BASE)
        self.assertEqual(self.scanner(p).sync('proof', now=BASE * 10 + 500)['state'], 'active')
        before = self.store.get_checkpoint('proof', self.source)
        p.changed[BASE + 18] = h(998)
        self.assertEqual(self.scanner(p).sync('proof', now=BASE * 10 + 500)['error'], 'checkpoint_reorg_mismatch')
        self.assertEqual(self.store.get_checkpoint('proof', self.source), before)

    def test_same_block_hash_or_timestamp_conflict_never_commits_partial_block(self):
        for changes in ({'blockHash': h(999)}, {'timeStamp': hex(BASE * 10 + 1)}):
            with self.subTest(changes=changes):
                # Interior block, so this tests within-response consistency.
                a = self.row(BASE + 1)
                b = dict(self.row(BASE + 1, 2, index=3), **changes)
                result = self.scanner(PinnedProvider([a, b])).sync('proof', now=BASE * 10 + 500)
                self.assertEqual(result['state'], 'stale', result)
                self.assertIsNone(self.store.get_checkpoint('proof', self.source))

    def test_known_overlap_timestamp_change_is_not_silently_reused(self):
        row = self.row(BASE + 1)
        p = PinnedProvider([row])
        self.scanner(p).sync('proof', now=BASE * 10 + 500)
        before = self.store.get_checkpoint('proof', self.source)
        row['timeStamp'] = hex((BASE + 1) * 10 + 1)
        self.assertEqual(self.scanner(p).sync('proof', now=BASE * 10 + 500)['error'], 'indexed_log_receipt_mismatch')
        self.assertEqual(self.store.get_checkpoint('proof', self.source), before)

    def test_builder_currency_payment_and_confirmation_rules_still_apply(self):
        paid = self.row(BASE + 1)
        free = self.row(BASE + 2, 2, index=3, amount=0)
        other = self.row(BASE + 3, 3, index=4); other['topics'][3] = h(8)
        future = self.row(BASE + 19, 4, index=5)  # confirmed bound is BASE+18
        early = self.row(BASE - 1, 5, index=6)
        p = PinnedProvider([paid, free, other, future, early])
        result = self.scanner(p).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['state'], 'active', result)
        rows = self.store.read_checkout_snapshot('proof')[1]
        self.assertEqual([(r['tx_hash'], r['draw_count'], int(r['paid_amount_raw'])) for r in rows],
                         [(h(1), 3, 30 * 10**18)])
        self.assertEqual(result['through_block'], BASE + 18)
        self.assertEqual(p.receipt_calls, [])

    def test_unsupported_currency_stops_without_receipt_fallback(self):
        row = self.row(BASE + 1)
        row['data'] = '0x' + f'{1:064x}' + row['data'][66:]
        p = PinnedProvider([row])
        result = self.scanner(p).sync('proof', now=BASE * 10 + 500)
        self.assertEqual(result['error'], 'unsupported_checkout_currency')
        self.assertIsNone(self.store.get_checkpoint('proof', self.source))
        self.assertIsNone(self.store.read_snapshot('proof')[0])
        self.assertEqual(p.receipt_calls, [])


if __name__ == '__main__':
    unittest.main()
