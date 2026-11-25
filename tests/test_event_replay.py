"""Five-minute late-index repair: durable deduplication, failure isolation and frozen evidence."""
import json
from pathlib import Path
import sqlite3
from contextlib import closing
import unittest
import uuid

from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.http import EventLeaderboardHTTP
from runtime.event_leaderboards.scanner import EventLeaderboardScanner, VRF_V3_CONTRACT
from runtime.event_leaderboards.store import EventLeaderboardStore, LedgerConflict
from tests.test_event_leaderboard_scanner import Provider, checkout, payment, h, OTHER

RUN_DIR = Path(__file__).resolve().parents[1] / '.scratch' / 'event-replay-tests' / ('run-' + uuid.uuid4().hex)


class ReplayTests(unittest.TestCase):
    def setUp(self):
        root = RUN_DIR / self._testMethodName
        root.mkdir(parents=True, exist_ok=True)
        self.store = EventLeaderboardStore(str(root / 'ledger.sqlite3'))
        self.definition = self.store.register_event(EventDefinition('ongoing', 100, None,
            (VRF_V3_CONTRACT,), builder=h(7)))
        self.source = source_key(self.definition, VRF_V3_CONTRACT)
        self.p = Provider(head=62)  # confirmed bound=60, timestamp=600
        self.scanner = EventLeaderboardScanner(self.store, self.p, confirmations=2)

    def add(self, *, visible=True, **kwargs):
        row = checkout(**kwargs)
        row['topics'][3] = h(7)
        if visible:
            self.p.rows.append(row)
        self.p.add_receipt([row], [payment(row)])
        return row

    def sync(self):
        result = self.scanner.sync('ongoing', now=1000)
        self.assertEqual(result['state'], 'active', result)
        return result

    def evidence(self, snapshot=None):
        return self.store.read_checkout_snapshot('ongoing', snapshot)[1]

    def test_repeated_rounds_and_restart_do_not_duplicate_successful_draws(self):
        self.add(block=40, tx=1, count=3)
        first = self.sync()['snapshot']
        for _ in range(3):
            report = self.sync()['overlap'][self.source]
            self.assertEqual((report['from_at'], report['from_block'], report['through_block']), (300, 30, 60))
            self.assertEqual(report['inserted'], 0)
        self.store = EventLeaderboardStore(self.store.path)
        self.scanner = EventLeaderboardScanner(self.store, self.p, confirmations=2)
        self.sync()
        self.assertEqual(len(self.evidence()), 1)
        self.assertEqual(self.store.read_snapshot('ongoing')[1][0]['draw_count'], 3)
        self.assertEqual(self.evidence(first['snapshot_id']), self.evidence())

    def test_late_same_transaction_different_log_index_counts_once_and_old_pin_stays_frozen(self):
        a = self.add(block=40, tx=1, count=2, index=2)
        old = self.sync()['snapshot']['snapshot_id']
        old_api = EventLeaderboardHTTP(self.store).dispatch('/v1/leaderboards/ongoing/checkouts',
            {'snapshot_id': old})[1]
        b = checkout(block=40, tx=1, count=4, index=3, wallet=OTHER)
        b['topics'][3] = h(7)
        self.p.rows.append(b)
        self.p.add_receipt([a, b], [payment(a), payment(b, index=1)])
        report = self.sync()
        self.assertEqual(report['overlap'][self.source]['inserted'], 1)
        self.assertEqual(sum(r['draw_count'] for r in self.evidence()), 6)
        self.assertEqual(len(self.evidence(old)), 1)
        self.assertEqual(EventLeaderboardHTTP(self.store).dispatch('/v1/leaderboards/ongoing/checkouts',
            {'snapshot_id': old})[1]['rows'], old_api['rows'])
        self.assertEqual(self.sync()['overlap'][self.source]['inserted'], 0)
        self.assertEqual(sum(r['draw_count'] for r in self.evidence()), 6)

    def test_window_uses_checkpoint_time_after_outage_and_includes_exact_five_minute_boundary(self):
        self.sync()
        self.add(block=29, tx=1)  # outside the five-minute overlap
        self.add(block=30, tx=2)  # inclusive lower boundary
        self.add(block=61, tx=3)  # new forward record
        self.p.height = 122  # a 10-minute outage; don't look back from wall-clock/head
        report = self.sync()['overlap'][self.source]
        self.assertEqual((report['from_block'], report['through_block'], report['inserted']), (30, 60, 1))
        self.assertEqual({r['tx_hash'] for r in self.evidence()}, {h(2), h(3)})
        self.assertEqual(self.store.get_checkpoint('ongoing', self.source)['next_block'], 121)

    def test_overlap_failure_does_not_advance_checkpoint_or_publish_and_recovers_by_retry(self):
        self.add(block=40, tx=1)
        old = self.sync()['snapshot']['snapshot_id']
        before = self.store.get_checkpoint('ongoing', self.source)
        self.p.height = 64
        self.p.fail_contract = VRF_V3_CONTRACT
        result = self.scanner.sync('ongoing', now=1000)
        self.assertEqual(result['state'], 'stale')
        self.assertEqual(self.store.get_checkpoint('ongoing', self.source), before)
        self.assertEqual(self.store.read_snapshot('ongoing')[0]['snapshot_id'], old)
        self.p.fail_contract = None
        self.sync()
        self.assertIsNone(self.store.read_snapshot('ongoing')[2]['last_error'])

    def test_empty_success_omitting_known_evidence_fails_instead_of_erasing_or_publishing(self):
        self.add(block=40, tx=1)
        old = self.sync()['snapshot']['snapshot_id']
        self.p.rows.clear()
        result = self.scanner.sync('ongoing', now=1000)
        self.assertEqual(result['state'], 'stale')
        self.assertEqual(result['error'], 'overlap_missing_known_checkout')
        self.assertEqual(self.store.read_snapshot('ongoing')[0]['snapshot_id'], old)
        self.assertEqual(len(self.evidence(old)), 1)

    def test_conflicting_identity_and_failed_new_receipt_roll_back_all_late_records(self):
        self.add(block=40, tx=1)
        old = self.sync()['snapshot']['snapshot_id']
        self.add(block=41, tx=2)
        self.add(block=42, tx=3)
        self.p.receipts[h(3)]['status'] = '0x0'
        self.assertEqual(self.scanner.sync('ongoing', now=1000)['state'], 'stale')
        self.assertEqual(len(self.evidence(old)), 1)
        self.assertEqual(len(self.store.checkouts_in_range('ongoing', VRF_V3_CONTRACT, 30, 60,
            expected_revision=self.definition.revision)), 1)
        self.p.receipts[h(3)]['status'] = '0x1'
        self.p.rows[0]['data'] = checkout(block=40, tx=1, count=7)['data']
        self.assertEqual(self.scanner.sync('ongoing', now=1000)['error'], 'indexed_log_receipt_mismatch')
        self.assertEqual(len(self.evidence(old)), 1)

    def test_reorg_anchor_still_stops_before_overlap_queries(self):
        self.sync()
        self.p.changed[60] = h(999)
        self.p.calls.clear()
        self.assertEqual(self.scanner.sync('ongoing', now=1000)['error'], 'checkpoint_reorg_mismatch')
        self.assertEqual(self.p.calls, [])

    def test_wrong_builder_free_logs_and_unconfirmed_records_do_not_enter_repair(self):
        self.sync()
        self.add(block=40, tx=1, amount=0)
        wrong = self.add(block=41, tx=2)
        wrong['topics'][3] = h(8)
        self.add(block=61, tx=3)  # bound is still60
        self.assertEqual(self.sync()['overlap'][self.source]['inserted'], 0)
        self.assertEqual(self.evidence(), [])

    def test_hard_interruption_before_overlap_commit_retains_cursor_and_can_retry(self):
        self.sync()
        before = self.store.get_checkpoint('ongoing', self.source)
        a = self.add(block=40, tx=1)
        b = self.add(block=41, tx=2)
        receipt = self.p.receipt
        def interrupted(tx):
            if tx == b['transactionHash']:
                raise KeyboardInterrupt('simulated process deadline')
            return receipt(tx)
        self.p.receipt = interrupted
        with self.assertRaises(KeyboardInterrupt):
            self.scanner.sync('ongoing', now=1000)
        self.assertEqual(self.store.get_checkpoint('ongoing', self.source), before)
        self.assertEqual(self.evidence(), [])
        self.p.receipt = receipt
        self.assertEqual(self.sync()['overlap'][self.source]['inserted'], 2)
        self.assertEqual({r['tx_hash'] for r in self.evidence()}, {a['transactionHash'], b['transactionHash']})

    def test_repeated_provider_log_is_deduplicated_before_repair(self):
        self.sync()
        row = self.add(block=40, tx=1)
        self.p.rows.append(dict(row))
        self.p.page_size = 100
        self.assertEqual(self.sync()['overlap'][self.source]['inserted'], 1)
        self.assertEqual(self.sync()['overlap'][self.source]['inserted'], 0)
        self.assertEqual(len(self.evidence()), 1)

    def test_repeated_known_evidence_reuses_proof_without_receipt_calls(self):
        self.add(block=40, tx=1)
        self.sync()
        def forbidden(tx):
            raise AssertionError('Unchanged known evidence does not need another paid receipt request')
        self.p.receipt = forbidden
        self.assertEqual(self.sync()['overlap'][self.source]['inserted'], 0)

    def test_legacy_snapshot_upgrade_preserves_metadata_and_evidence_after_vacuum_and_restart(self):
        self.add(block=40, tx=1)
        old = self.sync()['snapshot']['snapshot_id']
        # Construct the pre-upgrade schema, not a substitute product database.
        with self.store._connection(write=True) as db:
            metadata = json.loads(db.execute('SELECT metadata FROM snapshots WHERE snapshot_id=?', (old,)).fetchone()[0])
            metadata['checkout_evidence_storage'] = 'ledger_range_v1'
            del metadata['ledger_insertion_watermark']
            serialized = json.dumps(metadata)
            db.execute('UPDATE snapshots SET metadata=? WHERE snapshot_id=?', (serialized, old))
            db.execute('DROP TRIGGER record_ledger_insertion')
            db.execute('DROP TABLE ledger_insertions')
            db.execute('DROP TABLE snapshot_ledger_watermarks')
        self.store = EventLeaderboardStore(self.store.path)
        self.scanner = EventLeaderboardScanner(self.store, self.p, confirmations=2)
        self.add(block=42, tx=2, wallet=OTHER)
        self.assertEqual(self.sync()['overlap'][self.source]['inserted'], 1)
        with closing(sqlite3.connect(self.store.path)) as db:
            self.assertEqual(db.execute('SELECT metadata FROM snapshots WHERE snapshot_id=?', (old,)).fetchone()[0], serialized)
            db.execute('VACUUM')
        self.store = EventLeaderboardStore(self.store.path)
        self.assertEqual(len(self.evidence(old)), 1)
        self.assertEqual(len(self.evidence()), 2)

    def test_repair_transaction_checks_scope_cursor_and_rolls_back_conflicts(self):
        self.add(block=40, tx=1)
        self.sync()
        checkpoint = self.store.get_checkpoint('ongoing', self.source)
        known = self.store.checkouts_in_range('ongoing', VRF_V3_CONTRACT, 30, 60, expected_revision=1)
        with self.assertRaises(LedgerConflict):
            self.store.reconcile_confirmed_range('ongoing', VRF_V3_CONTRACT,
                {**checkpoint, 'next_block': 60}, 30, 59, known)
        conflict = {**known[0], 'draw_count': 999}
        with self.assertRaises(LedgerConflict):
            self.store.reconcile_confirmed_range('ongoing', VRF_V3_CONTRACT, checkpoint, 30, 60, [conflict])
        self.assertEqual(self.store.get_checkpoint('ongoing', self.source), checkpoint)


if __name__ == '__main__':
    unittest.main()
