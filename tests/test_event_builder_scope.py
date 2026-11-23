"""Builder isolation before aggregation, immutable scope views and revision migration."""
from dataclasses import replace
import base64
import json
import os
from pathlib import Path
import threading
import unittest
from unittest.mock import patch
import urllib.request
import uuid

from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.http import EventLeaderboardHTTP
from runtime.event_leaderboards.scanner import decode_checkout, EventLeaderboardScanner, VRF_V3_CONTRACT
from runtime.event_leaderboards.store import EventLeaderboardStore
from tests.test_event_leaderboard_scanner import Provider, checkout, payment, h


RUN_DIR = Path(__file__).resolve().parents[1] / '.scratch' / 'event-builder-tests' / ('run-' + uuid.uuid4().hex)
A, B, ZERO = h(101), h(102), h(0)
SAMPLE_BUILDER = '0x2e622bb0dfd32f36aefb84dc4eb14ced42afa80a84f3a4cb31b21488f2a61501'


def wallet(number):
    return '0x' + f'{number:040x}'


class BuilderScopeTests(unittest.TestCase):
    def setUp(self):
        RUN_DIR.mkdir(parents=True, exist_ok=True)
        self.store = EventLeaderboardStore(str(RUN_DIR / (self._testMethodName + '.sqlite3')))
        self.definition = self.store.register_event(EventDefinition('finite', 100, 200, (VRF_V3_CONTRACT,)))
        self.source = source_key(self.definition, VRF_V3_CONTRACT)
        self.http = EventLeaderboardHTTP(self.store)

    def row(self, user, identity, builder, count, **extra):
        return {'wallet': wallet(user), 'tx_hash': h(identity), 'log_index': 0,
                'pack_contract': VRF_V3_CONTRACT, 'pack_id': h(77), 'builder': builder,
                'draw_count': count, 'paid_amount_raw': str(count * 28 * 10**18),
                'block_number': 10, 'block_hash': h(10), 'timestamp': 110, **extra}

    def publish(self, rows, **kwargs):
        self.store.commit_chunk('finite', self.source, None, 20, rows, block_hash=h(19))
        return self.store.publish_snapshot('finite', 19, 120, **kwargs)

    def test_filter_precedes_aggregation_and_search_rank_is_within_builder(self):
        snapshot = self.publish([self.row(1, 1, A, 1), self.row(1, 2, B, 9), self.row(2, 3, A, 5)])
        _, all_rows = self.http.dispatch('/v1/leaderboards/finite', {'all': 'true'})
        self.assertEqual([r['wallet'] for r in all_rows['rows']], [wallet(1), wallet(2)])
        self.assertEqual(all_rows['rows'][0]['draw_count'], 10)
        code, selected = self.http.dispatch('/v1/leaderboards/finite', {'builder': A.upper(), 'all': 'true'})
        self.assertEqual(code, 200)
        self.assertEqual(selected['builder'], A)
        self.assertTrue(selected['builder_filter_supported'])
        self.assertEqual(selected['total_participants'], 2)
        self.assertEqual([(r['wallet'], r['draw_count'], r['rank']) for r in selected['rows']],
                         [(wallet(2), 5, 1), (wallet(1), 1, 2)])
        self.assertEqual(selected['rows'][1]['paid_amount_raw'], str(28 * 10**18))
        code, found = self.http.dispatch('/v1/leaderboards/finite/search', {
            'builder': A, 'snapshot_id': snapshot['snapshot_id'], 'q': wallet(1)})
        self.assertEqual(code, 200)
        self.assertEqual(found['rows'][0]['rank'], 2)
        self.assertEqual(found['rows'][0]['draw_count'], 1)

    def test_normal_http_server_preserves_builder_for_both_routes(self):
        import profile_api
        snapshot = self.publish([self.row(1, 1, A, 1), self.row(1, 2, B, 9), self.row(2, 3, A, 5)])
        token = 'synthetic-website-builder-read-only-token'
        with patch.object(profile_api, 'API_TOKEN', 'synthetic-internal'), \
             patch.object(profile_api, 'LEADERBOARD_CLIENT_TOKENS', {'site': token}), \
             patch.object(profile_api, '_EVENT_CONTROLLER', self.http), \
             patch.dict(os.environ, {'EVENT_LEADERBOARD_API_ENABLED': '1'}):
            server = profile_api.ProfileApiServer(('127.0.0.1', 0), profile_api.ProfileApiHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                origin = 'http://127.0.0.1:' + str(server.server_address[1])
                paths = [f'/v1/leaderboards/finite?builder={A}',
                         f'/v1/leaderboards/finite/search?builder={A}&snapshot_id={snapshot["snapshot_id"]}&q={wallet(1)}']
                results = []
                for path in paths:
                    request = urllib.request.Request(origin + path, headers={'Authorization': 'Bearer ' + token})
                    with urllib.request.urlopen(request, timeout=5) as response:
                        self.assertEqual(response.status, 200)
                        results.append(json.load(response))
                self.assertEqual(results[0]['builder'], A)
                self.assertTrue(results[0]['builder_filter_supported'])
                self.assertEqual(results[0]['rows'][0]['wallet'], wallet(2))
                self.assertEqual(results[1]['rows'][0]['draw_count'], 1)
                self.assertEqual(results[1]['rows'][0]['rank'], 2)
            finally:
                server.shutdown(); server.server_close(); thread.join()

    def test_cursor_keeps_scope_across_new_publication_and_rejects_switch(self):
        first = self.publish([self.row(1, 1, A, 1), self.row(2, 2, A, 3), self.row(1, 3, B, 20)])
        _, page = self.http.dispatch('/v1/leaderboards/finite', {'builder': A, 'limit': '1'})
        self.store.commit_chunk('finite', self.source, 20, 30,
            [self.row(1, 4, A, 99, block_number=25, timestamp=125)], block_hash=h(29))
        self.store.publish_snapshot('finite', 29, 130)
        code, second = self.http.dispatch('/v1/leaderboards/finite', {'cursor': page['next_cursor']})
        self.assertEqual(code, 200)
        self.assertEqual(second['snapshot_id'], first['snapshot_id'])
        self.assertEqual(second['builder'], A)
        self.assertEqual(second['rows'][0]['draw_count'], 1)
        self.assertEqual(self.http.dispatch('/v1/leaderboards/finite', {
            'cursor': page['next_cursor'], 'builder': B})[0], 400)

    def test_alias_identity_is_shared_but_counts_and_frozen_names_are_scoped(self):
        self.store.set_aliases('finite', [{'old': wallet(1), 'new': wallet(3), 'verified': True, 'evidence': 'receipt'}])
        first = self.publish([self.row(1, 1, A, 2), self.row(3, 2, B, 50)], usernames={wallet(1): '收藏者'})
        self.store.publish_snapshot('finite', 19, 120, usernames={wallet(3): 'changed'})
        _, selected = self.http.dispatch('/v1/leaderboards/finite/search', {
            'builder': A, 'snapshot_id': first['snapshot_id'], 'q': wallet(1)})
        self.assertEqual(selected['rows'][0]['wallet'], wallet(3))
        self.assertEqual(selected['rows'][0]['draw_count'], 2)
        self.assertEqual(selected['rows'][0]['username'], '收藏者')

    def test_zero_builder_is_real_and_unseen_builder_can_be_empty_after_complete_scan(self):
        self.publish([self.row(1, 1, ZERO, 1)])
        _, zero = self.http.dispatch('/v1/leaderboards/finite', {'builder': ZERO})
        self.assertEqual(zero['total_participants'], 1)
        code, absent = self.http.dispatch('/v1/leaderboards/finite', {'builder': B})
        self.assertEqual(code, 200)
        self.assertEqual(absent['total_participants'], 0)
        self.assertEqual(absent['rows'], [])
        for invalid in ['', wallet(1), '0x' + 'g' * 64]:
            self.assertEqual(self.http.dispatch('/v1/leaderboards/finite', {'builder': invalid})[0], 400)

    def test_v3_builder_is_required_and_legacy_is_not_attributed_to_zero(self):
        row = self.row(1, 1, A, 1)
        del row['builder']
        with self.assertRaises(ValueError):
            self.store.commit_chunk('finite', self.source, None, 20, [row], block_hash=h(19))
        legacy = wallet(900)
        definition = self.store.register_event(replace(self.definition, contracts=(legacy,),
                                                     contract_schemas={legacy: 'legacy_single'}))
        row.update(pack_contract=legacy)
        self.store.commit_chunk('finite', source_key(definition, legacy), None, 20, [row], block_hash=h(19))
        self.store.publish_snapshot('finite', 19, 120)
        self.assertEqual(self.http.dispatch('/v1/leaderboards/finite', {'builder': ZERO})[1]['rows'], [])
        self.assertEqual(self.http.dispatch('/v1/leaderboards/finite')[1]['total_participants'], 1)

    def test_platform_results_include_more_than_300_participants(self):
        self.publish([self.row(i, i, A, 1) for i in range(1, 352)] + [self.row(999, 999, B, 999)])
        code, all_rows = self.http.dispatch('/v1/leaderboards/finite', {'builder': A, 'all': 'true'})
        self.assertEqual(code, 200)
        self.assertEqual(all_rows['total_participants'], 351)
        self.assertEqual(len(all_rows['rows']), 351)
        self.assertNotIn(wallet(999), [row['wallet'] for row in all_rows['rows']])
        _, first = self.http.dispatch('/v1/leaderboards/finite', {'builder': A, 'limit': '100'})
        _, second = self.http.dispatch('/v1/leaderboards/finite', {'cursor': first['next_cursor']})
        self.assertEqual(second['builder'], A)
        self.assertEqual(second['rows'][0]['wallet'], wallet(101))

    def test_cursor_rejects_boolean_offset_and_empty_snapshot(self):
        self.publish([self.row(1, 1, A, 1)])
        for change in ({'offset': True}, {'snapshot_id': ''}):
            value = {'snapshot_id': 'snapshot', 'sort': 'rank', 'order': 'asc', 'offset': 0, 'q': '', **change}
            cursor = base64.urlsafe_b64encode(json.dumps(value).encode()).decode()
            self.assertEqual(self.http.dispatch('/v1/leaderboards/finite', {'cursor': cursor})[0], 400)

    def test_legacy_snapshot_refuses_filter_and_new_schema_creates_clean_revision(self):
        legacy = self.store.register_event(replace(self.definition, checkout_schema_version=1))
        row = self.row(1, 1, A, 1)
        del row['builder']
        self.store.commit_chunk('finite', source_key(legacy, VRF_V3_CONTRACT), None, 20, [row], block_hash=h(19))
        old = self.store.publish_snapshot('finite', 19, 120)
        self.assertEqual(self.http.dispatch('/v1/leaderboards/finite', {'builder': A})[0], 409)
        self.assertEqual(self.http.dispatch('/v1/leaderboards/finite')[0], 200)
        upgraded = self.store.register_event(self.definition)
        self.assertEqual(upgraded.revision, legacy.revision + 1)
        self.assertNotEqual(upgraded.config_hash, legacy.config_hash)
        self.assertIsNone(self.store.get_checkpoint('finite', source_key(upgraded, VRF_V3_CONTRACT)))
        self.assertEqual(self.http.dispatch('/v1/leaderboards/finite', {'builder': A})[0], 202)
        self.assertEqual(self.http.dispatch('/v1/leaderboards/finite', {
            'builder': A, 'snapshot_id': old['snapshot_id']})[0], 409)
        self.assertEqual(self.http.dispatch('/v1/leaderboards/finite', {'snapshot_id': old['snapshot_id']})[0], 200)

    def test_existing_global_cursor_has_no_builder_and_cannot_be_repurposed(self):
        snapshot = self.publish([self.row(1, 1, A, 1), self.row(2, 2, B, 2)])
        payload = {'snapshot_id': snapshot['snapshot_id'], 'sort': 'rank', 'order': 'asc', 'offset': 1, 'q': ''}
        cursor = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')
        self.assertEqual(self.http.dispatch('/v1/leaderboards/finite', {'cursor': cursor})[0], 200)
        self.assertEqual(self.http.dispatch('/v1/leaderboards/finite', {'cursor': cursor, 'builder': A})[0], 400)

    def test_user_supplied_real_v3_event_exposes_builder_and_paid_quantity(self):
        log = {'address': VRF_V3_CONTRACT, 'transactionHash': '0x6131491e3d1bce85bb3b25524a0857659478ab88824bc2c991cd1e720acb1c21',
               'blockNumber': '0x804a841', 'blockHash': '0x292b431c3161eda40970bf32d49d96ecaf1e25cdf8d900278eef0ddfa7255313',
               'logIndex': '0x3b8', 'topics': [
                   '0xd316cbf877f67321c8b602d7d52e44a8db95db232743952de19656b67ccbb419',
                   '0x000000000000000000000000c1d58a4e1d57e48408124b34dec6b5673a627fe9',
                   '0xcf11308cc7c642554a781b039a63a542c2f20b36f904b5b6981d48a2f76a5f90', SAMPLE_BUILDER],
               'data': '0x' + ''.join(f'{value:064x}' for value in [
                   int('55d398326f99059ff775485246999027b3197955', 16), 96, 28 * 10**18, 1, 195673])}
        decoded = decode_checkout(log, 'v3')
        self.assertEqual(decoded['builder'], SAMPLE_BUILDER)
        self.assertEqual(decoded['draw_count'], 1)
        self.assertEqual(decoded['paid_amount_raw'], 28 * 10**18)
        self.assertEqual(decoded['checkout_ids'], [195673])

    def test_scanner_upgrades_and_rebuilds_even_a_finalized_legacy_revision(self):
        legacy = self.store.register_event(replace(self.definition, checkout_schema_version=1))
        row = self.row(1, 1, A, 1)
        del row['builder']
        self.store.commit_chunk('finite', source_key(legacy, VRF_V3_CONTRACT), None, 20, [row], block_hash=h(19))
        old = self.store.publish_snapshot('finite', 19, 200, finalized=True)
        event = checkout()
        provider = Provider([event], head=22)
        provider.add_receipt([event], [payment(event)])
        result = EventLeaderboardScanner(self.store, provider, confirmations=2).sync('finite', now=250)
        self.assertEqual(result['state'], 'finalized')
        self.assertEqual(self.store.get_event('finite').revision, legacy.revision + 1)
        metadata, rows, _ = self.store.read_snapshot('finite', builder=ZERO)
        self.assertNotEqual(metadata['snapshot_id'], old['snapshot_id'])
        self.assertEqual(rows[0]['draw_count'], 3)

    def test_indexed_builder_must_match_receipt_builder(self):
        indexed = checkout()
        receipt_log = dict(indexed, topics=[*indexed['topics'][:3], A])
        provider = Provider([indexed])
        provider.add_receipt([receipt_log], [payment(receipt_log)])
        store = EventLeaderboardStore(str(RUN_DIR / 'builder-receipt.sqlite3'))
        store.register_event(EventDefinition('scan', 100, 140, (VRF_V3_CONTRACT,)))
        result = EventLeaderboardScanner(store, provider, confirmations=2).sync('scan', now=150)
        self.assertEqual(result['error'], 'indexed_log_receipt_mismatch')
        self.assertIsNone(store.read_snapshot('scan')[0])


if __name__ == '__main__':
    unittest.main()
