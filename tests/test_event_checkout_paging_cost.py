"""Exercise the production checkout HTTP/store path, never a chain provider."""
import json
from pathlib import Path
import time
import unittest
from unittest.mock import patch
import uuid
from concurrent.futures import ThreadPoolExecutor
from runtime.event_leaderboards.checkout_pages import CheckoutPages

from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.http import EventLeaderboardHTTP
from runtime.event_leaderboards.store import EventLeaderboardStore
from runtime.event_leaderboards import store as store_module

CONTRACT = '0x' + 'a' * 40
BUILDER = '0x' + 'b' * 64


class CheckoutPagingCostTests(unittest.TestCase):
    def fixture(self, count=20, *, scoped=True, retention=False):
        directory = Path(__file__).resolve().parents[1] / '.scratch' / 'checkout-paging-cost' / uuid.uuid4().hex
        directory.mkdir(parents=True)
        store = EventLeaderboardStore(str(directory / 'ledger.sqlite3'),
            query_retention_events={'paging'} if retention else set())
        definition = store.register_event(EventDefinition('paging', 100, None, (CONTRACT,),
            builder=BUILDER if scoped else None, contract_schemas={CONTRACT: 'v3'}))
        rows = [dict(wallet='0x' + f'{i % 100 + 1:040x}', tx_hash='0x' + f'{i + 1:064x}',
            log_index=0, pack_contract=CONTRACT, pack_id='0x' + 'c' * 64,
            builder=BUILDER, draw_count=1, paid_amount_raw='1000000000000000000',
            block_number=10, block_hash='0x' + 'd' * 64, timestamp=110)
            for i in range(count)]
        store.commit_chunk('paging', source_key(definition, CONTRACT), None, 20, rows, block_hash='0xanchor')
        return store, definition, store.publish_snapshot('paging', 19, 120), rows

    def test_health_and_retention_are_checked_even_when_rows_are_cached(self):
        store, definition, first, _ = self.fixture(retention=True)
        http = EventLeaderboardHTTP(store)
        query = {'snapshot_id': first['snapshot_id'], 'limit': '1'}
        self.assertFalse(http.dispatch('/v1/leaderboards/paging/checkouts', query)[1]['stale'])
        store.record_failure('paging', 'isolated known evidence failure')
        page = http.dispatch('/v1/leaderboards/paging/checkouts', query)[1]
        self.assertTrue(page['stale'])
        self.assertEqual(page['last_error'], 'isolated known evidence failure')
        second = store.publish_snapshot('paging', 19, 120)
        self.assertNotEqual(first['snapshot_id'], second['snapshot_id'])
        self.assertFalse(http.dispatch('/v1/leaderboards/paging/checkouts')[1]['stale'])
        with patch('time.time', return_value=time.time() + 4000):
            self.assertEqual(http.dispatch('/v1/leaderboards/paging/checkouts', query)[0], 409)
        # Altering a caller's response, including nested aliases, cannot poison another page.
        page['rows'][0]['wallet'] = 'corrupted'
        page['rows'][0]['aliases'].append('corrupted')
        again = http.dispatch('/v1/leaderboards/paging/checkouts', query)[1]
        self.assertNotEqual(again['rows'][0]['wallet'], 'corrupted')
        self.assertNotIn('corrupted', again['rows'][0]['aliases'])

    def test_new_snapshot_aliases_late_insert_and_builder_do_not_reuse_an_old_projection(self):
        store, definition, first, rows = self.fixture(scoped=False)
        http = EventLeaderboardHTTP(store)
        path = '/v1/leaderboards/paging/checkouts'
        old = http.dispatch(path, {'snapshot_id': first['snapshot_id'], 'builder': BUILDER})[1]
        rows = [{**row, 'chain_id': definition.chain_id} for row in rows]
        repaired = {**rows[0], 'tx_hash': '0x' + 'e' * 64, 'builder': '0x' + 'f' * 64}
        checkpoint = store.get_checkpoint('paging', source_key(definition, CONTRACT))
        store.reconcile_confirmed_range('paging', CONTRACT, checkpoint, 10, 19, [*rows, repaired])
        store.set_aliases('paging', [{'old': rows[0]['wallet'], 'new': '0x' + '9' * 40,
            'verified': True, 'evidence': 'isolated verified migration'}])
        second = store.publish_snapshot('paging', 19, 120)
        pinned = http.dispatch(path, {'snapshot_id': first['snapshot_id'], 'builder': BUILDER})[1]
        self.assertEqual(pinned['rows'], old['rows'])
        reopened = EventLeaderboardHTTP(EventLeaderboardStore(store.path))
        self.assertEqual(reopened.dispatch(path, {'snapshot_id': first['snapshot_id'], 'builder': BUILDER})[1]['rows'], old['rows'])
        current = http.dispatch(path, {'snapshot_id': second['snapshot_id']})[1]
        self.assertEqual(current['total_checkouts'], len(rows) + 1)
        filtered = http.dispatch(path, {'snapshot_id': second['snapshot_id'], 'builder': BUILDER})[1]
        self.assertEqual(filtered['total_checkouts'], len(rows))
        self.assertEqual(filtered['rows'][0]['wallet'], '0x' + '9' * 40)
        store.register_event(EventDefinition('paging', 105, None, (CONTRACT,),
            contract_schemas={CONTRACT: 'v3'}))
        self.assertEqual(http.dispatch(path)[0], 202)

    def test_concurrent_http_pages_build_a_snapshot_only_once(self):
        store, _, snapshot, _ = self.fixture(count=2000)
        http = EventLeaderboardHTTP(store)
        original = store_module.checkout_payloads
        hydrated = 0
        def counted(*args, **kwargs):
            nonlocal hydrated
            for row in original(*args, **kwargs):
                hydrated += 1
                yield row
        def request(_):
            return http.dispatch('/v1/leaderboards/paging/checkouts',
                {'snapshot_id': snapshot['snapshot_id'], 'limit': '100'})
        with patch.object(store_module, 'checkout_payloads', counted), ThreadPoolExecutor(max_workers=6) as pool:
            responses = list(pool.map(request, range(12)))
        self.assertEqual(hydrated, 2000)
        self.assertTrue(all(code == 200 and len(body['rows']) == 100 for code, body in responses))

    def test_complete_http_pagination_does_not_rehydrate_history_for_every_page(self):
        directory = Path(__file__).resolve().parents[1] / '.scratch' / 'checkout-paging-cost' / uuid.uuid4().hex
        directory.mkdir(parents=True)
        store = EventLeaderboardStore(str(directory / 'ledger.sqlite3'))
        definition = store.register_event(EventDefinition('paging', 100, None, (CONTRACT,),
            builder=BUILDER, contract_schemas={CONTRACT: 'v3'}))
        count = 5000
        rows = [dict(wallet='0x' + f'{i % 100 + 1:040x}', tx_hash='0x' + f'{i + 1:064x}',
            log_index=0, pack_contract=CONTRACT, pack_id='0x' + 'c' * 64,
            builder=BUILDER, draw_count=1, paid_amount_raw='1000000000000000000',
            block_number=10, block_hash='0x' + 'd' * 64, timestamp=110)
            for i in range(count)]
        store.commit_chunk('paging', source_key(definition, CONTRACT), None, 20, rows, block_hash='0xanchor')
        snapshot = store.publish_snapshot('paging', 19, 120)
        original = store_module.checkout_payloads
        hydrated = 0
        def counted(*args, **kwargs):
            nonlocal hydrated
            for row in original(*args, **kwargs):
                hydrated += 1
                yield row
        http = EventLeaderboardHTTP(store)
        cursor = None
        result = []
        pages = 0
        started = time.monotonic()
        with patch.object(store_module, 'checkout_payloads', counted):
            while True:
                code, page = http.dispatch('/v1/leaderboards/paging/checkouts', {
                    'limit': '1000', 'snapshot_id': snapshot['snapshot_id'],
                    **({'cursor': cursor} if cursor else {})})
                self.assertEqual(code, 200)
                result.extend(page['rows'])
                pages += 1
                cursor = page['next_cursor']
                if cursor is None:
                    break
        report = dict(rows=count, pages=pages, hydrated=hydrated,
            elapsed_seconds=round(time.monotonic() - started, 4))
        (directory / 'report.json').write_text(json.dumps(report))
        print('checkout-paging-cost', json.dumps(report))
        self.assertEqual(len(result), count)
        self.assertEqual(len({(row['tx_hash'], row['log_index']) for row in result}), count)
        self.assertLessEqual(hydrated, count * 2,
            'Downloading five pages rehydrates the entire frozen history five times; '
            'new successful scanner snapshots cannot reach the website promptly at scale')


class CheckoutPageBoundsTests(unittest.TestCase):
    def test_eviction_oversize_and_failed_load_never_substitute_another_snapshot(self):
        cache = CheckoutPages(max_entries=1, max_bytes=100)
        calls = []
        def load(value):
            calls.append(value)
            return [{'value': value}]
        self.assertEqual(cache.page('first', 0, 1, lambda: load('first'))[0][0]['value'], 'first')
        cache.page('second', 0, 1, lambda: load('second'))
        cache.page('first', 0, 1, lambda: load('first'))
        self.assertEqual(calls, ['first', 'second', 'first'])
        for _ in range(2):
            page, total = cache.page('large', 0, 1, lambda: load('x' * 200))
            self.assertEqual(total, 1)
            self.assertEqual(page[0]['value'], 'x' * 200)
        self.assertEqual(len(cache._entries), 1)
        self.assertLessEqual(cache._bytes, 100)
        self.assertEqual(calls.count('x' * 200), 2)
        for _ in range(2):
            with self.assertRaisesRegex(ValueError, 'projection failed'):
                cache.page('failed', 0, 1, lambda: (_ for _ in ()).throw(ValueError('projection failed')))
        self.assertNotIn('failed', cache._entries)
        with self.assertRaisesRegex(ValueError, 'offset'):
            cache.page('first', 2, 1, lambda: load('first'))


if __name__ == '__main__':
    unittest.main()
