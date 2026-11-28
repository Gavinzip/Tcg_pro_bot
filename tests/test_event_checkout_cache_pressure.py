"""Real HTTP/store pagination under the deployed cache's byte-pressure cases."""
import unittest
from unittest.mock import patch
from runtime.event_leaderboards import store as store_module
from tests import test_event_checkout_paging_cost as fixtures
from runtime.event_leaderboards.checkout_pages import CheckoutPages, CheckoutCacheUnavailable


class CheckoutCachePressureTests(unittest.TestCase):
    def exercise(self, *, alternating):
        store, _, first, _ = fixtures.CheckoutPagingCostTests().fixture(count=3000)
        self.addCleanup(store._checkout_pages.close)
        store._checkout_pages = CheckoutPages(max_entries=2, max_bytes=3_000_000 if alternating else 256)
        self.addCleanup(store._checkout_pages.close)
        second = store.publish_snapshot('paging', 19, 120)
        original = store_module.checkout_payloads
        hydrated = 0

        def counted(*args, **kwargs):
            nonlocal hydrated
            for row in original(*args, **kwargs):
                hydrated += 1
                yield row

        from runtime.event_leaderboards.http import EventLeaderboardHTTP
        http = EventLeaderboardHTTP(store)
        path = '/v1/leaderboards/paging/checkouts'
        snapshots = [first, second] if alternating else [first]
        cursors = {}
        observed = {s['snapshot_id']: [] for s in snapshots}
        with patch.object(store_module, 'checkout_payloads', counted):
            for _ in range(3):
                for snapshot in snapshots:
                    sid = snapshot['snapshot_id']
                    code, page = http.dispatch(path, {'snapshot_id': sid, 'limit': '1000',
                        **({'cursor': cursors[sid]} if cursors.get(sid) else {})})
                    self.assertEqual(code, 200)
                    self.assertEqual(page['snapshot_id'], sid)
                    observed[sid].extend((r['tx_hash'], r['log_index']) for r in page['rows'])
                    cursors[sid] = page['next_cursor']
        for rows in observed.values():
            self.assertEqual(len(rows), 3000)
            self.assertEqual(len(set(rows)), 3000)
        budget = 3000 * len(snapshots)
        print('checkout-cache-pressure', {'alternating': alternating, 'hydrated': hydrated,
            'unique_snapshot_rows': budget, 'byte_budget': store._checkout_pages.max_bytes})
        self.assertLessEqual(hydrated, budget,
            'Cache byte pressure makes every HTTP page rebuild the complete history')

    def test_one_oversized_snapshot_is_not_rebuilt_for_every_page(self):
        self.exercise(alternating=False)

    def test_interleaved_snapshot_downloads_do_not_rebuild_the_same_history(self):
        self.exercise(alternating=True)

    def test_partial_or_over_disk_budget_projection_is_never_returned(self):
        cache = CheckoutPages(max_disk_bytes=32768)
        self.addCleanup(cache.close)
        with self.assertRaises(CheckoutCacheUnavailable):
            cache.page('too-large', 0, 1, lambda: ({'value': 'x' * 1000} for _ in range(100)))
        self.assertNotIn('too-large', cache._entries)
        self.assertEqual(cache._disk_bytes, 0)
        self.assertEqual(list(__import__('pathlib').Path(cache._root.name).iterdir()), [])
        self.assertEqual(cache.page('valid', 0, 1, lambda: [{'value': 'valid'}])[0], [{'value': 'valid'}])

    def test_failed_read_errors_then_rebuilds_same_snapshot_without_borrowing_rows(self):
        cache = CheckoutPages()
        self.addCleanup(cache.close)
        cache.page('first', 0, 1, lambda: [{'value': 'first'}])
        cache.page('second', 0, 1, lambda: [{'value': 'second'}])
        cache._entries['first'].db.close()
        with self.assertRaises(CheckoutCacheUnavailable):
            cache.page('first', 0, 1, lambda: [{'value': 'first'}])
        self.assertNotIn('first', cache._entries)
        self.assertEqual(cache.page('first', 0, 1, lambda: [{'value': 'first'}])[0], [{'value': 'first'}])

    def test_validation_failure_after_writing_a_batch_never_publishes_partial_rows(self):
        cache = CheckoutPages()
        self.addCleanup(cache.close)
        cache.page('valid', 0, 1, lambda: [{'value': 'valid'}])
        def invalid():
            for _ in range(1001):
                yield {'value': 'synthetic evidence'}
            raise store_module.LedgerConflict('synthetic contradictory evidence')
        with self.assertRaises(store_module.LedgerConflict):
            cache.page('invalid', 0, 1, invalid)
        self.assertNotIn('invalid', cache._entries)
        self.assertEqual(len(list(__import__('pathlib').Path(cache._root.name).iterdir())), 1)
        self.assertEqual(cache.page('valid', 0, 1, lambda: [])[0], [{'value': 'valid'}])

    def test_warm_pages_do_not_decode_all_wallet_identities_again(self):
        store, _, snapshot, _ = fixtures.CheckoutPagingCostTests().fixture(count=3000)
        self.addCleanup(store._checkout_pages.close)
        from runtime.event_leaderboards.http import EventLeaderboardHTTP
        http = EventLeaderboardHTTP(store)
        path = '/v1/leaderboards/paging/checkouts'
        page = http.dispatch(path, {'snapshot_id': snapshot['snapshot_id'], 'limit': '1000'})[1]
        original = store.read_snapshot
        identity_reads = []
        def counted(*args, **kwargs):
            if kwargs.get('include_rows', True):
                identity_reads.append(1)
            return original(*args, **kwargs)
        with patch.object(store, 'read_snapshot', counted):
            self.assertEqual(http.dispatch(path, {'cursor': page['next_cursor']})[0], 200)
        self.assertEqual(identity_reads, [])

    def test_storage_failure_has_explicit_http_error_with_no_rows(self):
        store, _, snapshot, _ = fixtures.CheckoutPagingCostTests().fixture(count=20)
        store._checkout_pages = CheckoutPages(max_disk_bytes=4096)
        self.addCleanup(store._checkout_pages.close)
        from runtime.event_leaderboards.http import EventLeaderboardHTTP
        code, body = EventLeaderboardHTTP(store).dispatch('/v1/leaderboards/paging/checkouts',
            {'snapshot_id': snapshot['snapshot_id']})
        self.assertEqual(code, 503)
        self.assertEqual(body['error'], 'checkout_cache_unavailable')
        self.assertNotIn('rows', body)
