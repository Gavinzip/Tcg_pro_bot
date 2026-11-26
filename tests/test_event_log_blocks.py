"""Required provider metadata and explicit endpoint trust boundaries; no HTTP."""
import unittest

from runtime.event_leaderboards.errors import SourceIncomplete
from runtime.event_leaderboards.event_blocks import EventBlockEvidence
from tests.test_event_leaderboard_scanner import h


class Headers:
    def __init__(self):
        self.calls = []

    def block(self, height):
        self.calls.append(height)
        return dict(number=height, hash=h(height), timestamp=height * 10)


def event(height, *, stamp=None, block_hash=None):
    return dict(blockNumber=hex(height), blockHash=block_hash or h(height),
                timeStamp=hex(height * 10 if stamp is None else stamp))


class EventLogBlockTests(unittest.TestCase):
    def setUp(self):
        self.provider = Headers()
        self.evidence = EventBlockEvidence(self.provider)
        self.evidence.cover(10, 20)

    def test_many_logs_only_fetch_range_endpoints(self):
        for height in range(10, 21):
            self.evidence.from_log(event(height))
        self.assertEqual(self.provider.calls, [10, 20])
        self.assertEqual(self.evidence.block(15)['timestamp'], 150)

    def test_missing_malformed_timestamp_or_hash_never_falls_back(self):
        row = event(15); del row['timeStamp']
        with self.assertRaisesRegex(SourceIncomplete, 'missing_event_timestamp'):
            self.evidence.from_log(row)
        for stamp in (None, True, 1.5, -1, '', '0xzz', 'NaN', ' 150'):
            with self.subTest(timestamp=stamp), self.assertRaisesRegex(SourceIncomplete, 'invalid_event_timestamp'):
                self.evidence.from_log(dict(event(15), timeStamp=stamp))
        for block_hash in (None, True, '0x123', '0x' + 'g' * 64):
            with self.subTest(block_hash=block_hash), self.assertRaisesRegex(SourceIncomplete, 'invalid_event_block_hash'):
                self.evidence.from_log(dict(event(15), blockHash=block_hash))
        self.assertEqual(self.provider.calls, [10, 20])

    def test_same_block_must_have_one_hash_and_time(self):
        self.evidence.from_log(event(15))
        for changes in ({'blockHash': h(999)}, {'timeStamp': hex(151)}):
            with self.subTest(changes=changes), self.assertRaisesRegex(SourceIncomplete, 'inconsistent_event_block'):
                self.evidence.from_log(dict(event(15), **changes))

    def test_endpoint_log_must_match_actual_endpoint_header(self):
        for row in (event(10, block_hash=h(999)), event(20, stamp=199)):
            with self.subTest(row=row), self.assertRaisesRegex(SourceIncomplete, 'event_block_anchor_mismatch'):
                self.evidence.from_log(row)

    def test_bad_interval_or_time_fails_and_same_second_blocks_are_allowed(self):
        with self.assertRaisesRegex(SourceIncomplete, 'event_outside_anchored_range'):
            self.evidence.from_log(event(21))
        for stamp in (99, 201):
            with self.subTest(stamp=stamp), self.assertRaisesRegex(SourceIncomplete, 'event_timestamp_outside_anchors'):
                self.evidence.from_log(event(15, stamp=stamp))
        self.evidence.from_log(event(11, stamp=130))
        self.evidence.from_log(event(13, stamp=130))
        with self.assertRaisesRegex(SourceIncomplete, 'nonmonotonic_event_timestamp'):
            self.evidence.from_log(event(12, stamp=129))

    def test_interior_canonical_header_is_deliberately_not_independently_requested(self):
        # Document the authorized tradeoff: a plausible consistent interior
        # hash is provider evidence. Endpoint checks cannot prove it canonical.
        self.evidence.from_log(event(15, block_hash=h(999)))
        self.assertEqual(self.provider.calls, [10, 20])
        self.assertEqual(self.evidence.block(15)['hash'], h(999))

    def test_empty_window_still_anchors_both_ends_and_one_block_only_fetches_once(self):
        provider = Headers(); evidence = EventBlockEvidence(provider)
        evidence.cover(15, 15)
        self.assertEqual(provider.calls, [15])
        self.assertEqual(evidence.summary()['canonical_anchor_blocks'], [15])


if __name__ == '__main__':
    unittest.main()
