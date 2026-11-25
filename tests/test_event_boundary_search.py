"""Deterministic boundary-cost regression through the actual paced HTTP reader."""
import io
import json
import unittest
from bisect import bisect_left
import random
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from runtime.event_leaderboards.scanner import EtherscanProvider
from runtime.event_leaderboards.diagnostics import ScanDiagnostics
from runtime.event_leaderboards.boundary import first_block_at_timestamp


def measure_boundary(method=None, *, hint=980, target=320, head=3000, plateau=4):
    clock, calls = [0.], []
    def sleep(delay):
        clock[0] += delay
    def respond(url, timeout):
        query = parse_qs(urlsplit(url).query)
        action = query['action'][0]
        calls.append(action)
        clock[0] += .65
        if action == 'getblocknobytime':
            value = str(hint)
        elif action == 'eth_getBlockByNumber':
            height = int(query['tag'][0], 16)
            if not 0 <= height <= head:
                raise AssertionError('read outside canonical head')
            value = {'number': hex(height), 'hash': '0x' + f'{height:064x}',
                     'timestamp': hex(height // plateau)}
        else:
            raise AssertionError('unexpected provider action')
        return io.BytesIO(json.dumps({'status': '1', 'result': value}).encode())
    provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'test-only-not-a-real-key',
                                 'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '0.5'})
    with patch('time.monotonic', side_effect=lambda: clock[0]), patch('time.sleep', side_effect=sleep), \
         patch('urllib.request.urlopen', side_effect=respond), ScanDiagnostics(sink=None) as diagnostic:
        resolved = provider.first_block_at(target, head) if method is None else method(provider, target, head)
        report = diagnostic.summary()
    return {'block': resolved, 'requests': len(calls), 'elapsed_seconds': round(clock[0], 3),
            'diagnostics': report}


class BoundaryCostTests(unittest.TestCase):
    def test_300_block_hint_error_does_not_cause_hundreds_of_calls(self):
        result = measure_boundary()
        self.assertEqual(result['block'], 1280)
        self.assertLessEqual(result['requests'], 25)
        self.assertLess(result['elapsed_seconds'], 60)

    def test_boundary_matches_lower_bound_with_plateaus_and_inaccurate_hints(self):
        randomizer = random.Random(418)
        for _ in range(500):
            size = randomizer.randint(1, 1500)
            stamps, value = [], 0
            for i in range(size):
                value += randomizer.choice([0, 0, 1, 2, 5])
                stamps.append(value)
            target = randomizer.randint(0, value + 10)
            hint = randomizer.randrange(size)
            reads = []
            def block_at(height):
                self.assertTrue(0 <= height < size)
                reads.append(height)
                return {'timestamp': stamps[height]}
            self.assertEqual(first_block_at_timestamp(block_at, target, size - 1, hint),
                             bisect_left(stamps, target))
            self.assertLessEqual(len(reads), 25)

    def test_accurate_hint_uses_only_three_provider_requests(self):
        result = measure_boundary(hint=1279)
        self.assertEqual(result['block'], 1280)
        self.assertEqual(result['requests'], 3)

    def test_provider_hint_above_head_is_still_an_explicit_error(self):
        from runtime.event_leaderboards.scanner import SourceIncomplete
        with self.assertRaisesRegex(SourceIncomplete, 'boundary_above_head'):
            measure_boundary(hint=3001)


if __name__ == '__main__':
    unittest.main()
