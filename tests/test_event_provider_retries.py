"""Bounded same-source timeout recovery, rate limits and checkpoint regressions."""
import io
import json
import unittest
from unittest.mock import patch
import urllib.error

from runtime.event_leaderboards.scanner import EtherscanProvider, SourceIncomplete

class Clock:
    def __init__(self):
        self.now = 100.
        self.delays = []
    def read(self):
        return self.now
    def sleep(self, delay):
        self.delays.append(delay)
        self.now += delay

class ProviderRetryTests(unittest.TestCase):
    def run_requests(self, outcomes, *, count=1, action="getLogs"):
        clock, starts, urls, timeouts = Clock(), [], [], []
        provider = EtherscanProvider({"EVENT_LEADERBOARD_API_KEY": "private-test-key",
                                     "EVENT_LEADERBOARD_REQUESTS_PER_SECOND": "0.5"})
        def open_request(url, timeout):
            starts.append(clock.now); urls.append(url); timeouts.append(timeout)
            outcome = outcomes[len(starts) - 1]
            if isinstance(outcome, BaseException):
                if isinstance(outcome, TimeoutError):
                    clock.now += 15
                raise outcome
            return io.BytesIO(json.dumps({"status": "0", "message": "No records found", "result": []}).encode())
        error = None
        with patch("runtime.event_leaderboards.scanner.time.monotonic", side_effect=clock.read), \
                patch("runtime.event_leaderboards.scanner.time.sleep", side_effect=clock.sleep), \
                patch("runtime.event_leaderboards.scanner.urllib.request.urlopen", side_effect=open_request):
            try:
                for _ in range(count):
                    self.assertEqual(provider.request("logs", action, fromBlock=10, toBlock=20, page=1), [])
            except SourceIncomplete as caught:
                error = caught
        return clock, starts, urls, timeouts, error

    def test_timeout_then_success_replays_the_identical_read(self):
        clock, starts, urls, timeouts, error = self.run_requests([TimeoutError("private-test-key"), None])
        self.assertIsNone(error)
        self.assertEqual(starts, [100., 117.])
        self.assertEqual(clock.delays, [2])
        self.assertEqual(urls[0], urls[1])
        self.assertEqual(timeouts, [15., 15.])

    def test_persistent_timeout_stops_after_three_attempts_with_safe_details(self):
        clock, starts, urls, timeouts, error = self.run_requests([TimeoutError("private-test-key")] * 3)
        self.assertEqual(starts, [100., 117., 136.])
        self.assertEqual(clock.delays, [2, 4])
        self.assertEqual(clock.now, 151.)
        self.assertEqual(len(set(urls)), 1)
        self.assertEqual(timeouts, [15.] * 3)
        self.assertEqual(str(error), "provider_timeout")
        self.assertEqual(error.diagnostic["attempt"], 3)
        self.assertEqual(error.diagnostic["max_attempts"], 3)
        self.assertEqual(error.diagnostic["elapsed_ms"], 15000)
        self.assertEqual(error.diagnostic["from_block"], 10)
        self.assertNotIn("private-test-key", json.dumps(error.diagnostic))
        self.assertNotIn("https://", json.dumps(error.diagnostic))

    def test_successful_queries_keep_half_request_per_second_pacing(self):
        clock, starts, _, _, error = self.run_requests([None, None], count=2)
        self.assertIsNone(error)
        self.assertEqual(starts, [100., 102.])
        self.assertEqual(clock.delays, [2.])

    def test_http_rate_limit_after_timeout_stops_without_another_attempt(self):
        limited = urllib.error.HTTPError("https://private-test-key", 429, "private-test-key", {"Retry-After": "3600"}, io.BytesIO())
        clock, starts, _, _, error = self.run_requests([TimeoutError(), limited])
        self.assertEqual(starts, [100., 117.])
        self.assertEqual(clock.delays, [2])
        self.assertEqual(str(error), "provider_http_429")
        self.assertEqual(error.diagnostic["attempt"], 2)

    def test_unlisted_action_is_not_retried(self):
        clock, starts, _, _, error = self.run_requests([TimeoutError()], action="unknown-action")
        self.assertEqual(len(starts), 1)
        self.assertEqual(clock.delays, [])
        self.assertEqual(error.diagnostic["max_attempts"], 1)
