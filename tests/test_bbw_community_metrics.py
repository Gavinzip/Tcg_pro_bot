from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from http.server import ThreadingHTTPServer
import json
import threading
import time
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen

from requests import Response

import profile_api
from runtime import bbw_community_metrics as metrics


class BbwMetricsRateLimitTests(unittest.TestCase):
    def setUp(self):
        self.lookup = metrics.BbwCommunityMetrics({
            "BBW_METRICS_TOKEN": "test-token",
            "BSCSCAN_API_KEY": "test-key",
            "BBW_METRICS_CUTOFF": "2026-11-21T07:00:00Z",
        })
        self.addCleanup(self.lookup._pool.shutdown)
        self.request_times = deque()
        self.lock = threading.Lock()
        self.rejections = 0
        self.contract = metrics.DEFAULT_PACK_CONTRACTS[1]
        self.tx_hash = "0x" + "3" * 64
        self.amount = 36 * metrics.USDT_UNIT

    def response(self, data, status=200):
        response = Response()
        response.status_code = status
        response._content = json.dumps(data).encode()
        return response

    def provider_get(self, url, *, params, timeout):
        with self.lock:
            now = time.monotonic()
            while self.request_times and self.request_times[0] <= now - 1:
                self.request_times.popleft()
            if len(self.request_times) >= 5:
                self.rejections += 1
                return self.response({"status": "0", "message": "NOTOK",
                                      "result": "Max calls per sec rate limit reached (5/sec)"})
            self.request_times.append(now)

        action = params["action"]
        if action == "getblocknobytime":
            self.assertEqual(params["closest"], "before")
            return self.response({"status": "1", "message": "OK", "result": "134000000"})
        if action == "token1155tx":
            self.assertEqual(params["offset"], 1000)
            rows = [{"from": "0x" + "0" * 40, "to": params["address"], "timeStamp": "1700000000",
                     "contractAddress": metrics.SBT_CONTRACT, "tokenID": "1", "tokenValue": "3"}]
        elif action == "tokentx":
            self.assertEqual(params["offset"], 10000)
            rows = [{"from": params["address"], "to": self.contract, "timeStamp": "1700000000",
                     "contractAddress": metrics.USDT_CONTRACT, "tokenDecimal": "18", "value": str(self.amount),
                     "blockNumber": "100000000", "hash": self.tx_hash}]
        elif params["address"] == metrics.MIGRATION_HELPER_CONTRACT:
            self.assertEqual(params["offset"], 1000)
            rows = []
        else:
            rows = [{"transactionHash": self.tx_hash, "logIndex": "0x1", "address": self.contract,
                     "topics": [params["topic0"], params["topic1"], "0x" + "2" * 64], "data": f"0x{self.amount:064x}"}]
        return self.response({"status": "1", "message": "OK", "result": rows})

    def test_full_usdt_window_is_not_reported_as_complete(self):
        wallet = f"0x{1:040x}"
        rows = [{"from": wallet, "to": f"0x{2:040x}", "timeStamp": "1700000000"}] * 10_000
        with patch.object(metrics, "_fetch_tokentx_page", return_value=rows) as fetch_page:
            with self.assertRaisesRegex(RuntimeError, "metrics_provider_page_limit"):
                metrics._pack_payments(self.lookup.chain, wallet, 1_800_000_000)
        fetch_page.assert_called_once_with(self.lookup.chain, wallet, page=1, sort="asc", offset=10_000)

    def test_slow_success_keeps_a_full_cache_lifetime_after_completion(self):
        wallet = f"0x{1:040x}"
        clock = [1_800_000_000.0]
        result = (HTTPStatus.OK, {"sbtCount": 150, "paidPackSpendUsdt": "123.45"})

        def slow_compute(*_):
            clock[0] += metrics.SBT_AND_SPEND_CACHE_TTL + 30
            return result

        with (patch.object(metrics.time, "time", side_effect=lambda: clock[0]),
              patch.object(self.lookup, "_compute", side_effect=slow_compute) as compute):
            self.assertEqual(self.lookup.lookup(wallet), result)
            clock[0] += metrics.SBT_AND_SPEND_CACHE_TTL - 1
            self.assertEqual(self.lookup.lookup(wallet), result)
            self.assertEqual(compute.call_count, 1)
            clock[0] += 2
            self.assertEqual(self.lookup.lookup(wallet), result)
            self.assertEqual(compute.call_count, 2)


    def test_simultaneous_wallets_stay_below_provider_limit_and_keep_totals(self):
        wallets = [f"0x{i:040x}" for i in range(1, 5)]
        start = threading.Barrier(len(wallets))
        server = ThreadingHTTPServer(("127.0.0.1", 0), profile_api.ProfileApiHandler)
        self.addCleanup(server.server_close)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        self.addCleanup(server_thread.join)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_port}/v1/bbw-community-metrics"

        def query(wallet):
            start.wait(timeout=4)
            request = Request(url, data=json.dumps({"wallet": wallet}).encode(), headers={
                "Authorization": "Bearer test-token",
                "Content-Type": "application/json",
            })
            with urlopen(request, timeout=12) as response:
                return response.status, json.load(response)

        with (
            patch.object(profile_api, "_COMMUNITY_METRICS", self.lookup),
            patch.object(profile_api.ProfileApiHandler, "log_message"),
            patch("scripts.onchain_metrics.requests.get", side_effect=self.provider_get),
        ):
            with ThreadPoolExecutor(max_workers=len(wallets)) as callers:
                results = list(callers.map(query, wallets))

        self.assertEqual(results, [(HTTPStatus.OK, {"sbtCount": 1, "paidPackSpendUsdt": "36"})] * len(wallets))
        self.assertEqual(self.rejections, 0)

    def test_provider_rate_limit_delays_retry_for_both_response_formats(self):
        for index, status in enumerate((200, 429), start=1):
            with self.subTest(status=status):
                attempts = []

                def throttled_get(url, *, params, timeout):
                    if params["action"] == "tokentx":
                        attempts.append(time.monotonic())
                        if len(attempts) == 1:
                            return self.response({"status": "0", "message": "NOTOK",
                                                  "result": "Max calls per sec rate limit reached (5/sec)"}, status)
                    return self.provider_get(url, params=params, timeout=timeout)

                with patch("scripts.onchain_metrics.requests.get", side_effect=throttled_get):
                    result = self.lookup.lookup(f"0x{index:040x}")

                self.assertEqual(result, (HTTPStatus.OK, {"sbtCount": 1, "paidPackSpendUsdt": "36"}))
                self.assertEqual(len(attempts), 2)
                self.assertGreaterEqual(attempts[1] - attempts[0], 1)
