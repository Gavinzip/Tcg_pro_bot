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
                     "topics": [params["topic0"], params["topic1"]], "data": f"0x{self.amount:064x}"}]
        return self.response({"status": "1", "message": "OK", "result": rows})

    def test_full_usdt_window_is_not_reported_as_complete(self):
        wallet = f"0x{1:040x}"
        rows = [{"from": wallet, "to": f"0x{2:040x}", "timeStamp": "1700000000"}] * 10_000
        with patch.object(metrics, "_fetch_tokentx_page", return_value=rows) as fetch_page:
            with self.assertRaisesRegex(RuntimeError, "metrics_provider_page_limit"):
                metrics._pack_payments(self.lookup.chain, wallet, 1_800_000_000)
        fetch_page.assert_called_once_with(self.lookup.chain, wallet, page=1, sort="asc", offset=10_000)


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


class BoundedMigrationQueryTests(unittest.TestCase):
    def setUp(self):
        self.wallet = f"0x{1:040x}"
        self.cfg = metrics.OnchainConfig(api_url="https://example.invalid", chain_id=56, api_key="test-key",
            usdt_contract=metrics.USDT_CONTRACT, pack_contracts=metrics.DEFAULT_PACK_CONTRACTS,
            marketplace_contract="", page_size=2, retries=1, backoff_sec=.2)

    def migration(self, old_number, block):
        return {"address": metrics.MIGRATION_HELPER_CONTRACT,
            "topics": [metrics.MIGRATION_HELPER_TOPIC, "0x" + "0" * 64,
                       "0x" + f"{old_number:064x}", "0x" + self.wallet[2:].rjust(64, "0")],
            "timeStamp": "0x50", "blockNumber": hex(block), "transactionHash": "0x" + f"{block:064x}",
            "logIndex": "0x1"}

    def test_later_page_timeout_requeries_disjoint_full_ranges_without_loss_or_duplicates(self):
        from runtime.bounded_event_logs import complete_bounded_logs
        calls = []

        def fetch(start, end, page):
            calls.append((start, end, page))
            if start == 10 and end == 17 and page == 2:
                raise RuntimeError("bscscan logs request failed: Query Timeout occured. Please select a smaller result dataset")
            rows = [{"block": n} for n in range(start, end + 1)]
            return rows[(page - 1) * 2:page * 2]

        result = complete_bounded_logs(fetch, 10, 17, 2)
        self.assertEqual([row["block"] for row in result], list(range(10, 18)))
        self.assertEqual(calls[:2], [(10, 17, 1), (10, 17, 2)])
        self.assertEqual(set((start, end) for start, end, _ in calls[2:]), {(10, 13), (14, 17)})

    def test_full_result_window_splits_but_single_block_keeps_every_paginated_event(self):
        from runtime.bounded_event_logs import complete_bounded_logs, LogResultWindowFull

        def range_page(start, end, page):
            rows = [{"block": n} for n in range(start, end + 1)]
            return rows[(page - 1) * 2:page * 2]

        self.assertEqual(complete_bounded_logs(range_page, 10, 13, 2, result_limit=4),
                         [{"block": n} for n in range(10, 14)])
        rows = [{"index": n} for n in range(5)]
        self.assertEqual(complete_bounded_logs(lambda start, end, page: rows[(page - 1) * 2:page * 2], 10, 10, 2), rows)
        with self.assertRaisesRegex(LogResultWindowFull, "metrics_provider_page_limit"):
            complete_bounded_logs(lambda start, end, page: rows[(page - 1) * 2:page * 2], 10, 10, 2, result_limit=4)

    def test_single_block_timeout_and_other_errors_never_become_empty_or_split(self):
        from runtime.bounded_event_logs import complete_bounded_logs
        from unittest.mock import Mock
        for error, bounds in [("Query Timeout occured", (10, 10)), ("Max calls per sec rate limit reached", (10, 20)),
                              ("invalid API key", (10, 20)), ("invalid event", (10, 20))]:
            with self.subTest(error=error):
                fetch = Mock(side_effect=RuntimeError(error))
                with self.assertRaisesRegex(RuntimeError, error):
                    complete_bounded_logs(fetch, *bounds, 2)
                fetch.assert_called_once_with(*bounds, 1)
        with self.assertRaisesRegex(RuntimeError, "metrics_invalid_logs_page"):
            complete_bounded_logs(lambda start, end, page: [None], 10, 20, 2)

    def test_cutoff_bound_split_and_wallet_topic_are_preserved(self):
        rows = [self.migration(2, 10), self.migration(3, 17)]
        calls = []

        def logs(cfg, **kwargs):
            calls.append(kwargs)
            self.assertEqual(kwargs["indexed_wallet"], self.wallet)
            self.assertEqual(kwargs["indexed_wallet_topic"], 3)
            self.assertEqual(kwargs["contract"], metrics.MIGRATION_HELPER_CONTRACT)
            self.assertEqual(kwargs["topic0"], metrics.MIGRATION_HELPER_TOPIC)
            start, end, page = kwargs["from_block"], kwargs["to_block"], kwargs["page"]
            if end - start > 3:
                raise RuntimeError("Query Timeout occured. Please select a smaller result dataset")
            found = [row for row in rows if start <= int(row["blockNumber"], 16) <= end]
            return found[(page - 1) * 2:page * 2]

        with (patch.object(metrics, "MIGRATION_START_BLOCK", 10),
              patch.object(metrics, "block_at_or_before", return_value=17) as bound,
              patch.object(metrics, "_fetch_event_logs_page", side_effect=logs)):
            sources = metrics._spend_source_wallets(self.cfg, self.wallet, 100)
        bound.assert_called_once_with(self.cfg, 100)
        self.assertEqual(sources, tuple(f"0x{n:040x}" for n in (1, 2, 3)))
        self.assertEqual([(row["from_block"], row["to_block"]) for row in calls], [(10, 17), (10, 13), (14, 17)])
        self.assertTrue(all(row["to_block"] != 0 for row in calls))

    def test_invalid_migration_event_and_non_timeout_query_error_fail_closed(self):
        malformed = self.migration(2, 10)
        malformed["topics"][0] = "0x" + "f" * 64
        with (patch.object(metrics, "MIGRATION_START_BLOCK", 10),
              patch.object(metrics, "block_at_or_before", return_value=17),
              patch.object(metrics, "_fetch_event_logs_page", return_value=[malformed])):
            with self.assertRaisesRegex(RuntimeError, "invalid_wallet_migration_event"):
                metrics._spend_source_wallets(self.cfg, self.wallet, 100)
        with (patch.object(metrics, "MIGRATION_START_BLOCK", 10),
              patch.object(metrics, "block_at_or_before", return_value=17),
              patch.object(metrics, "_fetch_event_logs_page", side_effect=RuntimeError("provider unauthorized")) as query):
            with self.assertRaisesRegex(RuntimeError, "provider unauthorized"):
                metrics._spend_source_wallets(self.cfg, self.wallet, 100)
        query.assert_called_once()

    def test_shared_log_fetcher_rejects_mixed_malformed_provider_records(self):
        response = {"status": "1", "message": "OK", "result": [self.migration(2, 10), None]}
        with (patch.object(metrics, "MIGRATION_START_BLOCK", 10),
              patch.object(metrics, "block_at_or_before", return_value=17),
              patch("scripts.onchain_metrics._fetch_etherscan_json", return_value=response) as request):
            with self.assertRaisesRegex(RuntimeError, "bscscan logs malformed records"):
                metrics._spend_source_wallets(self.cfg, self.wallet, 100)
        request.assert_called_once()

    def test_cutoff_resolution_validates_response_and_uses_shared_pacer_request(self):
        from runtime import bounded_event_logs as bounded
        with patch.object(bounded, "_fetch_etherscan_json", return_value={"status": "1", "result": "134620239"}) as request:
            self.assertEqual(bounded.block_at_or_before(self.cfg, 100), 134620239)
        args = request.call_args.args
        self.assertIs(args[0], self.cfg)
        self.assertEqual(args[1]["action"], "getblocknobytime")
        self.assertEqual(args[1]["timestamp"], 100)
        for response in ({"status": "0", "result": "Query Timeout occured"}, {"status": "1", "result": None},
                         {"status": "1", "result": "latest"}):
            with self.subTest(response=response), patch.object(bounded, "_fetch_etherscan_json", return_value=response):
                with self.assertRaisesRegex(RuntimeError, "metrics_cutoff_block_unavailable"):
                    bounded.block_at_or_before(self.cfg, 100)
