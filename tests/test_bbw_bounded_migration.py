"""Preserve the bounded migration queries already deployed to Railway."""
import unittest
from unittest.mock import patch

from runtime import bbw_community_metrics as metrics


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
