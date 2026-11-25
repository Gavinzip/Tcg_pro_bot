"""Classify real urllib failure shapes while never serializing private context."""
import io
import json
import socket
import ssl
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import urllib.error

from runtime.event_leaderboards.scanner import EtherscanProvider, EventLeaderboardScanner, SourceIncomplete, VRF_V3_CONTRACT
from runtime.event_leaderboards.diagnostics import ScanDiagnostics
from tests.test_event_provider_retries import Clock


class ProviderErrorTests(unittest.TestCase):
    def provider(self):
        return EtherscanProvider({"EVENT_LEADERBOARD_API_KEY": "private-test-key"})

    def failure(self, error, expected_attempts=1):
        clock = Clock()
        with patch("runtime.event_leaderboards.scanner.urllib.request.urlopen", side_effect=error) as call, \
                patch("time.monotonic", side_effect=clock.read), patch("time.sleep", side_effect=clock.sleep):
            with self.assertRaises(SourceIncomplete) as caught:
                self.provider().request("logs", "getLogs", fromBlock=10, toBlock=20, page=1,
                                        private_value="private-test-key")
        self.assertEqual(call.call_count, expected_attempts)
        serialized = json.dumps({"code": str(caught.exception), "details": caught.exception.diagnostic})
        self.assertNotIn("private-test-key", serialized)
        self.assertNotIn("https://", serialized)
        self.assertEqual(caught.exception.diagnostic["from_block"], 10)
        self.assertEqual(caught.exception.diagnostic["to_block"], 20)
        return caught.exception

    def test_http_status_is_not_misclassified_as_connection_failure(self):
        for status in (401, 403, 429, 500, 502, 503, 504):
            with self.subTest(status=status):
                error = urllib.error.HTTPError("https://private-test-key", status,
                                              "private-test-key", {}, io.BytesIO(b"private-test-key"))
                result = self.failure(error)
                self.assertEqual(str(result), f"provider_http_{status}")
                self.assertEqual(result.diagnostic["http_status"], status)

    def test_direct_and_wrapped_timeouts_are_identifiable(self):
        for error in (TimeoutError("private-test-key"), urllib.error.URLError(TimeoutError("private-test-key"))):
            self.assertEqual(str(self.failure(error, expected_attempts=3)), "provider_timeout")

    def test_certificate_and_tls_failures_remain_distinct(self):
        for error, code in ((ssl.SSLCertVerificationError("private-test-key"), "provider_tls_verification_failure"),
                            (ssl.SSLError("private-test-key"), "provider_tls_failure")):
            self.assertEqual(str(self.failure(urllib.error.URLError(error))), code)

    def test_dns_and_connection_failures_remain_distinct(self):
        self.assertEqual(str(self.failure(urllib.error.URLError(socket.gaierror(-2, "private-test-key")))), "provider_dns_failure")
        self.assertEqual(str(self.failure(ConnectionResetError("private-test-key"))), "provider_connection_failure")

    def test_invalid_request_is_not_invalid_response_json(self):
        self.assertEqual(str(self.failure(ValueError("private-test-key"))), "provider_invalid_request")
        with patch("runtime.event_leaderboards.scanner.urllib.request.urlopen", return_value=io.BytesIO(b"private-test-key")):
            with self.assertRaises(SourceIncomplete) as caught:
                self.provider().head()
        self.assertEqual(str(caught.exception), "provider_invalid_json")
        self.assertEqual(caught.exception.diagnostic["stage"], "read_json")
        self.assertNotIn("private-test-key", json.dumps(caught.exception.diagnostic))

    def test_valid_empty_log_response_still_succeeds(self):
        response = io.BytesIO(json.dumps({"status": "0", "message": "No records found", "result": []}).encode())
        with patch("runtime.event_leaderboards.scanner.urllib.request.urlopen", return_value=response):
            self.assertEqual(self.provider().request("logs", "getLogs"), [])

    def test_scanner_persists_only_sanitized_diagnostic(self):
        diagnostic = {"module": "proxy", "action": "eth_blockNumber", "stage": "connect", "elapsed_ms": 15000}
        failure = SourceIncomplete("provider_timeout", diagnostic=diagnostic)
        definition = SimpleNamespace(checkout_schema_version=2, revision=2, chain_id=56, token_decimals=18,
                                     start_ts=0, end_ts=100, contracts=[VRF_V3_CONTRACT], builder=None,
                                     contract_schemas={VRF_V3_CONTRACT: "v3"})
        recorded = []
        store = SimpleNamespace(get_event=lambda _: definition, read_snapshot=lambda _: (None, [], None),
                                get_checkpoint=lambda *args: None,
                                record_failure=lambda event_id, code, progress: recorded.append((code, progress)))
        def head():
            raise failure
        result = EventLeaderboardScanner(store, SimpleNamespace(head=head)).sync("test", now=50)
        self.assertEqual(result["state"], "stale")
        self.assertEqual(recorded[0][0], "provider_timeout")
        self.assertEqual(recorded[0][1]["error_details"], diagnostic)


class ProviderResponseTests(unittest.TestCase):
    def read(self, payload, action='eth_getTransactionReceipt'):
        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'private-test-key',
                                     'EVENT_LEADERBOARD_HTTP_POOL': '1'})
        provider.spacing = 0
        clock = Clock()
        value, error = None, None
        with patch.object(provider._http, 'read_json', return_value=payload) as transport, \
                patch('time.monotonic', side_effect=clock.read), patch('time.sleep', side_effect=clock.sleep), \
                ScanDiagnostics(sink=None) as diagnostic:
            try:
                value = provider.receipt('0x' + '1' * 64) if action == 'eth_getTransactionReceipt' else provider.request('proxy', action)
            except SourceIncomplete as caught:
                error = caught
        provider.close()
        return value, error, diagnostic.summary(), transport.call_count

    def assert_failure(self, payload, code, action='eth_getTransactionReceipt'):
        value, error, summary, calls = self.read(payload, action)
        self.assertIsNone(value)
        self.assertIsNotNone(error)
        self.assertEqual(str(error), code)
        attempts = 2 if code == 'provider_rate_limit' else 1
        self.assertEqual(calls, attempts)
        self.assertEqual(summary['totals']['attempts'], attempts)
        self.assertEqual(summary['totals']['errors'], attempts)
        self.assertEqual(summary['totals']['retries'], attempts - 1)
        self.assertEqual(summary['failures'], {code: attempts})
        self.assertNotIn('private-test-key', json.dumps(summary))
        self.assertNotIn('https://', json.dumps(summary))

    def test_proxy_notok_rate_limit_is_not_a_successful_receipt_read(self):
        self.assert_failure({'status': '0', 'message': 'NOTOK',
                             'result': 'Max rate limit reached, private-test-key'}, 'provider_rate_limit')

    def test_daily_limit_is_distinct_from_per_second_limit(self):
        self.assert_failure({'status': '0', 'message': 'NOTOK',
                             'result': 'Daily API credit limit reached private-test-key'}, 'provider_quota_exhausted')

    def test_unknown_notok_body_is_rejected_without_guessing_or_leaking(self):
        self.assert_failure({'status': '0', 'message': 'NOTOK',
                             'result': 'Invalid API Key private-test-key https://private.example'}, 'provider_rejected_request')

    def test_null_receipt_is_already_a_missing_result_not_receipt_unavailable(self):
        self.assert_failure({'jsonrpc': '2.0', 'id': 1, 'result': None}, 'provider_missing_result')

    def test_non_object_receipt_shapes_are_counted_as_provider_failures(self):
        for value in ('private-test-key', '', [], 123, False):
            with self.subTest(kind=type(value).__name__):
                self.assert_failure({'jsonrpc': '2.0', 'id': 1, 'result': value}, 'receipt_unavailable')

    def test_rpc_error_object_remains_a_counted_failure(self):
        self.assert_failure({'jsonrpc': '2.0', 'error': {'code': -32000, 'message': 'private-test-key'}},
                            'provider_invalid_response')

    def test_gateway_errors_are_rejected_for_other_proxy_actions_too(self):
        for action in ('eth_blockNumber', 'eth_getBlockByNumber'):
            with self.subTest(action=action):
                self.assert_failure({'status': '0', 'message': 'NOTOK', 'result': 'Max rate limit reached'},
                                    'provider_rate_limit', action)

    def test_normal_json_rpc_result_does_not_need_an_etherscan_status_field(self):
        receipt = {'transactionHash': '0x' + '1' * 64, 'status': '0x1', 'logs': []}
        value, error, summary, calls = self.read({'jsonrpc': '2.0', 'id': 1, 'result': receipt})
        self.assertEqual(value, receipt)
        self.assertIsNone(error)
        self.assertEqual((calls, summary['totals']['errors']), (1, 0))
        self.assertEqual(self.read({'jsonrpc': '2.0', 'id': 1, 'result': '0xa'}, 'eth_blockNumber')[0], '0xa')


if __name__ == "__main__":
    unittest.main()
