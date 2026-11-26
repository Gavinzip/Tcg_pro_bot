"""Documented Etherscan envelopes plus explicitly labelled compatibility cases.

No credentialed calls. Examples follow the official endpoint documentation;
edge cases are synthetic robustness tests, not captured production responses.
"""
import io
import json
import random
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from runtime.event_leaderboards.diagnostics import PREFIX, ScanDiagnostics
from runtime.event_leaderboards.scanner import EtherscanProvider, SourceIncomplete, number
from tests.test_event_provider_retries import Clock


class IntegerContractTests(unittest.TestCase):
    def test_documented_decimal_and_rpc_hex_are_exact_without_float_conversion(self):
        for n in (0, 1, 9251482, 135750000, 2**53 + 1, 2**256 - 1):
            for value in (n, str(n), hex(n)):
                with self.subTest(value=value):
                    self.assertEqual(number(value), n)

    def test_compatible_hex_case_and_outer_whitespace_are_not_misclassified(self):
        for value in ('0X8D2A9A', ' 0x8d2a9a ', '\t0X8D2A9A\n', ' 9251482 ', '+9251482', '0009251482'):
            with self.subTest(value=value):
                self.assertEqual(number(value), 9251482)

    def test_python_only_numeric_spellings_are_not_wire_integers(self):
        for value in ('1_000', '0x1_00', '１２３', '١٢٣', '0xＦＦ'):
            with self.subTest(value=value):
                with self.assertRaisesRegex(SourceIncomplete, '^invalid_integer$'):
                    number(value)

    def test_invalid_values_are_rejected_without_coercion_or_rounding(self):
        for value in (None, True, False, 1.0, 1.5, float('nan'), float('inf'), {}, [],
                      '', ' ', 'null', '0x', '1.0', '1e3', '1,000', '123junk', '0b10', '0o10', '12\x003'):
            with self.subTest(value=repr(value)):
                with self.assertRaisesRegex(SourceIncomplete, '^invalid_integer$'):
                    number(value)
        for value in (-1, '-1', ' -123 '):
            with self.subTest(value=value):
                with self.assertRaisesRegex(SourceIncomplete, '^negative_integer$'):
                    number(value)

    def test_seeded_decimal_hex_round_trips_cover_full_uint256_precision(self):
        rng = random.Random(20261126)
        for _ in range(1000):
            n = rng.getrandbits(rng.randrange(257))
            self.assertEqual(number(str(n)), n)
            self.assertEqual(number(hex(n)), n)
            self.assertEqual(number(' \t' + hex(n).upper() + '\n'), n)


class ProviderIntegerContractTests(unittest.TestCase):
    def invoke(self, body, operation):
        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'fixture-no-real-key',
            'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '4'})
        clock, lines = Clock(), []
        with patch('urllib.request.urlopen', return_value=io.BytesIO(json.dumps(body).encode())) as read, \
             patch('time.monotonic', side_effect=clock.read), patch('time.sleep', side_effect=clock.sleep), \
             ScanDiagnostics(sink=lines.append):
            try:
                value, error = operation(provider), None
            except SourceIncomplete as caught:
                value, error = None, str(caught)
        return value, error, read.call_count, [json.loads(s[len(PREFIX):]) for s in lines]

    def test_documented_envelopes_use_separate_decimal_and_rpc_formats(self):
        value, error, calls, _ = self.invoke({'status': '1', 'message': 'OK', 'result': '9251482'},
            lambda p: number(p.request('block', 'getblocknobytime', timestamp=1578638524, closest='before')))
        self.assertEqual((value, error, calls), (9251482, None, 1))
        value, error, calls, _ = self.invoke({'jsonrpc': '2.0', 'id': 83, 'result': '0x1661760'}, lambda p: p.head())
        self.assertEqual((value, error, calls), (int('1661760', 16), None, 1))

    def test_valid_hint_variations_take_normal_search_without_fallback_or_retry(self):
        for hint in ('499', 499, '0x1f3', '0X1F3', ' \t0x1f3\n'):
            with self.subTest(hint=hint):
                clock, lines, actions = Clock(), [], []
                def read(url, timeout):
                    query = parse_qs(urlsplit(url).query)
                    action = query['action'][0]
                    actions.append(action)
                    if action == 'getblocknobytime':
                        data = {'status': '1', 'message': 'OK', 'result': hint}
                    else:
                        self.assertEqual(action, 'eth_getBlockByNumber')
                        height = int(query['tag'][0], 16)
                        data = {'jsonrpc': '2.0', 'id': 1, 'result': {'number': hex(height),
                            'hash': '0x' + f'{height:064x}', 'timestamp': hex(1790000000 + height // 4)}}
                    return io.BytesIO(json.dumps(data).encode())
                provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'fixture'})
                with patch('urllib.request.urlopen', side_effect=read), \
                     patch('time.monotonic', side_effect=clock.read), patch('time.sleep', side_effect=clock.sleep), \
                     ScanDiagnostics(sink=lines.append) as diagnostic:
                    self.assertEqual(provider.first_block_at(1790000125, 1000), 500)
                self.assertEqual(diagnostic.totals['errors'], 0)
                self.assertEqual(diagnostic.totals['retries'], 0)
                self.assertEqual(actions.count('getblocknobytime'), 1)
                records = [json.loads(s[len(PREFIX):]) for s in lines]
                evidence = next(r['observation'] for r in records if r['kind'] == 'provider_response')
                self.assertEqual(evidence['integer_issue'], 'valid')
                self.assertEqual(evidence['hint_number'], 499)

    def test_missing_null_and_foreign_nested_result_are_not_invented_as_blocks(self):
        for body, code in [({'status': '1'}, 'provider_missing_result'),
                           ({'status': '1', 'result': None}, 'provider_missing_result'),
                           ({'status': '1', 'result': {'blockNumber': '9251482'}}, 'provider_invalid_boundary_hint'),
                           ({'status': '1', 'result': True}, 'provider_invalid_boundary_hint'),
                           ({'status': '1', 'result': 9251482.0}, 'provider_invalid_boundary_hint'),
                           ({'status': 1, 'result': '9251482'}, 'provider_rejected_request'),
                           (['9251482'], 'provider_invalid_response')]:
            with self.subTest(body=body):
                _, error, calls, _ = self.invoke(body, lambda p: p.request('block', 'getblocknobytime'))
                self.assertEqual((error, calls), (code, 1))

    def test_documented_errors_stay_errors_even_when_result_looks_numeric(self):
        for result, code in [('9251482', 'provider_rejected_request'), ('Invalid API Key', 'provider_rejected_request'),
                             ('Daily credit quota exhausted', 'provider_quota_exhausted'),
                             ('Query Timeout occured. Please select a smaller result dataset', 'provider_query_limit')]:
            with self.subTest(result=result):
                _, error, calls, _ = self.invoke({'status': '0', 'message': 'NOTOK', 'result': result},
                    lambda p: p.request('block', 'getblocknobytime'))
                self.assertEqual((error, calls), (code, 1))

    def test_success_message_is_not_used_as_an_integer_or_as_an_extra_status_gate(self):
        for message in ('OK', 'OK-Missing/Invalid API Key, rate limit of 1/5sec applied', None):
            with self.subTest(message=message):
                value, error, calls, _ = self.invoke({'status': '1', 'message': message, 'result': '9251482'},
                    lambda p: number(p.request('block', 'getblocknobytime')))
                self.assertEqual((value, error, calls), (9251482, None, 1))

    def test_diagnostic_and_request_validation_agree_for_invalid_numeric_spellings(self):
        for value in ('1_000', '0x1_00', '１２３', '١٢٣', '-1', '', [], False):
            with self.subTest(value=value):
                _, error, calls, records = self.invoke({'status': '1', 'result': value},
                    lambda p: p.request('block', 'getblocknobytime'))
                self.assertEqual((error, calls), ('provider_invalid_boundary_hint', 1))
                obs = next(r['observation'] for r in records if r['kind'] == 'provider_response')
                expected = 'negative_integer' if value == '-1' else 'empty' if value == '' else \
                    'wrong_type' if type(value) is not str else 'invalid_integer'
                self.assertEqual(obs['integer_issue'], expected)

    def test_rpc_canonical_block_fields_are_parsed_and_matched_exactly(self):
        for height, stamp in [('0x1f4', '0x6ab13dbd'), ('0X1F4', ' 0X6AB13DBD '), (500, 1790000573)]:
            with self.subTest(height=height):
                body = {'jsonrpc': '2.0', 'id': 1, 'result': {'number': height, 'timestamp': stamp, 'hash': '0x' + 'a' * 64}}
                value, error, calls, _ = self.invoke(body, lambda p: p.block(500))
                self.assertIsNone(error)
                self.assertEqual(value['number'], 500)
                self.assertEqual(value['timestamp'], 1790000573)
                self.assertEqual(calls, 1)


class SocketIntegerContractTests(unittest.TestCase):
    def test_normal_and_compatible_formats_cross_both_real_http_transports(self):
        scenarios = [
            ('block', b'{"status":"1","message":"OK","result":"9251482"}'),
            ('block', b'\xef\xbb\xbf  {"status":"1","result":"9251482"}\r\n'),
            ('block', b'{"status":"1","result":9251482}'),
            ('block', b'{"status":"1","result":" 0X8D2A9A "}'),
            ('proxy', b'{"jsonrpc":"2.0","id":83,"result":"0x8d2a9a"}')]
        payload, paths = [b''], []
        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def do_GET(self):
                paths.append(parse_qs(urlsplit(self.path).query))
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(payload[0])))
                self.end_headers()
                self.wfile.write(payload[0])
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            for pooled in ('0', '1'):
                provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'fixture',
                    'EVENT_LEADERBOARD_HTTP_POOL': pooled, 'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '4',
                    'BSCSCAN_API_URL': 'http://127.0.0.1:' + str(server.server_port) + '/v2/api'})
                try:
                    for module, body in scenarios:
                        with self.subTest(pooled=pooled, module=module, body=body):
                            payload[0] = body
                            lines = []
                            with ScanDiagnostics(sink=lines.append) as diagnostic:
                                value = provider.head() if module == 'proxy' else number(provider.request(
                                    'block', 'getblocknobytime', timestamp=1578638524, closest='before'))
                            self.assertEqual(value, 9251482)
                            self.assertEqual((diagnostic.totals['attempts'], diagnostic.totals['errors']), (1, 0))
                finally:
                    provider.close()
            self.assertEqual(len(paths), 10)
            self.assertTrue(all(q['chainid'] == ['56'] for q in paths))
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
