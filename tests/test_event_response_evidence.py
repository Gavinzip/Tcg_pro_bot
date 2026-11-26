"""Actual provider/supervisor evidence contract; no external API or credentials."""
import asyncio
import hashlib
import io
import json
import threading
import unittest
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from runtime import production_jobs
from runtime.event_leaderboards.diagnostics import PREFIX, ScanDiagnostics, validated_line
from runtime.event_leaderboards.scanner import EtherscanProvider, SourceIncomplete
from runtime.event_leaderboards.response_evidence import MAX_BODY_SAMPLE, valid_observation
from tests.test_event_provider_retries import Clock


SECRET = 'fixture-private-key-never-log'


class Response(io.BytesIO):
    status = 200
    headers = {'Content-Type': 'application/json'}


class ResponseEvidenceTests(unittest.IsolatedAsyncioTestCase):
    def observe(self, payloads, *, request=None, raw=False):
        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': SECRET,
            'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '4'})
        clock, lines, calls = Clock(), [], []
        def read(url, timeout):
            calls.append(url)
            value = payloads[len(calls) - 1]
            return Response(value if raw else json.dumps(value).encode())
        error, value = None, None
        with patch('urllib.request.urlopen', side_effect=read), \
             patch('time.monotonic', side_effect=clock.read), patch('time.sleep', side_effect=clock.sleep), \
             ScanDiagnostics(sink=lines.append):
            try:
                value = (request or (lambda p: p.request('block', 'getblocknobytime', timestamp=1795652400,
                    closest='before', private_value=SECRET)))(provider)
            except SourceIncomplete as caught:
                error = caught
        records = [json.loads(line[len(PREFIX):]) for line in lines]
        observations = [row for row in records if row['kind'] == 'provider_response']
        self.assertTrue(observations, 'upstream response evidence is missing')
        serialized = '\n'.join(lines) + json.dumps(error.diagnostic if error else {})
        self.assertNotIn(SECRET, serialized)
        self.assertNotIn('https://', serialized)
        self.assertNotIn('private_value', serialized)
        return observations, lines, error, value, len(calls)

    def test_non_integer_success_keeps_envelope_shape_fingerprint_and_exact_query(self):
        body = {'status': '1', 'message': 'OK', 'result': 'not-a-number ' + SECRET}
        rows, _, error, _, calls = self.observe([body])
        row = rows[0]
        self.assertEqual(row['failure_code'], 'provider_invalid_boundary_hint')
        obs = row['observation']
        self.assertEqual(obs['http_status'], 200)
        self.assertEqual(obs['api_status'], 'success')
        self.assertEqual(obs['message'], 'ok')
        self.assertEqual(obs['result_type'], 'string')
        self.assertEqual(obs['integer_issue'], 'invalid_integer')
        self.assertEqual(obs['request'], {'timestamp': 1795652400, 'closest': 'before'})
        self.assertEqual(obs['body_digest'], hashlib.sha256(json.dumps(body).encode()).hexdigest())
        self.assertEqual((obs['attempt'], obs['max_attempts'], calls), (1, 3, 1))
        self.assertEqual(error.diagnostic['upstream'], obs)

    def test_negative_and_wrong_type_are_distinct_and_safe(self):
        for value, kind, issue in [('-1', 'string', 'negative_integer'),
                                   (False, 'bool', 'wrong_type'), ({'private': SECRET}, 'object', 'wrong_type')]:
            with self.subTest(kind=kind):
                rows, _, _, _, _ = self.observe([{'status': '1', 'result': value}])
                obs = rows[0]['observation']
                self.assertEqual((obs['result_type'], obs['integer_issue']), (kind, issue))
                if value == '-1':
                    self.assertEqual(obs['hint_number'], -1)

    def test_rejected_api_response_retains_known_reason_and_each_attempt(self):
        payloads = [{'status': '0', 'message': 'NOTOK', 'result': 'Max rate limit reached ' + SECRET},
                    {'status': '1', 'message': 'OK', 'result': '135750000'}]
        rows, _, error, value, calls = self.observe(payloads)
        self.assertIsNone(error)
        self.assertEqual(value, '135750000')
        self.assertEqual(calls, 2)
        self.assertEqual([r['observation']['attempt'] for r in rows], [1, 2])
        self.assertEqual([r['failure_code'] for r in rows], ['provider_rate_limit', 'ok'])
        self.assertEqual(rows[0]['observation']['result_message'], 'rate_limit')
        self.assertEqual(rows[1]['observation']['hint_number'], 135750000)
        self.assertEqual(rows[0]['observation']['request_ref'], rows[1]['observation']['request_ref'])
        self.assertTrue(all(valid_observation(row['observation']) for row in rows))

    def test_auth_quota_and_rpc_rejections_remain_distinguishable_without_new_retries(self):
        cases = [({'status': '0', 'result': 'Invalid API Key ' + SECRET}, 'invalid_api_key', 'provider_rejected_request'),
                 ({'status': '0', 'result': 'Daily credit quota exhausted ' + SECRET}, 'daily_quota', 'provider_quota_exhausted')]
        for body, reason, code in cases:
            with self.subTest(reason=reason):
                rows, _, error, _, calls = self.observe([body])
                self.assertEqual(str(error), code)
                self.assertEqual(calls, 1)
                self.assertEqual(rows[0]['observation']['result_message'], reason)
        rows, _, error, _, calls = self.observe([{'error': {'code': -32005, 'message': 'Max rate limit reached ' + SECRET}}],
            request=lambda p: p.request('proxy', 'eth_blockNumber'))
        self.assertEqual(str(error), 'provider_invalid_response')
        self.assertEqual(calls, 1)
        self.assertEqual(rows[0]['observation']['rpc_error_code'], -32005)
        self.assertEqual(rows[0]['observation']['rpc_error_message'], 'rate_limit')

    def test_urllib_http_error_keeps_status_and_bounded_fingerprint(self):
        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': SECRET})
        body = (SECRET.encode() + b'x' * (MAX_BODY_SAMPLE * 2))
        error = urllib.error.HTTPError('https://private.invalid/?apikey=' + SECRET, 503, SECRET,
            {'Content-Type': 'text/html'}, io.BytesIO(body))
        lines = []
        with patch('urllib.request.urlopen', side_effect=error) as read, ScanDiagnostics(sink=lines.append), \
             self.assertRaisesRegex(SourceIncomplete, 'provider_http_503'):
            provider.request('block', 'getblocknobytime', timestamp=1795652400)
        self.assertEqual(read.call_count, 1)
        rows = [json.loads(line[len(PREFIX):]) for line in lines]
        obs = next(r['observation'] for r in rows if r['kind'] == 'provider_response')
        self.assertEqual((obs['http_status'], obs['content_type'], obs['body_complete']), (503, 'html', False))
        self.assertEqual((obs['body_sample_bytes'], obs['digest_scope']), (MAX_BODY_SAMPLE, 'prefix'))
        self.assertEqual(obs['body_digest'], hashlib.sha256(body[:MAX_BODY_SAMPLE]).hexdigest())
        self.assertNotIn(SECRET, '\n'.join(lines))

    def test_connection_failure_does_not_invent_an_http_response(self):
        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': SECRET})
        lines = []
        with patch('urllib.request.urlopen', side_effect=urllib.error.URLError(SECRET)) as read, \
             ScanDiagnostics(sink=lines.append), self.assertRaisesRegex(SourceIncomplete, 'provider_connection_failure'):
            provider.request('proxy', 'eth_blockNumber')
        rows = [json.loads(line[len(PREFIX):]) for line in lines]
        obs = next(r['observation'] for r in rows if r['kind'] == 'provider_response')
        self.assertEqual((obs['http_status'], obs['digest_scope'], obs['json_state']), (0, 'none', 'unavailable'))
        self.assertEqual(read.call_count, 1)
        self.assertNotIn(SECRET, '\n'.join(lines))

    def test_large_success_body_is_fingerprinted_as_prefix_and_unknown_text_is_not_copied(self):
        body = {'status': '1', 'message': SECRET * 2000, 'result': SECRET * 4000}
        rows, _, error, _, calls = self.observe([body])
        obs = rows[0]['observation']
        self.assertEqual(str(error), 'provider_invalid_boundary_hint')
        self.assertEqual((calls, obs['message'], obs['integer_issue']), (1, 'other', 'not_inspected'))
        self.assertEqual((obs['body_sample_bytes'], obs['digest_scope'], obs['body_complete']), (MAX_BODY_SAMPLE, 'prefix', True))
        self.assertTrue(valid_observation(obs))

    def test_concurrent_requests_keep_separate_correlation_ids(self):
        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': SECRET,
            'EVENT_LEADERBOARD_REQUESTS_PER_SECOND': '4'})
        barrier = threading.Barrier(2)
        def read(url, timeout):
            barrier.wait(timeout=3)
            return Response(b'{"status":"1","result":"135750000"}')
        def work(timestamp):
            lines = []
            with ScanDiagnostics(sink=lines.append):
                provider.request('block', 'getblocknobytime', timestamp=timestamp)
            return next(json.loads(line[len(PREFIX):])['observation'] for line in lines
                        if json.loads(line[len(PREFIX):])['kind'] == 'provider_response')
        with patch('urllib.request.urlopen', side_effect=read), ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(work, [1795652400, 1795652401]))
        self.assertEqual(len({r['request_ref'] for r in results}), 2)
        self.assertEqual([r['request']['timestamp'] for r in results], [1795652400, 1795652401])
        self.assertTrue(all(valid_observation(r) for r in results))

    def test_invalid_json_has_body_metadata_without_plaintext(self):
        body = ('<html>' + SECRET + '</html>').encode()
        rows, _, error, _, _ = self.observe([body], raw=True)
        self.assertEqual(str(error), 'provider_invalid_json')
        obs = rows[0]['observation']
        self.assertEqual(obs['json_state'], 'invalid')
        self.assertEqual(obs['body_digest'], hashlib.sha256(body).hexdigest())
        self.assertEqual(obs['body_bytes'], len(body))

    async def test_supervisor_forwards_observation_and_refuses_unknown_text_fields(self):
        rows, lines, _, _, _ = self.observe([{'status': '1', 'result': SECRET}])
        reader = asyncio.StreamReader()
        reader.feed_data(('\n'.join(lines) + '\n').encode())
        reader.feed_eof()
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            await production_jobs._forward_diagnostics(reader, True)
        forwarded = [json.loads(s[len(PREFIX):]) for s in captured.getvalue().splitlines()]
        self.assertTrue(any(s['kind'] == 'provider_response' for s in forwarded))
        self.assertNotIn(SECRET, captured.getvalue())
        for change in ({'raw_body': SECRET}, {'request': {'apikey': SECRET}},
                       {'message': SECRET}, {'body_digest': SECRET}, {'hint_number': SECRET}):
            row = dict(rows[0], observation={**rows[0]['observation'], **change})
            self.assertIsNone(validated_line(PREFIX + json.dumps(row)))


class SocketResponseEvidenceTests(unittest.TestCase):
    def test_pooled_transport_captures_actual_http_status_and_non_json_error_body(self):
        body = ('<html>upstream failed ' + SECRET + '</html>').encode()
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(503)
                self.send_header('Content-Type', 'text/html')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': SECRET,
            'EVENT_LEADERBOARD_HTTP_POOL': '1',
            'BSCSCAN_API_URL': 'http://127.0.0.1:' + str(server.server_port) + '/v2/api'})
        lines = []
        try:
            with ScanDiagnostics(sink=lines.append), self.assertRaisesRegex(SourceIncomplete, 'provider_http_503'):
                provider.request('block', 'getblocknobytime', timestamp=1795652400, closest='before')
        finally:
            provider.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        rows = [json.loads(s[len(PREFIX):]) for s in lines]
        obs = next(r['observation'] for r in rows if r['kind'] == 'provider_response')
        self.assertEqual(obs['http_status'], 503)
        self.assertEqual(obs['content_type'], 'html')
        self.assertEqual(obs['body_digest'], hashlib.sha256(body).hexdigest())
        self.assertNotIn(SECRET, '\n'.join(lines))


if __name__ == '__main__':
    unittest.main()
