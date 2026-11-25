"""Real local sockets: explicit bounded stale recovery, no redirects, safe errors."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor
import socket
import threading
import unittest
from unittest.mock import patch

from runtime.event_leaderboards.diagnostics import ScanDiagnostics
from runtime.event_leaderboards.scanner import EtherscanProvider, SourceIncomplete
from runtime.event_leaderboards.transport import PooledJSONHTTP


class HTTPPoolTests(unittest.TestCase):
    def setUp(self):
        self.ports, self.paths = [], []
        ports, paths = self.ports, self.paths
        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def do_GET(self):
                ports.append(self.client_address[1])
                paths.append(self.path)
                barrier = getattr(self.server, 'response_barrier', None)
                if barrier is not None:
                    barrier.wait(timeout=2)
                remaining = getattr(self.server, 'remaining_dropped_responses', 0)
                if getattr(self.server, 'drop_next_response', False) or remaining:
                    self.server.drop_next_response = False
                    self.server.remaining_dropped_responses = max(0, remaining - 1)
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                if getattr(self.server, 'truncate_next_body', False):
                    self.server.truncate_next_body = False
                    self.send_response(200)
                    self.send_header('Content-Length', '100')
                    self.end_headers()
                    self.wfile.write(b'{')
                    self.wfile.flush()
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                if getattr(self.server, 'malformed_next_status', False):
                    self.server.malformed_next_status = False
                    self.wfile.write(b'private-fixture-key\r\n\r\n')
                    self.wfile.flush()
                    self.close_connection = True
                    return
                status = 302 if self.path.startswith('/redirect') else 429 if self.path.startswith('/limited') else 200
                body = b'private-fixture-key' if self.path.startswith('/invalid') else b'{"status":"1","result":"0xa"}'
                self.send_response(status)
                self.send_header('Content-Length', str(len(body)))
                if status == 302:
                    self.send_header('Location', '/must-not-follow?apikey=private-fixture-key')
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args):
                pass
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = 'http://127.0.0.1:' + str(self.server.server_port)
        self.provider = EtherscanProvider({'EVENT_LEADERBOARD_API_KEY': 'private-fixture-key',
                                          'EVENT_LEADERBOARD_HTTP_POOL': '1', 'BSCSCAN_API_URL': self.base})
    def tearDown(self):
        self.provider.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_two_actual_requests_reuse_one_tcp_connection(self):
        self.assertEqual(self.provider.head(), 10)
        self.assertEqual(self.provider.head(), 10)
        self.assertEqual(len(self.paths), 2)
        self.assertEqual(len(set(self.ports)), 1)

    def test_redirect_and_rate_limit_are_not_followed_or_retried(self):
        for path, code in (('/redirect', 'provider_http_302'), ('/limited', 'provider_http_429')):
            self.provider.api_url = self.base + path
            with self.assertRaises(SourceIncomplete) as caught:
                self.provider.head()
            self.assertEqual(str(caught.exception), code)
            self.assertNotIn('private-fixture-key', str(caught.exception.diagnostic))
        self.assertEqual(len(self.paths), 2)

    def test_invalid_json_is_sanitized(self):
        self.provider.api_url = self.base + '/invalid'
        with self.assertRaises(SourceIncomplete) as caught:
            self.provider.head()
        self.assertEqual(str(caught.exception), 'provider_invalid_json')
        self.assertNotIn('private-fixture-key', str(caught.exception.diagnostic))
        self.assertEqual(len(self.paths), 1)

    def test_closed_reused_connection_recovers_the_identical_paced_read(self):
        self.provider.spacing = 0
        self.assertEqual(self.provider.head(), 10)
        self.server.drop_next_response = True
        with patch('runtime.event_leaderboards.scanner.time.sleep') as sleep, ScanDiagnostics(sink=None) as diagnostic:
            self.assertEqual(self.provider.head(), 10)
        self.assertEqual(len(self.paths), 3)
        self.assertEqual(self.paths[1], self.paths[2])
        self.assertEqual(self.ports[0], self.ports[1])
        self.assertNotEqual(self.ports[1], self.ports[2])
        self.assertEqual(diagnostic.summary()['totals']['attempts'], 2)
        self.assertEqual(diagnostic.summary()['totals']['retries'], 1)
        self.assertEqual(diagnostic.summary()['failures'], {'provider_stale_connection': 1})
        sleep.assert_called_once_with(2)

    def test_fresh_connection_failure_is_not_retried(self):
        self.server.drop_next_response = True
        with ScanDiagnostics(sink=None) as diagnostic, self.assertRaises(SourceIncomplete) as caught:
            self.provider.head()
        self.assertEqual(str(caught.exception), 'provider_connection_failure')
        self.assertEqual(len(self.paths), 1)
        self.assertEqual(diagnostic.summary()['totals']['retries'], 0)

    def test_recovery_uses_a_fresh_socket_even_when_other_idle_sockets_exist(self):
        self.provider.spacing = 0
        self.server.response_barrier = threading.Barrier(2)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(self.provider._http.read_json, self.base + '/warm', 2) for _ in range(2)]
            for future in futures:
                future.result()
        self.server.response_barrier = None
        warm_ports = set(self.ports)
        self.assertEqual(len(warm_ports), 2)
        self.server.drop_next_response = True
        with patch('runtime.event_leaderboards.scanner.time.sleep'):
            self.assertEqual(self.provider.head(), 10)
        self.assertEqual(len(self.paths), 4)
        self.assertNotIn(self.ports[-1], warm_ports)

    def test_failed_reconnect_stops_after_two_attempts(self):
        self.provider.spacing = 0
        self.provider.head()
        self.server.remaining_dropped_responses = 2
        with patch('runtime.event_leaderboards.scanner.time.sleep'), ScanDiagnostics(sink=None) as diagnostic:
            with self.assertRaises(SourceIncomplete) as caught:
                self.provider.head()
        self.assertEqual(str(caught.exception), 'provider_connection_failure')
        self.assertEqual(len(self.paths), 3)
        self.assertEqual(diagnostic.summary()['totals']['attempts'], 2)
        self.assertEqual(diagnostic.summary()['totals']['retries'], 1)

    def test_partial_response_on_reused_socket_is_not_retried(self):
        self.provider.spacing = 0
        self.provider.head()
        self.server.truncate_next_body = True
        with ScanDiagnostics(sink=None) as diagnostic, self.assertRaises(SourceIncomplete) as caught:
            self.provider.head()
        self.assertEqual(str(caught.exception), 'provider_connection_failure')
        self.assertEqual(len(self.paths), 2)
        self.assertEqual(diagnostic.summary()['totals']['retries'], 0)

    def test_malformed_status_on_reused_socket_is_not_retried_or_leaked(self):
        self.provider.spacing = 0
        self.provider.head()
        self.server.malformed_next_status = True
        with ScanDiagnostics(sink=None) as diagnostic, self.assertRaises(SourceIncomplete) as caught:
            self.provider.head()
        self.assertEqual(str(caught.exception), 'provider_connection_failure')
        self.assertNotIn('private-fixture-key', str(caught.exception.diagnostic))
        self.assertEqual(len(self.paths), 2)
        self.assertEqual(diagnostic.summary()['totals']['retries'], 0)


if __name__ == '__main__':
    unittest.main()
