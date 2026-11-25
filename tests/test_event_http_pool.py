"""Real local HTTP sockets: keep-alive, no redirects/retries, safe errors."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import unittest

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


if __name__ == '__main__':
    unittest.main()
