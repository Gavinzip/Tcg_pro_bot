"""Bounded keep-alive connections, with no implicit retry or alternate provider."""
import http.client
from contextlib import contextmanager
from contextvars import ContextVar
import json
import queue
import ssl
import urllib.error
import urllib.parse


class ReusedConnectionClosed(ConnectionError):
    """An existing socket closed before any response headers were received."""


class PooledJSONHTTP:
    def __init__(self):
        self.connections = queue.LifoQueue(maxsize=8)
        self.force_fresh = ContextVar('event_http_force_fresh', default=False)

    @contextmanager
    def fresh_connection(self):
        token = self.force_fresh.set(True)
        try:
            yield
        finally:
            self.force_fresh.reset(token)

    def read_json(self, url, timeout, on_response=None):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError('invalid_provider_url')
        origin = parsed.scheme, parsed.hostname, parsed.port
        connection = None
        if not self.force_fresh.get():
            try:
                previous_origin, connection = self.connections.get_nowait()
                if previous_origin != origin:
                    connection.close()
                    connection = None
            except queue.Empty:
                pass
        if connection is None:
            if parsed.scheme == 'https':
                connection = http.client.HTTPSConnection(parsed.hostname, parsed.port, timeout=timeout,
                                                        context=ssl.create_default_context())
            else:
                connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=timeout)
        reused_socket = connection.sock is not None
        received_headers = False
        try:
            path = urllib.parse.urlunsplit(('', '', parsed.path or '/', parsed.query, ''))
            connection.request('GET', path, headers={'Accept': 'application/json', 'User-Agent': 'Python-event-scanner'})
            with connection.getresponse() as response:
                received_headers = True
                if on_response is not None:
                    on_response()
                payload = response.read()
                if response.status != 200:
                    raise urllib.error.HTTPError(url, response.status, 'provider_http_error', {}, None)
                result = json.loads(payload)
        except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError):
            connection.close()
            if reused_socket and not received_headers:
                raise ReusedConnectionClosed() from None
            raise
        except BaseException:
            connection.close()
            raise
        else:
            try:
                self.connections.put_nowait((origin, connection))
            except queue.Full:
                connection.close()
            return result

    def close(self):
        while True:
            try:
                _, connection = self.connections.get_nowait()
            except queue.Empty:
                break
            connection.close()
