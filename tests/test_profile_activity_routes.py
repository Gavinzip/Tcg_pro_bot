"""Use the normal Profile API HTTP server/auth path, without chain requests."""
import json
import os
from pathlib import Path
import threading
import unittest
import urllib.error
import urllib.request
import uuid

import profile_api


class ProfileActivityRoutes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old_token = profile_api.API_TOKEN
        cls.old_enabled = os.environ.get('EVENT_LEADERBOARD_API_ENABLED')
        cls.old_path = os.environ.get('EVENT_LEADERBOARD_DB_PATH')
        cls.root = Path('.scratch/profile-api-tests') / uuid.uuid4().hex
        cls.root.mkdir(parents=True)
        os.environ['EVENT_LEADERBOARD_API_ENABLED'] = '1'
        os.environ['EVENT_LEADERBOARD_DB_PATH'] = str(cls.root / 'events.sqlite3')
        profile_api.API_TOKEN = 'synthetic-test-server-token'
        profile_api._EVENT_CONTROLLER = None
        cls.server = profile_api.ProfileApiServer(('127.0.0.1', 0), profile_api.ProfileApiHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.origin = 'http://127.0.0.1:' + str(cls.server.server_address[1])

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown(); cls.server.server_close(); cls.thread.join()
        profile_api.API_TOKEN = cls.old_token
        profile_api._EVENT_CONTROLLER = None
        for name, value in [('EVENT_LEADERBOARD_API_ENABLED', cls.old_enabled), ('EVENT_LEADERBOARD_DB_PATH', cls.old_path)]:
            if value is None: os.environ.pop(name, None)
            else: os.environ[name] = value
        # Root lists these exact test-owned paths before cleanup.

    def request(self, path, authorized=False):
        headers = {'Authorization': 'Bearer synthetic-test-server-token'} if authorized else {}
        try:
            response = urllib.request.urlopen(urllib.request.Request(self.origin + path, headers=headers), timeout=5)
        except urllib.error.HTTPError as e:
            response = e
        with response:
            return response.status, response.headers, json.load(response)

    def test_auth_and_real_uninitialized_state(self):
        status, headers, body = self.request('/v1/leaderboards/2026-11-20')
        self.assertEqual(status, 401)
        self.assertEqual(headers['Cache-Control'], 'no-store')
        status, headers, body = self.request('/v1/leaderboards/2026-11-20', True)
        self.assertEqual(status, 202)
        self.assertIsNone(body['snapshot_id'])
        self.assertNotIn('rows', body)

    def test_unknown_event_and_search_contract(self):
        self.assertEqual(self.request('/v1/leaderboards/missing', True)[0], 404)
        status, _, body = self.request('/v1/leaderboards/2026-11-20/search?q=alice', True)
        self.assertEqual(status, 400)
        self.assertIn('snapshot_id', body['message'])

    def test_existing_profile_validation_and_health(self):
        self.assertEqual(self.request('/healthz')[0], 200)
        self.assertEqual(self.request('/v1/profile?wallet=invalid', True)[0], 400)


if __name__ == '__main__':
    unittest.main()
