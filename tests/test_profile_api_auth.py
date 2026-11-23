"""Website credential isolation, revocation and safe configuration failures."""
import json
import unittest

from runtime.profile_api_auth import authorized, load_leaderboard_client_tokens


INTERNAL = "synthetic-internal-token-for-tests-only"
OURS = "synthetic-renaiss-site-token-for-tests-only"
PARTNER = "synthetic-partner-site-token-for-tests-only"


class ProfileApiAuthTests(unittest.TestCase):
    def setUp(self):
        self.clients = load_leaderboard_client_tokens(
            json.dumps({"renaiss-site": OURS, "partner-site": PARTNER}), profile_token=INTERNAL)

    def test_website_credentials_cannot_call_profile(self):
        for token in (OURS, PARTNER):
            self.assertTrue(authorized("Bearer " + token, profile_token=INTERNAL,
                                       leaderboard_clients=self.clients, leaderboard=True))
            self.assertFalse(authorized("Bearer " + token, profile_token=INTERNAL,
                                        leaderboard_clients=self.clients))
        self.assertTrue(authorized("Bearer " + INTERNAL, profile_token=INTERNAL,
                                    leaderboard_clients=self.clients))

    def test_removing_one_client_does_not_revoke_the_other_or_internal(self):
        del self.clients["partner-site"]
        for token, expected in ((PARTNER, False), (OURS, True), (INTERNAL, True)):
            self.assertEqual(authorized("Bearer " + token, profile_token=INTERNAL,
                                       leaderboard_clients=self.clients, leaderboard=True), expected)

    def test_website_only_configuration_does_not_open_anonymous_profile(self):
        for header in (None, "", "Bearer " + OURS):
            self.assertFalse(authorized(header, profile_token="", leaderboard_clients=self.clients))
        self.assertFalse(authorized(None, profile_token="", leaderboard_clients=self.clients,
                                    leaderboard=True))
        self.assertTrue(authorized("Bearer " + OURS, profile_token="",
                                   leaderboard_clients=self.clients, leaderboard=True))

    def test_missing_invalid_and_partial_credentials_are_rejected(self):
        for header in (None, "", OURS, "Basic " + OURS, "Bearer " + OURS[:-1], "Bearer wrong"):
            self.assertFalse(authorized(header, profile_token=INTERNAL,
                                        leaderboard_clients=self.clients, leaderboard=True))

    def test_legacy_configuration_keeps_existing_behavior(self):
        self.assertEqual(load_leaderboard_client_tokens(None), {})
        self.assertTrue(authorized(None, profile_token="", leaderboard_clients={}))
        self.assertFalse(authorized(None, profile_token=INTERNAL, leaderboard_clients={}))
        self.assertTrue(authorized("Bearer " + INTERNAL, profile_token=INTERNAL,
                                   leaderboard_clients={}))

    def test_invalid_explicit_configuration_fails_without_leaking_values(self):
        cases = ["{", "{}", "[]", "null", json.dumps({"site": "short"}),
                 json.dumps({"site": "not a printable ASCII credential " + "x" * 32}),
                 json.dumps({"site": "密" * 40}), json.dumps({"bad name": OURS}),
                 json.dumps({"a": OURS, "b": OURS}), json.dumps({"site": INTERNAL}),
                 '{"site":' + json.dumps(OURS) + ',"site":' + json.dumps(PARTNER) + '}']
        for raw in cases:
            with self.subTest(kind=raw[:1]), self.assertRaises(ValueError) as raised:
                load_leaderboard_client_tokens(raw, profile_token=INTERNAL)
            for token in (OURS, PARTNER, INTERNAL):
                self.assertNotIn(token, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
