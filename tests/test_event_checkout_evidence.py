"""Frozen checkout evidence contract; synthetic fixtures never enter the product."""

import unittest
from tests import test_event_builder_scope as fixtures
from tests.test_event_leaderboard_scanner import h


class CheckoutEvidenceTests(unittest.TestCase):
    setUp = fixtures.BuilderScopeTests.setUp
    row = fixtures.BuilderScopeTests.row
    publish = fixtures.BuilderScopeTests.publish

    def test_normal_server_authorizes_checkout_evidence_with_the_website_credential(
        self,
    ):
        import os, json, profile_api, threading, urllib.request, urllib.error
        from unittest.mock import patch

        self.publish([self.row(1, 1, fixtures.A, 2)])
        token = "synthetic-checkout-website-read-token"
        with patch.object(profile_api, "API_TOKEN", "synthetic-internal"), patch.object(
            profile_api, "LEADERBOARD_CLIENT_TOKENS", {"site": token}
        ), patch.object(profile_api, "_EVENT_CONTROLLER", self.http), patch.dict(
            os.environ, {"EVENT_LEADERBOARD_API_ENABLED": "1"}
        ):
            server = profile_api.ProfileApiServer(
                ("127.0.0.1", 0), profile_api.ProfileApiHandler
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                url = (
                    "http://127.0.0.1:"
                    + str(server.server_address[1])
                    + "/v1/leaderboards/finite/checkouts?builder="
                    + fixtures.A
                )
                with self.assertRaises(urllib.error.HTTPError) as error:
                    urllib.request.urlopen(url, timeout=5)
                self.assertEqual(error.exception.code, 401)
                request = urllib.request.Request(
                    url, headers={"Authorization": "Bearer " + token}
                )
                with urllib.request.urlopen(request, timeout=5) as response:
                    data = json.load(response)
                    self.assertEqual(response.status, 200)
                self.assertEqual(data["rows"][0]["pack_id"], h(77))
                self.assertEqual(data["total_checkouts"], 1)
                self.assertEqual(data["identities"][0]["wallet"], fixtures.wallet(1))
            finally:
                server.shutdown()
                server.server_close()
                thread.join()

    def test_frozen_checkout_scope_pagination_and_aliases(self):
        self.store.set_aliases(
            "finite",
            [
                {
                    "old": fixtures.wallet(1),
                    "new": fixtures.wallet(3),
                    "verified": True,
                    "evidence": "test",
                }
            ],
        )
        first = self.publish(
            [
                self.row(1, 1, fixtures.A, 2),
                self.row(2, 2, fixtures.A, 3),
                self.row(1, 3, fixtures.B, 7),
            ]
        )
        status, page = self.http.dispatch(
            "/v1/leaderboards/finite/checkouts", {"builder": fixtures.A, "limit": "1"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(page["total_checkouts"], 2)
        self.assertEqual(page["evidence_type"], "confirmed_paid_checkout")
        self.assertEqual(page["rows"][0]["wallet"], fixtures.wallet(3))
        self.assertEqual(page["rows"][0]["source_wallet"], fixtures.wallet(1))
        self.assertIn(fixtures.wallet(1), page["rows"][0]["aliases"])
        self.store.commit_chunk(
            "finite",
            self.source,
            20,
            30,
            [self.row(1, 4, fixtures.A, 99, block_number=25, timestamp=125)],
            block_hash=h(29),
        )
        self.store.publish_snapshot("finite", 29, 130)
        status, next_page = self.http.dispatch(
            "/v1/leaderboards/finite/checkouts",
            {"cursor": page["next_cursor"], "limit": "1"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(next_page["snapshot_id"], first["snapshot_id"])
        self.assertEqual(next_page["rows"][0]["draw_count"], 3)
        self.assertEqual(
            self.http.dispatch(
                "/v1/leaderboards/finite/checkouts",
                {"cursor": page["next_cursor"], "builder": fixtures.B},
            )[0],
            400,
        )

    def test_initial_and_historical_snapshots_never_fabricate_evidence(self):
        self.assertEqual(
            self.http.dispatch("/v1/leaderboards/finite/checkouts")[0], 202
        )
        self.publish([self.row(1, 1, fixtures.A, 1)])
        with self.store._connection(write=True) as db:
            import json

            row = db.execute("SELECT snapshot_id,metadata FROM snapshots").fetchone()
            metadata = json.loads(row[1])
            metadata.pop("checkout_evidence_supported")
            db.execute(
                "UPDATE snapshots SET metadata=? WHERE snapshot_id=?",
                (json.dumps(metadata), row[0]),
            )
        status, data = self.http.dispatch("/v1/leaderboards/finite/checkouts")
        self.assertEqual(status, 409)
        self.assertNotIn("rows", data)


if __name__ == "__main__":
    unittest.main()
