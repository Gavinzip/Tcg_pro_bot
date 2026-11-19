"""Durability and public API contracts. SQLite artifacts remain under .scratch for explicit cleanup."""
from dataclasses import replace
from pathlib import Path
import unittest
from unittest.mock import patch
import uuid

from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.http import EventLeaderboardHTTP
from runtime.event_leaderboards.store import EventLeaderboardStore, LedgerConflict

CONTRACT = "0x" + "a" * 40
RUN_DIR = Path(__file__).resolve().parents[1] / ".scratch" / "event-store-tests" / ("run-" + uuid.uuid4().hex)


def address(number):
    return "0x" + f"{number:040x}"


class EventStoreTests(unittest.TestCase):
    def setUp(self):
        RUN_DIR.mkdir(parents=True, exist_ok=True)
        self.store = EventLeaderboardStore(str(RUN_DIR / (self._testMethodName + ".sqlite3")))
        self.definition = self.store.register_event(EventDefinition("finite", 100, 200, (CONTRACT,), contract_schemas={CONTRACT: "v3"}))
        self.source = source_key(self.definition, CONTRACT)
        self.http = EventLeaderboardHTTP(self.store)

    def row(self, number=1, **extra):
        return {"wallet": address(number), "tx_hash": "0x" + f"{number:064x}", "log_index": 0,
                "pack_contract": CONTRACT, "pack_id": "pack", "draw_count": 1,
                "paid_amount_raw": "1000000000000000001", "block_number": 10,
                "block_hash": "0xblock", "timestamp": 110, **extra}

    def publish(self, rows, usernames=None):
        self.store.commit_chunk("finite", self.source, None, 20, rows, block_hash="0xanchor")
        return self.store.publish_snapshot("finite", 19, 120, usernames=usernames)

    def test_all_wallets_and_immutable_pagination(self):
        first = self.publish([self.row(i) for i in range(1, 352)])
        code, page = self.http.dispatch("/v1/leaderboards/finite", {"limit": "100"})
        self.assertEqual(code, 200)
        self.assertEqual(page["total_participants"], 351)
        self.store.commit_chunk("finite", self.source, 20, 30, [self.row(999, draw_count=99, block_number=25, timestamp=125)], block_hash="0xnewanchor")
        self.store.publish_snapshot("finite", 29, 130)
        code, second = self.http.dispatch("/v1/leaderboards/finite", {"cursor": page["next_cursor"], "limit": "100"})
        self.assertEqual(second["snapshot_id"], first["snapshot_id"])
        self.assertEqual(second["rows"][0]["wallet"], address(101))
        code, all_rows = self.http.dispatch("/v1/leaderboards/finite", {"all": "true", "snapshot_id": first["snapshot_id"]})
        self.assertEqual(len(all_rows["rows"]), 351)
        self.assertEqual(self.store.get_checkpoint("finite", self.source)["block_hash"], "0xnewanchor")

    def test_alias_chain_merges_exact_amounts_and_frozen_unicode_search(self):
        huge = 10**80 + 7
        aliases = [{"old": address(1), "new": address(2), "verified": True, "evidence": "receipt1"},
                   {"old": address(2), "new": address(3), "verified": True, "evidence": "receipt2"}]
        self.store.commit_chunk("finite", self.source, None, 20, [self.row(1, draw_count=2, paid_amount_raw=str(huge)),
            self.row(2, draw_count=3, paid_amount_raw="3"), self.row(4, draw_count=8)], aliases=aliases, block_hash="0xanchor")
        snapshot = self.store.publish_snapshot("finite", 19, 120, usernames={address(3): "Straße 用戶"})
        code, found = self.http.dispatch("/v1/leaderboards/finite/search", {"q": "STRASSE", "snapshot_id": snapshot["snapshot_id"]})
        self.assertEqual(code, 200)
        self.assertEqual(found["rows"][0]["rank"], 2)
        self.assertEqual(found["rows"][0]["wallet"], address(3))
        self.assertEqual(found["rows"][0]["draw_count"], 5)
        self.assertEqual(found["rows"][0]["paid_amount_raw"], str(huge + 3))
        self.assertEqual(found["rows"][0]["paid_amount"], str((huge + 3) // 10**18) + ".000000000000000010")
        code, old_wallet = self.http.dispatch("/v1/leaderboards/finite/search", {"q": address(1), "snapshot_id": snapshot["snapshot_id"]})
        self.assertEqual(old_wallet["rows"][0]["rank"], 2)
        self.store.publish_snapshot("finite", 19, 120, usernames={address(3): "Changed"})
        self.assertEqual(self.http.dispatch("/v1/leaderboards/finite/search", {"q": "STRASSE", "snapshot_id": snapshot["snapshot_id"]})[1]["total_matches"], 1)

    def test_rank_ties_remain_global_under_client_sort(self):
        self.publish([self.row(1, draw_count=4), self.row(2, draw_count=4), self.row(3)])
        code, body = self.http.dispatch("/v1/leaderboards/finite", {"sort": "wallet", "order": "desc", "all": "true"})
        self.assertEqual([row["rank"] for row in body["rows"]], [3, 1, 1])
        self.assertIsNone(body["rows"][0]["username"])

    def test_invalid_partial_chunk_rolls_back_ledger_and_checkpoint(self):
        with self.assertRaises(ValueError):
            self.store.commit_chunk("finite", self.source, None, 20, [self.row(1), self.row(2, paid_amount_raw="0")])
        self.assertIsNone(self.store.get_checkpoint("finite", self.source))
        self.store.commit_chunk("finite", self.source, None, 20, [], block_hash="0xanchor")
        snapshot = self.store.publish_snapshot("finite", 19, 120)
        self.assertEqual(snapshot["total_participants"], 0)
        for invalid in [self.row(timestamp=99), self.row(timestamp=200), self.row(pack_contract=address(19))]:
            with self.assertRaises(ValueError):
                self.store.commit_chunk("finite", "bad", None, 20, [invalid])
        self.assertIsNone(self.store.get_checkpoint("finite", "bad"))

    def test_duplicate_content_mismatch_and_alias_cycle_roll_back(self):
        self.publish([self.row(1)])
        self.store.commit_chunk("finite", "second", None, 20, [self.row(1)])
        with self.assertRaises(LedgerConflict):
            self.store.commit_chunk("finite", "third", None, 20, [self.row(1, draw_count=8)])
        self.assertIsNone(self.store.get_checkpoint("finite", "third"))
        aliases = [{"old": address(1), "new": address(2), "verified": True, "evidence": "a"},
                   {"old": address(2), "new": address(1), "verified": True, "evidence": "b"}]
        with self.assertRaises(LedgerConflict):
            self.store.set_aliases("finite", aliases)
        with self.assertRaises(LedgerConflict):
            self.store.commit_chunk("finite", self.source, None, 30, [])
        self.assertEqual(self.store.publish_snapshot("finite", 19, 120)["total_participants"], 1)

    def test_definition_revision_is_new_ledger_scope_and_recovers_from_db(self):
        old = self.publish([self.row()])
        revised = self.store.register_event(replace(self.definition, start_ts=115))
        self.assertEqual(revised.revision, 2)
        self.assertIsNone(self.store.get_checkpoint("finite", self.source))
        with self.assertRaises(LedgerConflict):
            self.store.commit_chunk("finite", self.source, None, 20, [], expected_revision=1)
        with self.assertRaises(LedgerConflict):
            self.store.publish_snapshot("finite", 19, 120, expected_revision=1)
        self.assertEqual(self.http.dispatch("/v1/leaderboards/finite")[0], 202)
        recovered = EventLeaderboardStore(self.store.path)
        self.assertEqual(recovered.get_event("finite").start_ts, 115)
        self.assertEqual(recovered.read_snapshot("finite", old["snapshot_id"])[0]["total_participants"], 1)
        with self.assertRaises(ValueError):
            recovered.commit_chunk("finite", self.source, None, 20, [self.row()])

    def test_failure_preserves_snapshot_and_initial_failure_has_no_rows(self):
        self.assertEqual(self.http.dispatch("/v1/leaderboards/finite")[0], 202)
        self.store.record_failure("finite", "rpc unavailable", {"source": "first", "next_block": 10})
        code, body = self.http.dispatch("/v1/leaderboards/finite")
        self.assertEqual(code, 503)
        self.assertNotIn("rows", body)
        snapshot = self.publish([self.row()])
        self.store.record_failure("finite", "reorg detected")
        code, body = self.http.dispatch("/v1/leaderboards/finite")
        self.assertEqual((code, body["snapshot_id"], len(body["rows"])), (200, snapshot["snapshot_id"], 1))
        self.assertTrue(body["stale"])
        self.assertEqual(body["last_error"], "reorg detected")

    def test_bad_cursor_snapshot_and_sql_sort_are_400(self):
        first = self.publish([self.row(1), self.row(2)])
        _, page = self.http.dispatch("/v1/leaderboards/finite", {"limit": "1"})
        other = self.store.publish_snapshot("finite", 19, 120)
        cases = [{"cursor": "broken"}, {"cursor": page["next_cursor"], "snapshot_id": other["snapshot_id"]},
                 {"sort": "wallet; DROP TABLE ledger"}, {"snapshot_id": "not-a-snapshot"}, {"limit": "0"}]
        for query in cases:
            self.assertEqual(self.http.dispatch("/v1/leaderboards/finite", query)[0], 400)
        self.assertEqual(self.http.dispatch("/v1/leaderboards/missing")[0], 404)
        self.assertEqual(self.http.dispatch("/v1/leaderboards/finite/search", {"q": "a"})[0], 400)
        self.assertIsNone(self.http.dispatch("/api/profile"))
        self.assertEqual(self.store.read_snapshot("finite", first["snapshot_id"])[0]["total_participants"], 2)

    def test_publication_requires_every_contract_source_and_anchored_bound(self):
        second = address(900)
        definition = self.store.register_event(replace(self.definition, contracts=(CONTRACT, second),
            contract_schemas={CONTRACT: "v3", second: "legacy_single"}))
        first_source, second_source = source_key(definition, CONTRACT), source_key(definition, second)
        self.store.commit_chunk("finite", first_source, None, 20, [], block_hash="0xfirst")
        with self.assertRaises(LedgerConflict):
            self.store.publish_snapshot("finite", 19, 120)
        self.store.commit_chunk("finite", second_source, None, 20, [])
        with self.assertRaises(LedgerConflict):
            self.store.publish_snapshot("finite", 19, 120)
        self.store.commit_chunk("finite", second_source, 20, 21, [], block_hash="0xsecond")
        with self.assertRaises(LedgerConflict):
            self.store.publish_snapshot("finite", 20, 120)
        self.assertEqual(self.store.publish_snapshot("finite", 19, 120)["total_participants"], 0)

    def test_overdue_health_uses_latest_snapshot_and_exempts_final(self):
        with patch("runtime.event_leaderboards.store.time.time", return_value=1000):
            old = self.publish([self.row()])
        with patch("runtime.event_leaderboards.store.time.time", return_value=2301):
            metadata, _, health = self.store.read_snapshot("finite")
            self.assertTrue(health["stale"])
            self.assertEqual(health["stale_reason"], "update_overdue")
            self.assertIsNone(health["last_error"])
            self.assertEqual(health["update_interval_seconds"], 600)
            self.assertEqual(health["stale_after_seconds"], 1200)
            new = self.store.publish_snapshot("finite", 19, 120)
            metadata, _, health = self.store.read_snapshot("finite", old["snapshot_id"])
            self.assertEqual(metadata["snapshot_id"], old["snapshot_id"])
            self.assertFalse(health["stale"])
        with patch("runtime.event_leaderboards.store.time.time", return_value=2400):
            final = self.store.publish_snapshot("finite", 19, 200, finalized=True)
        with patch("runtime.event_leaderboards.store.time.time", return_value=100000):
            self.assertFalse(self.store.read_snapshot("finite", new["snapshot_id"])[2]["stale"])
            self.assertFalse(self.store.read_snapshot("finite", final["snapshot_id"])[2]["stale"])

    def test_finalize_requires_event_end_confirmed_and_snapshot_bound(self):
        self.publish([self.row()])
        with self.assertRaises(ValueError):
            self.store.publish_snapshot("finite", 19, 199, finalized=True)
        with self.assertRaises(ValueError):
            self.store.publish_snapshot("finite", 9, 120)
        self.assertEqual(self.store.publish_snapshot("finite", 19, 200, finalized=True)["status"], "final")


if __name__ == "__main__":
    print("SQLite test artifacts:", RUN_DIR)
    unittest.main()
