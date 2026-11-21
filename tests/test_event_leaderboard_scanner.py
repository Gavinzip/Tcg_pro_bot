"""Evidence/coverage tests; fixtures do not establish live registry completeness."""
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
import uuid

from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.scanner import (
    EtherscanProvider, EventLeaderboardScanner, SourceIncomplete, complete_logs, decode_checkout,
    verified_receipt_checkouts, VRF_V3_CONTRACT, VRF_V3_CHECKOUT_TOPIC,
    USDT, TRANSFER, LEGACY_SINGLE, LEGACY_BATCH, DEFAULT_PACK_CONTRACTS, SCHEMAS,
)
from runtime.event_leaderboards.http import EventLeaderboardHTTP
from runtime.event_leaderboards.store import EventLeaderboardStore
from runtime.event_leaderboards.seed import publish_confirmed_prefix

WALLET = "0x" + "1" * 40
OTHER = "0x" + "2" * 40
PACK = "0x" + "a" * 64
RUN_DIR = Path(__file__).resolve().parents[1] / '.scratch' / 'event-scanner-tests' / ('run-' + uuid.uuid4().hex)


def h(n):
    return "0x" + f"{n:064x}"


def word_address(a):
    return "0x" + "0" * 24 + a[2:]


def data(*values):
    return "0x" + "".join(f"{v:064x}" for v in values)


def checkout(block=10, *, wallet=WALLET, count=3, amount=30 * 10**18, index=2, tx=99, contract=VRF_V3_CONTRACT, pack=PACK):
    row = dict(address=contract, transactionHash=h(tx), blockNumber=hex(block), blockHash=h(block),
               logIndex=hex(index), removed=False)
    if contract == VRF_V3_CONTRACT:
        row.update(topics=[VRF_V3_CHECKOUT_TOPIC, word_address(wallet), pack, h(0)],
                   data=data(int(USDT, 16), 96, amount, count, *range(index * 10, index * 10 + count)))
    else:
        row.update(topics=[LEGACY_SINGLE, word_address(wallet), h(index)], data=data(amount))
    return row


def payment(checkout, *, amount=None, payer=None, recipient=None, index=0):
    return dict(checkout, address=USDT, logIndex=hex(index),
                topics=[TRANSFER, word_address(payer or "0x" + checkout["topics"][1][-40:]),
                        word_address(recipient or checkout["address"])],
                data=data(amount if amount is not None else int(checkout["data"][130:194], 16)))


class Provider:
    page_size = 2
    max_pages = 3

    def __init__(self, logs=(), head=16):
        self.rows, self.height = list(logs), head
        self.receipts, self.calls = {}, []
        self.builder_queries = []
        self.boundary_calls = []
        self.block_calls = []
        self.fail_contract = None
        self.changed = {}

    def head(self):
        return self.height

    def block(self, height):
        self.block_calls.append(height)
        return {"number": height, "hash": self.changed.get(height, h(height)), "timestamp": height * 10}

    def first_block_at(self, timestamp, head):
        self.boundary_calls.append((timestamp, head))
        return (timestamp + 9) // 10

    def logs_page(self, contract, topic, start, end, page, *, builder=None):
        self.builder_queries.append(builder)
        self.calls.append((contract, start, end, page))
        if contract == self.fail_contract:
            raise SourceIncomplete("provider_transport_failure")
        found = [x for x in self.rows if x["address"] == contract and x["topics"][0] == topic and start <= int(x["blockNumber"], 16) <= end]
        if builder is not None:
            found = [x for x in found if len(x["topics"]) == 4 and x["topics"][3] == builder]
        return found[(page - 1) * self.page_size:page * self.page_size]

    def receipt(self, tx):
        return self.receipts[tx]

    def add_receipt(self, rows, transfers=None):
        first = rows[0]
        self.receipts[first["transactionHash"]] = dict(transactionHash=first["transactionHash"], status="0x1",
            blockNumber=first["blockNumber"], blockHash=first["blockHash"],
            logs=list(transfers or []) + list(rows))


class ScannerTests(unittest.TestCase):
    def setUp(self):
        root = RUN_DIR / self._testMethodName
        root.mkdir(parents=True, exist_ok=True)
        self.temp = SimpleNamespace(name=str(root))
        self.store = EventLeaderboardStore(str(Path(self.temp.name) / "ledger.sqlite3"))

    # Root lists the exact test-owned paths before explicit cleanup.

    def setup_event(self, contracts=(VRF_V3_CONTRACT,)):
        return self.store.register_event(EventDefinition("test", 100, 140, contracts=contracts))

    def test_scoped_indexer_queries_only_its_builder_and_still_verifies_payment(self):
        target, other = checkout(tx=1), checkout(tx=2)
        target['topics'][3], other['topics'][3] = h(7), h(8)
        p=Provider([target, other],head=20)
        # No receipt exists for the unrelated builder: it must never be queried.
        p.add_receipt([target],[payment(target)])
        definition=self.store.register_event(EventDefinition('scoped',100,None,(VRF_V3_CONTRACT,),builder=h(7)))
        self.assertEqual(self.scanner(p).sync('scoped',now=200)['state'],'active')
        self.assertTrue(p.builder_queries and all(b==h(7) for b in p.builder_queries))
        metadata,rows,_=self.store.read_checkout_snapshot('scoped')
        self.assertEqual(metadata['builder'],h(7))
        self.assertEqual([r['tx_hash'] for r in rows],[h(1)])
        http=EventLeaderboardHTTP(self.store)
        status,response=http.dispatch('/v1/leaderboards/scoped')
        self.assertEqual((status,response['builder'],response['total_participants']),(200,h(7),1))
        self.assertEqual(http.dispatch('/v1/leaderboards/scoped?builder='+h(8))[0],400)
        with self.assertRaises(ValueError):self.store.read_snapshot('scoped',builder=h(8))
        self.assertIn(':builder:'+h(7),source_key(definition,VRF_V3_CONTRACT))
        p.receipts[h(1)]['logs']=[target] # No USDT payment: a matching builder is insufficient.
        self.store.register_event(EventDefinition('unpaid',100,None,(VRF_V3_CONTRACT,),builder=h(7)))
        self.assertEqual(self.scanner(p).sync('unpaid',now=200)['state'],'stale')

    def test_real_provider_builds_a_conjunctive_v3_builder_query(self):
        provider=EtherscanProvider({'BSCSCAN_API_KEY':'fixture-key'})
        calls=[]
        provider.request=lambda *args,**kwargs: calls.append((args,kwargs)) or []
        provider.logs_page(VRF_V3_CONTRACT,VRF_V3_CHECKOUT_TOPIC,10,20,1,builder=h(7))
        self.assertEqual(calls[0][1]['topic0'],VRF_V3_CHECKOUT_TOPIC)
        self.assertEqual(calls[0][1]['topic3'],h(7))
        self.assertEqual(calls[0][1]['topic0_3_opr'],'and')

    def test_scoped_range_splitting_preserves_builder_and_rejects_ignored_filters(self):
        rows=[checkout(block=n,tx=n) for n in range(10,14)]
        for r in rows:r['topics'][3]=h(7)
        p=Provider(rows,head=20)
        self.assertEqual(len(complete_logs(p,VRF_V3_CONTRACT,VRF_V3_CHECKOUT_TOPIC,10,13,builder=h(7))),4)
        self.assertTrue(len(p.builder_queries)>1 and all(b==h(7) for b in p.builder_queries))
        class IgnoresBuilder(Provider):
            def logs_page(self,*args,**kwargs):return [checkout()]
        with self.assertRaisesRegex(SourceIncomplete,'unexpected_checkout_builder'):
            complete_logs(IgnoresBuilder(),VRF_V3_CONTRACT,VRF_V3_CHECKOUT_TOPIC,10,13,builder=h(7))

    def test_scoped_prefix_copies_only_matching_builder_and_preserves_archive(self):
        a,b=checkout(block=10,tx=1),checkout(block=11,tx=2)
        a['topics'][3],b['topics'][3]=h(7),h(8)
        p=Provider([a,b],head=20)
        for r in (a,b):p.add_receipt([r],[payment(r)])
        self.setup_event();self.assertEqual(self.scanner(p).sync('test',now=200)['state'],'finalized')
        archive=self.store.read_snapshot('test')[0]['snapshot_id']
        self.store.register_event(EventDefinition('scoped',100,None,(VRF_V3_CONTRACT,),builder=h(7)))
        self.assertEqual(publish_confirmed_prefix(self.store,p,'test','scoped')['checkouts'],1)
        p.calls.clear();self.assertEqual(self.scanner(p).sync('scoped',now=200)['state'],'active')
        self.assertTrue(all(c[1]>=14 for c in p.calls))
        self.assertEqual([r['tx_hash'] for r in self.store.read_checkout_snapshot('scoped')[1]],[h(1)])
        self.assertEqual(self.store.read_snapshot('test')[0]['snapshot_id'],archive)

    def test_builder_scope_is_validated_persisted_and_does_not_change_global_hashes(self):
        plain=EventDefinition('plain',100,None,(VRF_V3_CONTRACT,))
        self.assertNotIn('builder',plain.payload())
        scoped=EventDefinition('scoped',100,None,(VRF_V3_CONTRACT,),builder=h(7).upper().replace('0X','0x'))
        stored=self.store.register_event(scoped)
        self.assertEqual(self.store.get_event('scoped').builder,h(7))
        self.assertNotEqual(source_key(plain,VRF_V3_CONTRACT),source_key(stored,VRF_V3_CONTRACT))
        with self.assertRaises(ValueError):EventDefinition('invalid',100,None,(VRF_V3_CONTRACT,),builder='0x1234')
        with self.assertRaises(ValueError):EventDefinition('legacy',100,None,(DEFAULT_PACK_CONTRACTS[0],),builder=h(7))
        key=source_key(stored,VRF_V3_CONTRACT)
        with self.assertRaises(ValueError):
            self.store.commit_chunk('scoped',key,None,11,[dict(wallet=WALLET,tx_hash=h(1),log_index=2,
                pack_contract=VRF_V3_CONTRACT,pack_id=PACK,draw_count=1,paid_amount_raw=1,timestamp=100,
                block_number=10,block_hash=h(10),builder=h(8))])
        self.assertIsNone(self.store.get_checkpoint('scoped',key))

    def test_ongoing_activity_reuses_a_verified_prefix_and_keeps_indexing_new_draws(self):
        a, b = checkout(block=10, tx=1), checkout(block=15, tx=2)
        p = Provider([a, b], head=20)
        for row in (a, b):
            p.add_receipt([row], [payment(row)])
        self.setup_event()
        self.assertEqual(self.scanner(p).sync('test', now=200)['state'], 'finalized')
        archived = self.store.read_snapshot('test')[0]
        self.store.register_event(EventDefinition('ongoing', 100, None, (VRF_V3_CONTRACT,)))
        seeded = publish_confirmed_prefix(self.store, p, 'test', 'ongoing')
        self.assertIsNone(seeded['snapshot']['end_at'])
        p.calls.clear()
        self.assertEqual(self.scanner(p).sync('ongoing', now=200)['state'], 'active')
        metadata, rows, _ = self.store.read_snapshot('ongoing')
        self.assertEqual((metadata['status'], rows[0]['draw_count']), ('live', 6))
        self.assertTrue(all(call[1] >= 14 for call in p.calls))
        self.assertEqual(self.store.read_snapshot('test')[0]['snapshot_id'], archived['snapshot_id'])
        self.assertEqual(publish_confirmed_prefix(self.store, p, 'test', 'ongoing')['snapshot']['snapshot_id'], metadata['snapshot_id'])
        with self.assertRaises(ValueError):
            self.store.publish_snapshot('ongoing', 18, 180, finalized=True)

    def test_seeded_prefix_must_pass_a_fresh_chain_anchor_before_publication(self):
        p = Provider()
        self.setup_event()
        self.scanner(p).sync('test', now=200)
        self.store.register_event(EventDefinition('ongoing', 100, None, (VRF_V3_CONTRACT,)))
        p.changed[13] = h(999)
        with self.assertRaises(ValueError):
            publish_confirmed_prefix(self.store, p, 'test', 'ongoing')
        self.assertIsNone(self.store.read_snapshot('ongoing')[0])

    def test_ongoing_chunk_budget_publishes_a_complete_watermarked_prefix(self):
        a,b=checkout(block=10,tx=1),checkout(block=15,tx=2)
        p=Provider([a,b],head=20)
        for row in (a,b):p.add_receipt([row],[payment(row)])
        self.store.register_event(EventDefinition('ongoing',100,None,(VRF_V3_CONTRACT,)))
        result=self.scanner(p,chunk_blocks=4,max_chunks=1).sync('ongoing',now=200)
        self.assertEqual(result['state'],'catching_up')
        metadata,rows,_=self.store.read_checkout_snapshot('ongoing')
        self.assertEqual(metadata['through_block'],13)
        self.assertEqual([r['tx_hash'] for r in rows],[h(1)])
        self.assertEqual(self.scanner(p).sync('ongoing',now=200)['state'],'active')
        self.assertEqual(len(self.store.read_checkout_snapshot('ongoing')[1]),2)

    def test_hard_deadline_keeps_complete_blocks_and_resumes_without_rechecking_them(self):
        a,b=checkout(block=10,tx=1),checkout(block=11,tx=2)
        class Interrupted(Provider):
            stopped=False
            receipt_calls=[]
            def receipt(self,tx):
                self.receipt_calls.append(tx)
                if tx==h(2) and not self.stopped:
                    self.stopped=True
                    raise KeyboardInterrupt('simulated process deadline')
                return super().receipt(tx)
        p=Interrupted([a,b])
        for row in [a,b]:p.add_receipt([row],[payment(row)])
        self.setup_event()
        with self.assertRaises(KeyboardInterrupt):self.scanner(p).sync('test',now=150)
        key=source_key(self.store.get_event('test'),VRF_V3_CONTRACT)
        self.assertEqual(self.store.get_checkpoint('test',key)['next_block'],11)
        self.assertIsNone(self.store.read_snapshot('test')[0])
        self.assertEqual(self.scanner(p).sync('test',now=150)['state'],'finalized')
        self.assertEqual(p.receipt_calls.count(h(1)),1)
        self.assertEqual(self.store.read_snapshot('test')[1][0]['draw_count'],6)

    def test_incomplete_block_never_advances_its_cursor(self):
        a,b=checkout(block=10,tx=1),checkout(block=10,tx=2,index=3)
        p=Provider([a,b]);p.add_receipt([a],[payment(a)]);p.add_receipt([b],[payment(b)])
        p.receipts[h(2)]['status']='0x0'
        self.setup_event()
        self.assertEqual(self.scanner(p).sync('test',now=150)['state'],'stale')
        key=source_key(self.store.get_event('test'),VRF_V3_CONTRACT)
        self.assertIsNone(self.store.get_checkpoint('test',key))
        self.assertIsNone(self.store.read_snapshot('test')[0])

    def test_final_snapshot_format_upgrade_uses_existing_ledger_without_chain_requests(self):
        a=checkout();p=Provider([a]);p.add_receipt([a],[payment(a)]);self.setup_event()
        self.scanner(p).sync('test',now=150)
        before=self.store.read_snapshot('test')[0]
        with self.store._connection(write=True) as db:
            old=dict(before);old.pop('checkout_evidence_supported')
            db.execute('UPDATE snapshots SET metadata=? WHERE snapshot_id=?',(json.dumps(old),before['snapshot_id']))
        class NoChain:
            def head(self):raise AssertionError('final format upgrade must not query chain')
        result=self.scanner(NoChain()).sync('test',now=150)
        self.assertTrue(result['snapshot']['checkout_evidence_supported'])
        self.assertNotEqual(result['snapshot']['snapshot_id'],before['snapshot_id'])
        self.assertEqual(self.store.read_checkout_snapshot('test')[1][0]['draw_count'],3)

    def scanner(self, provider, **kwargs):
        return EventLeaderboardScanner(self.store, provider, confirmations=2, **kwargs)

    def test_time_boundaries_multi_draw_and_free_exclusion(self):
        rows = [checkout(block=9, tx=1), checkout(block=10, tx=2), checkout(block=13, tx=3),
                checkout(block=14, tx=4), checkout(block=12, tx=5, amount=0)]
        p = Provider(rows)
        for row in rows:
            p.add_receipt([row], [payment(row)])
        self.setup_event()
        result = self.scanner(p).sync("test", now=150)
        self.assertEqual(result["state"], "finalized")
        metadata, ranking, _ = self.store.read_snapshot("test")
        self.assertEqual(metadata["data_through_at"], "1970-01-01T00:02:20Z")
        self.assertEqual(ranking[0]["draw_count"], 6)
        self.assertEqual(ranking[0]["paid_amount_raw"], str(60 * 10**18))
        requests = len(p.calls)
        self.scanner(p).sync("test", now=200)
        self.assertEqual(len(p.calls), requests, "Finalized activities stop requesting chain data")

    def test_multiple_users_packs_and_logs_same_transaction(self):
        a, b, c = checkout(index=2, count=2), checkout(index=3, wallet=OTHER, count=5), checkout(index=4, pack=h(900), count=4)
        p = Provider([a, b, c])
        p.add_receipt([a, b, c], [payment(a, amount=60 * 10**18), payment(b, index=1)])
        self.setup_event()
        self.assertEqual(self.scanner(p).sync("test", now=150)["state"], "finalized")
        _, ranking, _ = self.store.read_snapshot("test")
        self.assertEqual({r["wallet"]: r["draw_count"] for r in ranking}, {WALLET: 6, OTHER: 5})

    def test_unrelated_transfer_does_not_establish_payment(self):
        a = checkout()
        p = Provider([a]); p.add_receipt([a], [payment(a, recipient=OTHER)])
        self.setup_event()
        self.assertEqual(self.scanner(p).sync("test", now=150)["error"], "checkout_payment_amount_mismatch")
        self.assertIsNone(self.store.read_snapshot("test")[0])
        self.assertIsNone(self.store.get_checkpoint("test", "unused"))

    def test_malformed_quantity_never_defaults_to_one(self):
        a = checkout(); a["data"] = data(int(USDT, 16), 96, 30, 2, 1)
        with self.assertRaisesRegex(SourceIncomplete, "invalid_v3_checkout_data"):
            decode_checkout(a, "v3")
        a["data"] = data(int(USDT, 16), 96, 30, 0)
        with self.assertRaises(SourceIncomplete):
            decode_checkout(a, "v3")

    def test_checkout_id_zero_is_not_a_missing_quantity(self):
        row = checkout()
        row["data"] = data(int(USDT, 16), 96, 10**18, 1, 0)
        self.assertEqual(decode_checkout(row, "v3")["draw_count"], 1)

    def test_configured_schema_must_match_audit(self):
        self.store.register_event(EventDefinition("test", 100, 140, contracts=(VRF_V3_CONTRACT,),
            contract_schemas={VRF_V3_CONTRACT: "legacy_single"}))
        self.assertEqual(self.scanner(Provider()).sync("test", now=150)["error"], "audited_schema_configuration_mismatch")

    def test_cli_recovers_existing_event_definitions_without_seeding_default(self):
        import contextlib
        import io
        import os
        from unittest.mock import patch
        import scripts.sync_event_leaderboards as cli
        self.setup_event()
        env = {"EVENT_LEADERBOARD_CONFIG_PATH": "", "EVENT_LEADERBOARD_CONTRACT_SCHEMAS": "",
               "WALLET_MIGRATION_MAP_PATH": str(Path(self.temp.name) / "missing-map.json")}
        with patch.dict(os.environ, env), patch.object(cli, "DEFAULT_CONFIG_PATH", str(Path(self.temp.name) / "absent.json")), \
                patch.object(cli, "EtherscanProvider", return_value=Provider()), \
                patch.object(cli, "load_wallet_username_map", return_value={}), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(["--database", self.store.path, "--confirmations", "2"]), 0)
        self.assertEqual([row.event_id for row in self.store.list_events()], ["test"])
        self.assertEqual(self.store.read_snapshot("test")[0]["status"], "final")

    def test_full_pages_split_ranges_and_continue_same_block(self):
        rows = [checkout(index=i, tx=i) for i in range(1, 6)]
        p = Provider(rows)
        found = complete_logs(p, VRF_V3_CONTRACT, VRF_V3_CHECKOUT_TOPIC, 9, 11)
        self.assertEqual(len(found), 5)
        self.assertIn((VRF_V3_CONTRACT, 10, 10, 3), p.calls)
        p.rows.append(checkout(index=6, tx=6))
        with self.assertRaisesRegex(SourceIncomplete, "single_block_result_limit"):
            complete_logs(p, VRF_V3_CONTRACT, VRF_V3_CHECKOUT_TOPIC, 10, 10)

    def test_provider_failure_preserves_last_snapshot_and_cursor(self):
        a = checkout(block=10)
        p = Provider([a], head=14); p.add_receipt([a], [payment(a)])
        self.setup_event()
        result = self.scanner(p).sync("test", now=130)
        self.assertEqual(result["state"], "active")
        before = self.store.read_snapshot("test")[0]
        key = source_key(self.store.get_event("test"), VRF_V3_CONTRACT)
        cursor_before = self.store.get_checkpoint("test", key)
        p.height = 16; p.fail_contract = VRF_V3_CONTRACT
        failure = self.scanner(p).sync("test", now=150)
        after, rows, health = self.store.read_snapshot("test")
        self.assertEqual(failure["state"], "stale")
        self.assertEqual(before["snapshot_id"], after["snapshot_id"])
        self.assertTrue(health["stale"])
        self.assertEqual(self.store.get_checkpoint("test", key), cursor_before)
        self.assertEqual(rows[0]["draw_count"], 3)
        self.assertEqual(len(p.calls), 2, "Transport failures are not recursively retried across blocks")

    def test_all_sources_must_complete_before_publication(self):
        a = checkout()
        p = Provider([a]); p.add_receipt([a], [payment(a)])
        other_contract = DEFAULT_PACK_CONTRACTS[1]
        self.setup_event((VRF_V3_CONTRACT, other_contract))
        p.fail_contract = other_contract
        self.assertEqual(self.scanner(p).sync("test", now=150)["state"], "stale")
        self.assertIsNone(self.store.read_snapshot("test")[0])

    def test_max_chunks_does_not_publish_partial(self):
        p = Provider()
        self.setup_event()
        self.assertEqual(self.scanner(p, chunk_blocks=1, max_chunks=1).sync("test", now=150)["state"], "catching_up")
        self.assertIsNone(self.store.read_snapshot("test")[0])
        self.assertEqual(self.scanner(p, chunk_blocks=1, max_chunks=5).sync("test", now=150)["state"], "finalized")

    def test_resumed_all_source_cursors_avoid_start_boundary_requests(self):
        contracts = (VRF_V3_CONTRACT, DEFAULT_PACK_CONTRACTS[1])
        self.setup_event(contracts)
        initial = Provider(head=14)
        self.assertEqual(self.scanner(initial).sync("test", now=130)["state"], "active")
        self.assertEqual(initial.boundary_calls, [(100, 14)])
        # A new provider/scanner models a later process: optimization must rely
        # on SQLite progress rather than an in-process cached start block.
        resumed = Provider(head=15)
        self.assertEqual(self.scanner(resumed).sync("test", now=135)["state"], "active")
        self.assertEqual(resumed.boundary_calls, [])
        self.assertEqual(resumed.block_calls.count(12), len(contracts), "Every prior cursor anchor is still verified")
        self.assertEqual({(row[1], row[2]) for row in resumed.calls}, {(13, 13)})
        # With no newer confirmed block, retain the active snapshot behavior.
        self.assertEqual(self.scanner(resumed).sync("test", now=136)["state"], "active")
        self.assertEqual(resumed.boundary_calls, [])
        final = Provider(head=16)
        self.assertEqual(self.scanner(final).sync("test", now=150)["state"], "finalized")
        self.assertEqual(final.boundary_calls, [(140, 16)], "Finalization still resolves the exclusive end boundary")
        finished = Provider(head=17)
        self.assertEqual(self.scanner(finished).sync("test", now=160)["state"], "finalized")
        self.assertEqual(finished.boundary_calls, [])
        self.assertEqual(finished.block_calls, [], "Finalized snapshots make no chain requests")

    def test_missing_source_cursor_still_resolves_start_boundary(self):
        self.setup_event((VRF_V3_CONTRACT, DEFAULT_PACK_CONTRACTS[1]))
        initial = Provider(head=14)
        self.assertEqual(self.scanner(initial, max_chunks=1).sync("test", now=130)["state"], "catching_up")
        self.assertIsNone(self.store.read_snapshot("test")[0])
        resumed = Provider(head=14)
        self.assertEqual(self.scanner(resumed).sync("test", now=130)["state"], "active")
        self.assertEqual(resumed.boundary_calls, [(100, 14)])
        # The first source resumes at13; the missing source begins at10.
        self.assertEqual([(row[1], row[2]) for row in resumed.calls], [(10, 12)])

    def test_reorg_anchor_stops_advancing(self):
        p = Provider(head=14)
        self.setup_event()
        self.scanner(p).sync("test", now=130)
        p.height = 15; p.changed[12] = h(999)
        p.boundary_calls.clear()
        self.assertEqual(self.scanner(p).sync("test", now=135)["error"], "checkpoint_reorg_mismatch")
        self.assertEqual(p.boundary_calls, [])

    def test_duplicate_receipt_and_reverted_receipt_fail(self):
        a = checkout(); p = Provider([a]); p.add_receipt([a, a], [payment(a)])
        with self.assertRaisesRegex(SourceIncomplete, "duplicate_receipt_log"):
            verified_receipt_checkouts(p, h(99), SCHEMAS)
        p.receipts[h(99)]["status"] = "0x0"
        with self.assertRaisesRegex(SourceIncomplete, "unsuccessful_or_wrong_receipt"):
            verified_receipt_checkouts(p, h(99), SCHEMAS)

    def test_legacy_batch_individual_events_count_and_raw_remainder(self):
        contract = DEFAULT_PACK_CONTRACTS[1]
        a, b, c = [checkout(contract=contract, index=i, amount=3) for i in (2, 3, 4)]
        batch = dict(a, logIndex="0x5", topics=[LEGACY_BATCH, word_address(WALLET), h(500)], data=data(10, 3))
        p = Provider([a, b, c]); p.add_receipt([a, b, c, batch], [payment(a, amount=10)])
        rows = list(verified_receipt_checkouts(p, h(99), SCHEMAS).values())
        self.assertEqual(sum(row["draw_count"] for row in rows), 3)
        self.assertEqual([row["paid_amount_raw"] for row in rows], [3, 3, 4])
        self.setup_event((contract,))
        self.assertEqual(self.scanner(p).sync("test", now=150)["state"], "finalized")
        self.assertEqual(self.store.read_snapshot("test")[1][0]["paid_amount_raw"], "10")

    def test_changed_definition_restarts_ledger_scope(self):
        self.setup_event(); p = Provider()
        self.scanner(p).sync("test", now=150)
        updated = self.store.register_event(EventDefinition("test", 110, 150, contracts=(VRF_V3_CONTRACT,)))
        self.assertEqual(updated.revision, 2)
        self.assertIsNone(self.store.read_snapshot("test")[0])
        before = len(p.boundary_calls)
        self.assertEqual(self.scanner(p).sync("test", now=160)["state"], "active")
        self.assertEqual(p.boundary_calls[before:], [(110, 16)], "A new revision resolves its own start boundary")


class MigrationEvidenceTests(unittest.TestCase):
    def test_only_provenance_bearing_helper_maps_merge_wallets(self):
        from scripts.sync_event_leaderboards import verified_file_aliases, MIGRATION_HELPER, MIGRATION_TOPIC
        root = RUN_DIR / 'migration-evidence'
        root.mkdir(parents=True, exist_ok=True)
        path = root / "map.json"
        self.assertEqual(verified_file_aliases(path), [])
        path.write_text(json.dumps({WALLET: OTHER}))
        with self.assertRaisesRegex(SourceIncomplete, "unverified_migration_map"):
            verified_file_aliases(path)
        payload = {"source": "bsc_logs:helper_migration_event", "helper_contract": MIGRATION_HELPER,
            "helper_event_topic": MIGRATION_TOPIC, "last_scanned_block": 500,
            "mappings": [{"old": WALLET, "new": OTHER, "block": 400, "tx": h(99), "source": "helper_event_only"}]}
        path.write_text(json.dumps(payload))
        rows = verified_file_aliases(path)
        self.assertEqual(rows[0]["new"], OTHER)
        self.assertIn("map_sha256:", rows[0]["evidence"])
        payload["mappings"][0]["source"] = "existing_flat_map"
        path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(SourceIncomplete, "unverified_migration_row"):
            verified_file_aliases(path)


if __name__ == "__main__":
    unittest.main()
