import json
from dataclasses import replace
from tempfile import TemporaryDirectory
from pathlib import Path
import unittest
from unittest.mock import patch

from runtime.event_leaderboards.store import EventLeaderboardStore
from runtime.event_leaderboards.config import EventDefinition, source_key
from runtime.event_leaderboards.http import EventLeaderboardHTTP


class QueryRetentionTests(unittest.TestCase):
    def test_default_targets_the_production_campaign_not_the_closed_single_day_event(self):
        from runtime.event_leaderboards.query_retention import QueryRetention
        with patch.dict('os.environ', {}, clear=True):
            self.assertEqual(QueryRetention.configured().events, frozenset({'bnb-renaiss-2026'}))

    def test_only_selected_live_versions_expire_and_all_evidence_and_final_results_survive(self):
        with TemporaryDirectory(prefix='query-retention-test-') as root:
            store = EventLeaderboardStore(str(Path(root) / 'db.sqlite'), query_retention_events={'live'})
            contract = '0x' + 'a' * 40
            definition = store.register_event(EventDefinition('live',100,None,(contract,),contract_schemas={contract:'v3'}))
            source = source_key(definition,contract)
            row = {'wallet':'0x'+'1'*40,'tx_hash':'0x'+'2'*64,'log_index':0,'pack_contract':contract,
                   'pack_id':'pack','builder':'0x'+'3'*64,'draw_count':2,'paid_amount_raw':'1000000000000000000',
                   'block_number':10,'block_hash':'block','timestamp':110}
            store.commit_chunk('live',source,None,20,[row],block_hash='anchor')
            with patch('time.time',return_value=1000): first=store.publish_snapshot('live',19,120)
            # Legacy history is not silently deleted on installation.
            with store._connection(write=True) as db:
                db.execute('DELETE FROM event_query_versions WHERE snapshot_id=?',(first['snapshot_id'],))
            with patch('time.time',return_value=1001): old=store.publish_snapshot('live',19,120)
            with patch('time.time',return_value=4702): latest=store.publish_snapshot('live',19,120)
            http=EventLeaderboardHTTP(store)
            with patch('time.time',return_value=9000):
                self.assertEqual(http.dispatch('/v1/leaderboards/live',{'snapshot_id':old['snapshot_id']})[0],409)
                self.assertEqual(http.dispatch('/v1/leaderboards/live/checkouts',{'snapshot_id':first['snapshot_id']})[0],409)
                self.assertEqual(http.dispatch('/v1/leaderboards/live',{'snapshot_id':latest['snapshot_id']})[0],200)
            with store._connection() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM ledger').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM ledger_insertions').fetchone()[0],1)
                self.assertIsNotNone(db.execute('SELECT 1 FROM snapshots WHERE snapshot_id=?',(first['snapshot_id'],)).fetchone())
                for table in ['snapshots','snapshot_rows','snapshot_builder_rows','snapshot_checkout_rows','event_query_versions']:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table+' WHERE snapshot_id=?',(old['snapshot_id'],)).fetchone()[0],0)
            definition=store.register_event(replace(definition,end_ts=200))
            store.commit_chunk('live',source_key(definition,contract),None,20,[row],block_hash='anchor')
            with patch('time.time',return_value=10000): final=store.publish_snapshot('live',19,200,finalized=True)
            with patch('time.time',return_value=1000000):
                self.assertEqual(http.dispatch('/v1/leaderboards/live/checkouts',{'snapshot_id':final['snapshot_id']})[0],200)

    def test_count_cap_is_scoped_to_the_selected_event(self):
        from runtime.event_leaderboards.query_retention import QueryRetention
        import sqlite3
        db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
        self.addCleanup(db.close)
        for table in ['snapshot_rows','snapshot_builder_rows','snapshot_checkout_rows','snapshot_ledger_watermarks']:
            db.execute('CREATE TABLE '+table+'(snapshot_id TEXT)')
        db.execute('CREATE TABLE snapshots(snapshot_id TEXT PRIMARY KEY,event_id TEXT,created_seq INTEGER,metadata TEXT)')
        policy=QueryRetention(frozenset({'selected'}),3600,2);policy.initialize(db)
        db.execute('INSERT INTO snapshots VALUES(?,?,?,?)',('other','other',0,'{}'))
        for n in range(5):
            metadata={'snapshot_id':str(n),'status':'live'}
            db.execute('INSERT INTO snapshots VALUES(?,?,?,?)',(str(n),'selected',n+1,json.dumps(metadata)))
            policy.record(db,'selected',metadata,1000+n)
        self.assertEqual([r[0] for r in db.execute('SELECT snapshot_id FROM snapshots ORDER BY created_seq')],['other','3','4'])
