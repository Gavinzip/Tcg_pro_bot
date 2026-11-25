"""Opt-in local integration with the campaign's real ticket engine and Profile HTTP API.

Set CAMPAIGN_TEST_SOURCE_PATH to the existing campaign checkout. Only synthetic
chain input and test-owned SQLite files are used; no production/provider calls.
"""
import json
import os
from pathlib import Path
import subprocess
import threading
import unittest
from unittest.mock import patch
import uuid

from runtime.event_leaderboards.config import EventDefinition
from runtime.event_leaderboards.http import EventLeaderboardHTTP
from runtime.event_leaderboards.scanner import EventLeaderboardScanner, VRF_V3_CONTRACT
from runtime.event_leaderboards.store import EventLeaderboardStore
from tests.test_event_leaderboard_scanner import Provider, checkout, payment, h, WALLET

RUN_DIR = Path(__file__).resolve().parents[1] / '.scratch' / 'event-replay-campaign-tests' / ('run-' + uuid.uuid4().hex)
CAMPAIGN = os.getenv('CAMPAIGN_TEST_SOURCE_PATH')

NODE_CHECK = r"""
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
const input=JSON.parse(process.env.REPLAY_TEST_INPUT);
const moduleAt=(name)=>import(pathToFileURL(input.campaign+'/server/'+name+'.mjs').href);
const {CampaignStore}=await moduleAt('store');
const {createRewards}=await moduleAt('rewards');
const {createLeaderboard}=await moduleAt('leaderboard');
const {createCampaignRanking}=await moduleAt('campaign-ranking');
const {lockedBinding}=await import(pathToFileURL(input.campaign+'/server/tests/binding-fixture.mjs').href);
const store=new CampaignStore(input.database);
let ranking;
try {
 const rewards=createRewards(store);
 rewards.grant({event:'ongoing',wallet:input.wallet,kind:'cross-platform',subject:'synthetic-sol',
   bindingId:lockedBinding(store,input.wallet,'synthetic-sol'),
   proof:'same-verified-claim',tickets:20,shares:1,earnedAt:'1970-01-01T00:02:00Z',evidence:{test:true}});
 const config={eventId:'ongoing',builder:input.builder,tcgBase:input.origin,tcgToken:'local-test-token',
   campaignRules:{eventId:'ongoing',from:100000,until:null,
   ticketRates:input.packs.map((packId,i)=>({packId,builder:input.builder,tickets:[1,2,5][i]}))}};
 const source=createLeaderboard(config);
 ranking=createCampaignRanking({config,store,source,rewards});
 await ranking.start();
 const latest=await ranking.read({});
 assert.equal(latest.rows[0].effectiveTickets,input.tickets);
 assert.equal(latest.totalParticipants,1);
 assert.equal(rewards.all('ongoing').length,1);
 let oldTickets=null;
 if(input.oldSnapshot){
   const pinned=await ranking.read({snapshotId:input.oldSnapshot});
   oldTickets=pinned.rows[0].effectiveTickets;
   assert.equal(oldTickets,29);
 }
 console.log(JSON.stringify({snapshotId:latest.snapshotId,tickets:latest.rows[0].effectiveTickets,
   sourceSnapshotId:latest.sourceSnapshotId,bonusGrants:rewards.all('ongoing').length,oldTickets,
   summary:latest.summary}));
} finally {await ranking?.close(); store.close();}
"""


@unittest.skipUnless(CAMPAIGN, 'Set CAMPAIGN_TEST_SOURCE_PATH for real campaign ticket integration')
class ReplayCampaignTests(unittest.TestCase):
    def test_actual_http_evidence_to_persistent_ticket_ranking_counts_late_draws_once(self):
        import profile_api
        RUN_DIR.mkdir(parents=True, exist_ok=True)
        store = EventLeaderboardStore(str(RUN_DIR / 'source.sqlite3'))
        store.register_event(EventDefinition('ongoing', 100, None, (VRF_V3_CONTRACT,), builder=h(7)))
        provider = Provider(head=62)
        scanner = EventLeaderboardScanner(store, provider, confirmations=2)
        packs = [h(28), h(48), h(88)]
        originals = []
        for n, count, amount in [(0, 2, 56), (1, 1, 48), (2, 1, 88)]:
            row = checkout(block=40+n, tx=1+n, count=count, amount=amount*10**18, pack=packs[n])
            row['topics'][3] = h(7)
            provider.rows.append(row)
            provider.add_receipt([row], [payment(row)])
            originals.append(row)
        self.assertEqual(scanner.sync('ongoing', now=1000)['state'], 'active')
        controller = EventLeaderboardHTTP(store)
        with patch.object(profile_api, 'API_TOKEN', 'synthetic-internal'), \
             patch.object(profile_api, 'LEADERBOARD_CLIENT_TOKENS', {'site': 'local-test-token'}), \
             patch.object(profile_api, '_EVENT_CONTROLLER', controller), \
             patch.dict(os.environ, {'EVENT_LEADERBOARD_API_ENABLED': '1'}):
            server = profile_api.ProfileApiServer(('127.0.0.1', 0), profile_api.ProfileApiHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                def check(tickets, old=None):
                    payload = {'campaign': str(Path(CAMPAIGN).resolve()), 'origin': 'http://127.0.0.1:'+str(server.server_port),
                        'database': str(RUN_DIR / 'campaign.sqlite'), 'wallet': WALLET, 'builder': h(7),
                        'packs': packs, 'tickets': tickets, 'oldSnapshot': old}
                    result = subprocess.run(['node', '--input-type=module', '-e', NODE_CHECK],
                        env={**os.environ, 'REPLAY_TEST_INPUT': json.dumps(payload)}, capture_output=True,
                        text=True, timeout=30, check=False)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    return json.loads(result.stdout)
                before = check(29)  # 2x1 + 1x2 + 1x5 +20
                late = checkout(block=42, tx=3, index=3, count=2, amount=176*10**18, pack=packs[2])
                late['topics'][3] = h(7)
                provider.rows.append(late)
                provider.add_receipt([originals[2], late], [payment(originals[2], amount=264*10**18)])
                self.assertEqual(scanner.sync('ongoing', now=1000)['state'], 'active')
                repaired = check(39, before['snapshotId'])
                for _ in range(3):
                    self.assertEqual(scanner.sync('ongoing', now=1000)['state'], 'active')
                restarted = EventLeaderboardScanner(EventLeaderboardStore(store.path), provider, confirmations=2)
                self.assertEqual(restarted.sync('ongoing', now=1000)['state'], 'active')
                repeated = check(39, before['snapshotId'])
                self.assertNotEqual(before['sourceSnapshotId'], repaired['sourceSnapshotId'])
                report = {'before': before, 'lateRepair': repaired, 'afterThreeReplaysAndRestart': repeated,
                    'chainInput': 'synthetic delayed-index fixture', 'realModules': str(Path(CAMPAIGN).resolve()),
                    'providerRequests': 0, 'productionWrites': 0}
                output = os.getenv('REPLAY_TEST_REPORT')
                if output:
                    Path(output).write_text(json.dumps(report, indent=2)+'\n')
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == '__main__':
    unittest.main()
