// Offline integration against the website's real source reader and scorer.
// Usage: node scripts/verify_checkout_ranking.mjs WEBSITE_CHECKOUT PYTHON
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { resolve, dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import assert from 'node:assert/strict';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const [website, python] = process.argv.slice(2);
assert.ok(website && python, 'Provide the existing website checkout and Python interpreter');
const load = (name) => import(pathToFileURL(resolve(website, 'server', name)).href);
const [{ createLeaderboard }, { createCampaignRanking }, { CampaignStore, digest }, { createRewards }] =
  await Promise.all(['leaderboard.mjs', 'campaign-ranking.mjs', 'store.mjs', 'rewards.mjs'].map(load));
const out = join(root, '.scratch/payment-sync-20261129');
const fixture = JSON.parse(readFileSync(join(out, 'fixture.json')));
const child = spawn(python, [join(root, 'scripts/benchmark_checkout_pages.py'), 'fixed', '--serve'], {
  cwd: root, env: { ...process.env, PYTHONDONTWRITEBYTECODE: '1' }, stdio: ['pipe', 'pipe', 'inherit'],
});
const exited = once(child, 'exit');
let ranking, store;
try {
  const lines = createInterface({ input: child.stdout });
  const [line] = await Promise.race([once(lines, 'line'), exited.then(([code]) => {
    throw new Error('Local source exited before readiness: ' + code);
  })]);
  const { port } = JSON.parse(line);
  const rules = { eventId: 'paging', from: 100000, until: null, builder: '0x' + 'b'.repeat(64),
    contracts: ['0x' + 'a'.repeat(40)],
    ticketRates: [{ packId: '0x' + 'c'.repeat(64), builder: null, tickets: 5 }], crossPlatform: null };
  const dataDir = join(out, 'consumer-data');
  mkdirSync(dataDir, { recursive: true });
  const config = { eventId: 'paging', tcgToken: 'synthetic-local-read',
    tcgBase: `http://127.0.0.1:${port}`, builder: rules.builder, campaignRules: rules,
    checkoutSyncEnabled: true, dataDir };
  const source = createLeaderboard(config);
  store = new CampaignStore(':memory:');
  const rewards = createRewards(store);
  ranking = createCampaignRanking({ config, store, source, rewards,
    cacheOptions: { schedule: () => ({ unref() {} }), cancel() {} } });
  const began = performance.now();
  await ranking.start();
  const first = await ranking.read({ all: 'true' });
  const coldMs = performance.now() - began;
  assert.equal(first.summary.totalDraws, fixture.rows);
  assert.equal(first.summary.totalTickets, fixture.rows * 5);
  assert.equal(first.rows.length, fixture.players);
  const firstDigest = digest(JSON.stringify(first.rows));
  await ranking.refresh();
  const same = await ranking.read({ all: 'true' });
  assert.equal(digest(JSON.stringify(same.rows)), firstDigest);
  assert.deepEqual(same.summary, first.summary);
  const wallet = '0x' + '0'.repeat(39) + '1';
  const grant = { event: 'paging', wallet, kind: 'x-follow', subject: 'synthetic-local-task',
    proof: 'synthetic-verified-task', tickets: 2, evidence: { localFixture: true },
    earnedAt: '1970-01-01T00:01:51.000Z' };
  assert.equal(rewards.grant(grant).id, rewards.grant(grant).id);
  await ranking.refresh();
  const after = await ranking.read({ all: 'true' });
  assert.equal(after.summary.totalDraws, fixture.rows);
  assert.equal(after.summary.totalTickets, fixture.rows * 5 + 2);
  assert.equal(after.rows.find((r) => r.wallet === wallet).effectiveTickets,
    first.rows.find((r) => r.wallet === wallet).effectiveTickets + 2);
  const pinned = await ranking.read({ all: 'true', snapshotId: first.snapshotId });
  assert.deepEqual(pinned.summary, first.summary);
  assert.equal(digest(JSON.stringify(pinned.rows)), firstDigest);
  const report = { synthetic: true, sourceRows: fixture.rows, players: fixture.players,
    coldMs: Math.round(coldMs), elapsedMs: Math.round(performance.now() - began),
    initialSummary: first.summary, afterTaskSummary: after.summary,
    unchangedRefreshMatches: true, repeatedTaskGrantCount: 1, pinnedRowsUnchanged: true,
    sourceSnapshotId: first.sourceSnapshotId, dataThroughAt: first.dataThroughAt,
    chainApiCalls: 0 };
  writeFileSync(join(out, 'ranking-integration-report.json'), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report));
} finally {
  await ranking?.close();
  store?.close();
  if (child.exitCode === null) child.stdin.end('\n');
  const [code] = await exited;
  assert.equal(code, 0, 'Local source must close cleanly');
}
