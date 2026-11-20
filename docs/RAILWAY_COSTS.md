# TCG Pro Railway cost controls

The operating target is approximately **US$30/month**. This is a target, not a
hard-stop promise. Measure the actual production Bot after cutover; temporary
migration receiver metrics are not a production forecast.

## Verified on 2026-11-20 (Asia/Taipei)

- Workspace plan: Pro. The current billing period is 2026-11-19 to 2026-12-19.
- Native workspace usage alert was set to US$30 and read back successfully.
  `hardLimit` remains null: the alert does not shut down the Bot or other projects.
  This is a workspace alert, not a TCG-only meter.
- The latest TCG warning was **volume 87% full**, sent at 00:46 Taipei time.
  Migration/backup temporarily reached 45.8 GB. After staging cleanup, `du`
  reported 32,203,616,256 allocated bytes in `/data` before retired-copy cleanup.
  After the announced cleanup of only the new Railway Git-clone/ranking copies,
  it reports 14,590,795,776 bytes (approximately US$2.19/month storage).
  Originals on Zeabur and the old GitHub backup remain untouched.
- The US$50.06 email was sent on November 18, before this billing period and
  before the new TCG service began operating. It is an account usage notice,
  not a TCG project cost measurement.
- Do not delete or change CorpMind's retained volumes to meet TCG's budget.

## Controls in this release

- Old permanent/month/week/year leaderboard commands and full historical
  ranking/NFT background jobs are gated off with `LEGACY_LEADERBOARDS_ENABLED=0`.
- Runtime Git backups, including the separate market-cache autopush, are off.
- New activity jobs only scan the configured finite interval. Confirmed source
  checkpoints are durable and incremental. A finalized activity does not scan
  the chain again on later scheduler ticks.
- Production activity scanning is paced at 0.5 requests/second, reserving room
  for interactive BBW requests on the existing five-request/second key. It does
  not buy a new paid key or claim to eliminate every upstream timeout.
- The obsolete subpack/legendary one-minute monitors are disabled in deployment
  configuration after the old pack API was observed returning 404.
- Daily encrypted Drive backups deduplicate with restic. Mutable data is staged;
  bulk media is read in place. Cached SHA-256 values are reused only with
  unchanged size, nanosecond mtime and inode; post-upload validation remains.
  The product's `.img` cache format is included; copying these images into
  mutable scratch caused the initial volume warning and has been corrected.
- One scheduler and a per-volume lock prevent overlapping backups. Do not add
  an independent Railway backup worker for the same daily task.
- Serve the event API from the Bot's combined Profile API owning `/data`.
  Once consumers pass checks on that API, retire the redundant standalone API
  process. Do not stop the current consumer endpoint before switching it.

## Forecast method and remaining work

Railway currently lists RAM US$10/GB-month, CPU US$20/vCPU-month, volume
US$0.15/GB-month, and egress US$0.05/GB. The Pro US$20 subscription is credited
toward eligible usage; it is not an additional US$20 on top of the same usage.
See [official pricing](https://docs.railway.com/pricing).

Use sustained production averages, including the daily incremental backup:

`resources/month ≈ RAM_GB*10 + vCPU*20 + stored_GB*0.15 + egress_GB*0.05`

The retained 14.6 GB file allocation is approximately US$2.19/month for storage
alone. Railway's volume metric including filesystem overhead is about 15.7 GB,
or roughly US$2.35/month; use platform measurements for billing forecasts.
CPU/RAM limits are ceilings, not reservations: lowering an idle ceiling alone
does not reduce the bill. Never choose a memory limit below measured Bot,
renderer and backup concurrency peaks merely to make a calculator look cheaper.

Keep originals on the old volume. The announced cleanup removed only the new
Railway copies `/data/renaiss_sync/backup_repo` and `/data/renaiss_sync/rankings`
after Drive recovery verification, freeing approximately 17.6 GB allocated.
The final migration delta must exclude those retired trees rather than copy them
back. Active market data, runtime maps, user settings and event data remain.
Changing plans requires checking volume-size and account-wide requirements.
