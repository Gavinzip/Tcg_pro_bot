# NFT13 Incremental Sync

This project includes:

- `scripts/sync_nft13_incremental.py`
- `bot.py` startup bootstrap + daily scheduled sync (`12:00 Asia/Taipei`)
- `/sync_status` slash command to inspect latest sync result

## Core behavior

- Incremental fetch from Moralis owners endpoint.
- Stops immediately when it hits the first duplicate holder record.
- Writes:
  - latest: `SYNC_DATA_DIR/snapshots/nft_<token_id>_holders.latest.json`
  - daily history: `SYNC_DATA_DIR/snapshots/history/YYYY-MM-DD.json`
  - state: `SYNC_DATA_DIR/state/nft_<token_id>_state.json`
- Optional git backup (SSH repo).
- Optional Discord webhook notifications.

## Environment variables

- `APP_ENV=server|local`
- `SYNC_DATA_DIR=/data/renaiss_sync` (server) or `./data/renaiss_sync` (local)
- `MORALIS_API_KEY=...`
- `MORALIS_CHAIN=bsc`
- `NFT_CONTRACT=0x7d1b7db704d722295fbaa284008f526634673dbf`
- `NFT_TOKEN_ID=13`
- `MORALIS_PAGE_LIMIT=20`
- `BACKUP_GIT_ENABLED=1`
- `BACKUP_GIT_REPO=git@github.com:Gavinzip/renaiss_data.git`
- `BACKUP_GIT_BRANCH=main`
- `BACKUP_GIT_DIR=/data/renaiss_sync/backup_repo`
- `BOOTSTRAP_FROM_GIT=1` (recommended on server)
- `SYNC_TEST_MODE=1` to disable git push side effects
- `SYNC_WEBHOOK_URL=<discord-webhook-url>`

## Manual commands

Bootstrap only (startup-like):

```bash
python scripts/sync_nft13_incremental.py --trigger startup --bootstrap-only
```

Run incremental sync once:

```bash
python scripts/sync_nft13_incremental.py --trigger manual
```

## Discord command

- Use `/sync_status` to read:
  - last success/failure
  - trigger source (`startup` / `daily` / `manual`)
  - new rows/pages/holders
  - latest snapshot path
