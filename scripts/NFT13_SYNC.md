# NFT13 Incremental Sync

This project includes:

- `scripts/sync_nft13_incremental.py`
- `bot.py` startup bootstrap + daily scheduled sync (`NFT_SYNC_HOUR:NFT_SYNC_MINUTE`, default `06:00 Asia/Taipei`)
- `/sync_status` slash command to inspect latest sync result

## Core behavior

- Uses BSC/BscScan API logs, not Moralis.
- Reads ERC1155 `TransferSingle` / `TransferBatch` logs for the configured NFT contract.
- Rebuilds or incrementally updates the current holder balances for `NFT_TOKEN_ID`.
- Writes:
  - latest: `SYNC_DATA_DIR/snapshots/nft_<token_id>_holders.latest.json`
  - daily history: `SYNC_DATA_DIR/snapshots/history/YYYY-MM-DD.json`
  - state: `SYNC_DATA_DIR/state/nft_<token_id>_state.json`
- Optional git backup (SSH repo).
- Optional Discord webhook notifications.

## Environment variables

- `APP_ENV=server|local`
- `SYNC_DATA_DIR=/data/renaiss_sync` (server) or `./data/renaiss_sync` (local)
- `BSCSCAN_API_KEY=...`
- `BSCSCAN_API_URL=https://api.etherscan.io/v2/api`
- `BSCSCAN_CHAIN_ID=56`
- `NFT_CONTRACT=0x7d1b7db704d722295fbaa284008f526634673dbf`
- `NFT_TOKEN_ID=13`
- `NFT_SYNC_START_BLOCK=72800000`
- `NFT_SYNC_BLOCK_CHUNK=200000`
- `NFT_SYNC_LOG_PAGE_LIMIT=1000`
- `BACKUP_GIT_ENABLED=1`
- `BACKUP_GIT_REPO=git@github.com:Gavinzip/renaiss_data.git`
- `BACKUP_GIT_BRANCH=main`
- `BACKUP_GIT_DIR=/data/renaiss_sync/backup_repo`
- `BOOTSTRAP_FROM_GIT=1` (recommended on server)
- `SYNC_TEST_MODE=1` to disable git push side effects
- `SYNC_WEBHOOK_URL=<discord-webhook-url>`
- `NFT_SYNC_COMPARE_ON_STARTUP=1` (bot startup does one incremental compare after bootstrap)
- `NFT_SYNC_TZ=Asia/Taipei` (falls back to UTC+8 if container has no tzdata)

## Manual commands

Bootstrap only (startup-like):

```bash
python scripts/sync_nft13_incremental.py --trigger startup --bootstrap-only
```

Run incremental sync once:

```bash
python scripts/sync_nft13_incremental.py --trigger manual
```

Full rebuild from `NFT_SYNC_START_BLOCK`:

```bash
python scripts/sync_nft13_incremental.py --trigger manual_full --full-rebuild
```

## Discord command

- Use `/sync_status` to read:
  - last success/failure
  - trigger source (`startup` / `daily` / `manual`)
  - scanned blocks / log count / matched token events / holder count
  - latest snapshot path
