# Daily Ranking Update (Asia/Taipei 14:00)

This project now provides `scripts/update_rankings.py`.
It updates and persists:

- `volume` ranking (`trade_volume_usdt`)
- `holdings` ranking (`holdings_value_usdt`, current card holdings value)
- `sbt` ranking (`sbt_owned_total`)

## Output paths

Set your data volume path with `RANKING_DATA_DIR` (recommended on Zeabur: `/data/rankings`).

- latest snapshot: `/data/rankings/latest.json`
- daily history: `/data/rankings/history/YYYY-MM-DD.json`

## Run once manually

```bash
cd /Users/gavin/renaiss_project/old/tcg_pro
RANKING_DATA_DIR=/data/rankings ./.venv/bin/python scripts/update_rankings.py
```

## Cron (every day at 14:00 Asia/Taipei)

```cron
CRON_TZ=Asia/Taipei
0 14 * * * cd /Users/gavin/renaiss_project/old/tcg_pro && RANKING_DATA_DIR=/data/rankings ./.venv/bin/python scripts/update_rankings.py >> logs/ranking_cron.log 2>&1
```

## Notes

- Keep your volume mounted to `/data` so files survive restarts/redeploys.
- The script uses atomic writes (`*.tmp` then rename) to avoid partial JSON files.
- If API rate limits happen, the script retries with backoff automatically.
