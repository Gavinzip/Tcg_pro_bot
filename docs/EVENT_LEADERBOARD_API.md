# Finite activity leaderboard API

The activity is one finite interval, not a repeating daily/monthly/yearly ranking. The built-in activity `2026-11-20` runs from **2026-11-20 00:00:00 +08:00** inclusive to **2026-11-21 00:00:00 +08:00** exclusive. A missing optional default configuration selects this explicit built-in activity; an explicitly configured missing or malformed file is an error. No API response invents scan results.

The Profile API serves these routes through its existing authentication. Supply its configured bearer token when enabled. Responses must not use long-lived HTTP caches. Machine-readable contract: [OpenAPI](EVENT_LEADERBOARD_OPENAPI.yaml).

The [checkout evidence extension](EVENT_CHECKOUT_EVIDENCE.md) adds frozen per-checkout pack/builder/payment evidence for campaign reward calculation and preserves complete-block progress across hard job deadlines. It is deployed as `733a925`; [production release verification](EVENT_LEADERBOARD_RELEASE_2026_11_21.md) covers the complete schema-v2 snapshot and live HTTP/consumer checks. Existing draw-count leaderboard semantics are unchanged.

The separate website-token implementation is documented in [API client tokens](API_CLIENT_TOKENS.md). It requires deploying the updated server and configuring `EVENT_LEADERBOARD_CLIENT_TOKENS_JSON`; the two website credentials are not provisioned merely by adding this code. They authorize only leaderboard GET routes, while the existing internal `PROFILE_API_TOKEN` retains its original access.

Website-scoped credentials and the [builder-filter extension](EVENT_LEADERBOARD_BUILDER.md) are live. The November 20 finite activity's schema-v2 rebuild completed on November 21 at 01:46 Taipei time: snapshot `3140f6689c664479a7a24892899c7107` is final, with 407 wallets, 3,607 verified paid checkouts and 7,568 draws. Complete scoped results, cursor/search and frozen checkout reads passed live checks. Filtering precedes aggregation; snapshot_id plus builder pins a scope and cursors bind it. Search ranks remain global within that builder. Historical snapshots lacking builder evidence still return 409 when filtered.

## Counting and ranking

`counting_rule=confirmed_paid_checkout`: count each successfully settled, confirmed checkout's draw quantity only when the transaction's actual payment is verified. Free/gift draws and failed transactions do not count. The scanner reconciles successful checkout events with payment receipts. Publication independently checks in its SQLite transaction that every configured audited contract source has a checkpoint beyond the requested confirmed block and a nonempty reorg anchor. A published zero-participant snapshot is valid only after this complete scan.

All wallets remain in the result, including wallets without a username. Verified old-to-new wallet migration chains are merged into one canonical wallet; conflicting migrations or cycles stop the update. Usernames and aliases are frozen in each snapshot. Search for any historical alias finds the canonical participant.

Rank uses **competition ranking**: draw count descending; equal counts share rank (`1,1,3`). Wallet ascending provides a deterministic tie order. Changing client sorting never changes the participant's global rank. Paid amount is informational and does not break draw-count ties. Raw integer amounts and formatted amounts are decimal **strings**, including values larger than SQLite's 64-bit integer range; clients must not parse them as floating point.

## Read a complete snapshot or pages

```http
GET /v1/leaderboards/2026-11-20?limit=100
Authorization: Bearer <PROFILE_API_TOKEN>
```

The first response pins `snapshot_id`. Preserve it for the whole client session, or use `next_cursor`, which includes that snapshot. A new publication cannot change existing pages.

```http
GET /v1/leaderboards/2026-11-20?limit=100&cursor=<next_cursor>
GET /v1/leaderboards/2026-11-20?all=true&snapshot_id=<snapshot_id>
```

Parameters:

| Parameter | Values | Default |
| --- | --- | --- |
| `snapshot_id` | Previously returned immutable snapshot | Latest successful snapshot of current config revision |
| `builder` | Optional 0x + 64 hex platform code; continuation cursor can carry it | Entire activity |
| `limit` | Integer 1–1000 | 100 |
| `cursor` | Opaque `next_cursor`, retaining the same sort/order/search | None |
| `all` | `true` or `false`; `true` cannot be combined with cursor | `false` |
| `sort` | `rank`, `draw_count`, `wallet`, `username`, `paid_amount` | `rank` |
| `order` | `asc` or `desc` | `asc` |

Wallet ascending breaks client-sort ties in both directions. Username sorting uses Unicode casefold. `paid_amount` sorting uses exact raw integers. All rows are retained; the API does not truncate at 300 wallets. `all=true` transfers the complete snapshot and can be large.

Successful response fields include `event_id`, `revision`, `config_hash`, `start_at`, `end_at`, `snapshot_id`, `generated_at`, `data_through_at`, `through_block`, `total_participants`, `status` (`live`, `settling`, `final`), `counting_rule`, `contracts`, `chain_id`, `token_decimals`, `ranking_rule`, `stale`, `stale_reason`, `last_error`, `progress`, `update_interval_seconds`, `stale_after_seconds`, `sort`, `order`, `total_matches`, `next_cursor`, and `rows`. Timestamps are UTC ISO 8601. Each row contains:

```json
{"rank":2,"wallet":"0x0000000000000000000000000000000000000003","username":null,"aliases":["0x0000000000000000000000000000000000000003"],"draw_count":5,"paid_amount_raw":"1000000000000000001","paid_amount":"1.000000000000000001"}
```

Snapshot rows and counting metadata are immutable. `stale`, `stale_reason`, `last_error`, `progress`, `update_interval_seconds` and `stale_after_seconds` describe current event health and can change independently of the chosen snapshot. Freshness uses the latest successful snapshot of the current configuration revision, including when reading an older pinned snapshot. A nonfinal source is stale when its latest publication is overdue, even without a recorded error; `stale_reason=update_overdue` then leaves `last_error=null`. A final source is exempt from overdue checks. A pinned prior revision with no current snapshot reports `stale_reason=no_current_snapshot`. `settling` means the event end has passed but the full end boundary is not yet confirmed; `final` requires a complete confirmed scan through the end. Once final, subsequent scanner ticks must not scan beyond the event.

## 欄位字典（前端串接）

排行榜與搜尋共用以下欄位；搜尋結果的名次仍是整個活動的名次。

| name | type | description／計算方式 |
| --- | --- | --- |
| `rows[].rank` | integer | 以有效付費抽數由多至少排名，同抽數同名次，下一名跳號（1、1、3）。 |
| `rows[].wallet` | string | 參與者標準化小寫錢包；僅依已驗證的搬遷證據合併舊、新錢包。 |
| `rows[].username` | string/null | 發布此快照時已知的名稱；沒有名稱仍納入排行。 |
| `rows[].aliases` | string[] | 該參與者的已驗證錢包別名，包含標準錢包；可用於搜尋。 |
| `rows[].draw_count` | integer | 活動起始時間（含）至結束時間（不含）內，已確認交易、抽卡成功且同交易實際付款核對成功的抽數總和；多抽算實際數量，免費／贈送不算。 |
| `rows[].paid_amount_raw` | decimal string | 上述有效抽卡所對應付款的最小單位整數總和；不可轉浮點數。 |
| `rows[].paid_amount` | decimal string | `paid_amount_raw / 10^token_decimals` 的精確十進位表示，僅供資訊展示。 |
| `event_id` | string | 有限期間活動的識別碼。 |
| `builder` | string/null | 本次平台來源；null 為全活動。 |
| `builder_filter_supported` | boolean | 此快照是否具備完整 builder 證據；接入方啟用篩選前必須核對。 |
| `revision`, `config_hash` | integer, string | 活動設定版本與雜湊；時間或合約變更會另開版本，不沿用不相容進度。 |
| `start_at`, `end_at` | timestamp | 計分起訖，UTC ISO 8601；起始含、結束不含。 |
| `snapshot_id` | string/null | 不可變快照識別碼；首次未完成為 null。分頁與搜尋應固定同一 ID。 |
| `generated_at` | timestamp | 此快照發布時間。 |
| `data_through_at`, `through_block` | timestamp, integer | 此快照已完整掃描且確認的資料時間與區塊上界，不等同伺服器現在時間。 |
| `total_participants` | integer | 快照內有效參與者總數，合併已驗證錢包後計算，沒有 Top 300 限制。 |
| `total_matches` | integer | 本次查詢篩選後的總筆數，尚未分頁。 |
| `status` | string | `initializing` 初次掃描；`live` 進行中；`settling` 等待結束邊界確認；`final` 完整結算。 |
| `counting_rule`, `ranking_rule` | string | 固定回傳實際付費成功抽卡與同分跳號的規則識別碼。 |
| `contracts`, `chain_id`, `token_decimals` | string[], integer, integer | 本活動明確納入的合約、鏈與付款代幣小數位數；範例為 BSC、USDT 18 位。 |
| `stale`, `stale_reason`, `last_error` | boolean, string/null, string/null | 目前來源是否更新逾期或失敗及原因；保留最後成功快照，不以零筆掩蓋錯誤。 |
| `progress` | object/null | 目前同步狀態與各來源的 `next_block`／`complete`；屬即時健康狀態，不是凍結的計分資料。 |
| `update_interval_seconds` | integer | 更新間隔；預設 600 秒，活動完成後停止向鏈上掃描。 |
| `stale_after_seconds` | integer | 更新逾期門檻，預設 1200 秒；已 final 不因時間經過變成逾期。 |
| `sort`, `order` | string | 本次顯示排序欄位與方向，不改變全活動名次。 |
| `next_cursor` | string/null | 下一頁游標，綁定快照、排序與搜尋詞；null 代表最後一頁。 |
| `rows` | object[] | 本頁或 `all=true` 的完整資料；初次掃描未完成時不回傳虛構的空排行榜。 |

## Search with global rank

```http
GET /v1/leaderboards/2026-11-20/search?q=<wallet_or_name>&snapshot_id=<snapshot_id>
```

`q` is a case-insensitive substring (Unicode casefold for usernames), 1–200 characters. `snapshot_id` is required for search, or supplied by its continuation cursor. Search uses frozen usernames and wallet aliases; `rank` always refers to the **whole snapshot**, while `total_matches` refers to this search. Search supports the same sort/order/limit/all/cursor parameters. URL-encode query values.

## Progress and failures

| HTTP status | Meaning |
| --- | --- |
| 200 | Complete published snapshot, possibly stale after a later scanner failure |
| 202 | Initial scan incomplete; `snapshot_id:null`, `status:initializing`, progress and no fabricated `rows` |
| 400 | Invalid parameters, malformed cursor, cursor/snapshot/query mismatch, or snapshot belonging to another event |
| 404 | Unknown event or route |
| 409 | Selected snapshot lacks complete builder evidence; refresh after the versioned rescan |
| 503 | Initial scan failed before any successful snapshot; error/progress and no fabricated `rows` |

The production update interval defaults to 600 seconds (10 minutes), configured with `EVENT_LEADERBOARD_SYNC_INTERVAL_SECONDS` (minimum 60). The overdue threshold defaults to `max(2 * interval, interval + 120)`, or 1200 seconds at the default interval; `EVENT_LEADERBOARD_STALE_AFTER_SECONDS` can explicitly set a positive threshold. These values are returned in every response as `update_interval_seconds` and `stale_after_seconds`.

The scheduler waits that interval after a synchronization finishes; scan time and
chain confirmations add latency. Use `generated_at` and `data_through_at` for the
actual publication and coverage times. Client reads use SQLite snapshots and do
not trigger per-wallet chain scans.

Failures preserve the last successful snapshot. A new config revision intentionally has no current snapshot until its own scan completes; explicit IDs can still read old revisions. Error state never converts an incomplete scan to an empty final result.

## Configuration and recovery

`EVENT_LEADERBOARD_DB_PATH` defaults to `/data/events/leaderboards.sqlite3`. SQLite WAL mode holds definitions, current revisions, checkout ledger, per-source checkpoints with boundary block hashes, verified migration evidence, immutable snapshots, and progress/errors. This durable file is the recovery source. Back up SQLite with its supported online backup API, not by copying only the main file while WAL writes are active.

`EVENT_LEADERBOARD_CONFIG_PATH` optionally points to an explicit JSON file; the conventional optional file is `/data/events/events.json`:

```json
{"events":[{"event_id":"2026-11-20","start_at":"2026-11-20T00:00:00+08:00","end_at":"2026-11-21T00:00:00+08:00","chain_id":56,"token_decimals":18,"contracts":["0xd4d18607d6111c5fa2f93a4a5b2c0e28f1563f9f"]}]}
```

Without a `contracts` key, the current explicit Renaiss pack contract allowlist in `runtime/pack_contracts.py` is used. Contract selection changes the counted population; set the intended allowlist before launching an activity.

Applying a different start/end/contract/chain/decimal definition creates a new revision and config hash. Old ledger/checkpoints are never reused, and old snapshots are retained. Event ID re-use does not silently continue an incompatible scan. The scanner passes `expected_revision` into commits/publications so a concurrent configuration change cannot contaminate a new revision.

A completed chunk commits all verified checkout ledger entries, migrations and checkpoint atomically; validation failure rolls back the whole chunk. Checkout identity is `(event_id,revision,chain_id,tx_hash,log_index)`. Identical duplicates are idempotent, inconsistent duplicates halt visibly. Publication verifies coverage using the same audited chain/contract/schema/topic source keys as the scanner and writes the metadata and every participant row in one transaction. Incomplete, unanchored, or lagging sources prevent publication. Reorg boundary checks are required before advancing a stored checkpoint; a mismatch preserves prior published results and marks the event stale for investigation.

## Verification

Run `python3 -m unittest tests.test_event_leaderboard_store -v`. Tests cover more than 300 wallets, immutable pagination across new publications, global tie ranks, verified multi-hop aliases, Unicode search, very large exact amounts, atomic rollback, revision isolation/recovery, invalid cursor and sort handling, failure preservation, publication coverage guards, overdue detection and pinned-snapshot freshness. Test SQLite files are isolated under `.scratch/event-store-tests/run-<id>/` and should be listed before cleanup.
