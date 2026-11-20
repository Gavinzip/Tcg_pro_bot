# TCG Pro 活動排行榜 API 交接文件

文件日期：2026-11-20。適用於「有限期間、實際付費成功抽卡次數」排行榜。

## 交付內容與環境

- 本文件：呼叫方式、參數、欄位、計算、更新與 snapshot 規則。
- [OpenAPI 規格](EVENT_LEADERBOARD_OPENAPI.yaml)：供工程師匯入支援 OpenAPI 3.1 的工具。
- [完整技術說明](EVENT_LEADERBOARD_API.md)：補充同步、版本與復原機制。
- 目前 Base URL：`https://tcg-api.up.railway.app`。
- `Authorization: Bearer <TCG_LEADERBOARD_API_TOKEN>`。實際金鑰另以安全管道交付，不放入本文件、Git、前端程式或網址。
- 這是網站後端使用的服務憑證，可讀取已配置活動中的參與者，並非個別玩家的登入憑證。

2026-11-20 已完成兩網站獨立憑證的正式部署與驗證：兩組各自可讀排行榜／搜尋，不能讀 Profile／BBW。server-only 變數為 `TCG_LEADERBOARD_API_TOKEN`，Header 格式不變，詳見 [網站 token 分工與部署](API_CLIENT_TOKENS.md)。原先分享的舊項目仍屬內部共用憑證，合作方應使用新的專用項目。

新增的 [builder 平台來源篩選](EVENT_LEADERBOARD_BUILDER.md) 為後續本機變更，尚未部署。正式 b02f13f 不具備此篩選，接入方須核對回顯與支援旗標。

範例活動 `event_id=2026-11-20`：台北時間 2026-11-20 00:00（含）至 2026-11-21 00:00（不含）。活動 ID 必須由後端預先設定，不能任填日期，也不能在查詢時帶 start/end 自行改期間。

12 月活動的開始日已討論為 2026-12-01，但此交接版本尚未新增該活動，結束時間待定。票券權重、指定卡加成、跨平台與 X 任務加成尚未納入此 API；不可將 `draw_count` 當成活動票券。

## 兩個接口

| Method | Path | 功能 |
| --- | --- | --- |
| GET | `/v1/leaderboards/{event_id}` | 讀取完整排行榜或分頁；支援排序。 |
| GET | `/v1/leaderboards/{event_id}/search` | 搜尋錢包地址、已驗證歷史錢包別名或名稱；保留全榜名次。 |

所有符合活動條件的錢包都納入，沒有 Top 100／300 人數限制；無名稱者也納入。對方若一次取全量，只需串第一個接口的 `all=true`；若採分頁並提供玩家查詢，則串兩個接口。

| name | 位置／型別 | 必填與預設 | description |
| --- | --- | --- | --- |
| `event_id` | path / string | 必填 | 已設定的活動識別碼。 |
| `builder` | query / string | 可省略 | 新版平台來源篩選：0x 加 64 hex，下一頁可由 cursor 攜帶；尚待正式部署。 |
| `snapshot_id` | query / string | 榜單可省略；搜尋首次必填 | 榜單省略時取目前設定版本最新成功快照；搜尋續頁可由 cursor 攜帶。 |
| `q` | query / string | 搜尋每次必填 | 去除首尾空白後 1–200 字；地址／別名／名稱不分大小寫子字串搜尋。搜尋翻頁仍須帶同一 q。 |
| `limit` | query / integer | 預設 100 | 每頁 1–1000 人。 |
| `sort` | query / string | 預設 `rank` | `rank`、`draw_count`、`wallet`、`username`、`paid_amount`。 |
| `order` | query / string | 預設 `asc` | `asc` 升序；`desc` 降序。抽數由多至少使用 draw_count + desc；名次由前至後使用 rank + asc。 |
| `all` | query / boolean | 預設 false | true 一次回傳全部符合資料；忽略分頁筆數，不能與 cursor 同用。 |
| `cursor` | query / string | 可省略 | 使用回傳的 next_cursor；不要自行組裝。保留相同 sort/order，搜尋也保留 q；若另帶 snapshot_id 必須相符。 |

query 值須 URL encode。相同 query 參數不可重複傳入。

## 呼叫範例

以下只使用占位金鑰與快照 ID，並非實際回應資料。

```http
GET https://tcg-api.up.railway.app/v1/leaderboards/2026-11-20?all=true&sort=rank&order=asc
Authorization: Bearer <TCG_LEADERBOARD_API_TOKEN>
```

```http
GET https://tcg-api.up.railway.app/v1/leaderboards/2026-11-20?limit=100&sort=rank&order=asc
Authorization: Bearer <TCG_LEADERBOARD_API_TOKEN>
```

```http
GET https://tcg-api.up.railway.app/v1/leaderboards/2026-11-20?limit=100&sort=rank&order=asc&cursor=<next_cursor>
Authorization: Bearer <TCG_LEADERBOARD_API_TOKEN>
```

```http
GET https://tcg-api.up.railway.app/v1/leaderboards/2026-11-20/search?snapshot_id=<榜單回傳的snapshot_id>&q=<錢包地址或名稱>&sort=rank&order=asc&limit=100
Authorization: Bearer <TCG_LEADERBOARD_API_TOKEN>
```

## 欄位 name、description 與計算方式

以下為成功快照的欄位；初次未完成的回應不同，見狀態碼說明。時間均為 UTC ISO 8601，前端可轉台北時間顯示。

| name | type | description／計算方式 |
| --- | --- | --- |
| `event_id` | string | 有限期間活動識別碼。 |
| `builder` | string/null | 本次平台來源；null 為全活動。 |
| `builder_filter_supported` | boolean | 此快照是否具備完整 builder 證據；接入方啟用篩選前必須核對。 |
| `revision` | integer | 活動設定版本；時間、合約、鏈、小數位或 schema 等計分來源設定改變時建立新版本。 |
| `config_hash` | string | 該版本有效活動設定的 SHA-256 雜湊。 |
| `start_at` | timestamp | 計分起點，包含此時間。 |
| `end_at` | timestamp | 計分終點，不包含此時間。 |
| `snapshot_id` | string | 此次不可變快照 ID。 |
| `generated_at` | timestamp | 此快照發布時間。 |
| `data_through_at` | timestamp | 此快照完整掃描且確認的資料時間上界。 |
| `through_block` | integer | 完整掃描且確認的區塊上界。 |
| `total_participants` | integer | 合併已驗證舊、新錢包後的有效參與者總數。 |
| `status` | string | live 活動進行中；settling 已過結束時間、等待完整確認；final 已完整結算。 |
| `counting_rule` | string | 固定 confirmed_paid_checkout：已確認、成功抽卡並核對實際付款。 |
| `ranking_rule` | string | 固定 competition_draw_count_desc_wallet_asc：抽數降序競賽名次，錢包升序穩定顯示同分。 |
| `contracts` | string[] | 此活動明確納入的合約清單。 |
| `chain_id` | integer | 鏈 ID，現有範例為 BSC 56。 |
| `token_decimals` | integer | 付款代幣小數位，現有範例為 USDT 18。 |
| `stale` | boolean | 目前來源是否失敗、缺少目前版本快照或更新逾期；不改寫已發布成績。 |
| `stale_reason` | string/null | stale 原因；沒有則 null。 |
| `last_error` | string/null | 最近同步錯誤；單純逾期時可為 null。 |
| `progress` | object/null | 同步進度，各來源可含 next_block、complete；屬即時健康資訊。 |
| `update_interval_seconds` | integer | 同步完成後等待秒數，預設 600。 |
| `stale_after_seconds` | integer | 非 final 活動更新逾期門檻，預設 1200 秒。 |
| `sort` | string | 本次顯示排序欄位。 |
| `order` | string | 本次顯示排序方向。 |
| `total_matches` | integer | 本次查詢符合筆數，尚未分頁；無搜尋時等於參與者總數。 |
| `next_cursor` | string/null | 下一頁游標；null 代表沒有下一頁。 |
| `rows` | object[] | 本頁參與者；all=true 時為全部符合者。 |
| `rows[].rank` | integer | 依有效付費抽數由多至少的全榜競賽名次；同抽數同名次，下一名跳號，例如 1、1、3。搜尋不重算名次。 |
| `rows[].wallet` | string | 標準化小寫錢包；僅依已驗證搬遷證據合併。 |
| `rows[].username` | string/null | 發布快照時已知的名稱；沒有名稱為 null，仍列入榜。 |
| `rows[].aliases` | string[] | 已驗證錢包別名，包含標準錢包，可供搜尋。 |
| `rows[].draw_count` | integer | 活動期間有效 checkout 的實際抽卡數量總和；批次多抽依成功數量計算；免費、贈送、失敗不算。 |
| `rows[].paid_amount_raw` | decimal string | 有效抽卡付款最小單位整數總和；以字串保留完整精度。 |
| `rows[].paid_amount` | decimal string | paid_amount_raw / 10^token_decimals 的精確十進位字串。 |

付款金額僅供顯示，不用來打破抽數同分。不可把金額字串轉成 JavaScript Number／浮點數。切換 sort/order 只改顯示順序，不會重算 rank；各欄位同值時依 wallet 升序穩定顯示。

## 更新與 snapshot 處理

1. 預設一次同步完成後等待 600 秒再啟動下次；掃描耗時與鏈上確認會增加延遲，不保證每個整十分鐘發布。以 generated_at、data_through_at 及健康欄位顯示實際狀態。
2. 所有指定來源都完整掃描並確認後，才在同一資料庫交易內發布完整快照，避免玩家只被計入一部分。
3. 首頁取得 snapshot_id；本輪分頁與搜尋固定此 ID，或使用帶有該 ID 的 next_cursor。新快照發布不會使舊頁名次移動、重複或漏人。
4. 使用者刷新時，重新請求不帶 snapshot_id/cursor 的榜單，取得最新版本成功快照；重新開始分頁。
5. 已發布快照的 rows、名稱、別名與計分 metadata 不變。stale、stale_reason、last_error、progress 與更新間隔等健康資訊仍反映目前來源，不能假定整包回應永遠不變。
6. 同步失敗保留最後成功快照並標記 stale；沒有任何成功快照時回 202 或 503，不以空榜冒充完整結果。已 final 的活動停止鏈上掃描，仍可查詢。
7. 活動設定變更會建立新 revision，不沿用不相容進度；舊快照保留可查。目前版本首輪尚未成功時，不自動拿舊版本冒充新結果。

## 狀態碼與整合限制

| HTTP | 意義／前端處理 |
| --- | --- |
| 200 | 有完整快照；仍須檢查 stale、status 與時間。 |
| 202 | 初次同步未完成，status=initializing、snapshot_id=null，顯示準備中，不視為零人榜。 |
| 400 | 參數、游標、活動／快照或查詢組合不符。 |
| 401 | 缺少或錯誤的服務金鑰。 |
| 404 | 活動或路徑不存在。 |
| 409 | 指定快照沒有完整 builder 證據，請刷新至重掃完成的新快照。 |
| 503 | 尚無成功快照且同步失敗，顯示暫時無法取得資料。 |

排行榜查詢只讀 SQLite 快照，不會因每次查詢重新掃鏈。現行實作每次讀取選定快照後排序／篩選，分頁只限制輸出量；目前沒有榜單專用限流或大量併發承載驗證，且與 Bot 共用運算資源，不承諾無限制大量呼叫。

建議由網站自己的 server proxy 保管金鑰，限制可查活動及參數，加約 60 秒快取、限流、併發控制與搜尋 debounce。這些是接入方待實作建議，並非現有端點已具備。上游回應 Cache-Control: no-store；若 proxy 需要快取，必須明確實作，且保留逾期／錯誤資訊。

## 本次交接檢查

- 2026-11-20：現行 API health 回 200；未授權榜單請求回 401。
- 文件依 codex/tcg-drive-backup 的 d214be3 版程式核對；此交接文件為其後新增文件，不代表已部署新功能。
- 正式交接使用 https://tcg-api.up.railway.app；原長網址暫時保留，兩者連至同一 API 服務，既有接入方可持續使用。
