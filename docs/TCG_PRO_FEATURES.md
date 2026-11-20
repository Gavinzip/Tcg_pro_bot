# TCG Pro 功能盤點（2026-11-20）

範圍是 TCG Pro 的程式、Discord Bot、資料 API 與必要背景工作。舊 Bot 在
Zeabur 已暫停，原服務設定／卷仍保留；新 Bot 在 Railway 運行。Fourmeme
網站仍在原本的 Vercel，這次沒有搬遷該網站專案。

以下是目前保留的功能與實際指令。指令已註冊及設定已移轉，不代表每一項都已
發送真實 Discord 操作驗收；不會為驗收擅自發送公開訊息或提交活動作品。

| 編號 | 功能 | 入口／觸發方式 |
| --- | --- | --- |
| 1 | 自動卡圖分析、報告與海報 | 指定 Discord 頻道收到圖片時；目前啟用。 |
| 2 | 手動選卡片版本、分析與海報 | `/manual_analyze` |
| 3 | 卡盒／系列 Top 10 報告與海報 | `/cardset`，輸入系列代號。 |
| 4 | 個人錢包 Profile、收藏／交易歷史／極值海報 | `/profile`，可選錢包、卡片、語言與背景；舊全站排名標記停用。 |
| 5 | 指定卡包的開包 Flex 海報 | `/flex_pack` |
| 6 | Flex 海報 Beta 版 | `/flex_pack_beta`，天堂地獄採不同背景效果。 |
| 7 | WTS／WTB 市場討論串瀏覽 | `/market` |
| 8 | 市場討論串完整重掃與索引重建 | `/market_bootstrap`，含封存討論串；不再預設推 Git。 |
| 9 | 使用者預設錢包 | `/settings` |
| 10 | Bug／新功能意見回饋 | `/ask`；這是回饋功能，不是 AI 聊天。 |
| 11 | Bot 使用次數統計 | `/bot_usage`；統計指令使用量，不是 Railway 帳單。 |
| 12 | NFT holders 同步狀態查詢 | `/sync_status`；舊 NFT 背景同步已停，可能顯示保留的舊狀態。 |
| 13 | 管理者指令同步／清理 | `!sync`、`/clear_stale_commands`。 |
| 14 | 既有即時錢包資料 API | Profile 與 TCG 內既有的 BBW 社群資格資料（SBT／實付開包金額）。 |
| 15 | 有限期間活動排行榜與搜尋 API | 活動起訖內的實際付款成功抽卡，完整錢包資料、排序／分頁、固定快照。 |
| 16 | Drive 加密資料備份 | 每日備份目前資料、設定／映射與新活動 SQLite，保留還原能力。 |
| 17 | 已驗證錢包搬遷對應表同步 | 開機恢復既有表、每日增量更新；供個人查詢與活動錢包合併。 |
| 18 | 市場索引自動更新 | 啟動與相關 Discord 市場事件觸發；不再自動 Git 備份。 |

卡圖／系列報告在使用時可能產生模型與第三方查價 API 費用；沒有新圖片／指令
時不會因為保留指令就持續呼叫模型。這些供應商費用不包含在 Railway 月費內。

## 已停用、未刪除原始資料

- 舊永久／月等固定抽卡排行榜與全量重算：`/ranking`、`/pack_rank`、
  `/ranking_rebuild`、`/pack_rank_rebuild`、`/ranking_sync_status`。
- 舊 NFT／排名背景同步與 Profile 裡依賴舊全站榜的排名標記。
- Git 資料備份、`/market_push_backup` 與市場資料自動 Git push。
- 舊 subpack／legendary 每分鐘監控；部署配置已停用失效的舊包 API 輪詢，
  個人即時查詢保留。它們的程式碼沒有刪除。

新增的活動榜是指定期間的抽卡資料，沒有恢復全歷史永久榜。

## 活動 API

基底：`https://tcg-profile-tcgpro-production.up.railway.app`

- `GET /v1/leaderboards/2026-11-20`
- `GET /v1/leaderboards/2026-11-20/search?q=<wallet_or_name>&snapshot_id=<snapshot_id>`

使用既有伺服器端 `PROFILE_API_TOKEN` Bearer 認證，不要把密鑰放在公開前端。
例子期間是台北時間 2026-11-20 00:00（含）到 2026-11-21 00:00（不含），
只算實際付款且成功的抽卡。新一期同步完整完成後才發布；前端可固定快照讀完整
資料或分頁，不會在讀取頁面時直接重掃每位使用者的鏈上歷史。

欄位與計算說明：[EVENT_LEADERBOARD_API.md](EVENT_LEADERBOARD_API.md)。
