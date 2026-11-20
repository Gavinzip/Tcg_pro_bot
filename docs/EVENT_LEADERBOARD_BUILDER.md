# 依 builder code 查詢平台來源

2026-11-20：builder 擴充已完成本機實作與測試，尚未部署。正式 b02f13f 已支援兩網站獨立 token，但不支援 builder 篩選；不能因舊版接受未知 query，就認定資料已篩選。

## 用途與接口

同一抽卡合約可由多個平台呼叫。V3 CheckoutSuccess 的 indexed builder（topics[3]）辨識平台來源，packId 辨識卡機，實際付款另外核對。兩個既有 GET 端點新增可選 builder query：0x 加 64 個 hex 字元，輸入可用大小寫，回應統一小寫。

```http
GET /v1/leaderboards/2026-11-20?builder=0x2e622bb0dfd32f36aefb84dc4eb14ced42afa80a84f3a4cb31b21488f2a61501&all=true&sort=rank&order=asc
Authorization: Bearer <該網站的token>
```

```http
GET /v1/leaderboards/2026-11-20/search?builder=0x2e622bb0dfd32f36aefb84dc4eb14ced42afa80a84f3a4cb31b21488f2a61501&snapshot_id=<榜單回傳ID>&q=<錢包或名稱>
Authorization: Bearer <該網站的token>
```

不帶 builder 維持全活動榜。帶 builder 時，先篩選已核對的 checkout，再合併錢包並計算抽數、金額及名次。例如同一錢包在 A 平台抽 1 次、B 平台抽 9 次，查 A 只算 1 次。

搜尋的 rank 是所選平台全榜名次；total_participants 是該平台總人數，total_matches 是同平台搜尋後尚未分頁的筆數。舊合約沒有 builder 的紀錄不歸給任一平台，但仍可進全榜。真正全零的 bytes32 builder 是有效 code，不能用來表示資料缺失。

## 回應、快照與游標

| name | type | description |
| --- | --- | --- |
| builder | string/null | 本次平台來源；null 是全活動。 |
| builder_filter_supported | boolean | 此快照是否具備完整 builder 證據。 |

一次發布在同一 SQLite 交易中保存全榜及各 builder 的不可變排行榜，名稱與錢包別名一起凍結。snapshot_id 可有多個 builder 視圖，**snapshot_id＋builder** 共同決定固定結果，發布時間與確認區塊上界相同。

next_cursor 綁定 builder、snapshot_id、sort、order、q。下一頁可只帶 cursor，由其恢復 builder；另帶 builder 時必須相同。搜尋翻頁仍須保留 q。舊版不含 builder 的全榜游標維持全榜，不能用來切換平台。

網站 proxy 可由 server-only TCG_LEADERBOARD_BUILDER 固定平台，瀏覽器不得覆寫；快取鍵須含 builder。接入方採用資料前，必須核對 builder_filter_supported=true 且回顯 builder 與預期相同，避免舊版忽略新 query 而回全榜。

## 舊帳本與錯誤

舊版未保存 builder，不能從聚合抽數或金額還原。checkout schema 升為 v2，建立新活動 revision/config_hash 及來源 checkpoint，只重掃該活動已設定的有限期間；不恢復永久／月／年歷史榜。

- 全部來源完整掃描並確認後，才發布支援 builder 的新快照。
- 新版本初次未完成回 202，初次失敗回 503；不回假零人榜。
- 指定沒有完整 builder 證據的歷史快照回 **409 builder_filter_unavailable**；刷新最新快照，等待重掃完成。舊快照仍可不帶 builder 查詢。
- 完整快照中沒有該 builder 的紀錄，才回 200、rows=[]、total_participants=0。
- builder 格式錯誤、重複參數或 cursor 與 builder 不符回 400。
- 後續同步失敗保留上次完整快照，並顯示 stale。

## 真實事件核對

使用者提供的[交易事件](https://bscscan.com/tx/0x6131491e3d1bce85bb3b25524a0857659478ab88824bc2c991cd1e720acb1c21#eventlog)已從 bsc-dataseed.binance.org 與 bsc-rpc.publicnode.com 讀取 receipt，成功狀態、區塊雜湊及目標 log 一致：

- 合約：0xd4d18607d6111c5fa2f93a4a5b2c0e28f1563f9f；log index：952。
- builder：0x2e622bb0dfd32f36aefb84dc4eb14ced42afa80a84f3a4cb31b21488f2a61501。
- packId：0xcf11308cc7c642554a781b039a63a542c2f20b36f904b5b6981d48a2f76a5f90。
- checkoutIds：[195673]；事件標示付款 28 USDT。

這筆交易用於核對事件格式，不表示它位於 11/20 範例計分期間，也不據此把 builder 命名為某平台或認定專屬 88 U 資格。正式抽數仍核對成功 receipt、同交易實際 USDT 付款及確認區塊。

builder 只解決平台歸屬，不會自動產生 1／2／5／8 票券、指定卡或任務加成；仍需已定義的卡機／渠道及計分規則。
