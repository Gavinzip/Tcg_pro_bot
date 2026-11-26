# V3 與備份協調整合發布驗證（2026-11-26）

## 範圍與來源

沿用 `codex/scanner-boundary-root-cause`，既有 PR #11 的備份協調提交 `7bd7a8f`，整合時 main `f8c4990`。
原演算法 checkout 保持唯讀，來源 manifest 的全部雜湊未變；沒有上傳其原本的 audit 腳本或原始資料。

保留：64 confirmations、指定 builder、USDT 與活動日期／計票、交易雜湊＋事件序號去重、完整五分鐘回掃、原有有限重試與共用 4 RPS。
V3 以已稽核付款實作與完整 CheckoutSuccess logs 代替逐交易 receipt 及中間區塊查詢；區間端點與游標仍查獨立 canonical header。
中間區塊 hash／時間採信 provider logs，並非密碼學 inclusion proof；不能將此描述成原模式完全相同的獨立證據。
未知版本、缺欄位、矛盾證據或區間升級都停止；沒有 receipt/provider fallback。

## 真實接口發現及修正

Etherscan V2 對近期 confirmed block 的 storage/code 查詢成功；較舊區塊會回 RPC -32000「historical state unavailable」。
依 Gavin 明確授權，版本 guard 改為：在本輪已確認終點讀 EIP-1967 implementation slot 與實作程式碼 hash，再完整讀回掃／掃描起點至終點的 Upgraded logs。任一升級（包括換走又換回）都停止；不查舊起點 storage。
補封閉錯誤分類 `provider_historical_state_unavailable`，保留安全指紋／action／區塊，不記錄原文、key 或 URL，亦不做泛用重試。

## 驗證結果

- 243 項 scanner／API／備份協調測試通過，0 skip；同一份備份程式另有 4 項真實 restic 加密備份／還原測試通過。
- 100 筆真實保存交易重播：V3 11 次 API；1,000 筆合成壓測：13 次。所有模式的明細、金額、抽數、排名 checksum 與游標一致；這些是邏輯呼叫數，不是網路速度。
- 真實接口另外取回 236 筆指定 builder 事件，欄位完整，區間升級 0；抽樣 3 筆與 receipt 付款明細及獨立 canonical hash／時間全數一致，樣本共 9 次成功請求。
- 使用專用 key、真實 Etherscan、隔離 SQLite 跑完整 V3，而非只測零散接口；測試刻意限 1 RPS，沒有寫正式資料。
  - 第 1 輪：165 筆累計交易／189 抽，12 次實際請求，13.511 秒，重試 0，receipt 0。
  - 第 2 輪：173 筆累計交易／198 抽，11 次實際請求，11.045 秒，重試 0，receipt 0。
- 第二輪包含五分鐘回掃及新交易，沒有重複累計；metadata 保留 audited_v3_event 與 guard 範圍。
- PR #11 的掃描優先協調保留：備份在 bounded I/O／SQLite page 邊界讓出，upload process group 暫停後再釋放鎖；仍完整加密備份與還原驗證。不得宣稱已完成自然正式備份重疊驗證。

## 正式啟用與驗收

需在既有 scanner 設定 `EVENT_LEADERBOARD_V3_EVENT_PROOF=1` 與 `EVENT_LEADERBOARD_COMBINE_LOG_QUERIES=1`，其餘 key／RPS／日期／計票不變。
精確 PR head CI 通過後，依既有授權合併 main，等待 GitHub 自動部署；核對 merge/main/deployment SHA 與正式 domain。
至少觀察 6 個完整自然輪次，記錄新交易數、各 action 真實請求數、receipt=0、速度、stale 與快照推進。
網站／Partner API 核對同 snapshot、分頁／排序／搜尋、no-store 與真實排行榜畫面。
本文件的正式驗收記錄另以部署後證據為準，不把本機結果當正式數字。
