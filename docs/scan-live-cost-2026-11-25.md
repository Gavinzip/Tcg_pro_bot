# BSC 整輪成本與受控 I/O 實測（2026-11-25）

這是本機真實網路量測，不是 production 驗收或 SLA。正常 provider、完整付款 verifier、SQLite、300 秒補掃、64 confirmations 與 snapshot 路徑均保留。沒有改日期、票券規則、去重或掃描完成後等待 60 秒的排程。

## 已確認的瓶頸

先前 300 次硬上限取樣耗時 286.4578 秒，只保存了 154 筆已驗證付款事件，首輪未完成。receipt／canonical block 階段約 274.185 秒、定位約 5.245 秒、commit 約 1.236 秒。300 次實際請求成功，最後額外 1 次 attempt 是本機 budget 擋下，沒有送出。不能把這次未完成結果當成正式榜。

每筆新的交易仍需要完整 receipt 及 canonical block；同一輪相同區塊原本就有 cache。新增 single-flight 避免並行時重複讀同區塊，並沒有省略區塊驗證。

## 固定交易量比較

每組先用真實區塊錨點建立空的已完成前綴，接著執行完整增量輪次，再執行一次無新交易的穩態輪次。所有 log、receipt、block 都從真實 Etherscan V2 讀取。唯一的測試限制是把實際已確認 head 截在選定歷史區塊，避免追著現在的 head 移動；沒有假 API 回應。準備空前綴另計每組 1 次 block 請求。

固定 2 RPS；各欄為完整輪次秒數：

| 新交易 | 原 urllib 串行 | keep-alive 串行 | keep-alive＋3 workers | 請求數／輪 |
| --- | ---: | ---: | ---: | ---: |
| 0 | 8.25944 | 3.24461 | 3.25245 | 7 |
| 1 | 5.69392 | 3.23749 | 3.92159 | 7 |
| 5 | 12.85739 | 6.88550 | 6.77006 | 14 |
| 10 | 20.90729 | 11.81341 | 11.82604 | 24 |
| 20 | 38.19596 | 30.47259 | 21.88583 | 44 |

無新交易的穩態輪次：原版 5.72046–6.08564 秒；pool 串行 3.01934–6.31057 秒；pool＋3 workers 3.00587–4.10828 秒，各 6–7 次請求，不重新讀既有 receipt。各組付款證據 checksum 完全相同。每種設定只量一次，網路延遲有波動，不能把欄間差異當成獨立、可重複的因果估計。5／10 筆的額外並行收益很小；連線重用已占大部分改善。

真實回應的離線重播只拆出 CPU／SQLite／snapshot 成本，沒有網路與限速等待：0／1／5／10／20 筆完整新增輪次分別為 0.01947／0.01593／0.02434／0.04424／0.07387 秒。它不是 live 掃描成績，也不是模擬網路的時間模型。

## 發布開關與不變條件

- `EVENT_LEADERBOARD_HTTP_POOL=1` 啟用最多保留 8 條 idle connection 的標準庫 keep-alive pool；未設定時保留目前 urllib。TLS 仍驗證憑證；不跟隨轉址、不做隱藏重試、不切換 provider 或資料來源。錯誤仍明確失敗，沒有新增 fallback。
- `EVENT_LEADERBOARD_VERIFICATION_WORKERS=3` 或 `--verification-workers 3` 啟用 3 個 receipt 讀取執行緒；預設仍 1，合法範圍 1–8。最多預取 2×workers 個未消費交易；同交易去重。
- 上述開關不會更改 RPS。所有 thread 仍共用同一 pacing gate；正式 `EVENT_LEADERBOARD_REQUESTS_PER_SECOND=0.5` 保留。2 RPS 僅為本機測試值，不能把 2 RPS 成績直接用於正式站承諾。
- 結果按原始區塊順序消費，由主執行緒完成整區塊 SQLite 提交。較晚區塊先失敗不會取消較早完整前綴；同一區塊任一交易失敗就不提交該區塊。取消時只等待已在執行的有限 I/O，已完成區塊可續掃。
- 每個 worker 用獨立 ContextVar 診斷 scope，完成後在 lock 內合併至原輪次。並行時各 receipt elapsed_ms 相加可能大於 wall time；請看整輪 elapsed，勿相加當整輪時間。

## 配額、證據與限制

本機測試經使用者指定改讀 1Password `API Keys / BSCSCAN_API_KEY`，未更改正式環境 key。該 key 查詢時已用 398、可用 99,602、daily limit 100,000；活動專用 key 當時已用 6,831、可用 93,169。兩個 key 值確實不同，用量也不同。未完成帳戶頁的 key 指紋對應，因此不把帳戶頁 Lite 標籤視為此 key 的已核實方案。

官方用量 endpoint：https://docs.etherscan.io/api-reference/endpoint/getapilimit 。官方目前 Free／Lite 分別列 3／5 calls/sec，均 100,000 calls/day：https://docs.etherscan.io/rate-limits 。不由每日上限相同推定方案。

第一階段新測試上限 600，完成三組 matrix 後已用 404（含 2 次 quota）；在開始冷補掃續測前已明確告知擴為本輪合計最多 1,200，終點固定台灣 14:40。這與前一輪 300 次各自記錄，不以帳戶總用量差異當作測試請求數。

完整真實原始回應與 SQLite 留在忽略目錄 `.scratch/scan-matrix-20261125/`，不進 Git。舊 `.scratch/whole-cost-live-20261125/` 完整保留。安全彙總另存 `scan-live-cost-results.json`。原先未提交的 `scripts/audit_event_scan.py` 保留原狀。

本機完整回歸 143 項全過；新增 coverage 包含 bounded prefetch、I/O overlap、同區塊 single-flight、順序提交、同區塊失敗、KeyboardInterrupt 前綴續掃、重組拒絕、ContextVar 統計與真實本機 TCP keep-alive／轉址／錯誤。新增測試已納入既有 GitHub workflow。新版 CI 與 PR review 仍需以 GitHub 當前狀態為準。

## 固定冷補掃與正式 pacing 收尾

固定至台灣 14:40 的正常掃描已完成：沿舊 154 筆已驗證前綴備份續掃，新增 326 筆，完整 snapshot 為 **480 筆付款交易／573 抽**，watermark block `135608409`。續掃為 **323.81587 秒／642 次請求**；緊接著無新交易的一輪 **3.90833 秒／7 次請求**，兩輪 evidence checksum 完全相同。這是截至固定歷史水位的本機完整活動資料，不是正式榜或現時最新 head。

準備歷史終點另花 5 次請求，該組合計 654 次；整個本輪測試至此 1,058 次（包括先前兩個 quota 查詢），低於已告知的 1,200 次上限。前一輪 300 次已用 286.4578 秒保存的 154 筆仍原樣保留；不能把不同 key／傳輸設定且跨中斷的兩段相加，當成同設定冷掃 SLA。

正式 0.5 RPS 的額外 live 對照正在既有上限內執行；上方 matrix 僅是 2 RPS。所有本機資料、程序預算與完整結果保留。143 項回歸與固定 matrix 已驗證正確性，正式環境的延遲及環境開關仍須另外驗收。
