2026-11-26 整合版已調整為終點版本 guard；本文件保留歷史候選量測。最新結果見[整合發布報告](v3-backup-release-validation-2026-11-26.md)。

# V3 批次事件區塊資料：本機修改與驗證

使用者明確授權拿掉逐區塊額外查詢、保留端點等檢查，並要求只做本機 100／1000 測試、不 push／部署。
沿用 `/Users/gavin/.codex/worktrees/bnb-api-scan-cadence/tcg_pro_bot`、`codex/api-scan-cadence`，
HEAD 仍為 `efd7b6a6c694338f13a7ce1d5be05c631095b4f2`，含上一輪尚未提交的修改；未新開 PR。

## 需要的資料本來就在事件回覆

先前有硬上限的真實捕捉檔中，共 383 筆不同事件：383／383 均有
`timeStamp`、`blockNumber`、`blockHash`、`transactionHash`、`logIndex`、`topics`、`data`。
其中 327 筆有相同區塊的獨立保存 header 可比對，hash／timestamp 差異為 0。
剩下的事件沒有在這份檔案保存對應 header，沒有把未比對部分寫成已比對。

user／pack／builder 由 indexed topics 解出，USDT 幣種、checkoutIds 張數與 totalAmount 由 data 解出。
這些既有欄位及篩選不變；合約版本固定與付款事件語義見
[第一階段報告](scan-v3-event-proof-2026-11-25.md)。
官方 [getLogs 格式](https://docs.etherscan.io/api-reference/endpoint/getlogs) 亦含 blockHash／timeStamp。

## 最新行為與信任範圍

啟用既有 `--v3-event-proof --combine-log-queries` 時，指定 V3 使用：

- 來源仍為同一 Etherscan provider；64 confirmations、builder／USDT／正金額篩選、活動窗口、五分鐘回掃及去重不變。
- 新增 `event_blocks.py` 負責讀取事件的時間／hash，核對每個完整邏輯查詢範圍的開始與結束 header。
- 同區塊多筆事件必須具有相同 hash／時間，時間隨區塊高度不得倒退，並須落在端點時間內。
- 位於端點區塊的事件，須與實際端點 header 完全相符；同一筆交易不能出現在不同區塊。
- 既有游標仍在每輪以 canonical header 重新確認。區塊資料快取只在本輪使用，不拿舊快取冒充新查核。
- 保存 cursor 的 hash 改從已檢查的事件區塊資料取得；整區塊驗證完成才提交。下次恢復時仍重新讀 canonical cursor header。
- 掃描區間結尾（包括沒有事件的結尾）仍以實際 header 建立最終 cursor／snapshot 水位。
- 已知回掃事件也核對 timeStamp 是否與 ledger 一致；不默默沿用被上游改動的時間。
- 缺欄位／格式錯／矛盾／版本或升級不符，明確停止；不讀 receipt 補洞、不切 provider、不補零時間。
- 不再啟動 V3 receipt 或逐事件區塊 header workers。舊合約仍保留原本有界 receipt workers。

這不是與逐區塊核對等價的證據：中間區塊的 hash／時間直接信任供應商事件回覆。
端點與一致性檢查不能證明每個中間事件在主鏈上的 inclusion；一個格式正確、順序合理的錯誤中間 hash 可能不被發現。
測試有明確覆蓋此接受的邊界，沒有把「多做端點檢查」描述成仍有逐區塊獨立驗證。
snapshot 的 `scan_verification.block_evidence` 明示 `provider_logs_with_range_anchors` 並保存範圍／canonical anchor blocks。
這份記錄與舊 snapshot 一起凍結，不改寫原始 ledger 的付款／張數明細。

## A/B 本機比較

執行正常 scanner、SQLite commit、snapshot 及排名流程；沒有 HTTP 網路等待。

- 原版：直接載入 git HEAD 的 scanner。
- 上一輪本機版：修改前先保存 scanner、V3 guard、header prefetch 完整原始碼，再以隔離 module 載入；不是用公式冒充執行結果。
- 最新版：目前未提交程式，開啟上面兩個旗標。

| 新交易數 | 原版 | 上一輪省 receipt＋保留 header | 最新版 |
| --- | ---: | ---: | ---: |
| 100（真實紀錄重播、97 個不同新區塊） | 205 | 108 | 12 |
| 1000（合成負載、970 個不同新區塊） | 1980 | 983 | 14 |

表為程式實際走到的邏輯 API 呼叫數，不是本輪實際發出的付費請求。
1000 筆不是 1000 筆完整真實鏈上交易重播；使用保存的 100 筆真實形狀擴成十倍、明確產生合成識別值／區塊／時間。
新增歷史 storage／upgrade 回覆仍是本機測試輸入；runtime bytecode 沿用上一輪保存的真實公開資料。

兩種負載的最新版 action 數：

| Action | 100 | 1000 |
| --- | ---: | ---: |
| eth_blockNumber | 1 | 1 |
| eth_getBlockByNumber | 5 | 5 |
| getblocknobytime | 1 | 1 |
| eth_getStorageAt | 2 | 2 |
| eth_getCode | 1 | 1 |
| getLogs（含不帶 builder 的 upgrade 查核） | 2 | 4 |
| eth_getTransactionReceipt | 0 | 0 |

1000 的事件清單滿頁，仍完整拆分區間，沒有截斷或跳過結果。
沒有合併 logs 的單獨 V3 變體會新增一個原捕捉檔沒有保存的起點 header，首次探索執行明確報 `offline_missing_captured_header`。
它不屬本次要求的合併版比較，最終比較沒有捏造該 header 或切到其他來源；失敗紀錄亦保留。
只合併 logs 的對照也已執行，為 204／1979 次，證明主要減量來自取消逐筆 receipt 及逐事件區塊讀取。

四個最終變體的完整 checkout evidence checksum、ranking checksum、checkout_count、draws、checkpoint 全部一致。
這是來源計票結果一致；沒有把它說成完整正式網站／票券發布驗收。

最後樣本的新版本機 wall time：100 筆 0.608 秒、1000 筆 6.295 秒；CPU 為 0.225／2.009 秒。
它包含 SQLite／計票，不含網路；主要剩餘本機成本包含逐區塊原子提交。
4 RPS 的純配速量為 12/4=3 秒、14/4=3.5 秒，不能當成整輪實際耗時，更不能忽略 SQLite、回應等待、重試／冷卻及完成後 60 秒。

## 驗證結果與限制

本機完整回歸 **201 項通過、0 failure/error**（Python 3.9.6，含 builder／幣種／零付款排除與確認數的新模式回歸），`git diff --check` 通過。
已覆蓋缺時間／壞格式、同區塊 hash／時間矛盾、時間倒退、同秒多區塊、端點不符、游標重組、未確認範圍排除、滿頁拆分、已知事件改動、版本／code／upgrade 不符、錯誤時不發布及完整前綴續掃、同區塊不部分提交、舊 snapshot 凍結、舊合約 receipt 不被繞過。
兩個新測試模組已加入既有 workflow，但未 push，所以本次沒有新版遠端 CI。

這不構成零 bug 保證。尚未真實打 Etherscan 新增的歷史 storage／code 路徑，亦尚無新版 100／1000 真實網路測速。
本輪付費鏈 API、官方公開鏈 RPC 與正式寫入皆為 **0**。
既有新模式旗標仍預設關閉；本機驗證未改正式 RPS、key、builder、日期、資料或任何部署。

完整報告／測試 log／修改前基準碼／清理 manifest 保存在 ignored `.scratch/v3-log-blocks-20261125/`。
原始真實 captures、source ledger、未追蹤 `scripts/audit_event_scan.py` 均以處理前 SHA-256 核對不變。
