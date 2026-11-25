# 4 RPS 共享保護與正確錯誤分類

基底 `5b6499bee48d4e4d00568d7b3e5ad07028238aea`，分支 `codex/api-scan-cadence`。本文記錄補修與本機驗證，不代表正式發布驗收；發布依 PR 合併到 main、自動部署及正式多輪核對的順序執行。

## 目標與明確限制

使用者希望保留 4 次 API 查詢／秒的上限：可恢復的暫時失敗先有限恢復，持續失敗才停止。4 是所有 worker 與重試合計的上限，不是每個 worker 各 4。預設設定仍為 2，實際正式環境變更由發布協調聊天負責；這份程式不自行修改 Railway。

這不是上游永遠不會拒絕的保證。正式先前兩次 `receipt_unavailable` 沒有保存錯誤 payload，尚不能確定是限流、額度或其他回應。5/s 的帳戶與專用 key 直接對應仍未完成；不能以本機測試取代實際 4 RPS 的多輪驗收。

## 程式缺口與分類

舊 proxy 路徑跳過 Etherscan JSON `status` 判斷，因此 HTTP 成功但 `status=0` 的拒絕字串會先被計為 request 成功，再於 receipt 不是 dict 時失敗。補修在受量測的請求內驗證 envelope 及 receipt 型別，分開記錄：

- 明確的每秒限流拒絕：`provider_rate_limit`。
- 明確的每日額度拒絕：`provider_quota_exhausted`。
- 其他拒絕／未知非物件 receipt：維持明確失敗，不猜測為限流。
- 標準 JSON-RPC null：維持 `provider_missing_result`；正常 dict／head hex 結果照常接受。

只在記憶體內判讀固定訊息特徵，輸出封閉錯誤碼，不輸出原文、URL、key 或完整回應。[官方錯誤封裝](https://docs.etherscan.io/common-error-messages)

## 明確限流時的恢復規則

1. 僅對原本白名單內的冪等讀取，且安全分類確定為 `provider_rate_limit` 時，啟用本次新規則。
2. 所有 worker 共用 5 秒冷卻期限，完全相同的查詢最多補讀一次。冷卻後仍使用同一 4 RPS gate，不會同時釋放一批累積請求。
3. 正在 gate 等待的 worker 醒來後會重查期限；另一個 worker 可在它等待時延長冷卻，避免只有出錯 thread 暫停。
4. 同一查詢再次被限流，或合計 attempt 預算已耗盡，停止該 provider 的後續網路發送。排隊的工作在本機停止，不偽算成實際 API attempt；已送出的有限 I/O 仍須依原 timeout 結束。
5. 與既有 timeout／stale-socket 恢復合計仍最多 3 attempts。每日額度、認證、付款／canonical 錯誤、未知 receipt 形狀及 HTTP 429 不因本次規則增加重試。
6. 冷卻時間由共用 gate 實際執行，計入 `pacing_wait_ms`；`retries` 計數保留，不另外把同一段冷卻重算成 `retry_wait_ms`。

持續失敗時不發布不完整快照，保存完整區塊前綴。停止的是本輪 provider；背景工作仍按既有設定於輪次結束後等 60 秒，再以新程序正常續掃。沒有切換來源、降成另一個 RPS 或使用舊榜冒充 fresh 的 fallback。

## Builder、重讀與可省查詢

正式回應的 `indexed_builder` 與來源鍵 `:builder:` 已由正式環境負責聊天核對，只有指定 builder 的單一 V3 ongoing 來源。舊全 builder 活動已 finalized，每輪 0 次鏈 API 呼叫。

一輪實際 104 次為 getLogs 2、receipt 52、區塊 48、head／time hint 2。回掃 133 筆既有 checkout，而 receipt 只有新增的 52 次；沒有把已驗證舊交易再逐筆讀 receipt。同交易及同區塊也已有本輪去重。

合併相鄰的回掃／新增 getLogs 是可能節省少量請求的候選。既有冷掃擷取中，相鄰兩段同 filter 的回應聯集為 209 筆，少於 1,000；但這僅是離線回應分析，沒有證明合併後上游一定接受，也未驗證較大區間的分頁成本。本次不改這個流程，更不省略付款／canonical 驗證來湊速度。

## 本機驗證

**174 項測試通過，0 fail、0 skip。新增付費鏈 API 呼叫為 0。**

新增案例先紅後綠，包含明確拒絕分類、冷卻後相同 query 成功、多 workers 共用冷卻、等待中期限延長、持續拒絕後不再發送、非白名單不重試、與 timeout 共用總 attempt 上限，以及正常掃描的暫時失敗成功／持續失敗保存前綴並於下一輪續掃。審查後補強的真實執行緒測試，先以 event 證明一個 peer 在 gate 內等待、另兩個 peer 非阻塞取鎖已失敗，才放出持續拒絕；確認它們全數零新增 API attempt。另一個三執行緒案例在等待中由兩個真實請求先後延長共享期限，證明原期限到達仍不會提早發送。既有 builder、付款、重組、去重與 4 RPS pacing 回歸亦全部通過。

證據留在忽略目錄 `.scratch/safe-four-rps-20261125/`，包括 `red.log`、`green.log`、`safety-final.log` 與 `full-regression.log`。舊 ledger、audit 與其他取樣均保留。
