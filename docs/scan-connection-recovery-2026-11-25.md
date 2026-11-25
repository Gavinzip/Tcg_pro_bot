# Scanner 連線恢復與 4 RPS 補修

基底為正式 `d6e40610a7e4eb27c02acdbd99f613f3278787ad`。沿用同一 checkout／分支，保留原本未提交的 `scripts/audit_event_scan.py`、真實 ledger 與所有取樣，不重新打冷補掃。

## 已知問題與證據邊界

正式 deployment `56014e4c-77a3-4236-a9fd-714298545162` 第一輪於 2026-11-25 07:40:23Z 以 `provider_connection_failure` 失敗：105 attempts、1 error、0 retries／timeouts。日誌未記底層 exception subtype，因此不能從該錯誤碼直接證明當時一定是 RemoteDisconnected。

本機真實 TCP 伺服器可確定重現缺口：一條完成過正常回應的 keep-alive 連線，在下一次查詢、送回回應標頭之前被關閉，Python 引發 RemoteDisconnected；舊分類變成 `provider_connection_failure`，舊 policy 只重試 timeout，故整輪立即失敗。新增測試先在舊碼報相同錯誤，再在補修後通過。多 idle sockets 的另一個紅燈案例也確認，只丟棄出錯 socket 並不足以保證下一次選到新連線。

## 精確的恢復範圍

- transport 本身不暗中重試。只有重用的既有 socket 在收到標頭前遇到 RemoteDisconnected／BrokenPipe／ConnectionReset，才分類成 `provider_stale_connection`；失敗的 socket 關閉並丟棄。
- provider 對既有白名單的冪等讀取，最多做一次這類恢復，經原本 2 秒退避及同一共享限速器，重送完全相同 query。此次重讀用 ContextVar scope 強制新建 socket，不會改拿另一條 idle 連線，也不影響其他 thread 的 scope。
- 與既有 timeout policy 合計仍最多 3 attempts；不為任何一般 connection_failure 開放泛用 retry。全新連線失敗、已收到標頭後的半截回應、畸形 HTTP、429、TLS／DNS 錯誤及非白名單 action 不套用新的 stale recovery。
- stale failure、attempt、retry 與退避都會進封閉診斷統計，成功恢復不掩蓋曾發生錯誤。失敗仍保留整區塊已驗證前綴，沒有更換 provider、資料 fallback 或把舊榜當成 fresh。

## 4 RPS 的程式與配額

原本 `max(.5, 1 / RPS)` 使填 4 仍只有 2 RPS。現在明確接受有限數值 **0.1–4**，spacing 直接是 `1 / RPS`；預設仍 2。0、負數、NaN、Infinity、非數字與超過 4 的設定明確回 `invalid_chain_request_rate`，不悄悄使用另一速率。多 workers、block、receipt、getLogs 與重試仍共用一個 provider gate。

正式環境設定由發布協調聊天負責；本補修不會自行把環境改為 4，也不改 key、overage 或訂閱。使用者已提出 4 RPS 的發布意圖，實際切換仍與補修 PR 合併及正常 main 自動部署一起協調。

[Etherscan 官方 rate limits](https://docs.etherscan.io/rate-limits) 列 Free 3/s、Lite 5/s，兩者均 100,000/day；[官方 supported chains](https://docs.etherscan.io/supported-chains.md) 把 BSC chain 56 的鏈上 API 列為 Paid tier only。專用 key 已成功讀 BSC proxy／getLogs 且回報 100,000/day，因此可以推論為 Lite 相容的最低付費配額，4 小於公開的 5/s；這仍是文件與成功實際呼叫的推論，尚未直接完成登入帳戶與 key 指紋對應，不能當成帳戶方案已被直接核實。

每輪完成後仍等待 60 秒。每日用量應加總實際輪次與重試，不能把設定 4 RPS 直接乘 86,400 當成日預估。大量積壓的一輪仍可能很長；若 1,000 筆新交易大多各在不同區塊，約 2,000 次 receipt＋block 請求在 4 RPS 下光排程就約 **500 秒**，這是成本模型，不是 1,000 筆真實測速，也不是一分鐘 SLA。100,000/day 的獨立上限仍需監控。

## 驗證與未完成項

此次只用本機 TCP、虛擬時鐘與既有回歸；新增鏈上 API 請求為 **0**。不讀新 secret、不另開帳戶頁、不修改正式服務。原本 64 confirmations、300 秒補掃、去重、完整付款查核、日期及票券規則均保持。

正式底層 exception subtype 的歷史歸因、實際 4 RPS 的上游接受度與發布後多輪穩定性，仍須由正式驗證確認。本機修正只證明上述缺口與有上限的恢復行為。
