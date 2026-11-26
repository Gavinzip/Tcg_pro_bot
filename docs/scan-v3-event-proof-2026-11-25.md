# V3 事件付款證據與查詢合併：歷史候選紀錄

2026-11-26 整合版的實際接口限制、已授權終點版本 guard 與最新驗證，見
[整合發布報告](v3-backup-release-validation-2026-11-26.md)。以下保留當時候選與量測，非目前正式狀態。

本文保留第一階段「省 receipt、仍逐區塊讀 header」的量測與設計紀錄。
使用者其後已授權取消逐事件區塊的額外讀取；目前本機行為與最新 100／1000 比較見
[第二階段報告](scan-v3-log-blocks-2026-11-25.md)。以下第一階段數字沒有冒充最新版結果。

本次僅本機修改、驗證，未 commit／push、未新開 PR、未部署或改正式變數。
既有 checkout `codex/api-scan-cadence` 起始 HEAD 為
`efd7b6a6c694338f13a7ce1d5be05c631095b4f2`；先前 PR8 已合併，並不包含本次修改。

## 改變的證據邊界

原本每筆新交易讀 receipt，將同交易的 USDT Transfer 與 checkout 金額獨立對帳。
新模式只對指定 V3 proxy 使用已稽核的合約付款語義，不再做逐筆 Transfer 對帳。
它不是「相同證據、只是更快」；信任邊界改為已固定版本的付款實作與供應商完整 logs。

[V3 proxy](https://bscscan.com/address/0xd4d18607d6111c5fa2f93a4a5b2c0e28f1563f9f#code)
的[已驗證實作](https://bscscan.com/address/0x3272bd8b81ebb53254b4ed1d8b67fe832e6fb2c7#code)
在 `permitFund` 先驗證平台簽章並呼叫 Permit2，再更新 counter／產生 checkoutIds，最後唯一一處 emit CheckoutSuccess。
付款使用 auth.user、auth.token、amountPerToken × numOfTokens，收款方固定為 proxy 本身。
扣款錯誤向外傳遞，整筆 revert，不保留成功事件。此結論限該實作及固定 USDT，不泛化至任意幣種或舊合約。

鏈上區塊 135643238 已只讀確認 proxy 指向 `0x3272…b2c7`，runtime Keccak-256
`0x0646d66925eb42b5284bce70741df7921ca2942caca101467d3ebd97b1861ca3`
與保存的稽核一致。程式使用相同 17,626 bytes 的 SHA-256
`42cbf5853b9746ce648ba0bfe21923b351d021e55c2c9d4c777232433d6f880c`
固定內容，避免新增密碼學套件；沒有把 SHA3-256 當成 Ethereum Keccak-256。

新模式每輪首次讀取範圍前：

1. 範圍起點須在已稽核實作啟用區塊 115246746 之後。
2. 使用同一 provider 在起點前一塊及本輪 confirmed bound 讀 EIP-1967 implementation slot，均須為已稽核地址。
3. 在相同 bound 讀實作 bytecode，比對固定 SHA-256。
4. 完整取得整段的 Upgraded logs，不帶 builder 篩選。任何升級（包括換走後又換回）均停止，不能只比對兩端。

未支援歷史狀態、錯誤形狀、版本不符、code 不符或升級均明確失敗；不切換 provider、不改成 receipt fallback、不發布未完成資料。
新讀取仍共用原有 pacing、有限同源重試／冷卻及 stop gate。
這裡沿用對供應商正確提供 storage/code/logs 的信任，並非密碼學 inclusion proof。

## 保留及合併

- 新紀錄仍逐區塊核對 canonical hash／時間；64 confirmations、指定 builder、USDT、活動日期、正金額、去重及五分鐘回掃不變。
- 回掃已驗證且欄位完全相同的舊紀錄，沿用原本的 anchored-prefix 證據，不逐筆重讀舊區塊。
- 回掃與首個前進 chunk 共用完整 getLogs 清單；各自仍按原範圍消費、對帳及提交。滿頁仍使用原本拆分／分頁，不能截斷。
- 省 receipt 後將既有有界 workers 用於 header 預取，避免把剩餘網路 I/O 串行化。所有驗證／SQLite 提交仍由主執行緒按區塊順序完成。
- 同區塊出錯不提交該區塊；後面出錯或 KeyboardInterrupt 保留先前完整前綴。舊 pinned snapshot 不因補掃而改變。
- 舊合約仍用 receipt 對帳；明確指定但未稽核的 V3 proxy 不使用新模式。
- 新 snapshot 另保存本輪 `scan_verification`（模式／實作 digest／guard 範圍），不改既有 ledger payload，也不宣稱舊 ledger 全是同一種驗證；舊 snapshot 的這份紀錄不會被後續輪次覆蓋。

兩個獨立本機開關（預設關閉，並非失敗時切換）：

```
--v3-event-proof          # 或 EVENT_LEADERBOARD_V3_EVENT_PROOF=1
--combine-log-queries     # 或 EVENT_LEADERBOARD_COMBINE_LOG_QUERIES=1
```

不需要換 key、改正式 RPS、confirmations、回掃或 timer。

## 100／1000 筆比較

執行 `scripts/benchmark_v3_event_proof.py`，跑完整 scanner、真實 SQLite 提交及 snapshot。
baseline 直接載入 git HEAD efd7b6a 的 scanner 原始碼，不以自行簡化的成本公式代替。
100 筆使用先前保存的真實 checkout／receipt／header／time-hint，凍結已確認上界，97 個不同新交易區塊。
1000 筆是明確標記的十倍合成負載，970 個不同新交易區塊；不冒稱已有 1000 筆真實鏈上完整回應。
歷史 storage/Upgraded 回覆是測試輸入；新 guard 的歷史 Etherscan 整合尚未做付費實測。
實作 bytecode 來自本輪官方公開 RPC，只讀保存，與固定 digest 一致。

| 負載 | 原版 | 只合併 logs | 只省 receipt（含 guard） | 兩者一起 |
| --- | ---: | ---: | ---: | ---: |
| 100 TX 真實資料重播 | 205 | 204 | 109 | 108 |
| 1000 TX 合成壓測 | 1980 | 1979 | 984 | 983 |

表為邏輯 API 呼叫數，包含 guard；不是本輪真的發出這些付費請求。
1000 筆新資料剛好滿 1000 筆一頁，因此包含為完整性而拆分的查詢；不能拿一次滿頁就假定查完。
四種模式在每組的完整 checkout evidence checksum、張數、金額、watermark 全部一致。

本機最後獨立執行樣本的 wall time：100 筆原版 0.524s、修改後 0.660s；1000 筆原版 3.440s、修改後 3.876s。
對應 process CPU：0.159→0.223s、1.636→1.572s。
這是零網路等待的 CPU／SQLite 重播，表列為最後一次樣本，不能據此聲稱 CPU 或真實網路延遲已顯著改善。
依 4 RPS 單算配速：205/4=51.25→108/4=27s；1980/4=495→983/4=245.75s。
此值不含回應延遲、重試／冷卻，並非實際輪次耗時或 SLA，也不含掃完後等 60 秒。

原始 report、診斷、public runtime 與清理 manifest 留在 ignored `.scratch/v3-event-proof-20261125/`。
先前所有真實捕捉檔、已保存 ledger 及未追蹤 `scripts/audit_event_scan.py` 原樣保留。
本次測速付費請求為 0；另有 3 次官方公開 RPC code 查核（一次舊狀態回 missing trie node，兩次 latest 成功），沒有拿 latest 冒充舊狀態。

## 尚未證實的部分

- 尚無這個修改版本的真實網路 100／1000 TX 耗時。
- 官方 Etherscan 文件支援指定歷史 block 的
  [eth_getStorageAt](https://docs.etherscan.io/api-reference/endpoint/ethgetstorageat) 與
  [eth_getCode](https://docs.etherscan.io/api-reference/endpoint/ethgetcode)，但本次尚未用付費 key 實際核對新 guard 的歷史查詢。
  不能拿官方公開 RPC 的 latest 成功當成已測過 Etherscan 歷史 storage。
- 新開關預設關閉；任何未來發布／啟用另需使用者同意。本機檢查不代表使用者已驗收或 production 已切換。

本機完整回歸 **188 項通過、0 failure/error**（Python 3.9.6，與既有 CI 相同測試集合加 14 項新測試）；CLI 兩個開關入口與 git diff --check 亦通過。未 push，所以尚無本次新版遠端 CI／PR。
