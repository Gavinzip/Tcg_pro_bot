# 回掃核對異常：2026/11/27 11:05–11:08（台灣時間）

本文件區分正式已知事實與本機診斷候選。修改尚未提交、push、開 PR 或部署。

## 正式紀錄能證明的部分

Scanner 正式版本仍是 `b6b9ce9459e43e7c30508be830234c6c0be2005d`；11/27 合併的網站 PR #10 沒有更新 scanner。

| 時間 | run_id | 結果 |
| --- | --- | --- |
| 11:04:46 | d58d8e7a714e4f1ab6f27e2d943d3df1 | 完整發布成功，游標涵蓋至 135963388 |
| 11:05:42 | 904d0e71044c40d386683e8c85e0000b | overlap_missing_known_checkout |
| 11:06:42 | 8df0ecc54540454e9eb2273bf1a4bf20 | 同上 |
| 11:07:42 | 0a2709a186144b1598ea24f514b50e52 | 同上 |
| 11:08:51 | ffe10d22676c4fa390bc095ff245903f | 同一回掃範圍通過 97 筆、補入 0 筆，發布至 135963936 |
| 11:09:45 | eaefd0dd969541c9b4c7e963a982917d | 下一輪再成功，發布至 135964066 |

前三個失敗輪次與恢復輪次的回掃比較範圍都是 **135962722–135963388**，時間定位均成功，目標時間均為 1795748343。實際合併 getLogs 查詢還包含新區塊，其上限依序為 135963533、135963667、135963793、135963936；不能把它們描述成完全相同的 HTTP 請求。失敗輪次均為 11 個 API attempts，無 request error/retry，完整性比較未通過才停止發布。

11:10 的網站已回 200、stale=false，dataThroughAt=2026-11-27T03:08:09Z。這證明後續恢復，不證明故障根因已修好。

## 根因判斷與限制

1. 優先懷疑查詢結果暫時不完整。預測：缺少的已存 identity 不在當輪完整查詢結果中。
2. 合併查詢後的區間過濾錯誤。預測：identity 在完整查詢結果、卻不在回掃區間結果。
3. 驗證後投影或帳本 identity／範圍不一致。預測：identity 在回掃原始集合、卻不在驗證完成集合。

原始故障 getLogs 回覆未保存；原診斷只留下錯誤碼，沒有回傳筆數、集合指紋或缺少 identity 的對照。現在重新查詢成功的資料不能還原當時的回覆。因此上述第 1 項目前只是較符合現象的推論，不能宣稱已證實上游漏資料，也不能宣稱已排除我們所有邏輯。

## 已完成的本機工作

- 先重現診斷缺口：`python3 -m unittest tests.test_event_overlap_evidence -v`，原版測試為 `0 != 3`，三輪漏項均缺少可區分階段的比較證據。
- 使用真實 scanner/store/合併查詢路徑及明確標示的隔離 provider fixture，重播「已存 97 筆、三輪回覆 96 筆、第四輪 97 筆」。失敗期間游標與舊榜不動，恢復後補入 0 筆、不重複計數。這是合成契約測試，不是原始故障 payload 的重播。
- 1,000 組固定種子的區間邊界驗證，未發現合併查詢過濾漏項；不能據此排除未涵蓋的正式資料形狀。
- 新增 `overlap_comparison` 安全診斷：區間、已存／完整查詢／區間內／驗證完成集合的筆數與完整 identity 集合 SHA-256；最多 8 個缺項的 identity 指紋、區塊、事件序號，以及它是否出現在查詢和區間結果中。沒有可讀取的合併查詢集合時明確標記 unavailable，不冒充已觀察到原始集合。
- 已成功的 getLogs 也記錄既有 closed-schema 回覆證據：HTTP／API 狀態、頁碼與區間、result_length、有限 body fingerprint。body 超過 64 KiB 時只是 prefix fingerprint；完整 identity 集合另外使用集合 digest，不宣稱保存了完整原始 body。
- 不記錄 key、URL、錢包、交易雜湊或原始回覆。封閉欄位驗證與大小上限仍經 supervisor 再驗證。
- 沒有新增 API 請求、重試或 fallback，付款驗證、確認數、五分鐘回掃、去重、builder、游標與計票規則都不變。

本機 CI 對應的 **255 項 scanner 回歸全過**，含新增 5 項診斷測試；指令使用既有 `.venv/bin/python` 與 workflow 的測試模組。第一次用系統 Python 執行 discover 的紀錄保留：環境缺少 requests/dotenv、備份測試未設隔離路徑，另有一項需按 action 區分的新日誌數量斷言，已修正並用專案環境重跑全過。未將第一次失敗紀錄刪除冒充全綠。未修改的 encrypted drive-backup 專項未在本輪重跑。

## 合作方可見性

網站的正式 Partner 路由與網站共用完整榜單。上游失敗、有可用完整榜時仍回 200、保留原 snapshotId/times/rows，標記 stale=true；沒有可用完整榜時回一般的 503，沒有回傳 scanner 的 overlap_missing_known_checkout、last_error、terminal_failures 或 stack。

stale=true 也可能是正常同步／重啟，不能僅憑此欄位判斷哪一輪失敗；dataThroughAt 和 snapshotId 能讓合作方辨識資料延遲。Partner 兩項正常 HTTP／授權路由的本機契約測試通過，沒有使用正式密碼；本次未冒充完成帶正式 Partner Token 的重新驗證。

若要取得下次正式異常的決定性證據，需要先審查並授權此診斷版本走 PR/main/自動部署。現階段不能把「診斷補齊」當作「正式根因已查明／已修復」。
