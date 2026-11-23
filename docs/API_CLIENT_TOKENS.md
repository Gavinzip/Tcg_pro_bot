# 各網站獨立的排行榜 Bearer token

2026-11-20 已部署 b02f13f。兩項網站憑證已存進 1Password 並回讀核對，服務端設定已驗證。兩組各自對正式排行榜／搜尋回 200、Profile／BBW 回 401；原內部查詢、匿名拒絕與 Bot 健康檢查正常。分享連結和密鑰不放進本文件。

## 憑證分工

| 用途 | 設定 | 可讀範圍 |
| --- | --- | --- |
| 既有內部查詢 | PROFILE_API_TOKEN | 原有 Profile 與排行榜；既有使用者不因新增網站 token 失效。 |
| 我們的活動網站 | EVENT_LEADERBOARD_CLIENT_TOKENS_JSON 中的 renaiss-site | 只讀排行榜與搜尋。 |
| 合作方網站 | EVENT_LEADERBOARD_CLIENT_TOKENS_JSON 中的 partner-site | 只讀排行榜與搜尋。 |
| 既有 BBW 社群資格查詢 | BBW_METRICS_TOKEN | 沿用自己的驗證，網站排行榜 token 不能使用。 |

兩個網站可讀同一活動榜，但 token 值必須不同，也不能與內部 token 相同。網站 token 不能呼叫 `/v1/profile` 或 `/v1/bbw-community-metrics`，也不會新增寫入或管理權限。此版本沒有逐 token 活動白名單、限流、流量統計或 token 自動到期機制；各網站 proxy 應限制可查的活動。

## 伺服器端設定

API 服務的 `EVENT_LEADERBOARD_CLIENT_TOKENS_JSON` 是一個 JSON object，key 是網站識別碼，value 是該網站的獨立隨機 token。命名使用 `renaiss-site` 和 `partner-site`；識別碼支援 1–64 個英文字母、數字、底線或連字號。

token 應以密碼安全隨機方式產生，例如 32 bytes 的 URL-safe 隨機資料；每個值須為 32–256 個可列印 ASCII 字元，不含空白。不得把真實值提交 Git、貼在 chat、放進 VITE_* 或瀏覽器程式碼。明確設定錯誤、重複網站 ID、重複 token 或與內部 token 相同時，服務會拒絕啟動並只回報設定規則，不記錄憑證值。

兩邊網站的伺服器代理各自使用：

- `TCG_LEADERBOARD_API_BASE_URL=https://tcg-api.up.railway.app`
- `TCG_LEADERBOARD_API_TOKEN`：只放該網站的 token；兩邊使用相同變數名稱、不同值。
- 對上游送出 `Authorization: Bearer <該網站的token>`。

網站前端只呼叫自己的同源 server proxy。各網站 token 目前仍能讀取所有已配置活動，不能靠瀏覽器藏住活動 ID 或 token。

## 部署、交接與驗證

1. 先完成程式與測試；依專案規則取得 push／部署同意後部署新版 API。
2. 生成兩個不同的隨機 token；在 1Password 分開儲存、回讀核對。建議項目名稱為 `TCG Pro Leaderboard — Renaiss Website`、`TCG Pro Leaderboard — Partner Website`，建立前依保險庫規則確認清單與授權。
3. 透過安全管道把兩個值設定到 Railway 的 JSON 設定，重新部署使其載入。不要只修改本機檔就宣稱正式生效。
4. 對兩個 token 分別驗證排行榜與搜尋成功；無 token、錯誤 token 回 401；兩個網站 token 呼叫 Profile 與 BBW 端點皆回 401。再次確認既有內部查詢可用。
5. 我們網站的 token 僅交給自己的 server runtime；合作方只取得合作方那一項的分享連結，不分享包含兩個值的項目。
6. 分享連結的到期只控制該連結；已取得的 token 不會自動失效。先前分享的 `TCG Pro API Bearer Token` 仍屬內部共用 token，新的網站專用 token 生效後應另行確認舊交接的替換與撤回範圍，不擅自更換已有使用者的內部憑證。

## 單獨撤銷或更換

從 API 的 JSON 設定移除指定網站的 key，或只更換其 value，重新部署後即可撤銷／更新該網站。另一網站及內部 token 不需跟著更換。因 API 與 Bot 共用部署，套用設定時可能短暫重新啟動該服務。

未設定網站 token 時沿用原先 Profile API 驗證；只設定網站 token、未設定內部 token 時，Profile 會拒絕所有保護路由請求，不會變成匿名開放。原本完全沒有設定任何憑證的本機開發行為維持不變，不適合作為正式環境設定。

## 我們網站的目前狀態（2026-11-20）

已與「建置構想中的網站」chat 核對：checkout 為 `/Users/gavin/Documents/ChatGPT/b-w`、分支 `codex/bnb-renaiss-campaign`。已新增本機同源 Node proxy；部署平台仍待定，前端 repo 尚未 push／部署。網站 chat 已使用我們專用憑證獨立驗證正式排行榜／搜尋；facade 對未實作的票數回 ticket_scoring_not_ready，沒有把 draw_count 當票數。正常 5188 完整網站已以 op run 的 secret reference 啟動；12 月 event 尚未配置，沒有套用 11 月範例冒充活動。

瀏覽器 adapter：`src/services/campaignApi.ts`；回應驗證：`src/services/campaignSchemas.ts`；整合說明：`docs/frontend-integration.md`。公開的 `VITE_CAMPAIGN_API_BASE` 僅供未來 facade 路徑使用。

這次 token 支援不會把付費抽數改成活動票券；12 月活動設定、票數權重及其他加成仍是另外的後端工作。網站已要求 effective_tickets 資料，不能把目前 draw_count 回覆當成已完成活動票券串接。
