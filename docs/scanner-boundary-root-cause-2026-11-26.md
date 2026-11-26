# 回掃時間邊界失敗：本機補修

此文件記錄本機調查及驗證結果；實際合併／部署狀態以 PR 及平台 Git SHA 為準。分支 `codex/scanner-boundary-root-cause`，基底及事故時正式版本均為 `1ca894b4b76bdbfdc6a1d88205652fa8199bf12c`。原演算法聊天的 dirty checkout 與前端工作均未修改。

## 能確認的根因與證據界線

台灣時間 2026/11/26 08:23:24、08:24:26、08:25:28、08:26:30、08:28:37，共五輪在 `overlap → boundary` 終止。每輪只有四次實際請求：head 一次、canonical block 兩次、`getblocknobytime` 一次。網路請求 errors/retries 皆為零，沒有 getLogs 或 receipt 呼叫。

失敗的 boundary 診斷只有 `head_block`，尚未寫入 `hint_block`。對照同一正式版本，請求成功後、寫入 hint 前，只有 `number(result)` 會做內容驗證並拒絕不可解析或負數的編號。因此可定位到**時間索引成功回覆的編號解析失敗，以及程式把加速用提示當成必要依賴**。不是 receipt 慢查、付款驗證失敗或已記錄的限流／額度拒絕。

但歷史 JSON 原文沒有留存；當時的 `invalid_integer`／`negative_integer` 也未轉送到正式日誌，成功發布又清除了 SQLite 的 `last_error`。因此無法還原當時的具體回覆、區分這兩種錯誤碼，或判定是回覆不符規格還是本機解析相容性不足。離線測試使用相同失敗路徑的合成回覆，**不是聲稱取得了當時原文**。正式來源恢復後的唯讀 metadata 為 active、last_error=null；沒有為取證停掃或打壞正式來源。

另一個根因是診斷缺口：HTTP 層把回覆算成功，內容驗證錯誤未進入 request failure；scanner 回傳 stale 的具體錯誤只在 stdout／SQLite，supervisor 丟棄 stdout，stderr 的安全 schema 又只接受請求錯誤統計，所以最後只看得到 `status=error`。

## 修改

- `getblocknobytime` 的成功回覆在 request 邊界驗證；無效編號記成 `provider_invalid_boundary_hint`，不印原文、不作泛用重試。
- 使用者已另外確認的定位備援：只遇到上述無效提示時，以本輪已驗證 canonical 區塊作種子，執行既有精確時間下界搜尋。沒有沿用舊快照／上一輪區塊快取，也不跳過五分鐘回掃。正常有效提示的路徑不變。
- quota、auth、限流、timeout、錯誤區塊與付款證據仍走原有處理；有效但超過 head 的提示仍明確拒絕。
- 診斷新增數字型 target timestamp、hint-invalid 標記，以及封閉白名單的 `terminal_failures`。事件及整輪都保留原因碼；與 request errors 分開，不重複計成 API 用量。supervisor 仍拒絕未知欄位及任意錯誤字串，token／URL 不可輸出。
- 新增回歸測試及 CI 執行入口；本機依賴放在忽略的 `.venv/`。

## 驗證

- 原版在新測試上紅燈：完整回掃案例回 `stale / invalid_integer`；終止原因碼的兩個測試也能抓到原版診斷缺口。原版只在隔離 Python 程序的 RAM 載入，沒有覆蓋來源檔。
- 新版完整 CI 對應回歸 **183 項通過、0 skip**；同案例連續五輪保持完整回掃、新增交易、交易雜湊＋log index 去重與原始快照。
- 120 組 seeded 測試涵蓋非數字、負數、布林、容器及浮點提示，不規則／同秒區塊、有無本輪 anchor、genesis 及 head 邊界；精確結果與 lower_bound 相同，讀取數維持對數界線。
- 認證／額度拒絕不走定位備援；無效 canonical block 仍失敗；重組仍保留游標及原始快照。安全原因碼可經 supervisor 轉送，任意敏感字串被拒絕。
- 高度約 1.3575 億、五分鐘回掃、4 RPS、每讀虛擬 0.22 秒的模型：有效提示 4 requests／0.97 秒，無效提示 14 requests／3.47 秒。是模型，不能當正式耗時或每輪一分鐘保證。
- 調查／驗證沒有額外付費 BSC／Solana 查鏈呼叫。讀取過自家 snapshot API；1Password 僅用既有 Environment 注入，未修改 vault、未保存秘密檔。

證據位於此 checkout 的 `.scratch/boundary-root-cause-evidence/`：`incident-summary.json`、`baseline-red.log`、`regression.log`、`boundary-cost-model.json`、`cleanup-manifest.txt`。測試 SQLite 與本次編譯快取依 manifest 清理；證據、原始碼與本機依賴保留。

本機驗證完成時尚未發布，當時正式服務仍使用原版。驗證結果不能代替使用者驗收，也不能宣稱事故原始回覆已找回。

## 追加：上游回覆安全取證（同分支、本機）

使用者要求繼續查明上游原因，不能只靠定位備援。新增獨立的 `response_evidence.py`，provider 產生封閉結構，diagnostics 與 supervisor 再次驗證後才輸出。既有定位備援範圍不變，本次不增加備援或重試，不改正式設定。

每個時間定位回覆（包括成功）及其他 action 的失敗會留下：

- 實際 HTTP status、content type 分類、JSON 是否可解析、API status／message 的固定分類、result 型別與長度。
- 時間提示的 missing／null／型別錯誤／空字串／非整數／負數等細分類；只保留有界的數字編號。超長字串不再為紀錄做整數解析，標為 `not_inspected`。
- 查詢 timestamp／closest 或區塊範圍、run／event、邏輯請求識別碼、attempt／max attempts、階段、耗時。同請求重試共用識別碼，並行請求分開。
- 已知限流、每日額度、認證拒絕等訊息只轉成固定分類；RPC error 僅留下有界數字 code 與分類。這些分類不改動現有重試判斷。
- 原始 body 不寫檔、不寫日誌；最多取前 64 KiB 的 SHA-256、取樣長度、已讀長度及 full／prefix 標記。urllib HTTP error 額外讀取最多 64 KiB + 1 byte，超過即標成未讀完整，`body_bytes` 是已擷取長度，不冒稱上游總長。

金鑰、任意錯誤原文、URL、headers、交易雜湊與未核准 query 欄位不會序列化。未知回覆可以靠型別／長度／指紋區分，但不能從指紋還原原文；這是明確的取證限制。連線未取得 response 時 HTTP status 為 0（未知），不捏造 200。失敗的安全 observation 也附在 SourceIncomplete diagnostic；即使定位備援成功，獨立異常紀錄仍保留。

驗證包含真實本機 HTTP socket 的 503 HTML、urllib HTTP error、非 JSON、無 response 的連線失敗、額度／認證／RPC 拒絕、限流後原有重試成功、並行關聯、超長內容截斷指紋，以及正式 supervisor 轉送／拒絕敏感欄位。整合測試確認連續五輪成功時仍保留五筆異常回覆；五分鐘回掃、付款驗證及去重保持通過。

本次完整 CI 對應回歸 **194 項通過、0 skip**，紀錄在 `response-evidence-regression.log`；`response-evidence-safe-example.json` 是經 supervisor 轉送的**離線合成範例**，不是歷史原始回覆。原版缺少這些紀錄的新測試失敗證據在 `response-evidence-red.log`。所有新增驗證均未呼叫外部付費鏈 API。

這能補上下一次發生時的原因與回覆結構證據，不能補回已遺失的五輪原文，也不能據此歸責給上游。本機完成不代表已發布，正式服務是否取得新紀錄須另行核對 deployment SHA 及實際日誌。

## 追加：解析格式主動驗證

已核對 [Etherscan 時間查詢](https://docs.etherscan.io/api-reference/endpoint/getblocknobytime)、[區塊高度 RPC](https://docs.etherscan.io/api-reference/endpoint/ethblocknumber)、[區塊 RPC](https://docs.etherscan.io/api-reference/endpoint/ethgetblockbynumber) 與 [錯誤格式](https://docs.etherscan.io/common-error-messages)。我們使用的是 Etherscan V2，未把其他 explorer 的巢狀 result 當成本服務的正常格式。

| 類別 | 修改前 | 本機修改後 |
| --- | --- | --- |
| 官方十進位 result 字串、RPC `0x` 十六進位 | 正確 | 正確 |
| JSON 整數、零、超過 2^53 至 uint256 的整數精度 | 正確，未經 float | 保持正確 |
| 十六進位 `0X` 或外層空白（防禦性相容，非官方保證） | 誤拒絕／觸發定位備援 | 正常解析，不需要備援 |
| Python 底線數字及 Unicode 數字（非 wire 規格） | 意外接受 | 明確拒絕 |
| null、浮點、布林、容器、科學記號、錯誤文字 | 拒絕 | 繼續拒絕，不猜數值／不四捨五入 |
| `status="0"` 即使 result 看起來是數字 | API 拒絕 | 保持拒絕，不進整數成功路徑 |

整數字串現在經獨立 `integers.py` 明確解析；安全紀錄共用同一函式，不再複製另一份 Python `int()` 規則。保留既有十進位正號、前導零與負數錯誤碼；不增加替代資料來源、fallback、重試或巢狀物件猜測。

新增 `tests.test_event_integer_contract`：直接數值、真實 provider envelope、精確時間下界搜尋及兩種實際本機 HTTP transport；13 項測試另涵蓋 1,000 組固定種子的 uint256 十進位／十六進位往返。JSON 外層空白及 UTF-8 BOM 可由兩種 transport 正常讀取。原版不通過的新案例已保存於 `integer-contract-before.log`；修改前後對照為 `integer-contract-before-after.json`（原版函式僅在隔離程序 RAM 載入），完整回歸 **207 項通過、0 skip**，紀錄為 `integer-contract-regression.log`。

結論：官方文件正常格式原本即能解析；本機已重現並修正額外的相容性與接受過寬缺口。它們是實測發現，不能據此斷言那五輪就是同一格式。上述測試僅本機，付費鏈 API 呼叫為零。發布維持工作分支 → PR 檢查 → 使用者授權合併 main → GitHub 自動部署 → 正式版本與功能驗證的順序。
