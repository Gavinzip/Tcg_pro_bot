#!/usr/bin/env python3
import discord
from discord import app_commands
import os
import shutil
import tempfile
import base64
import io
import threading
import asyncio
import traceback
import sys
import json
import re
import html as html_lib
import time
from datetime import datetime
import requests
import market_report_vision
import image_generator
from dotenv import load_dotenv
from http.server import BaseHTTPRequestHandler, HTTPServer

# ============================================================
# ⚠️ JINA AI RATE LIMITER 說明（重要！請勿刪除此說明）
# ============================================================
# market_report_vision.py 內部的 fetch_jina_markdown() 函數使用了一個
# 全域的 sliding window rate limiter：
#   - _jina_requests_queue: 記錄最近60秒內的請求時間戳
#   - _jina_lock: threading.Lock，確保多執行緒安全
#   - 限制：18 requests / 60 seconds（留兩次緩衝給 Jina 每分鐘20次的限額）
#
# 在併發情境下（多個用戶同時送圖），rate limiter 依然有效，因為：
# 1. Python module 是 singleton，所有 task 共用同一份 _jina_requests_queue
# 2. _jina_lock 是 threading.Lock，在 tasks 跑的 executor threads 中也是 thread-safe 的
# 3. 超過限額的 task 會自動 sleep 等待，不會炸掉 Jina
#
# 每次分析一張卡約會用掉 4~8 次 Jina 請求（PC: 2~3次, SNKRDUNK: 2~5次）
# ============================================================

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass  # 安靜模式

def run_health_server():
    server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
    server.serve_forever()

load_dotenv()
TOKEN = os.getenv('DISCORD_BOT_TOKEN')
MAX_CONCURRENT_REPORTS = max(
    1, int(os.getenv("MAX_CONCURRENT_REPORTS", os.getenv("MAX_CONCURRENT_IMAGES", "6")))
)
MAX_CONCURRENT_POSTERS = max(1, int(os.getenv("MAX_CONCURRENT_POSTERS", "3")))

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)
REPORT_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REPORTS)
POSTER_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_POSTERS)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# /profile uses an isolated template bundle to avoid impacting normal card posters.
PROFILE_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "profile", "wallet_profile.html")
PROFILE_LOGO_PATH = os.path.join(BASE_DIR, "templates", "profile", "logo.png")
RENAISS_COLLECTIBLE_LIST_URL = "https://www.renaiss.xyz/api/trpc/collectible.list"
RENAISS_SBT_BADGES_URL = "https://www.renaiss.xyz/api/trpc/sbt.getUserBadges"
PROFILE_PAGE_LIMIT = max(10, min(100, int(os.getenv("PROFILE_PAGE_LIMIT", "100"))))
PROFILE_SCAN_MAX_OFFSET = max(PROFILE_PAGE_LIMIT, int(os.getenv("PROFILE_SCAN_MAX_OFFSET", "5000")))
PROFILE_API_MAX_RETRIES = max(1, int(os.getenv("PROFILE_API_MAX_RETRIES", "4")))
PROFILE_API_RETRY_BACKOFF_SEC = max(0.2, float(os.getenv("PROFILE_API_RETRY_BACKOFF_SEC", "0.8")))
_WALLET_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
_TRANSPARENT_CARD_IMAGE = "data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs="


def _normalize_wallet_address(address: str) -> str | None:
    text = (address or "").strip()
    if not _WALLET_RE.fullmatch(text):
        return None
    return text.lower()


def _parse_int(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.startswith("-"):
        return int(text[1:]) * -1 if text[1:].isdigit() else None
    return int(text) if text.isdigit() else None


def _format_usd(value: int | None) -> str:
    if value is None:
        return "N/A"
    return f"${value:,.0f}"


def _format_number(value: int | None) -> str:
    if value is None:
        return "0"
    return f"{value:,.0f}"


def _format_fmv_display(value: int | None) -> str:
    """FMV source values are cent-like units, display as whole-dollar style numbers."""
    if value is None:
        return "0"
    return f"{(value / 100):,.0f}"


def _format_fmv_usd(value: int | None) -> str:
    if value is None:
        return "$0"
    return f"${(value / 100):,.0f}"


def _clamp_profile_card_count(value: int | None) -> int:
    if value in (1, 3, 10):
        return int(value)
    return 10


def _profile_lang_from_locale(locale_like) -> str:
    text = str(locale_like or "").lower().strip()
    if text in ("zhs", "zh_cn", "zh-cn", "zh_hans", "zh-hans", "zh-sg"):
        return "zhs"
    if text in ("zh", "zh_tw", "zh-tw", "zh_hant", "zh-hant", "zh-hk"):
        return "zh"
    if text.startswith("zh"):
        if any(x in text for x in ("hans", "zh-cn", "zh-sg")):
            return "zhs"
        return "zh"
    if text.startswith("ko"):
        return "ko"
    return "en"


def _profile_top_value_label(count: int, lang: str = "en") -> str:
    n = max(1, int(count or 0))
    if lang in ("zh", "zhs"):
        return f"TOP {n} 展示總價值"
    if lang == "ko":
        return f"TOP {n} 표시 총가치"
    if n == 10:
        return "Top Ten Total Value"
    if n == 3:
        return "Top Three Total Value"
    return f"Top {n} Total Value"


def _profile_ui_labels(lang: str) -> dict[str, str]:
    if lang == "zh":
        return {
            "items_count_label": "資產數量",
            "assets_unit": "張",
            "sbt_badges_label": "SBT 徽章",
            "no_sbt_label": "無 SBT",
            "owned_prefix": "持有",
            "brand_name": "Renaiss",
            "brand_site": "renaiss.xyz",
        }
    if lang == "zhs":
        return {
            "items_count_label": "资产数量",
            "assets_unit": "张",
            "sbt_badges_label": "SBT 徽章",
            "no_sbt_label": "无 SBT",
            "owned_prefix": "持有",
            "brand_name": "Renaiss",
            "brand_site": "renaiss.xyz",
        }
    if lang == "ko":
        return {
            "items_count_label": "자산 수량",
            "assets_unit": "장",
            "sbt_badges_label": "SBT 배지",
            "no_sbt_label": "SBT 없음",
            "owned_prefix": "보유",
            "brand_name": "Renaiss",
            "brand_site": "renaiss.xyz",
        }
    return {
        "items_count_label": "Items Count",
        "assets_unit": "Assets",
        "sbt_badges_label": "SBT Badges",
        "no_sbt_label": "No SBT",
        "owned_prefix": "owned ",
        "brand_name": "Renaiss",
        "brand_site": "renaiss.xyz",
    }


def _profile_wizard_texts(lang: str) -> dict[str, str]:
    lang = _profile_lang_from_locale(lang)
    if lang == "zh":
        return {
            "lang_prompt": "🌐 請先選擇語言 / Please choose language",
            "lang_timeout": "⏱️ 未選擇語言，已使用預設：繁體中文",
            "panel_title": "海報設定面板",
            "wallet_label": "Wallet",
            "collection_label": "收藏數量",
            "selectable_sbt_label": "可選 SBT",
            "setup_tip": "請選模板、SBT、卡片後按「生成海報」。若不選 SBT/卡片，會用預設（依 FMV 由高到低）。",
            "template_placeholder": "1) 選擇模板（Top 1 / Top 3 / Top 10）",
            "sbt_placeholder": "2) 複選 SBT（可略過）",
            "card_placeholder": "3) 複選卡片（可略過）",
            "default_button": "使用預設直接生成",
            "generate_button": "生成海報",
            "only_owner_msg": "只有發起指令的人可以操作此面板。",
            "timeout_msg": "⏰ 設定面板已逾時，請重新輸入 `/profile`。",
            "generating_msg": "⏳ 生成中，請稍候...",
            "summary_done": "收藏海報已完成",
            "shown_label": "展示張數",
            "shown_fmv_label": "展示總值",
            "no_sbt_option": "無可選 SBT",
            "no_card_option": "無可選卡片",
            "sbt_balance_prefix": "持有 ",
            "current_selection_title": "目前選擇",
            "lang_selected_label": "語言",
            "template_selected_label": "模板",
            "sbt_selected_label": "SBT",
            "cards_selected_label": "卡片",
            "default_short": "預設",
            "selected_short": "已選",
        }
    if lang == "zhs":
        return {
            "lang_prompt": "🌐 请先选择语言 / Please choose language",
            "lang_timeout": "⏱️ 未选择语言，已使用默认：简体中文",
            "panel_title": "海报设置面板",
            "wallet_label": "Wallet",
            "collection_label": "收藏数量",
            "selectable_sbt_label": "可选 SBT",
            "setup_tip": "请选择模板、SBT、卡片后点击“生成海报”。若不选 SBT/卡片，将使用默认（按 FMV 从高到低）。",
            "template_placeholder": "1) 选择模板（Top 1 / Top 3 / Top 10）",
            "sbt_placeholder": "2) 多选 SBT（可跳过）",
            "card_placeholder": "3) 多选卡片（可跳过）",
            "default_button": "使用默认直接生成",
            "generate_button": "生成海报",
            "only_owner_msg": "只有发起指令的人可以操作此面板。",
            "timeout_msg": "⏰ 设置面板已超时，请重新输入 `/profile`。",
            "generating_msg": "⏳ 生成中，请稍候...",
            "summary_done": "收藏海报已完成",
            "shown_label": "展示张数",
            "shown_fmv_label": "展示总值",
            "no_sbt_option": "无可选 SBT",
            "no_card_option": "无可选卡片",
            "sbt_balance_prefix": "持有 ",
            "current_selection_title": "当前选择",
            "lang_selected_label": "语言",
            "template_selected_label": "模板",
            "sbt_selected_label": "SBT",
            "cards_selected_label": "卡片",
            "default_short": "默认",
            "selected_short": "已选",
        }
    if lang == "ko":
        return {
            "lang_prompt": "🌐 언어를 먼저 선택하세요 / Please choose language",
            "lang_timeout": "⏱️ 언어 미선택, 기본값 한국어로 진행합니다.",
            "panel_title": "포스터 설정 패널",
            "wallet_label": "Wallet",
            "collection_label": "컬렉션 수",
            "selectable_sbt_label": "선택 가능 SBT",
            "setup_tip": "템플릿, SBT, 카드를 선택한 뒤 \"포스터 생성\"을 누르세요. SBT/카드를 선택하지 않으면 기본값(FMV 내림차순)을 사용합니다.",
            "template_placeholder": "1) 템플릿 선택 (Top 1 / Top 3 / Top 10)",
            "sbt_placeholder": "2) SBT 다중 선택 (선택 사항)",
            "card_placeholder": "3) 카드 다중 선택 (선택 사항)",
            "default_button": "기본값으로 생성",
            "generate_button": "포스터 생성",
            "only_owner_msg": "명령을 실행한 사용자만 이 패널을 조작할 수 있습니다.",
            "timeout_msg": "⏰ 설정 시간이 만료되었습니다. `/profile`을 다시 실행하세요.",
            "generating_msg": "⏳ 생성 중입니다...",
            "summary_done": "컬렉션 포스터 생성 완료",
            "shown_label": "표시 카드 수",
            "shown_fmv_label": "표시 FMV",
            "no_sbt_option": "선택 가능한 SBT 없음",
            "no_card_option": "선택 가능한 카드 없음",
            "sbt_balance_prefix": "보유 ",
            "current_selection_title": "현재 선택",
            "lang_selected_label": "언어",
            "template_selected_label": "템플릿",
            "sbt_selected_label": "SBT",
            "cards_selected_label": "카드",
            "default_short": "기본",
            "selected_short": "선택",
        }
    return {
        "lang_prompt": "🌐 Please choose language",
        "lang_timeout": "⏱️ No language selected. Using default: English.",
        "panel_title": "Poster Setup Panel",
        "wallet_label": "Wallet",
        "collection_label": "Collection",
        "selectable_sbt_label": "Selectable SBT",
        "setup_tip": "Choose template, SBT, and cards, then click Generate Poster. If SBT/cards are not selected, defaults are used (FMV high to low).",
        "template_placeholder": "1) Select template (Top 1 / Top 3 / Top 10)",
        "sbt_placeholder": "2) Select SBT (optional)",
        "card_placeholder": "3) Select cards (optional)",
        "default_button": "Generate with Defaults",
        "generate_button": "Generate Poster",
        "only_owner_msg": "Only the command user can operate this panel.",
        "timeout_msg": "⏰ Setup panel timed out. Run `/profile` again.",
        "generating_msg": "⏳ Generating poster...",
        "summary_done": "Collection poster generated",
        "shown_label": "Shown",
        "shown_fmv_label": "Shown FMV",
        "no_sbt_option": "No selectable SBT",
        "no_card_option": "No selectable cards",
        "sbt_balance_prefix": "Owned ",
        "current_selection_title": "Current Selection",
        "lang_selected_label": "Language",
        "template_selected_label": "Template",
        "sbt_selected_label": "SBT",
        "cards_selected_label": "Cards",
        "default_short": "Default",
        "selected_short": "Selected",
    }


def _compact_sbt_label(name: str, max_len: int = 24) -> str:
    text = str(name or "").strip()
    if not text:
        return "SBT"
    for sep in ("—", "–", "-", "|"):
        if sep in text:
            tail = text.split(sep)[-1].strip()
            if tail and len(tail) < len(text):
                text = tail
                break
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def _select_profile_items(parsed_sorted: list[dict], card_count: int, selected_tokens: list[str] | None) -> list[dict]:
    if not parsed_sorted:
        return []
    target = max(1, min(card_count, len(parsed_sorted)))
    tokens = [str(x).strip() for x in (selected_tokens or []) if str(x).strip()]
    selected_indices: list[int] = []
    used: set[int] = set()

    if tokens:
        for tok in tokens:
            candidate_idx = None
            if tok.isdigit():
                rank = int(tok)
                if 1 <= rank <= len(parsed_sorted):
                    candidate_idx = rank - 1
            else:
                needle = tok.lower()
                for idx, row in enumerate(parsed_sorted):
                    raw = row.get("raw") or {}
                    name = str(raw.get("name") or "").lower()
                    set_name = str(raw.get("setName") or "").lower()
                    if needle == name or needle in name or needle in set_name:
                        candidate_idx = idx
                        break
            if candidate_idx is not None and candidate_idx not in used:
                used.add(candidate_idx)
                selected_indices.append(candidate_idx)
            if len(selected_indices) >= target:
                break

    if len(selected_indices) < target:
        for idx in range(len(parsed_sorted)):
            if idx in used:
                continue
            used.add(idx)
            selected_indices.append(idx)
            if len(selected_indices) >= target:
                break

    return [parsed_sorted[idx] for idx in selected_indices[:target]]


def _prepare_collectible_image_for_poster(image_url: str) -> str:
    src = str(image_url or "").strip()
    if not src:
        return _TRANSPARENT_CARD_IMAGE
    try:
        from PIL import Image
        import numpy as np

        resp = requests.get(
            src,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            },
            timeout=20,
        )
        resp.raise_for_status()
        img = Image.open(io.BytesIO(resp.content)).convert("RGB")
        w, h = img.size
        if w < 50 or h < 50:
            return src

        # Crop only pure black outer margins while preserving the full slab frame.
        gray_np = np.asarray(img.convert("L"), dtype=np.uint8)
        active_cols = (gray_np > 10).mean(axis=0) > 0.01
        active_rows = (gray_np > 10).mean(axis=1) > 0.01

        if active_cols.any() and active_rows.any():
            l = int(np.argmax(active_cols))
            r = int(len(active_cols) - np.argmax(active_cols[::-1]))
            t = int(np.argmax(active_rows))
            b = int(len(active_rows) - np.argmax(active_rows[::-1]))

            # Keep a tiny safety padding so we never shave slab edges.
            pad = 2
            l = max(0, l - pad)
            t = max(0, t - pad)
            r = min(w, r + pad)
            b = min(h, b + pad)

            if (r - l) > w * 0.35 and (b - t) > h * 0.35:
                img = img.crop((l, t, r, b))

        out = io.BytesIO()
        img.save(out, format="WEBP", quality=92, method=6)
        b64 = base64.b64encode(out.getvalue()).decode("utf-8")
        return f"data:image/webp;base64,{b64}"
    except Exception:
        return src


def _trpc_collectible_list(query_payload: dict) -> dict:
    input_payload = {"0": {"json": query_payload}}
    params = {
        "batch": "1",
        "input": json.dumps(input_payload, separators=(",", ":"), ensure_ascii=False),
    }

    last_err = None
    for attempt in range(1, PROFILE_API_MAX_RETRIES + 1):
        try:
            resp = requests.get(RENAISS_COLLECTIBLE_LIST_URL, params=params, timeout=25)
            status = int(resp.status_code or 0)
            # Retry transient upstream issues.
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError("collectible.list 回傳格式異常")
            row = data[0]
            if row.get("error"):
                err = row["error"].get("json", {}).get("message") or "unknown error"
                raise RuntimeError(f"collectible.list error: {err}")
            result = (((row.get("result") or {}).get("data") or {}).get("json") or {})
            if not isinstance(result, dict):
                raise RuntimeError("collectible.list result 缺失")
            return result
        except Exception as e:
            last_err = e
            status = None
            if isinstance(e, requests.RequestException) and getattr(e, "response", None) is not None:
                status = int(e.response.status_code or 0)
            retryable = status in (408, 409, 425, 429) or (status is not None and status >= 500) or status is None
            if retryable and attempt < PROFILE_API_MAX_RETRIES:
                wait_sec = PROFILE_API_RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                time.sleep(wait_sec)
                continue
            break

    raise RuntimeError(f"collectible.list 請求失敗（已重試 {PROFILE_API_MAX_RETRIES} 次）：{last_err}")


def _fetch_user_sbt_badges(username: str | None) -> list[dict]:
    uname = str(username or "").strip()
    if not uname:
        return []
    input_payload = {"0": {"json": {"username": uname}}}
    params = {
        "batch": "1",
        "input": json.dumps(input_payload, separators=(",", ":"), ensure_ascii=False),
    }
    try:
        resp = requests.get(RENAISS_SBT_BADGES_URL, params=params, timeout=25)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or not data:
            return []
        row = data[0]
        if row.get("error"):
            return []
        badges = (((row.get("result") or {}).get("data") or {}).get("json") or {}).get("badges") or []
        out = []
        for b in badges:
            obj = b or {}
            name = str(obj.get("badgeName") or "").strip()
            if not name:
                continue
            bal = _parse_int(obj.get("balance")) or 0
            image_url = str(obj.get("badgeImageUrl") or "").strip()
            out.append(
                {
                    "name": name,
                    "balance": bal,
                    "is_owned": bool(obj.get("isOwned")),
                    "sbt_id": obj.get("sbtId"),
                    "image_url": image_url,
                }
            )
        return out
    except Exception:
        return []


def _resolve_user_from_wallet(wallet_address: str) -> tuple[str | None, str | None]:
    offset = 0
    while offset <= PROFILE_SCAN_MAX_OFFSET:
        payload = {
            "limit": PROFILE_PAGE_LIMIT,
            "offset": offset,
            "sortBy": "mintDate",
            "sortOrder": "desc",
            "includeOpenCardPackRecords": True,
        }
        result = _trpc_collectible_list(payload)
        collection = result.get("collection") or []
        for item in collection:
            owner_addr = str(item.get("ownerAddress") or "").lower()
            if owner_addr != wallet_address:
                continue
            owner = item.get("owner") or {}
            user_id = owner.get("id")
            username = owner.get("username")
            if user_id:
                return user_id, username
        pagination = result.get("pagination") or {}
        if not pagination.get("hasMore"):
            break
        step = len(collection) if isinstance(collection, list) and len(collection) > 0 else PROFILE_PAGE_LIMIT
        offset += step
    return None, None


def _fetch_user_collection(user_id: str) -> list[dict]:
    offset = 0
    all_items: list[dict] = []
    while True:
        payload = {
            "limit": PROFILE_PAGE_LIMIT,
            "offset": offset,
            "sortBy": "mintDate",
            "sortOrder": "desc",
            "userId": user_id,
            "includeOpenCardPackRecords": True,
        }
        result = _trpc_collectible_list(payload)
        collection = result.get("collection") or []
        if isinstance(collection, list):
            all_items.extend(collection)
        pagination = result.get("pagination") or {}
        if not pagination.get("hasMore"):
            break
        step = len(collection) if isinstance(collection, list) and len(collection) > 0 else PROFILE_PAGE_LIMIT
        offset += step
    return all_items


def _build_wallet_features_html(top_items: list[dict]) -> str:
    if not top_items:
        return "<p class='text-sm text-text-muted font-medium'>No collectible data.</p>"

    lines = []
    for idx, item in enumerate(top_items[:4], start=1):
        name = html_lib.escape(str(item.get("name") or "Unknown Collectible"))
        set_name = html_lib.escape(str(item.get("setName") or "Unknown Set"))
        fmv = _parse_int(item.get("fmvPriceInUSD"))
        lines.append(
            f"""
            <div class="flex items-start justify-between py-2 border-b border-black/5 last:border-b-0 gap-3">
                <div class="min-w-0">
                    <p class="text-[13px] font-black text-text-main leading-snug truncate">TOP {idx} · {name}</p>
                    <p class="text-[12px] text-text-muted leading-snug truncate">{set_name}</p>
                </div>
                <p class="text-[13px] font-black dark-metal-text whitespace-nowrap">{_format_usd(fmv)}</p>
            </div>
            """
        )
    return "".join(lines)


def _build_wallet_profile_picker_data(wallet_address: str) -> dict:
    user_id, username = _resolve_user_from_wallet(wallet_address)
    if not user_id:
        raise RuntimeError("找不到此地址的公開收藏，無法反查 userId")

    collection = _fetch_user_collection(user_id)
    if not collection:
        raise RuntimeError("已找到 userId，但目前沒有可用收藏資料")

    parsed = []
    for item in collection:
        parsed.append(
            {
                "raw": item,
                "fmv": _parse_int(item.get("fmvPriceInUSD")) or 0,
                "set": str(item.get("setName") or "").strip(),
            }
        )
    parsed_sorted = sorted(parsed, key=lambda x: x["fmv"], reverse=True)

    card_options = []
    for idx, row in enumerate(parsed_sorted[:25], start=1):
        raw = row.get("raw") or {}
        name = str(raw.get("name") or "Unknown")
        set_name = str(raw.get("setName") or "")
        fmv = _format_fmv_usd(row.get("fmv"))
        label = f"#{idx} {fmv} {name}"
        if set_name:
            label = f"{label} ({set_name})"
        card_options.append(
            {
                "value": str(idx),
                "label": label[:100],
                "full_label": label[:260],
                "description": (set_name or "Ranked by FMV")[:100],
            }
        )

    sbt_badges = _fetch_user_sbt_badges(username)
    owned_badges = [b for b in sbt_badges if b.get("is_owned") and (b.get("balance") or 0) > 0]
    sbt_options = []
    for b in owned_badges[:25]:
        name = str(b.get("name") or "").strip()
        if not name:
            continue
        sbt_id = str(b.get("sbt_id") or "").strip()
        balance = _parse_int(b.get("balance")) or 0
        value = f"id:{sbt_id}" if sbt_id else f"name:{name.lower()}"
        sbt_options.append(
            {
                "value": value[:100],
                "label": name[:100],
                "full_label": name[:160],
                "description": f"Balance {balance}"[:100],
            }
        )

    return {
        "username": str(username or "Unknown User"),
        "user_id": user_id,
        "collection_count": len(parsed_sorted),
        "card_options": card_options,
        "owned_sbt_count": len(owned_badges),
        "sbt_options": sbt_options,
    }


def _build_wallet_profile_context(
    wallet_address: str,
    selected_sbt_names: list[str] | None = None,
    enable_tilt: bool = False,
    card_count: int = 10,
    selected_cards: list[str] | None = None,
    profile_lang: str = "en",
) -> dict:
    profile_lang = _profile_lang_from_locale(profile_lang)
    ui_labels = _profile_ui_labels(profile_lang)
    user_id, username = _resolve_user_from_wallet(wallet_address)
    if not user_id:
        raise RuntimeError("找不到此地址的公開收藏，無法反查 userId")

    collection = _fetch_user_collection(user_id)
    if not collection:
        raise RuntimeError("已找到 userId，但目前沒有可用收藏資料")

    parsed = []
    for item in collection:
        parsed.append(
            {
                "raw": item,
                "fmv": _parse_int(item.get("fmvPriceInUSD")) or 0,
                "ask": _parse_int(item.get("askPriceInUSDT")),
                "set": str(item.get("setName") or "").strip(),
            }
        )

    parsed_sorted = sorted(parsed, key=lambda x: x["fmv"], reverse=True)
    hero = parsed_sorted[0]["raw"]
    total_count = len(parsed_sorted)
    listed_count = sum(1 for x in parsed_sorted if x["ask"] is not None)
    listed_ratio = (listed_count / total_count) if total_count else 0.0
    total_fmv = sum(x["fmv"] for x in parsed_sorted)
    avg_fmv = int(total_fmv / total_count) if total_count else 0

    unique_sets = {x["set"] for x in parsed_sorted if x["set"]}
    diversity_ratio = (len(unique_sets) / total_count) if total_count else 0.0

    if listed_ratio >= 0.6:
        market_heat_level, market_heat_desc = "High", "掛單比例高，市場流動性活躍。"
    elif listed_ratio >= 0.3:
        market_heat_level, market_heat_desc = "Medium", "掛單比例穩定，市場維持活躍。"
    else:
        market_heat_level, market_heat_desc = "Low", "掛單比例偏低，偏向收藏持有。"

    if total_fmv >= 100000:
        collection_value_level, collection_value_desc = "High", "整體 FMV 規模高，收藏價值強。"
    elif total_fmv >= 30000:
        collection_value_level, collection_value_desc = "Medium", "整體 FMV 穩健，具備成長空間。"
    else:
        collection_value_level, collection_value_desc = "Low", "收藏規模較小，建議持續擴充。"

    if diversity_ratio >= 0.45:
        competitive_freq_level, competitive_freq_desc = "High", "卡池分散度高，組合彈性佳。"
    elif diversity_ratio >= 0.2:
        competitive_freq_level, competitive_freq_desc = "Medium", "卡池分散適中，策略空間穩定。"
    else:
        competitive_freq_level, competitive_freq_desc = "Low", "卡池集中，偏重單一主題。"

    market_heat_width = int(min(95, max(12, round(20 + listed_ratio * 75))))
    collection_value_width = int(min(95, max(12, round(20 + min(total_fmv / 150000, 1.0) * 75))))
    competitive_freq_width = int(min(95, max(12, round(20 + diversity_ratio * 75))))

    card_count = _clamp_profile_card_count(card_count)
    top_items_raw = [x["raw"] for x in parsed_sorted[:4]]
    selected_rows = _select_profile_items(parsed_sorted, card_count, selected_cards)
    poster_items_raw = [x["raw"] for x in selected_rows]
    poster_total_fmv = sum(int(x.get("fmv") or 0) for x in selected_rows)
    top_fmv = parsed_sorted[0]["fmv"] if parsed_sorted else 0
    badge_mode = "both" if top_fmv >= 10000 else "ungraded"
    badge_html = """
    <div data-kind="ungraded" class="badge-ungraded">Wallet Holder</div>
    <div data-kind="psa10" class="badge-psa10">
        <span class="relative z-10 text-center leading-tight text-[1.1rem]">TOP<br/>VALUE</span>
    </div>
    """.strip()

    short_wallet = f"{wallet_address[:6]}...{wallet_address[-4:]}"
    hero_name = str(hero.get("name") or "Collection Overview")
    sbt_badges = _fetch_user_sbt_badges(username)
    sbt_total = sum((b.get("balance") or 0) for b in sbt_badges)
    owned_badges = [b for b in sbt_badges if b.get("is_owned") and (b.get("balance") or 0) > 0]

    selected_lookup = {str(x).strip().lower() for x in (selected_sbt_names or []) if str(x).strip()}
    if selected_lookup:
        chosen = []
        for b in owned_badges:
            name_key = str(b.get("name") or "").strip().lower()
            id_key = str(b.get("sbt_id") or "").strip().lower()
            if (
                name_key in selected_lookup
                or (name_key and f"name:{name_key}" in selected_lookup)
                or (id_key and f"id:{id_key}" in selected_lookup)
            ):
                chosen.append(b)
    else:
        chosen = list(owned_badges)
    if not chosen:
        chosen = list(owned_badges)
    chosen = chosen[:7]
    display_badges = []
    for b in chosen:
        name = str(b.get("name") or "").strip()
        if not name:
            continue
        display_badges.append(
            {
                "name": name,
                "label": _compact_sbt_label(name),
                "balance": _parse_int(b.get("balance")) or 0,
                "image": str(b.get("image_url") or "").strip(),
            }
        )
    poster_items = []
    for item in poster_items_raw:
        fmv = _parse_int(item.get("fmvPriceInUSD"))
        image = _prepare_collectible_image_for_poster(item.get("frontImageUrl") or "")
        poster_items.append(
            {
                "name": str(item.get("name") or "Unknown Collectible"),
                "image": image,
                # Use raw FMV value from source (formatted number only, no currency box/prefix).
                "value": _format_fmv_display(fmv),
            }
        )
    return {
        "username": str(username or "Unknown User"),
        "user_id": user_id,
        "wallet_address": wallet_address,
        "count": total_count,
        "total_fmv": total_fmv,
        "shown_count": len(poster_items_raw),
        "shown_fmv": poster_total_fmv,
        "template_context": {
            "collection_name": f"{username or 'Unknown'} Collection",
            "sbt_total": sbt_total,
            "sbt_badges_display": display_badges,
            "items": poster_items,
            "total_value": _format_fmv_display(poster_total_fmv),
            "total_value_label": _profile_top_value_label(len(poster_items), profile_lang),
            "items_count_label": ui_labels["items_count_label"],
            "assets_unit": ui_labels["assets_unit"],
            "sbt_badges_label": ui_labels["sbt_badges_label"],
            "no_sbt_label": ui_labels["no_sbt_label"],
            "owned_prefix": ui_labels["owned_prefix"],
            "brand_name": ui_labels["brand_name"],
            "brand_site": ui_labels["brand_site"],
            "update_date": datetime.now().strftime("%Y-%m-%d"),
            "enable_tilt": bool(enable_tilt),
        },
        "replacements": {
            "{{ card_name }}": html_lib.escape(f"{username or 'Unknown'} Vault"),
            "{{ card_set }}": html_lib.escape("Renaiss Collection"),
            "{{ card_number }}": html_lib.escape(str(total_count)),
            "{{ category }}": html_lib.escape("WALLET PROFILE"),
            "{{ badge_html }}": badge_html,
            "{{ badge_mode }}": badge_mode,
            "{{ card_image }}": html_lib.escape(str(hero.get("frontImageUrl") or _TRANSPARENT_CARD_IMAGE)),
            "{{ target_grade }}": "FMV",
            "{{ recent_avg_price }}": html_lib.escape(_format_fmv_usd(avg_fmv)),
            "{{ market_heat_level }}": market_heat_level,
            "{{ market_heat_width }}": str(market_heat_width),
            "{{ market_heat_desc }}": html_lib.escape(market_heat_desc),
            "{{ collection_value_level }}": collection_value_level,
            "{{ collection_value_width }}": str(collection_value_width),
            "{{ collection_value_desc }}": html_lib.escape(collection_value_desc),
            "{{ competitive_freq_level }}": competitive_freq_level,
            "{{ competitive_freq_width }}": str(competitive_freq_width),
            "{{ competitive_freq_desc }}": html_lib.escape(competitive_freq_desc),
            "{{ features_html }}": _build_wallet_features_html(top_items_raw),
            "{{ illustrator }}": html_lib.escape(str(username or "Unknown")),
            "{{ release_info }}": html_lib.escape(f"{short_wallet} · TOP: {hero_name[:30]}"),
        },
    }


async def _render_wallet_profile_poster(template_payload: dict, out_dir: str, safe_name: str = "wallet_profile") -> str:
    if not os.path.exists(PROFILE_TEMPLATE_PATH):
        raise FileNotFoundError(f"找不到 profile template: {PROFILE_TEMPLATE_PATH}")

    if isinstance(template_payload, dict) and (
        "replacements" in template_payload or "template_context" in template_payload
    ):
        replacements = template_payload.get("replacements") or {}
        template_context = template_payload.get("template_context") or {}
    else:
        replacements = template_payload or {}
        template_context = {}

    with open(PROFILE_TEMPLATE_PATH, "r", encoding="utf-8") as f:
        html_doc = f.read()

    if os.path.exists(PROFILE_LOGO_PATH):
        with open(PROFILE_LOGO_PATH, "rb") as logo_f:
            logo_bytes = logo_f.read()
        # Align with the original card-poster pipeline: remove edge-connected white background.
        logo_bytes = image_generator._strip_white_border_background_png(logo_bytes)
        logo_b64 = base64.b64encode(logo_bytes).decode("utf-8")
        logo_src = f"data:image/png;base64,{logo_b64}"
        html_doc = html_doc.replace('src="logo.png"', f'src="{logo_src}"').replace("src='logo.png'", f"src='{logo_src}'")

    # If the template is Jinja-style, render blocks/loops first.
    if template_context and ("{%" in html_doc or "{{" in html_doc):
        try:
            from jinja2 import Template
        except ModuleNotFoundError as e:
            raise RuntimeError("缺少套件 jinja2，請先安裝 `pip install -r requirements.txt`") from e

        html_doc = Template(html_doc).render(**template_context)

    for key, value in replacements.items():
        html_doc = html_doc.replace(key, str(value))

    os.makedirs(out_dir, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", safe_name).strip("_") or "wallet_profile"
    out_path = os.path.join(out_dir, f"{safe}_profile.png")
    await image_generator._render_single_html_poster(
        html_doc,
        out_path,
        width=1200,
        height=900,
        device_scale_factor=2,
    )
    return out_path


def smart_split(text, limit=1900):
    chunks = []
    current_chunk = ""
    for line in text.split('\n'):
        if len(current_chunk) + len(line) + 1 > limit:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = line + "\n"
        else:
            current_chunk += line + "\n"
    if current_chunk.strip():
        chunks.append(current_chunk.strip())
    return chunks


LANG_LABELS = {
    "zh": "繁體中文",
    "zhs": "简体中文",
    "en": "English",
    "ko": "한국어",
}


LANG_FLAGS = {
    "zh": "🇹🇼",
    "zhs": "🇨🇳",
    "en": "🇺🇸",
    "ko": "🇰🇷",
}


def _t(lang: str, zh: str, en: str, ko: str, zhs: str | None = None):
    if lang == "en":
        return en
    if lang == "ko":
        return ko
    if lang == "zhs":
        return zhs if zhs is not None else zh
    return zh


def _translation_target_name(lang: str) -> str:
    return {
        "zh": "Traditional Chinese",
        "zhs": "Simplified Chinese",
        "en": "English",
        "ko": "Korean",
    }.get(lang, "Traditional Chinese")


def _parse_lang_override(content_lower: str) -> str | None:
    if any(tok in content_lower for tok in ["!zhs", "!cn", "简体", "簡體", "簡中", "zh-cn", "zh_hans"]):
        return "zhs"
    if any(tok in content_lower for tok in ["!ko", " ko", "韓文", "韓語", "korean"]):
        return "ko"
    if any(tok in content_lower for tok in ["!en", " en", "英文", "english"]):
        return "en"
    if any(tok in content_lower for tok in ["!zh", " zh", "中文", "chinese"]):
        return "zh"
    return None


def _json_loads_loose(text: str):
    cleaned = (text or "").replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(cleaned)
    except Exception:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start >= 0 and end > start:
            return json.loads(cleaned[start:end + 1])
        raise


class LanguageSelectView(discord.ui.View):
    def __init__(self, author_id: int, timeout_seconds: int = 10):
        super().__init__(timeout=timeout_seconds + 1)
        self.author_id = author_id
        self.timeout_seconds = timeout_seconds
        self.selected_lang = None
        self._event = asyncio.Event()

    def _disable_all(self):
        for child in self.children:
            child.disabled = True

    async def _choose(self, interaction: discord.Interaction, lang_code: str):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the original sender can choose language.", ephemeral=True)
            return
        self.selected_lang = lang_code
        self._disable_all()
        self._event.set()
        done_msg = _t(
            lang_code,
            f"✅ 語言已選擇：{LANG_FLAGS.get(lang_code, '🌐')} **{LANG_LABELS.get(lang_code, '繁體中文')}**",
            f"✅ Language selected: {LANG_FLAGS.get(lang_code, '🌐')} **{LANG_LABELS.get(lang_code, 'English')}**",
            f"✅ 언어 선택 완료: {LANG_FLAGS.get(lang_code, '🌐')} **{LANG_LABELS.get(lang_code, '한국어')}**",
            f"✅ 语言已选择：{LANG_FLAGS.get(lang_code, '🌐')} **{LANG_LABELS.get(lang_code, '简体中文')}**",
        )
        await interaction.response.edit_message(
            content=done_msg,
            view=self
        )
        self.stop()

    @discord.ui.button(label="🇹🇼 繁體中文", style=discord.ButtonStyle.primary)
    async def choose_zh(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, "zh")

    @discord.ui.button(label="🇨🇳 简体中文", style=discord.ButtonStyle.primary)
    async def choose_zhs(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, "zhs")

    @discord.ui.button(label="🇺🇸 English", style=discord.ButtonStyle.secondary)
    async def choose_en(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, "en")

    @discord.ui.button(label="🇰🇷 한국어", style=discord.ButtonStyle.success)
    async def choose_ko(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._choose(interaction, "ko")

    async def wait_for_choice(self):
        try:
            await asyncio.wait_for(self._event.wait(), timeout=self.timeout_seconds)
            return self.selected_lang, True
        except asyncio.TimeoutError:
            return None, False


async def choose_language_for_message(message: discord.Message) -> str:
    prompt = "🌐 請選擇輸出語言（10 秒內未選擇將預設 🇹🇼 繁體中文）"
    view = LanguageSelectView(message.author.id, timeout_seconds=10)
    choose_msg = await message.reply(prompt, view=view)
    lang, selected = await view.wait_for_choice()

    if not selected:
        lang = "zh"
        view._disable_all()
        try:
            await choose_msg.edit(content="⏱️ 10 秒未選擇，已使用預設語言：🇹🇼 **繁體中文**。", view=view)
        except Exception:
            pass
    return lang


def _get_translation_provider_order():
    preferred = (os.getenv("TRANSLATION_PROVIDER") or os.getenv("VISION_PROVIDER") or "google").strip().lower()
    providers = ["google", "openai"]
    if preferred in providers:
        return [preferred] + [p for p in providers if p != preferred]
    return providers


def _get_translation_keys():
    return {
        "google": (os.getenv("GOOGLE_API_KEY") or "").strip(),
        "openai": (os.getenv("OPENAI_API_KEY") or "").strip(),
    }


async def _translate_json_with_google(payload: dict, target_lang: str, api_key: str):
    model = (
        os.getenv("GOOGLE_TEXT_MODEL")
        or os.getenv("GOOGLE_VISION_MODEL")
        or os.getenv("GEMINI_MODEL")
        or market_report_vision.DEFAULT_GEMINI_MODEL
    ).strip()
    if model.startswith("models/"):
        model = model.split("/", 1)[1]
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    prompt = (
        f"You are a translation engine. Translate all user-facing text in the input JSON to {_translation_target_name(target_lang)}.\n"
        "Rules:\n"
        "1) Keep JSON structure and keys exactly the same.\n"
        "2) Preserve URLs, markdown links, emojis, numbers, currency values, set codes, card numbers, and grades.\n"
        "3) For market_heat / collection_value / competitive_freq, keep the first level token exactly High or Medium or Low, then a space and translated description.\n"
        "4) Output valid JSON only.\n\n"
        f"Input JSON:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"responseMimeType": "application/json"},
    }

    def _do_request():
        try:
            resp = requests.post(url, headers={"Content-Type": "application/json"}, json=body, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return None
            parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
            for p in parts:
                if isinstance(p, dict) and p.get("text"):
                    return _json_loads_loose(p["text"])
            return None
        except Exception:
            return None

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _do_request)


async def _translate_json_with_openai(payload: dict, target_lang: str, api_key: str):
    model = (os.getenv("OPENAI_TEXT_MODEL") or "gpt-4o-mini").strip()
    prompt = (
        f"Translate all user-facing text in this JSON to {_translation_target_name(target_lang)}.\n"
        "Keep keys/structure unchanged. Preserve URLs, markdown links, emojis, numbers, currency, set codes, card numbers, grades.\n"
        "For market_heat / collection_value / competitive_freq, keep the first token exactly High/Medium/Low.\n"
        "Return JSON only.\n\n"
        f"JSON:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    req_body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
    }

    def _do_request():
        try:
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=req_body,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
            return _json_loads_loose(content)
        except Exception:
            return None

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _do_request)


async def translate_report_and_poster(report_text, poster_data, lang="zh"):
    if lang == "zh":
        return report_text, poster_data

    source_fields = {}
    if isinstance(poster_data, dict):
        card_info = poster_data.get("card_info") or {}
        source_fields = {
            "c_name": card_info.get("c_name", ""),
            "release_info": card_info.get("release_info", ""),
            "market_heat": card_info.get("market_heat", ""),
            "features": card_info.get("features", ""),
            "collection_value": card_info.get("collection_value", ""),
            "competitive_freq": card_info.get("competitive_freq", ""),
            "illustrator": card_info.get("illustrator", ""),
        }

    payload = {
        "report_text": report_text or "",
        "poster_fields": source_fields,
    }

    keys = _get_translation_keys()
    translated = None
    for provider in _get_translation_provider_order():
        api_key = keys.get(provider, "")
        if not api_key:
            continue
        if provider == "google":
            translated = await _translate_json_with_google(payload, lang, api_key)
        elif provider == "openai":
            translated = await _translate_json_with_openai(payload, lang, api_key)
        if translated:
            break

    if not isinstance(translated, dict):
        return report_text, poster_data

    new_report = translated.get("report_text", report_text) or report_text
    if not isinstance(poster_data, dict):
        return new_report, poster_data

    poster_fields = translated.get("poster_fields") if isinstance(translated.get("poster_fields"), dict) else {}
    new_poster = dict(poster_data)
    new_card_info = dict(new_poster.get("card_info") or {})
    for k, v in poster_fields.items():
        if isinstance(v, str) and v.strip():
            new_card_info[k] = v.strip()
    if lang == "en":
        new_card_info["c_name"] = poster_fields.get("c_name") or new_card_info.get("name") or new_card_info.get("c_name", "")
    elif lang == "ko":
        new_card_info["c_name"] = poster_fields.get("c_name") or new_card_info.get("c_name") or new_card_info.get("name", "")
    elif lang == "zhs":
        new_card_info["c_name"] = poster_fields.get("c_name") or new_card_info.get("c_name") or new_card_info.get("name", "")

    new_card_info["ui_lang"] = lang
    new_poster["card_info"] = new_card_info
    return new_report, new_poster

class VersionSelectView(discord.ui.View):
    """
    版本選擇按鈕 View (航海王專用)。
    """
    def __init__(self, candidates, lang="zh"):
        super().__init__(timeout=180)  # 3 分鐘超時
        self.chosen_url = None
        self._event = asyncio.Event()
        self.candidates = candidates
        self.lang = lang
        
        # 動態建立按鈕
        for i, url in enumerate(candidates, start=1):
            btn_label = _t(lang, f"選擇版本 {i}", f"Select Version {i}", f"버전 {i} 선택", f"选择版本 {i}")
            btn = discord.ui.Button(label=btn_label, style=discord.ButtonStyle.primary, custom_id=f"ver_{i}")
            btn.callback = self.make_callback(url, i)
            self.add_item(btn)

    def make_callback(self, url, idx):
        async def callback(interaction: discord.Interaction):
            self.chosen_url = url
            self._event.set()
            done_msg = _t(
                self.lang,
                f"✅ 已選擇 **第 {idx} 個版本**，繼續生成報告...",
                f"✅ Selected **Version {idx}**, continuing report generation...",
                f"✅ **버전 {idx}** 선택 완료, 리포트를 계속 생성합니다...",
                f"✅ 已选择 **第 {idx} 个版本**，继续生成报告..."
            )
            await interaction.response.edit_message(content=done_msg, view=None)
        return callback

    async def wait_for_choice(self) -> str | None:
        try:
            await asyncio.wait_for(self._event.wait(), timeout=180)
            return self.chosen_url
        except asyncio.TimeoutError:
            return None


async def _handle_image_impl(attachment, message, lang="zh"):
    """
    ** 並發核心函數（stream 模式）**

    流程：
    1. 建立討論串並加入使用者
    2. 下載圖片
    3. AI 分析 + 爬蟲 → 立即傳送文字報告
    4. （非同步）生成海報 → 生成完成後補傳
    """
    # 根據語言設定討論串名稱
    thread_name = _t(lang, "卡片分析報表", "Card Analysis Report", "카드 분석 리포트", "卡片分析报表")
    
    # 1. 建立討論串並加入使用者
    # 先發送一個初始訊息作為討論串的起點
    init_msg = await message.reply(
        _t(
            lang,
            f"🃏 收到圖片，分析語言：{LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '繁體中文')}**...",
            f"🃏 Image received. Analysis language: {LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, 'English')}**...",
            f"🃏 이미지를 받았습니다. 분석 언어: {LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '한국어')}**...",
            f"🃏 收到图片，分析语言：{LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '简体中文')}**..."
        )
    )
    
    thread = await init_msg.create_thread(name=thread_name, auto_archive_duration=60)
    
    # 主動把使用者加入討論串，確保他會收到通知並看到視窗
    await thread.add_user(message.author)

    # 立即傳送第一則訊息，提供即時回饋
    analyzing_msg = _t(
        lang,
        "🔍 正在分析圖片中，請稍候...",
        "🔍 Analyzing image, please wait...",
        "🔍 이미지를 분석 중입니다. 잠시만 기다려주세요...",
        "🔍 正在分析图片，请稍候..."
    )
    await thread.send(analyzing_msg)

    # 3. 建立暫存資料夾（海報存這裡）
    card_out_dir = tempfile.mkdtemp(prefix=f"tcg_bot_{message.id}_")
    img_path = os.path.join(card_out_dir, attachment.filename)
    await attachment.save(img_path)

    try:
        print(f"⚙️ [並發] 開始分析: {attachment.filename} (lang={lang}, 來自 {message.author})")
        poster_data = None

        async with REPORT_SEMAPHORE:
            market_report_vision.REPORT_ONLY = True
            api_key = os.getenv("MINIMAX_API_KEY")

            # 統一先用中文產出，再針對使用者語言做後置翻譯（確保報告格式穩定）。
            result = await market_report_vision.process_single_image(
                img_path, api_key, out_dir=card_out_dir, stream_mode=True, lang="zh"
            )

            # 傳送 AI 模型切換通知（如 Minimax → GPT-4o-mini 備援）
            for _status in market_report_vision.get_and_clear_notify_msgs():
                await thread.send(_status)

            # 處理「需要版本選擇」的狀態 (航海王)
            if isinstance(result, dict) and result.get("status") == "need_selection":
                candidates = result["candidates"]
                # 去重並保留順序
                candidates = list(dict.fromkeys(candidates))
                
                await thread.send(
                    _t(
                        lang,
                        "⚠️ 偵測到**航海王**有多個候選版本，請根據下方預覽圖選擇正確的版本：",
                        "⚠️ Multiple **One Piece** candidates found. Please choose the correct version from the previews below:",
                        "⚠️ **원피스** 후보가 여러 개 감지되었습니다. 아래 미리보기에서 올바른 버전을 선택해주세요:",
                        "⚠️ 检测到**航海王**有多个候选版本，请根据下方预览图选择正确版本："
                    )
                )
                
                # 抓取每個候選版本的縮圖並以 Embed 呈現
                loading_msg = await thread.send(
                    _t(
                        lang,
                        "🖼️ 正在抓取版本預覽中...",
                        "🖼️ Fetching candidate previews...",
                        "🖼️ 후보 미리보기를 불러오는 중...",
                        "🖼️ 正在抓取版本预览中..."
                    )
                )
                loop = asyncio.get_running_loop()
                
                for i, url in enumerate(candidates, start=1):
                    # 這裡改為順序執行並加上 skip_hi_res=True 以加快速度
                    print(f"DEBUG: Fetching thumbnail for candidate {i}: {url}")
                    _re, _url, thumb_url = await loop.run_in_executor(None, lambda: market_report_vision._fetch_pc_prices_from_url(url, skip_hi_res=True))
                    slug = url.split('/')[-1]
                    
                    embed = discord.Embed(title=f"版本 #{i}", description=f"Slug: `{slug}`", url=url, color=0x3498db)
                    if thumb_url:
                        embed.set_thumbnail(url=thumb_url)
                    else:
                        embed.description += "\n*(無法取得預覽圖)*"
                        print(f"DEBUG: Failed to find thumbnail for {url}")
                    await thread.send(embed=embed)

                await loading_msg.delete()

                ver_view = VersionSelectView(candidates, lang=lang)
                await thread.send(
                    _t(
                        lang,
                        "請點選下方按鈕進行選擇：",
                        "Please choose by clicking one of the buttons below:",
                        "아래 버튼 중 하나를 눌러 선택해주세요:",
                        "请点击下方按钮进行选择："
                    ),
                    view=ver_view
                )
                selected_url = await ver_view.wait_for_choice()

                if not selected_url:
                    await thread.send(
                        _t(lang, "⏰ 選擇逾時，已中止。", "⏰ Selection timed out. Stopped.", "⏰ 선택 시간이 초과되어 중단되었습니다.", "⏰ 选择超时，已中止。")
                    )
                    return

                # 使用選擇的 URL 重新抓取並完成報告
                final_pc_res = await loop.run_in_executor(None, market_report_vision._fetch_pc_prices_from_url, selected_url)
                pc_records, pc_url, pc_img_url = final_pc_res
                
                snkr_result = result["snkr_result"]
                snkr_records, final_img_url, snkr_url = snkr_result if snkr_result else (None, None, None)
                if not final_img_url and pc_img_url:
                    final_img_url = pc_img_url
                
                jpy_rate = market_report_vision.get_exchange_rate()
                # 呼叫 helper 完成剩餘流程
                result = await market_report_vision.finish_report_after_selection(
                    result["card_info"],
                    pc_records,
                    pc_url,
                    pc_img_url,
                    snkr_records,
                    final_img_url,
                    snkr_url,
                    jpy_rate,
                    result["out_dir"],
                    result.get("poster_version", "v3"),
                    result["lang"],
                    stream_mode=True,
                )

            if isinstance(result, tuple):
                report_text, poster_data = result
            else:
                report_text = result
                poster_data = None

            if isinstance(poster_data, dict):
                ci = dict(poster_data.get("card_info") or {})
                ci["ui_lang"] = lang
                poster_data["card_info"] = ci

            if report_text and lang in ("en", "ko", "zhs"):
                try:
                    report_text, poster_data = await translate_report_and_poster(report_text, poster_data, lang=lang)
                except Exception as translate_err:
                    print(f"⚠️ 翻譯失敗，回退原始中文: {translate_err}")
                    await thread.send(
                        _t(
                            lang,
                            "⚠️ 翻譯失敗，已改用中文原文輸出。",
                            "⚠️ Translation failed. Falling back to Chinese output.",
                            "⚠️ 번역에 실패하여 중국어 원문으로 출력합니다.",
                            "⚠️ 翻译失败，已改用繁体中文原文输出。"
                        )
                    )

            # 4. 立即傳送文字報告
            if report_text:
                if report_text.startswith("❌"):
                    await thread.send(report_text)
                else:
                    for chunk in smart_split(report_text):
                        await thread.send(chunk)
            else:
                err_msg = _t(
                    lang,
                    "❌ 分析失敗：未發現卡片資訊或發生未知錯誤。",
                    "❌ Analysis failed: No card info found or unknown error.",
                    "❌ 분석 실패: 카드 정보를 찾지 못했거나 알 수 없는 오류가 발생했습니다.",
                    "❌ 分析失败：未发现卡片信息或发生未知错误。"
                )
                await thread.send(err_msg)
                return

        # 5. 生成海報
        if poster_data:
            wait_msg = _t(
                lang,
                "🖼️ 海報生成中，請稍候...",
                "🖼️ Generating poster, please wait...",
                "🖼️ 포스터 생성 중입니다. 잠시만 기다려주세요...",
                "🖼️ 海报生成中，请稍候..."
            )
            await thread.send(wait_msg)
            try:
                async with POSTER_SEMAPHORE:
                    out_paths = await market_report_vision.generate_posters(poster_data)
                if out_paths:
                    for path in out_paths:
                        if os.path.exists(path):
                            await thread.send(file=discord.File(path))
                else:
                    fail_msg = _t(
                        lang,
                        "⚠️ 海報生成失敗，但文字報告已完成。",
                        "⚠️ Poster generation failed, but the text report is complete.",
                        "⚠️ 포스터 생성은 실패했지만 텍스트 리포트는 완료되었습니다.",
                        "⚠️ 海报生成失败，但文字报告已完成。"
                    )
                    await thread.send(fail_msg)
            except Exception as poster_err:
                err_msg = _t(
                    lang,
                    f"⚠️ 海報生成時發生錯誤：{poster_err}",
                    f"⚠️ Poster generation error: {poster_err}",
                    f"⚠️ 포스터 생성 중 오류가 발생했습니다: {poster_err}",
                    f"⚠️ 海报生成时发生错误：{poster_err}"
                )
                await thread.send(err_msg)

    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"❌ 分析失敗 ({attachment.filename}): {e}", file=sys.stderr)
        await thread.send(
            f"❌ System error:\n```python\n{error_trace[-1900:]}\n```"
        )

    finally:
        shutil.rmtree(card_out_dir, ignore_errors=True)
        print(f"✅ [並發] 完成並清理: {attachment.filename}")


async def handle_image(attachment, message, lang="zh"):
    await _handle_image_impl(attachment, message, lang=lang)


@client.event
async def on_ready():
    # Attempt to sync global commands
    try:
        await tree.sync()
        print(f'✅ 機器人已成功登入為 {client.user}')
        print(f"✅ 全域 Slash Commands 同步嘗試完成 (全球更新可能需 1 小時)")
    except Exception as e:
        print(f"⚠️ 全域同步失敗: {e}")

class PCSelect(discord.ui.Select):
    def __init__(self, candidates):
        options = []
        for i, c in enumerate(candidates[:25], start=1):
            prefix = f"#{i} — "
            slug = c.split('/')[-1][:100 - len(prefix)]
            label = prefix + slug
            options.append(discord.SelectOption(label=label, value=c[:100], description=c[:100]))
        super().__init__(placeholder="請選擇 PriceCharting 的正確版本...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_pc = self.values[0]
        for orig in self.view.pc_candidates:
            if orig.startswith(self.values[0]):
                self.view.selected_pc = orig
                break
        await interaction.response.defer()

class SnkrSelect(discord.ui.Select):
    def __init__(self, candidates):
        options = []
        for i, c in enumerate(candidates[:25], start=1):
            prefix = f"#{i} — "
            raw_text = c.split(" — ")[1] if " — " in c else c.split("/")[-1]
            label_text = raw_text[:100 - len(prefix)]
            label = prefix + label_text
            val = c.split(" — ")[0][:100]
            options.append(discord.SelectOption(label=label, value=val))
        super().__init__(placeholder="請選擇 SNKRDUNK 的正確版本...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_snkr = self.values[0]
        await interaction.response.defer()

class ManualCandidateView(discord.ui.View):
    def __init__(self, card_info, pc_candidates, snkr_candidates, lang="zh"):
        super().__init__(timeout=600)
        self.card_info = card_info
        self.pc_candidates = pc_candidates
        self.snkr_candidates = snkr_candidates
        self.lang = lang
        self.selected_pc = None
        self.selected_snkr = None
        self.original_message = None
        
        if pc_candidates:
            self.add_item(PCSelect(pc_candidates))
        if snkr_candidates:
            self.add_item(SnkrSelect(snkr_candidates))
            
    @discord.ui.button(label="確認選擇並生成報告", style=discord.ButtonStyle.primary, row=2)
    async def submit(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("🔍 正在生成報告與海報，請稍候...", ephemeral=False)
        self.stop()
        for child in self.children:
            child.disabled = True
        if self.original_message:
            await self.original_message.edit(view=self)
        asyncio.create_task(self.generate_report(interaction))
        
    async def generate_report(self, interaction: discord.Interaction):
        thread = interaction.channel # In this version, we start in the thread
        card_out_dir = tempfile.mkdtemp(prefix=f"tcg_manual_{id(self)}_")
        try:
            # 2. Generate text report and poster_data
            async with REPORT_SEMAPHORE:
                result = await market_report_vision.generate_report_from_selected(
                    self.card_info, self.selected_pc, self.selected_snkr, out_dir=card_out_dir, lang=self.lang
                )
            
            report_text, poster_data = result if isinstance(result, tuple) else (result, None)
            
            if report_text:
                for chunk in smart_split(report_text):
                    await thread.send(chunk)
            
            # 3. Generate and send posters
            if poster_data:
                wait_msg = "🖼️ Generating poster..." if self.lang == "en" else "🖼️ 海報生成中..."
                await thread.send(wait_msg)
                async with POSTER_SEMAPHORE:
                    out_paths = await market_report_vision.generate_posters(poster_data)
                if out_paths:
                    for path in out_paths:
                        if os.path.exists(path):
                            await thread.send(file=discord.File(path))
        except Exception as e:
            error_trace = traceback.format_exc()
            await thread.send(f"❌ 執行異常：\n```python\n{error_trace[-1900:]}\n```")
        finally:
            shutil.rmtree(card_out_dir, ignore_errors=True)


class ProfileLangSelect(discord.ui.Select):
    def __init__(self, default_lang: str):
        options = [
            discord.SelectOption(
                label=f"{LANG_FLAGS.get('zh', '🇹🇼')} {LANG_LABELS.get('zh', '繁體中文')}",
                value="zh",
                default=(default_lang == "zh"),
            ),
            discord.SelectOption(
                label=f"{LANG_FLAGS.get('zhs', '🇨🇳')} {LANG_LABELS.get('zhs', '简体中文')}",
                value="zhs",
                default=(default_lang == "zhs"),
            ),
            discord.SelectOption(
                label=f"{LANG_FLAGS.get('en', '🇺🇸')} {LANG_LABELS.get('en', 'English')}",
                value="en",
                default=(default_lang == "en"),
            ),
            discord.SelectOption(
                label=f"{LANG_FLAGS.get('ko', '🇰🇷')} {LANG_LABELS.get('ko', '한국어')}",
                value="ko",
                default=(default_lang == "ko"),
            ),
        ]
        super().__init__(placeholder="1) 選擇語言", min_values=1, max_values=1, options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_lang = self.values[0]
        await self.view.refresh(interaction)


class ProfileTemplateSelect(discord.ui.Select):
    def __init__(self, default_count: int = 10, placeholder: str = "Template"):
        options = [
            discord.SelectOption(label="Top 1", value="1", default=(default_count == 1)),
            discord.SelectOption(label="Top 3", value="3", default=(default_count == 3)),
            discord.SelectOption(label="Top 10", value="10", default=(default_count == 10)),
        ]
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, options=options, row=1)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_template = _clamp_profile_card_count(_parse_int(self.values[0]))
        await self.view.refresh(interaction)


class ProfileSBTSelect(discord.ui.Select):
    def __init__(self, options_data: list[dict], placeholder: str = "SBT", no_option_label: str = "No SBT option"):
        options = []
        for item in options_data[:25]:
            options.append(
                discord.SelectOption(
                    label=str(item.get("label") or "SBT")[:100],
                    value=str(item.get("value") or "")[:100],
                    description=str(item.get("description") or "")[:100] or None,
                )
            )
        max_vals = min(7, len(options)) if options else 1
        super().__init__(
            placeholder=placeholder,
            min_values=0,
            max_values=max_vals,
            options=options or [discord.SelectOption(label=no_option_label[:100], value="__none__")],
            row=2,
        )
        if not options:
            self.disabled = True

    async def callback(self, interaction: discord.Interaction):
        vals = [v for v in self.values if v != "__none__"]
        self.view.selected_sbt_values = vals
        await self.view.refresh(interaction)


class ProfileCardSelect(discord.ui.Select):
    def __init__(self, options_data: list[dict], placeholder: str = "Cards", no_option_label: str = "No card option"):
        options = []
        for item in options_data[:25]:
            options.append(
                discord.SelectOption(
                    label=str(item.get("label") or "Card")[:100],
                    value=str(item.get("value") or "")[:100],
                    description=str(item.get("description") or "")[:100] or None,
                )
            )
        max_vals = min(10, len(options)) if options else 1
        super().__init__(
            placeholder=placeholder,
            min_values=0,
            max_values=max_vals,
            options=options or [discord.SelectOption(label=no_option_label[:100], value="__none__")],
            row=3,
        )
        if not options:
            self.disabled = True

    async def callback(self, interaction: discord.Interaction):
        vals = [v for v in self.values if v != "__none__"]
        self.view.selected_card_values = vals
        await self.view.refresh(interaction)


class ProfileConfigView(discord.ui.View):
    def __init__(self, author_id: int, wallet: str, picker_data: dict, selected_lang: str):
        super().__init__(timeout=180)
        self.author_id = author_id
        self.wallet = wallet
        self.picker_data = picker_data or {}
        self.username = str((picker_data or {}).get("username") or "Unknown")

        self.selected_lang = _profile_lang_from_locale(selected_lang)
        self.texts = _profile_wizard_texts(self.selected_lang)
        self.selected_template = 10
        self.selected_sbt_values: list[str] = []
        self.selected_card_values: list[str] = []
        self.bound_message: discord.Message | None = None

        self.card_options = list((picker_data or {}).get("card_options") or [])
        self.sbt_options = list((picker_data or {}).get("sbt_options") or [])
        self.card_label_map = {
            str(x.get("value") or ""): str(x.get("full_label") or x.get("label") or "")
            for x in self.card_options
        }
        self.sbt_label_map = {
            str(x.get("value") or ""): str(x.get("full_label") or x.get("label") or "")
            for x in self.sbt_options
        }

        self.add_item(ProfileTemplateSelect(self.selected_template, placeholder=self.texts["template_placeholder"]))
        self.add_item(
            ProfileSBTSelect(
                self.sbt_options,
                placeholder=self.texts["sbt_placeholder"],
                no_option_label=self.texts["no_sbt_option"],
            )
        )
        self.add_item(
            ProfileCardSelect(
                self.card_options,
                placeholder=self.texts["card_placeholder"],
                no_option_label=self.texts["no_card_option"],
            )
        )
        self.quick_default.label = self.texts["default_button"][:80]
        self.generate.label = self.texts["generate_button"][:80]

    def _selected_preview(self, values: list[str], label_map: dict[str, str], max_items: int = 3) -> str:
        names = []
        for v in values:
            text = str(label_map.get(v) or v).strip()
            if str(v).strip().isdigit():
                text = f"#{v}"
            if text:
                names.append(text)
        if not names:
            return self.texts["default_short"]
        sample = ", ".join(names[:max_items])
        if len(names) > max_items:
            sample += " ..."
        return f"{self.texts['selected_short']} {len(names)} ({sample})"

    def _selected_lines(self, values: list[str], label_map: dict[str, str]) -> str:
        if not values:
            return f"- {self.texts['default_short']}"
        rows = []
        for v in values:
            text = str(label_map.get(v) or v).strip()
            if str(v).strip().isdigit():
                rank_prefix = f"#{v}"
                if text.startswith(rank_prefix):
                    pass
                else:
                    text = f"{rank_prefix} · {text}"
            rows.append(f"- {text}")
        return "\n".join(rows)

    def bind_message(self, message: discord.Message):
        self.bound_message = message

    def render_message(self) -> str:
        collection_count = int((self.picker_data or {}).get("collection_count") or 0)
        owned_sbt_count = int((self.picker_data or {}).get("owned_sbt_count") or 0)
        lang_text = f"{LANG_FLAGS.get(self.selected_lang, '🌐')} {LANG_LABELS.get(self.selected_lang, self.selected_lang)}"
        template_text = f"Top {self.selected_template}"
        sbt_text = self._selected_lines(self.selected_sbt_values, self.sbt_label_map)
        card_text = self._selected_lines(self.selected_card_values, self.card_label_map)
        return (
            f"🎛️ **{self.username}** · {self.texts['panel_title']}\n"
            f"{self.texts['wallet_label']}: `{self.wallet}`\n"
            f"{self.texts['collection_label']}: **{collection_count}** ｜ "
            f"{self.texts['selectable_sbt_label']}: **{owned_sbt_count}**\n\n"
            f"**{self.texts['current_selection_title']}**\n"
            f"{self.texts['lang_selected_label']}: **{lang_text}**\n"
            f"{self.texts['template_selected_label']}: **{template_text}**\n"
            f"{self.texts['sbt_selected_label']}:\n{sbt_text}\n"
            f"{self.texts['cards_selected_label']}:\n{card_text}\n\n"
            f"{self.texts['setup_tip']}"
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(self.texts["only_owner_msg"], ephemeral=True)
            return False
        return True

    async def refresh(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content=self.render_message(), view=self)

    def _disable_all(self):
        for child in self.children:
            child.disabled = True

    async def on_timeout(self):
        self._disable_all()
        if self.bound_message:
            try:
                await self.bound_message.edit(content=self.texts["timeout_msg"], view=self)
            except Exception:
                pass

    async def _run_generate(self, interaction: discord.Interaction, use_default: bool = False):
        self._disable_all()
        await interaction.response.edit_message(content=self.texts["generating_msg"], view=self)

        selected_sbt = [] if use_default else list(self.selected_sbt_values)
        selected_cards = [] if use_default else list(self.selected_card_values)
        template_count = self.selected_template
        out_dir = tempfile.mkdtemp(prefix=f"tcg_wallet_cfg_{interaction.id}_")
        loop = asyncio.get_running_loop()

        try:
            profile_ctx = await loop.run_in_executor(
                None,
                _build_wallet_profile_context,
                self.wallet,
                selected_sbt,
                False,
                template_count,
                selected_cards,
                self.selected_lang,
            )
            safe_name = f"{self.username}_{self.wallet[-6:]}"
            async with POSTER_SEMAPHORE:
                poster_path = await _render_wallet_profile_poster(profile_ctx, out_dir, safe_name=safe_name)
            summary = (
                f"✅ **{self.username}** · {self.texts['summary_done']}\n"
                f"{self.texts['wallet_label']}: `{self.wallet}`\n"
                f"{self.texts['shown_label']}: **{profile_ctx.get('shown_count', 0)}** | "
                f"{self.texts['shown_fmv_label']}: **{_format_fmv_usd(_parse_int(profile_ctx.get('shown_fmv')))}**"
            )
            await interaction.followup.send(summary, file=discord.File(poster_path))
        except Exception as e:
            print(f"❌ ProfileConfigView 生成失敗: {e}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            await interaction.followup.send(f"❌ 生成失敗：{e}")
        finally:
            shutil.rmtree(out_dir, ignore_errors=True)
            self.stop()

    @discord.ui.button(label="使用預設直接生成", style=discord.ButtonStyle.secondary, row=4)
    async def quick_default(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run_generate(interaction, use_default=True)

    @discord.ui.button(label="生成海報", style=discord.ButtonStyle.primary, row=4)
    async def generate(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run_generate(interaction, use_default=False)

@tree.command(name="manual_analyze", description="手動選擇版本生成報告與海報")
async def manual_analyze(interaction: discord.Interaction, image: discord.Attachment, lang: str = "zh"):
    if not any(image.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.webp']):
        await interaction.response.send_message("❌ 請上傳有效的圖片", ephemeral=True)
        return
        
    await interaction.response.send_message(f"🔍 收到圖片，正在建立分析討論串...", ephemeral=False)
    resp = await interaction.original_response()
    thread_name = "卡片分析 (手動模式)" if lang == "zh" else "Card Analysis (Manual)"
    thread = await resp.create_thread(name=thread_name, auto_archive_duration=60)
    await thread.add_user(interaction.user)
    
    temp_dir = tempfile.gettempdir()
    img_path = os.path.join(temp_dir, f"manual_{image.id}_{image.filename}")
    await image.save(img_path)
    
    try:
        await thread.send(f"⏳ 正在辨識卡片影像中 (語言: {lang})...")
        api_key = os.getenv("MINIMAX_API_KEY")
        res = await market_report_vision.process_image_for_candidates(img_path, api_key, lang=lang)
        if not res or len(res) < 2:
            await thread.send(f"❌ 分析失敗: {res[1] if res else '未知錯誤'}")
            return
            
        card_info, candidates = res
        pc_candidates = candidates.get("pc", [])
        snkr_candidates = candidates.get("snkr", [])
        
        if not pc_candidates and not snkr_candidates:
            await thread.send("❌ 找不到任何符合的卡片候補。")
            return
            
        # --- 抓取預覽圖 ---
        await thread.send("🖼️ 正在抓取候選版本預覽圖，請稍候...")
        loop = asyncio.get_running_loop()
        embeds = []
        
        # 1. PC 預覽 (Top 5)
        for i, url in enumerate(pc_candidates[:5], start=1):
            try:
                _re, _url, thumb_url = await loop.run_in_executor(None, lambda: market_report_vision._fetch_pc_prices_from_url(url, skip_hi_res=True))
                slug = url.split('/')[-1]
                embed = discord.Embed(title=f"PriceCharting 候選 #{i}", description=f"Slug: `{slug}`", url=url, color=0x3498db)
                if thumb_url: embed.set_thumbnail(url=thumb_url)
                embeds.append(embed)
            except: pass
            
        # 2. SNKRDUNK 預覽 (Top 5)
        for i, cand in enumerate(snkr_candidates[:5], start=1):
            try:
                url = cand.split(" — ")[0]
                title = cand.split(" — ")[1] if " — " in cand else "SNKRDUNK Item"
                _re, thumb_url = await loop.run_in_executor(None, market_report_vision._fetch_snkr_prices_from_url_direct, url)
                embed = discord.Embed(title=f"SNKRDUNK 候選 #{i}", description=title, url=url, color=0xe67e22)
                if thumb_url: embed.set_thumbnail(url=thumb_url)
                embeds.append(embed)
            except: pass

        # 分批發送 Embeds
        for i in range(0, len(embeds), 5):
            await thread.send(embeds=embeds[i:i+5])
            
        view = ManualCandidateView(card_info, pc_candidates, snkr_candidates, lang=lang)
        info_text = f"**手動模式：候選卡片選擇**\n"
        info_text += f"> **名稱:** {card_info.get('name', '')} ({card_info.get('jp_name', '')})\n"
        info_text += f"> **編號:** {card_info.get('number', '')}\n"
        info_text += "請根據上方圖片，選擇正確版本後按下確認："
        
        view_msg = await thread.send(content=info_text, view=view)
        view.original_message = view_msg
        
    except Exception as e:
        error_trace = traceback.format_exc()
        await thread.send(f"❌ 發生異常：\n```python\n{error_trace[-1900:]}\n```")
    finally:
        if os.path.exists(img_path):
            os.remove(img_path)


@tree.command(name="profile", description="輸入錢包地址，開啟互動式海報設定面板")
@app_commands.describe(address="錢包地址（0x 開頭）")
async def profile(interaction: discord.Interaction, address: str):
    wallet = _normalize_wallet_address(address)
    if not wallet:
        await interaction.response.send_message(
            "❌ 錢包地址格式錯誤，請輸入 `0x` 開頭且長度 42 的地址。",
            ephemeral=True,
        )
        return

    try:
        # Keep the original clean UX: immediate response -> create thread from that message.
        await interaction.response.send_message("🎛️ 正在建立收藏海報設定討論串...", ephemeral=False)
        resp = await interaction.original_response()
    except discord.NotFound:
        # Fallback for occasional interaction timeout: still create a thread from channel message.
        channel = interaction.channel
        if channel is None:
            print("❌ /profile 互動已失效且找不到可用頻道。", file=sys.stderr)
            return
        resp = await channel.send("🎛️ 正在建立收藏海報設定討論串...")
    thread = await resp.create_thread(name="收藏海報設定", auto_archive_duration=60)
    await thread.add_user(interaction.user)

    default_lang = "zh"
    default_texts = _profile_wizard_texts(default_lang)

    lang_view = LanguageSelectView(interaction.user.id, timeout_seconds=20)
    lang_msg = await thread.send(default_texts["lang_prompt"], view=lang_view)
    picked_lang, selected = await lang_view.wait_for_choice()
    profile_lang = picked_lang if selected and picked_lang else "zh"
    if not selected:
        lang_view._disable_all()
        try:
            await lang_msg.edit(content=_profile_wizard_texts(profile_lang)["lang_timeout"], view=lang_view)
        except Exception:
            pass

    loop = asyncio.get_running_loop()

    try:
        picker_data = await loop.run_in_executor(
            None,
            _build_wallet_profile_picker_data,
            wallet,
        )
        view = ProfileConfigView(interaction.user.id, wallet, picker_data, profile_lang)
        msg = await thread.send(view.render_message(), view=view)
        view.bind_message(msg)
    except Exception as e:
        print(f"❌ /profile 失敗: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        await thread.send(f"❌ 生成失敗：{e}")

@tree.command(name="clear_stale_commands", description="[Owner] 清除所有全域斜線指令並重置 (建議改用 !sync)")
async def clear_stale(interaction: discord.Interaction):
    await interaction.response.send_message("⚙️ 正在清除全域指令並重新同步，請稍候...", ephemeral=True)
    # Note: Global sync can be slow. 
    await tree.sync()
    await interaction.followup.send("✅ 已發送同步請求。若指令未更新，請嘗試使用文字指令 `!sync` (需標註機器人)。", ephemeral=True)

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if client.user in message.mentions:
        content_lower = message.content.lower().strip()
        
        # --- 強制同步指令 (文字備援方案) ---
        if "!sync" in content_lower:
            try:
                # 1. 先同步到當前伺服器 (Guild Sync - 幾乎即時生效)
                print(f"⚙️ 正在同步指令到伺服器: {message.guild.id}")
                tree.copy_global_to(guild=message.guild)
                await tree.sync(guild=message.guild)
                
                # 2. 同步到全域 (Global Sync - 需 1 小時)
                await tree.sync()
                
                await message.reply("✅ **指令同步成功！**\n1. 伺服器專屬指令已更新 (應可立即使用)。\n2. 全域更新已送出 (可能需 1 小時)。\n\n*注意：若仍未看到指令，請重啟 Discord APP。*")
                return
            except Exception as e:
                await message.reply(f"❌ 同步失敗: {e}")
                return

        # 原本的圖片處理邏輯
        if message.attachments:
            content_lower = message.content.lower()
            valid_attachments = [
                a for a in message.attachments
                if any(a.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.webp'])
            ]
            if not valid_attachments:
                return

            # 可用 !zh / !zhs(!cn) / !en / !ko 強制指定；未指定時用按鈕詢問，5 秒逾時預設繁中。
            lang_override = _parse_lang_override(content_lower)
            if lang_override:
                lang = lang_override
            else:
                lang = await choose_language_for_message(message)

            for attachment in valid_attachments:
                # 每張圖建立獨立 Task；並發上限由 REPORT/POSTER 雙 Semaphore 控制
                asyncio.create_task(handle_image(attachment, message, lang=lang))


if __name__ == "__main__":
    if not TOKEN:
        print("❌ 錯誤：找不到 DISCORD_BOT_TOKEN。")
    else:
        threading.Thread(target=run_health_server, daemon=True).start()
        print("啟動中...")
        client.run(TOKEN)
