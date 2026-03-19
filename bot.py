#!/usr/bin/env python3
import discord
from discord import app_commands
from discord.ext import tasks
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
import mimetypes
import hashlib
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as dt_time
from decimal import Decimal, InvalidOperation
import requests
import market_report_vision
import image_generator
from dotenv import load_dotenv
from http.server import BaseHTTPRequestHandler, HTTPServer
from zoneinfo import ZoneInfo

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


def _env_true(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


NFT_SYNC_ENABLE = _env_true("NFT_SYNC_ENABLE", True)
NFT_SYNC_SCRIPT_PATH = os.path.join(BASE_DIR, "scripts", "sync_nft13_incremental.py")
NFT_SYNC_STARTUP_DONE = False
NFT_SYNC_STARTUP_LOCK: asyncio.Lock | None = None
NFT_SYNC_TZ = str(os.getenv("NFT_SYNC_TZ", "Asia/Taipei")).strip() or "Asia/Taipei"
NFT_SYNC_HOUR = max(0, min(23, int(os.getenv("NFT_SYNC_HOUR", "12"))))
NFT_SYNC_MINUTE = max(0, min(59, int(os.getenv("NFT_SYNC_MINUTE", "0"))))
try:
    NFT_SYNC_RUN_TIME = dt_time(hour=NFT_SYNC_HOUR, minute=NFT_SYNC_MINUTE, tzinfo=ZoneInfo(NFT_SYNC_TZ))
except Exception:
    NFT_SYNC_RUN_TIME = dt_time(hour=12, minute=0, tzinfo=ZoneInfo("Asia/Taipei"))


def _nft_sync_data_dir() -> str:
    app_env = str(os.getenv("APP_ENV", "local")).strip().lower() or "local"
    default_dir = "/data/renaiss_sync" if app_env == "server" else "./data/renaiss_sync"
    return str(os.getenv("SYNC_DATA_DIR", default_dir)).strip() or default_dir


def _nft_sync_status_path() -> str:
    token_id = str(os.getenv("NFT_TOKEN_ID", "13")).strip() or "13"
    return os.path.join(_nft_sync_data_dir(), "state", f"nft_{token_id}_status.json")
# /profile uses an isolated template bundle to avoid impacting normal card posters.
PROFILE_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "profile", "wallet_profile＿beta.html")
PROFILE_HISTORY_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "profile", "wallet_profile_history_beta.html")
PROFILE_EXTREMES_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "profile", "wallet_profile_extremes_beta.html")
PROFILE_LOGO_PATH = os.path.join(BASE_DIR, "templates", "profile", "logo.png")
PROFILE_BACKGROUND_DIR = os.path.join(BASE_DIR, "templates", "backgorund")
RENAISS_COLLECTIBLE_LIST_URL = "https://www.renaiss.xyz/api/trpc/collectible.list"
RENAISS_COLLECTIBLE_BY_TOKEN_URL = "https://www.renaiss.xyz/api/trpc/collectible.getCollectibleByTokenId"
RENAISS_SBT_BADGES_URL = "https://www.renaiss.xyz/api/trpc/sbt.getUserBadges"
RENAISS_ACTIVITY_LIST_URL = "https://www.renaiss.xyz/api/trpc/activity.getSubgraphUserActivities"
RENAISS_TOKEN_ACTIVITY_URL = "https://www.renaiss.xyz/api/trpc/activity.getSubgraphTokenActivities"
PROFILE_PAGE_LIMIT = max(10, min(100, int(os.getenv("PROFILE_PAGE_LIMIT", "100"))))
PROFILE_SCAN_MAX_OFFSET = max(PROFILE_PAGE_LIMIT, int(os.getenv("PROFILE_SCAN_MAX_OFFSET", "5000")))
PROFILE_API_MAX_RETRIES = max(1, int(os.getenv("PROFILE_API_MAX_RETRIES", "4")))
PROFILE_API_RETRY_BACKOFF_SEC = max(0.2, float(os.getenv("PROFILE_API_RETRY_BACKOFF_SEC", "0.8")))
PROFILE_ACTIVITY_PAGE_LIMIT = max(1, min(50, int(os.getenv("PROFILE_ACTIVITY_PAGE_LIMIT", "50"))))
PROFILE_ACTIVITY_MAX_PAGES = max(1, int(os.getenv("PROFILE_ACTIVITY_MAX_PAGES", "120")))
PROFILE_TOKEN_ACTIVITY_PAGE_LIMIT = max(1, min(50, int(os.getenv("PROFILE_TOKEN_ACTIVITY_PAGE_LIMIT", "50"))))
PROFILE_TOKEN_ACTIVITY_MAX_PAGES = max(1, int(os.getenv("PROFILE_TOKEN_ACTIVITY_MAX_PAGES", "20")))
PROFILE_HTTP_POOL_MAXSIZE = max(8, int(os.getenv("PROFILE_HTTP_POOL_MAXSIZE", "48")))
PROFILE_RESOLVE_SCAN_WORKERS = max(1, int(os.getenv("PROFILE_RESOLVE_SCAN_WORKERS", "8")))
PROFILE_COLLECTION_FETCH_WORKERS = max(1, int(os.getenv("PROFILE_COLLECTION_FETCH_WORKERS", "8")))
PROFILE_WITHDRAW_VALUE_WORKERS = max(1, int(os.getenv("PROFILE_WITHDRAW_VALUE_WORKERS", "10")))
PROFILE_ENABLE_RUNTIME_CACHE = str(os.getenv("PROFILE_ENABLE_RUNTIME_CACHE", "0")).strip().lower() in ("1", "true", "yes", "on")
PROFILE_ENABLE_DISK_IMAGE_CACHE = str(os.getenv("PROFILE_ENABLE_DISK_IMAGE_CACHE", "0")).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
PROFILE_CARD_WITHDRAW_ADDRESS = str(
    os.getenv("PROFILE_CARD_WITHDRAW_ADDRESS", "0x341Edb3EdC1E45612E5704F29eC8d26fBb4072b4")
).strip().lower()
# SellActivity amount is gross (pre-fee) in current upstream data; convert to net received.
PROFILE_MARKET_SELL_GROSS_DIVISOR = Decimal(os.getenv("PROFILE_MARKET_SELL_GROSS_DIVISOR", "1.02"))
_WALLET_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
_TRANSPARENT_CARD_IMAGE = "data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs="
_WEI_DECIMAL = Decimal("1000000000000000000")
_PROFILE_RELEASE_CARD_TYPES = {
    "PerpetualReleaseTokenActivity",
    "ReleaseTokenActivity",
    "MintActivity",
    "PerpetualMintActivity",
}
_CARD_FMV_CACHE: dict[str, Decimal] = {}
_CARD_IMAGE_CACHE: dict[str, str] = {}
_CARD_COLLECTIBLE_CACHE: dict[str, dict] = {}
_PREPARED_CARD_IMAGE_CACHE: dict[str, str] = {}
PROFILE_PREPARED_CARD_CACHE_DIR = os.path.join(BASE_DIR, "templates", "profile", "cache_cards")
_HTTP_SESSION_LOCAL = threading.local()
_PROFILE_BACKGROUND_FILES = {
    "classic": None,
    "1": "1.jpg",
    "2": "2.png",
    "3": "3.jpg",
}


def _normalize_wallet_address(address: str) -> str | None:
    text = (address or "").strip()
    if not _WALLET_RE.fullmatch(text):
        return None
    return text.lower()


def _http_session() -> requests.Session:
    sess = getattr(_HTTP_SESSION_LOCAL, "session", None)
    if sess is not None:
        return sess
    sess = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=PROFILE_HTTP_POOL_MAXSIZE,
        pool_maxsize=PROFILE_HTTP_POOL_MAXSIZE,
        max_retries=0,
    )
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    _HTTP_SESSION_LOCAL.session = sess
    return sess


def _http_get(url: str, **kwargs):
    return _http_session().get(url, **kwargs)


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


def _to_decimal(value) -> Decimal:
    if value is None or isinstance(value, bool):
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    text = str(value).strip()
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _wei_to_usdt(value) -> Decimal:
    wei = _to_decimal(value)
    if wei == 0:
        return Decimal("0")
    return wei / _WEI_DECIMAL


def _format_usdt_decimal(value: Decimal | None, signed: bool = False) -> str:
    amount = _to_decimal(value).quantize(Decimal("0.01"))
    if signed:
        return f"{amount:+,.2f}"
    return f"{amount:,.2f}"


def _format_usdt_currency(value: Decimal | None, signed: bool = False) -> str:
    amount = _to_decimal(value).quantize(Decimal("0.01"))
    if not signed:
        return f"${amount:,.2f}"
    sign = "+" if amount > 0 else "-" if amount < 0 else ""
    return f"{sign}${abs(amount):,.2f}"


def _short_hex(value: str | None) -> str:
    text = str(value or "").strip()
    if len(text) <= 12:
        return text
    return f"{text[:8]}...{text[-6:]}"


def _clamp_profile_card_count(value: int | None) -> int:
    if value in (1, 3, 4, 5, 7, 10):
        return int(value)
    return 10


def _normalize_profile_background_key(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if text in ("classic", "經典", "经典", "default", "0"):
        return "classic"
    if text in ("1", "2", "3"):
        return text
    return "classic"


def _profile_background_data_uri(background_key: str | None) -> str:
    key = _normalize_profile_background_key(background_key)
    filename = _PROFILE_BACKGROUND_FILES.get(key)
    if not filename:
        return ""
    img_path = os.path.join(PROFILE_BACKGROUND_DIR, filename)
    if not os.path.exists(img_path):
        return ""
    try:
        with open(img_path, "rb") as f:
            raw = f.read()
        mime, _ = mimetypes.guess_type(img_path)
        if not mime:
            ext = os.path.splitext(img_path)[1].lower()
            mime = "image/png" if ext == ".png" else "image/jpeg"
        b64 = base64.b64encode(raw).decode("utf-8")
        return f"data:{mime};base64,{b64}"
    except Exception:
        return ""


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


def _profile_history_labels(lang: str) -> dict[str, str]:
    if lang == "zh":
        return {
            "title": "Collection History",
            "subtitle": "抽卡與交易歷史統計",
            "kpi_opened": "開包次數",
            "kpi_pack_spent": "抽卡花費",
            "kpi_card_withdraw": "提領卡片金額",
            "kpi_total_spent": "總花費",
            "kpi_total_earned": "總賺取",
            "kpi_total_spent_note": "抽卡花費＋市場買入",
            "kpi_total_earned_note": "BUYBACK＋市場賣出",
            "kpi_net": "淨值",
            "kpi_net_note": "含提領卡片",
            "kpi_cash_net": "淨值",
            "kpi_trade_volume": "交易總額",
            "kpi_assets_value": "目前卡片價值",
            "kpi_buyback": "Buyback 總額",
            "kpi_market_buy": "市場買入總額",
            "kpi_market_sell": "市場賣出總額",
            "kpi_active_days": "參與項目天數",
            "section_contract": "合約 / 包型明細",
            "section_activity": "活動筆數",
            "section_note": "計算說明",
            "head_pack": "包型",
            "head_contract": "合約",
            "head_open_count": "開包",
            "head_direct_count": "直接對價",
            "head_inferred_count": "補價",
            "head_unit_price": "單價(USDT)",
            "head_spent_total": "合計(USDT)",
            "empty_contract": "無可用開包資料",
            "activity_total": "總活動",
            "chip_direct": "直接",
            "chip_inferred": "補價",
            "chip_unknown": "未知",
            "note_line_1": "PerpetualPullActivity 先做直接計價；缺值時用同合約的既有單價補齊。",
            "note_line_2": "淨值 = 總賺取 - 總花費；持倉總值(FMV)獨立展示。",
        }
    if lang == "zhs":
        return {
            "title": "Collection History",
            "subtitle": "抽卡与交易历史统计",
            "kpi_opened": "开包次数",
            "kpi_pack_spent": "抽卡花费",
            "kpi_card_withdraw": "提领卡片金额",
            "kpi_total_spent": "总花费",
            "kpi_total_earned": "总赚取",
            "kpi_total_spent_note": "抽卡花费＋市场买入",
            "kpi_total_earned_note": "BUYBACK＋市场卖出",
            "kpi_net": "净值",
            "kpi_net_note": "含提领卡片",
            "kpi_cash_net": "净值",
            "kpi_trade_volume": "交易总额",
            "kpi_assets_value": "目前卡片价值",
            "kpi_buyback": "Buyback 总额",
            "kpi_market_buy": "市场买入总额",
            "kpi_market_sell": "市场卖出总额",
            "kpi_active_days": "参与项目天数",
            "section_contract": "合约 / 包型明细",
            "section_activity": "活动笔数",
            "section_note": "计算说明",
            "head_pack": "包型",
            "head_contract": "合约",
            "head_open_count": "开包",
            "head_direct_count": "直接对价",
            "head_inferred_count": "补价",
            "head_unit_price": "单价(USDT)",
            "head_spent_total": "合计(USDT)",
            "empty_contract": "暂无可用开包数据",
            "activity_total": "总活动",
            "chip_direct": "直接",
            "chip_inferred": "补价",
            "chip_unknown": "未知",
            "note_line_1": "先用 PerpetualPullActivity 直接计价；缺值时用同合约已有单价补齐。",
            "note_line_2": "净值 = 总赚取 - 总花费；持仓总值(FMV)独立展示。",
        }
    if lang == "ko":
        return {
            "title": "Collection History",
            "subtitle": "팩 오픈 및 거래 히스토리",
            "kpi_opened": "팩 오픈",
            "kpi_pack_spent": "오픈 비용",
            "kpi_card_withdraw": "카드 출고 금액",
            "kpi_total_spent": "총 지출",
            "kpi_total_earned": "총 수익",
            "kpi_total_spent_note": "팩 비용＋마켓 매수",
            "kpi_total_earned_note": "BUYBACK＋마켓 매도",
            "kpi_net": "순손익",
            "kpi_net_note": "카드 출고 포함",
            "kpi_cash_net": "순손익",
            "kpi_trade_volume": "거래 총액",
            "kpi_assets_value": "보유자산 가치",
            "kpi_buyback": "Buyback 총액",
            "kpi_market_buy": "마켓 매수 총액",
            "kpi_market_sell": "마켓 매도 총액",
            "kpi_active_days": "프로젝트 참여 일수",
            "section_contract": "컨트랙트 / 팩 상세",
            "section_activity": "활동 건수",
            "section_note": "계산 규칙",
            "head_pack": "팩",
            "head_contract": "컨트랙트",
            "head_open_count": "오픈",
            "head_direct_count": "직접 가격",
            "head_inferred_count": "보정",
            "head_unit_price": "단가(USDT)",
            "head_spent_total": "합계(USDT)",
            "empty_contract": "오픈 데이터가 없습니다",
            "activity_total": "총 활동",
            "chip_direct": "직접",
            "chip_inferred": "보정",
            "chip_unknown": "미확인",
            "note_line_1": "PerpetualPullActivity 가격을 우선 사용하고, 누락 시 같은 컨트랙트 단가로 보정합니다.",
            "note_line_2": "순손익 = 총수익 - 총지출, 보유자산(FMV)은 별도 표기.",
        }
    return {
        "title": "Collection History",
        "subtitle": "Pack and trade history overview",
        "kpi_opened": "Packs Opened",
        "kpi_pack_spent": "Pack Spend",
        "kpi_card_withdraw": "Card Withdrawal Value",
        "kpi_total_spent": "Total Spent",
        "kpi_total_earned": "Total Earned",
        "kpi_total_spent_note": "Pack Spend + Market Buy",
        "kpi_total_earned_note": "BUYBACK + Market Sell",
        "kpi_net": "Net",
        "kpi_net_note": "incl. Card Withdrawal",
        "kpi_cash_net": "Net",
        "kpi_trade_volume": "Trade Volume",
        "kpi_assets_value": "Holdings Value",
        "kpi_buyback": "Buyback Total",
        "kpi_market_buy": "Market Buy Total",
        "kpi_market_sell": "Market Sell Total",
        "kpi_active_days": "Active Days",
        "section_contract": "Contract / Pack Breakdown",
        "section_activity": "Activity Counts",
        "section_note": "Calculation Notes",
        "head_pack": "Pack",
        "head_contract": "Contract",
        "head_open_count": "Opened",
        "head_direct_count": "Direct",
        "head_inferred_count": "Inferred",
        "head_unit_price": "Unit (USDT)",
        "head_spent_total": "Total (USDT)",
        "empty_contract": "No pack-open data available",
        "activity_total": "Total Activities",
        "chip_direct": "Direct",
        "chip_inferred": "Inferred",
        "chip_unknown": "Unknown",
        "note_line_1": "Prices use PerpetualPullActivity first; missing rows are inferred from the same contract price.",
        "note_line_2": "Net = Total Earned - Total Spent; Holdings FMV is shown separately.",
    }


def _profile_extreme_labels(lang: str) -> dict[str, str]:
    if lang == "zh":
        return {
            "title": "Historical Pull Extremes",
            "subtitle": "歷史抽到卡片的最高 / 最低價值",
            "high_label": "最高價值",
            "low_label": "最低價值",
            "no_data": "目前沒有可用的歷史抽卡資料",
            "top_value_label": "歷史高低兩張總值",
            "items_count_label": "歷史抽卡數",
            "assets_unit": "張",
            "sbt_badges_label": "歷史樣本",
        }
    if lang == "zhs":
        return {
            "title": "Historical Pull Extremes",
            "subtitle": "历史抽到卡片的最高 / 最低价值",
            "high_label": "最高价值",
            "low_label": "最低价值",
            "no_data": "暂无可用的历史抽卡数据",
            "top_value_label": "历史高低两张总值",
            "items_count_label": "历史抽卡数",
            "assets_unit": "张",
            "sbt_badges_label": "历史样本",
        }
    if lang == "ko":
        return {
            "title": "Historical Pull Extremes",
            "subtitle": "과거 뽑기 카드의 최고 / 최저 가치",
            "high_label": "최고 가치",
            "low_label": "최저 가치",
            "no_data": "사용 가능한 과거 뽑기 데이터가 없습니다",
            "top_value_label": "히스토리 상/하 2장 총가치",
            "items_count_label": "히스토리 뽑기 수",
            "assets_unit": "장",
            "sbt_badges_label": "히스토리 샘플",
        }
    return {
        "title": "Historical Pull Extremes",
        "subtitle": "Highest / Lowest value among historical pulls",
        "high_label": "Highest Value",
        "low_label": "Lowest Value",
        "no_data": "No historical pull data available",
        "top_value_label": "Historical Top/Bottom Pair Value",
        "items_count_label": "Historical Pulls",
        "assets_unit": "Cards",
        "sbt_badges_label": "History Samples",
    }


def _profile_activity_display_name(activity_type: str, lang: str) -> str:
    mappings = {
        "PerpetualReleaseTokenActivity": {
            "zh": "總開包數",
            "zhs": "总开包数",
            "ko": "총 팩 오픈",
            "en": "Total Packs Opened",
        },
        "PerpetualPullActivity": {
            "zh": "抽卡扣款",
            "zhs": "抽卡扣款",
            "ko": "팩 결제",
            "en": "Pack Pull Charge",
        },
        "PerpetualBuybackActivity": {
            "zh": "回購賣出",
            "zhs": "回购卖出",
            "ko": "바이백 수익",
            "en": "Buyback Earn",
        },
        "BuyActivity": {
            "zh": "市場買入",
            "zhs": "市场买入",
            "ko": "마켓 매수",
            "en": "Market Buy",
        },
        "SellActivity": {
            "zh": "市場賣出",
            "zhs": "市场卖出",
            "ko": "마켓 매도",
            "en": "Market Sell",
        },
        "SBTMintActivity": {
            "zh": "SBT 鑄造",
            "zhs": "SBT 铸造",
            "ko": "SBT 민팅",
            "en": "SBT Mint",
        },
    }
    entry = mappings.get(activity_type) or {}
    return entry.get(lang) or entry.get("en") or activity_type


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
            "template_placeholder": "1) 選擇模板（Top 1 / Top 3 / Top 4 / Top 5 / Top 7 / Top 10）",
            "background_placeholder": "2) 選擇背景（經典 / 超級甲賀忍蛙 / 妙蛙花 / 小智與皮卡丘）",
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
            "background_selected_label": "背景",
            "sbt_selected_label": "SBT",
            "cards_selected_label": "卡片",
            "background_classic_label": "經典",
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
            "template_placeholder": "1) 选择模板（Top 1 / Top 3 / Top 4 / Top 5 / Top 7 / Top 10）",
            "background_placeholder": "2) 选择背景（经典 / 超级甲贺忍蛙 / 妙蛙花 / 小智与皮卡丘）",
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
            "background_selected_label": "背景",
            "sbt_selected_label": "SBT",
            "cards_selected_label": "卡片",
            "background_classic_label": "经典",
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
            "template_placeholder": "1) 템플릿 선택 (Top 1 / Top 3 / Top 4 / Top 5 / Top 7 / Top 10)",
            "background_placeholder": "2) 배경 선택 (Classic / 개굴닌자(유대변화) / 이상해꽃 / 지우와 피카츄)",
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
            "background_selected_label": "배경",
            "sbt_selected_label": "SBT",
            "cards_selected_label": "카드",
            "background_classic_label": "Classic",
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
        "template_placeholder": "1) Select template (Top 1 / Top 3 / Top 4 / Top 5 / Top 7 / Top 10)",
        "background_placeholder": "2) Select background (Classic / Ash-Greninja / Venusaur / Ash & Pikachu)",
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
        "background_selected_label": "Background",
        "sbt_selected_label": "SBT",
        "cards_selected_label": "Cards",
        "background_classic_label": "Classic",
        "default_short": "Default",
        "selected_short": "Selected",
    }


def _profile_background_display_labels(lang: str) -> dict[str, str]:
    lang = _profile_lang_from_locale(lang)
    if lang == "zh":
        return {
            "classic": "經典",
            "1": "超級甲賀忍蛙",
            "2": "妙蛙花",
            "3": "小智與皮卡丘",
        }
    if lang == "zhs":
        return {
            "classic": "经典",
            "1": "超级甲贺忍蛙",
            "2": "妙蛙花",
            "3": "小智与皮卡丘",
        }
    if lang == "ko":
        return {
            "classic": "Classic",
            "1": "개굴닌자(유대변화)",
            "2": "이상해꽃",
            "3": "지우와 피카츄",
        }
    return {
        "classic": "Classic",
        "1": "Ash-Greninja",
        "2": "Venusaur",
        "3": "Ash & Pikachu",
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
    cache_key = hashlib.sha1(src.encode("utf-8")).hexdigest()
    cache_path = os.path.join(PROFILE_PREPARED_CARD_CACHE_DIR, f"{cache_key}.txt")
    if PROFILE_ENABLE_RUNTIME_CACHE:
        cached_data_uri = _PREPARED_CARD_IMAGE_CACHE.get(src)
        if cached_data_uri:
            return cached_data_uri
    if PROFILE_ENABLE_DISK_IMAGE_CACHE:
        try:
            if os.path.exists(cache_path):
                with open(cache_path, "r", encoding="utf-8") as f:
                    cached_data_uri = str(f.read() or "").strip()
                if cached_data_uri.startswith("data:image/"):
                    if PROFILE_ENABLE_RUNTIME_CACHE:
                        _PREPARED_CARD_IMAGE_CACHE[src] = cached_data_uri
                    return cached_data_uri
        except Exception:
            pass
    try:
        from PIL import Image, ImageDraw
        import numpy as np

        resp = _http_get(
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

        # Vercel graded-card renders are square with dark stage background.
        # Use tighter fixed slab crop ratios to remove black side/floor regions.
        if "graded-cards-renders" in src and abs(w - h) <= int(min(w, h) * 0.08):
            l = int(w * 0.274)
            r = int(w * 0.726)
            t = int(h * 0.070)
            btm = int(h * 0.888)
            # Fine crop offsets from user tuning:
            # left outward 8px, right outward 15px, bottom upward 20px.
            l = max(0, l - 8)
            r = min(w, r + 15)
            btm = max(t + 1, btm - 20)
            if (r - l) > w * 0.3 and (btm - t) > h * 0.6:
                img = img.crop((l, t, r, btm))
        else:
            # Fallback: remove dark background connected to image edges.
            rgb = np.asarray(img, dtype=np.uint8)
            hh0, ww0 = rgb.shape[:2]
            rr = rgb[..., 0].astype(np.uint16)
            gg = rgb[..., 1].astype(np.uint16)
            bb = rgb[..., 2].astype(np.uint16)
            gray = ((rr * 299 + gg * 587 + bb * 114) // 1000).astype(np.uint8)
            near_black = gray <= 46

            visited = np.zeros((hh0, ww0), dtype=np.uint8)
            stack: list[tuple[int, int]] = []

            top_x = np.where(near_black[0])[0]
            bot_x = np.where(near_black[hh0 - 1])[0]
            left_y = np.where(near_black[:, 0])[0]
            right_y = np.where(near_black[:, ww0 - 1])[0]

            for x in top_x:
                stack.append((0, int(x)))
            for x in bot_x:
                stack.append((hh0 - 1, int(x)))
            for y in left_y:
                stack.append((int(y), 0))
            for y in right_y:
                stack.append((int(y), ww0 - 1))

            while stack:
                y, x = stack.pop()
                if y < 0 or y >= hh0 or x < 0 or x >= ww0:
                    continue
                if visited[y, x] or not near_black[y, x]:
                    continue
                visited[y, x] = 1
                stack.append((y - 1, x))
                stack.append((y + 1, x))
                stack.append((y, x - 1))
                stack.append((y, x + 1))

            fg = visited == 0
            ys, xs = np.where(fg)
            if xs.size and ys.size:
                l = int(xs.min())
                r = int(xs.max()) + 1
                t = int(ys.min())
                btm = int(ys.max()) + 1
                if (r - l) > ww0 * 0.35 and (btm - t) > hh0 * 0.35:
                    img = img.crop((l, t, r, btm))

        # Apply real rounded-corner cutout at preprocessing stage (not CSS-only).
        rgba = img.convert("RGBA")
        ww, hh = rgba.size
        corner_radius = max(22, int(min(ww, hh) * 0.055))
        mask = Image.new("L", (ww, hh), 0)
        draw = ImageDraw.Draw(mask)
        draw.rounded_rectangle((0, 0, ww - 1, hh - 1), radius=corner_radius, fill=255)
        rgba.putalpha(mask)

        out = io.BytesIO()
        rgba.save(out, format="PNG", optimize=True)
        b64 = base64.b64encode(out.getvalue()).decode("utf-8")
        result_data_uri = f"data:image/png;base64,{b64}"
        if PROFILE_ENABLE_RUNTIME_CACHE:
            _PREPARED_CARD_IMAGE_CACHE[src] = result_data_uri
        if PROFILE_ENABLE_DISK_IMAGE_CACHE:
            try:
                os.makedirs(PROFILE_PREPARED_CARD_CACHE_DIR, exist_ok=True)
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(result_data_uri)
            except Exception:
                pass
        return result_data_uri
    except Exception:
        return src


def _prepare_sbt_badge_image_for_poster(image_url: str) -> str:
    src = str(image_url or "").strip()
    if not src:
        return ""
    try:
        from PIL import Image, ImageDraw
        import numpy as np
        import math

        resp = _http_get(
            src,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            },
            timeout=20,
        )
        resp.raise_for_status()
        img = Image.open(io.BytesIO(resp.content)).convert("RGBA")
        w, h = img.size
        if w < 24 or h < 24:
            return src

        # Normalize to square first so all templates can reuse one stable prepared asset.
        side = min(w, h)
        l = (w - side) // 2
        t = (h - side) // 2
        img = img.crop((l, t, l + side, t + side))

        # Build a clean hex mask (similar logic for all SBT badges).
        s = img.size[0]
        pad = max(1.0, s * 0.06)
        cx = (s - 1) / 2.0
        cy = (s - 1) / 2.0
        radius = (s / 2.0) - pad
        pts = []
        for i in range(6):
            ang = math.radians(-90 + i * 60)
            x = cx + radius * math.cos(ang)
            y = cy + radius * math.sin(ang)
            pts.append((x, y))

        mask = Image.new("L", (s, s), 0)
        draw = ImageDraw.Draw(mask)
        draw.polygon(pts, fill=255)

        arr = np.array(img, dtype=np.uint8, copy=True)
        alpha_src = arr[..., 3].astype(np.uint16)
        alpha_hex = np.asarray(mask, dtype=np.uint16)
        arr[..., 3] = np.minimum(alpha_src, alpha_hex).astype(np.uint8)
        out_img = Image.fromarray(arr, mode="RGBA")

        out = io.BytesIO()
        out_img.save(out, format="PNG", optimize=True)
        b64 = base64.b64encode(out.getvalue()).decode("utf-8")
        return f"data:image/png;base64,{b64}"
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
            resp = _http_get(RENAISS_COLLECTIBLE_LIST_URL, params=params, timeout=25)
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


def _trpc_user_activities(wallet_address: str, cursor: str | None = None, limit: int = PROFILE_ACTIVITY_PAGE_LIMIT) -> dict:
    payload = {
        "address": str(wallet_address or "").strip().lower(),
        "filter": "all",
        "mode": "private",
        "limit": int(limit),
        "cursor": cursor,
    }
    row = {"json": payload}
    if cursor is None:
        # tRPC endpoint expects explicit undefined-cursor meta on first page.
        row["meta"] = {"values": {"cursor": ["undefined"]}}

    params = {
        "batch": "1",
        "input": json.dumps({"0": row}, separators=(",", ":"), ensure_ascii=False),
    }

    last_err = None
    for attempt in range(1, PROFILE_API_MAX_RETRIES + 1):
        try:
            resp = _http_get(RENAISS_ACTIVITY_LIST_URL, params=params, timeout=25)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError("activity.getSubgraphUserActivities 回傳格式異常")
            root = data[0]
            if root.get("error"):
                err = root.get("error", {}).get("json", {}).get("message") or "unknown error"
                raise RuntimeError(f"activity.getSubgraphUserActivities error: {err}")

            result = (((root.get("result") or {}).get("data") or {}).get("json") or {})
            if not isinstance(result, dict):
                raise RuntimeError("activity.getSubgraphUserActivities result 缺失")
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

    raise RuntimeError(f"activity.getSubgraphUserActivities 請求失敗（已重試 {PROFILE_API_MAX_RETRIES} 次）：{last_err}")


def _card_price_to_usd(value) -> Decimal:
    amount = _to_decimal(value)
    if amount <= 0:
        return Decimal("0")
    # Card page usually stores USD in cent-like integer units (e.g. 10815 -> 108.15).
    if amount == amount.to_integral_value() and amount >= Decimal("1000"):
        return amount / Decimal("100")
    return amount


def _extract_card_field_value(text: str, field: str) -> Decimal:
    normalized = str(text or "").replace('\\"', '"')
    patterns = [
        rf'"{re.escape(field)}"\s*:\s*"([0-9]+(?:\.[0-9]+)?)"',
        rf'"{re.escape(field)}"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
    ]
    for pattern in patterns:
        m = re.search(pattern, normalized)
        if m:
            return _to_decimal(m.group(1))
    return Decimal("0")


def _trpc_collectible_by_token(token_id: str) -> dict:
    token = str(token_id or "").strip()
    if not token:
        return {}
    params = {
        "batch": "1",
        "input": json.dumps(
            {"0": {"json": {"tokenId": token}, "meta": {"values": {"tokenId": ["bigint"]}}}},
            separators=(",", ":"),
            ensure_ascii=False,
        ),
    }

    last_err = None
    for attempt in range(1, PROFILE_API_MAX_RETRIES + 1):
        try:
            resp = _http_get(RENAISS_COLLECTIBLE_BY_TOKEN_URL, params=params, timeout=25)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError("collectible.getCollectibleByTokenId 回傳格式異常")
            root = data[0]
            if root.get("error"):
                err_data = ((root.get("error") or {}).get("json") or {}).get("data") or {}
                if int(err_data.get("httpStatus") or 0) == 404:
                    return {}
                err_msg = ((root.get("error") or {}).get("json") or {}).get("message") or "unknown error"
                raise RuntimeError(f"collectible.getCollectibleByTokenId error: {err_msg}")
            result = (((root.get("result") or {}).get("data") or {}).get("json") or {})
            if not isinstance(result, dict):
                raise RuntimeError("collectible.getCollectibleByTokenId result 缺失")
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

    raise RuntimeError(f"collectible.getCollectibleByTokenId 請求失敗（已重試 {PROFILE_API_MAX_RETRIES} 次）：{last_err}")


def _collectible_by_token_cached(token_id: str) -> dict:
    tid = str(token_id or "").strip()
    if not tid:
        return {}
    if not PROFILE_ENABLE_RUNTIME_CACHE:
        data = _trpc_collectible_by_token(tid)
        return data if isinstance(data, dict) else {}
    cached = _CARD_COLLECTIBLE_CACHE.get(tid)
    if isinstance(cached, dict):
        return cached
    data = _trpc_collectible_by_token(tid)
    if not isinstance(data, dict):
        data = {}
    _CARD_COLLECTIBLE_CACHE[tid] = data
    return data


def _trpc_token_activities(
    token_id: str,
    cursor: str | None = None,
    limit: int = PROFILE_TOKEN_ACTIVITY_PAGE_LIMIT,
) -> dict:
    token = str(token_id or "").strip()
    if not token:
        return {"activities": [], "nextCursor": None}
    row = {
        "json": {
            "tokenId": token,
            "limit": int(max(1, min(50, limit))),
            "cursor": cursor,
        }
    }
    if cursor is None:
        row["meta"] = {"values": {"cursor": ["undefined"]}}
    params = {
        "batch": "1",
        "input": json.dumps({"0": row}, separators=(",", ":"), ensure_ascii=False),
    }

    last_err = None
    for attempt in range(1, PROFILE_API_MAX_RETRIES + 1):
        try:
            resp = _http_get(RENAISS_TOKEN_ACTIVITY_URL, params=params, timeout=25)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError("activity.getSubgraphTokenActivities 回傳格式異常")
            root = data[0]
            if root.get("error"):
                err_msg = ((root.get("error") or {}).get("json") or {}).get("message") or "unknown error"
                raise RuntimeError(f"activity.getSubgraphTokenActivities error: {err_msg}")
            result = (((root.get("result") or {}).get("data") or {}).get("json") or {})
            if not isinstance(result, dict):
                raise RuntimeError("activity.getSubgraphTokenActivities result 缺失")
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

    raise RuntimeError(f"activity.getSubgraphTokenActivities 請求失敗（已重試 {PROFILE_API_MAX_RETRIES} 次）：{last_err}")


def _fetch_card_trade_fallback_by_token_id(token_id: str) -> Decimal:
    tid = str(token_id or "").strip()
    if not tid:
        return Decimal("0")
    try:
        activities = _fetch_token_activities(tid)
        best_value = Decimal("0")
        best_ts = -1
        for row in activities:
            if not isinstance(row, dict):
                continue
            row_type = str(row.get("__typename") or "").strip()
            ts = _parse_int(row.get("timestamp")) or 0
            price = Decimal("0")
            if row_type in ("SellActivity", "BuyActivity"):
                price = _wei_to_usdt(row.get("amount"))
            elif row_type in ("PerpetualBuybackActivity", "BuybackActivity"):
                price = _wei_to_usdt(row.get("priceInUsdt"))
                if price <= 0:
                    price = _wei_to_usdt(row.get("amount"))
            if price > 0 and ts >= best_ts:
                best_ts = ts
                best_value = price
        return best_value if best_value > 0 else Decimal("0")
    except Exception:
        return Decimal("0")


def _fetch_token_activities(token_id: str) -> list[dict]:
    all_rows: list[dict] = []
    seen_cursors: set[str] = set()
    cursor: str | None = None
    tid = str(token_id or "").strip()
    if not tid:
        return all_rows

    for _ in range(PROFILE_TOKEN_ACTIVITY_MAX_PAGES):
        page = _trpc_token_activities(tid, cursor=cursor, limit=PROFILE_TOKEN_ACTIVITY_PAGE_LIMIT)
        activities = page.get("activities") or []
        if isinstance(activities, list):
            all_rows.extend([x for x in activities if isinstance(x, dict)])
        next_cursor = page.get("nextCursor")
        if not next_cursor:
            break
        next_cursor = str(next_cursor)
        if next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    return all_rows


def _fetch_card_fmv_by_token_id(token_id: str, allow_trade_fallback: bool = True) -> Decimal:
    tid = str(token_id or "").strip()
    if not tid:
        return Decimal("0")
    cache_key = tid if allow_trade_fallback else f"{tid}|no_trade"
    if PROFILE_ENABLE_RUNTIME_CACHE and cache_key in _CARD_FMV_CACHE:
        return _CARD_FMV_CACHE[cache_key]

    value = Decimal("0")
    try:
        collectible = _collectible_by_token_cached(tid)
        fmv_raw = _to_decimal(collectible.get("fmvPriceInUSD"))
        buyback_raw = _to_decimal(collectible.get("buybackBaseValueInUSD"))
        ask_raw = _to_decimal(collectible.get("askPriceInUSDT"))
        for candidate in (
            _card_price_to_usd(fmv_raw),
            _card_price_to_usd(buyback_raw),
            _card_price_to_usd(ask_raw),
        ):
            if candidate > 0:
                value = candidate
                break
    except Exception:
        value = Decimal("0")

    # Burned/withdrawn cards may no longer be queryable by collectible token API.
    # Fallback to the latest token-level trade/buyback amount as an estimated card value.
    if allow_trade_fallback and value <= 0:
        value = _fetch_card_trade_fallback_by_token_id(tid)

    if PROFILE_ENABLE_RUNTIME_CACHE:
        _CARD_FMV_CACHE[cache_key] = value
    return value


def _fetch_card_image_by_token_id(token_id: str) -> str:
    tid = str(token_id or "").strip()
    if not tid:
        return ""
    if PROFILE_ENABLE_RUNTIME_CACHE and tid in _CARD_IMAGE_CACHE:
        return _CARD_IMAGE_CACHE[tid]

    image_url = ""
    try:
        collectible = _collectible_by_token_cached(tid)
        image_url = str(
            collectible.get("frontImageUrl")
            or collectible.get("imageUrl")
            or collectible.get("collectibleImageUrl")
            or ""
        ).strip()
    except Exception:
        image_url = ""

    if PROFILE_ENABLE_RUNTIME_CACHE:
        _CARD_IMAGE_CACHE[tid] = image_url
    return image_url


def _fetch_user_activities(wallet_address: str) -> list[dict]:
    all_rows: list[dict] = []
    seen_cursors: set[str] = set()
    cursor: str | None = None

    for _ in range(PROFILE_ACTIVITY_MAX_PAGES):
        page = _trpc_user_activities(wallet_address, cursor=cursor)
        activities = page.get("activities") or []
        if isinstance(activities, list):
            all_rows.extend([x for x in activities if isinstance(x, dict)])
        next_cursor = page.get("nextCursor")
        if not next_cursor:
            break
        next_cursor = str(next_cursor)
        if next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    return all_rows


def _pick_contract_unit_price(price_counter: Counter) -> Decimal:
    if not price_counter:
        return Decimal("0")
    # Prefer the most frequent observed price for this contract.
    pairs = sorted(price_counter.items(), key=lambda x: (x[1], x[0]), reverse=True)
    return _to_decimal(pairs[0][0])


def _pack_label_from_pull_item(item: dict | None) -> str:
    obj = item or {}
    title = str(obj.get("title") or "").strip()
    subtitle = str(obj.get("subtitle") or "").strip()
    if title and subtitle:
        return f"{title} | {subtitle}"
    if title:
        return title
    if subtitle:
        return subtitle
    return "Unknown Pack"


def _build_wallet_activity_history(wallet_address: str, profile_lang: str = "en") -> dict:
    lang = _profile_lang_from_locale(profile_lang)
    labels = _profile_history_labels(lang)
    wallet_norm = _normalize_wallet_address(wallet_address) or str(wallet_address or "").strip().lower()
    activities = _fetch_user_activities(wallet_norm)

    type_counts: Counter = Counter()
    pull_price_by_checkout: dict[str, Decimal] = {}
    contract_pull_price_counter: defaultdict[str, Counter] = defaultdict(Counter)
    contract_pack_counter: defaultdict[str, Counter] = defaultdict(Counter)
    contract_pack_name: dict[str, str] = {}
    contract_open_count: defaultdict[str, int] = defaultdict(int)
    contract_direct_count: defaultdict[str, int] = defaultdict(int)
    contract_inferred_count: defaultdict[str, int] = defaultdict(int)
    contract_spent_total: defaultdict[str, Decimal] = defaultdict(lambda: Decimal("0"))

    pull_spent_total = Decimal("0")
    buyback_earned_total = Decimal("0")
    trade_spent_total = Decimal("0")
    trade_earned_total = Decimal("0")
    card_withdraw_total = Decimal("0")
    direct_price_count = 0
    inferred_price_count = 0
    unknown_price_count = 0
    inferred_spent_total = Decimal("0")

    ts_values: list[int] = []
    legacy_missing_price_keys: list[str] = []
    release_seen_open_keys: set[str] = set()
    legacy_seen_open_keys: set[str] = set()
    seen_withdraw_events: set[str] = set()
    withdraw_token_ids: set[str] = set()
    release_cards_by_token: dict[str, dict] = {}
    token_latest_values: dict[str, tuple[int, Decimal]] = {}

    def _remember_token_value(token_id: str, value: Decimal, ts_value: int):
        tid = str(token_id or "").strip()
        if not tid or value <= 0:
            return
        prev = token_latest_values.get(tid)
        if prev is None or ts_value >= prev[0]:
            token_latest_values[tid] = (ts_value, value)

    for row in activities:
        row_type = str(row.get("__typename") or "").strip()
        if not row_type:
            continue
        type_counts[row_type] += 1

        ts = _parse_int(row.get("timestamp"))
        if ts and ts > 0:
            ts_values.append(ts)
        row_item = row.get("item") if isinstance(row.get("item"), dict) else {}
        token_hint = str(row.get("nftTokenId") or row.get("tokenId") or row_item.get("tokenId") or "").strip()

        if row_type in _PROFILE_RELEASE_CARD_TYPES:
            if token_hint:
                current_release = release_cards_by_token.get(token_hint) or {}
                current_ts = _parse_int(current_release.get("timestamp_raw"))
                if not current_release or ts >= current_ts:
                    release_cards_by_token[token_hint] = {
                        "token_id": token_hint,
                        "name": str(row_item.get("collectibleName") or row_item.get("title") or "Unknown Collectible").strip(),
                        "image": str(row_item.get("imageUrl") or row_item.get("collectibleImageUrl") or "").strip(),
                        "timestamp_raw": ts,
                    }

        if row_type == "PerpetualPullActivity":
            checkout_id = str(row.get("checkoutId") or "").strip()
            contract = str(row.get("contractAddress") or "").strip().lower()
            price = _wei_to_usdt(row.get("priceInUsdt"))
            if checkout_id and price > 0:
                pull_price_by_checkout[checkout_id] = price
            if contract and price > 0:
                contract_pull_price_counter[contract][price] += 1
                pull_spent_total += price
            if contract:
                pack_label = _pack_label_from_pull_item(row.get("item") if isinstance(row.get("item"), dict) else None)
                contract_pack_counter[contract][pack_label] += 1
                if contract not in contract_pack_name:
                    contract_pack_name[contract] = pack_label
        elif row_type == "PullActivity":
            checkout_id = str(row.get("checkoutId") or "").strip()
            pack_id = str(row.get("packId") or "").strip()
            pack_label = _pack_label_from_pull_item(row.get("item") if isinstance(row.get("item"), dict) else None)
            # Legacy pull events may not include contractAddress, so group by packId/name.
            legacy_key = f"legacy:{pack_id}" if pack_id else f"legacy-name:{pack_label}"
            legacy_event_key = checkout_id or str(row.get("id") or "").strip() or f"{legacy_key}:{_parse_int(row.get('timestamp')) or 0}"
            if legacy_event_key not in legacy_seen_open_keys:
                legacy_seen_open_keys.add(legacy_event_key)
                contract_open_count[legacy_key] += 1
            contract_pack_counter[legacy_key][pack_label] += 1
            if legacy_key not in contract_pack_name:
                contract_pack_name[legacy_key] = pack_label

            price = _wei_to_usdt(row.get("priceInUsdt"))
            if checkout_id and price > 0:
                pull_price_by_checkout[checkout_id] = price
            if price > 0:
                contract_pull_price_counter[legacy_key][price] += 1
                pull_spent_total += price
                direct_price_count += 1
                contract_direct_count[legacy_key] += 1
                contract_spent_total[legacy_key] += price
            else:
                legacy_missing_price_keys.append(legacy_key)
        elif row_type in ("PerpetualBuybackActivity", "BuybackActivity"):
            buyback_price = _wei_to_usdt(row.get("priceInUsdt"))
            if buyback_price <= 0:
                buyback_price = _wei_to_usdt(row.get("amount"))
            if buyback_price > 0:
                buyback_earned_total += buyback_price
            fmv_hint = _card_price_to_usd(row.get("fmvPriceInUsd"))
            _remember_token_value(token_hint, fmv_hint if fmv_hint > 0 else buyback_price, ts)
        elif row_type in ("BuyActivity", "SellActivity"):
            amount = _wei_to_usdt(row.get("amount"))
            if amount <= 0:
                continue
            bidder = str(row.get("bidder") or "").strip().lower()
            asker = str(row.get("asker") or "").strip().lower()
            if bidder == wallet_norm:
                trade_spent_total += amount
            if asker == wallet_norm:
                divisor = PROFILE_MARKET_SELL_GROSS_DIVISOR if PROFILE_MARKET_SELL_GROSS_DIVISOR > 0 else Decimal("1")
                trade_earned_total += (amount / divisor)
        elif row_type == "TransferActivity":
            target = str(row.get("to") or "").strip().lower()
            if target != PROFILE_CARD_WITHDRAW_ADDRESS:
                continue
            row_item = row.get("item") if isinstance(row.get("item"), dict) else {}
            token_id = str(row.get("tokenId") or row_item.get("tokenId") or "").strip()
            tx_hash = str(row.get("txHash") or "").strip().lower()
            if not tx_hash and not token_id:
                continue
            event_key = f"{tx_hash}:{token_id}"
            if event_key in seen_withdraw_events:
                continue
            seen_withdraw_events.add(event_key)
            if token_id:
                withdraw_token_ids.add(token_id)

    # Calculate withdraw-card value with local activity hints first to avoid token-by-token RPC calls.
    unresolved_withdraw_tokens: list[str] = []
    for token_id in withdraw_token_ids:
        hinted_value = _to_decimal((token_latest_values.get(token_id) or (0, Decimal("0")))[1])
        if hinted_value > 0:
            card_withdraw_total += hinted_value
        else:
            unresolved_withdraw_tokens.append(token_id)

    if unresolved_withdraw_tokens:
        workers = min(PROFILE_WITHDRAW_VALUE_WORKERS, len(unresolved_withdraw_tokens))
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_fetch_card_fmv_by_token_id, tid): tid for tid in unresolved_withdraw_tokens}
                for future in as_completed(futures):
                    try:
                        value = _to_decimal(future.result())
                    except Exception:
                        value = Decimal("0")
                    if value > 0:
                        card_withdraw_total += value
        else:
            for token_id in unresolved_withdraw_tokens:
                value = _to_decimal(_fetch_card_fmv_by_token_id(token_id))
                if value > 0:
                    card_withdraw_total += value

    release_rows = [x for x in activities if str(x.get("__typename") or "").strip() == "PerpetualReleaseTokenActivity"]
    legacy_pull_rows = [x for x in activities if str(x.get("__typename") or "").strip() == "PullActivity"]
    for row in release_rows:
        contract = str(row.get("contractAddress") or "").strip().lower()
        checkout_id = str(row.get("checkoutId") or "").strip()
        release_event_key = checkout_id or str(row.get("id") or "").strip() or f"{contract}:{_parse_int(row.get('timestamp')) or 0}"
        if contract and release_event_key not in release_seen_open_keys:
            release_seen_open_keys.add(release_event_key)
            contract_open_count[contract] += 1

        price = Decimal("0")
        direct_hit = False
        if checkout_id and checkout_id in pull_price_by_checkout:
            price = pull_price_by_checkout[checkout_id]
            direct_hit = price > 0
        if not direct_hit:
            inferred = _pick_contract_unit_price(contract_pull_price_counter.get(contract) or Counter())
            if inferred > 0:
                price = inferred
                inferred_price_count += 1
                inferred_spent_total += inferred
                if contract:
                    contract_inferred_count[contract] += 1
            else:
                unknown_price_count += 1
        else:
            direct_price_count += 1
            if contract:
                contract_direct_count[contract] += 1

        if contract and price > 0:
            contract_spent_total[contract] += price

    # Legacy pull rows can miss priceInUsdt; infer by same pack key's observed unit price.
    for legacy_key in legacy_missing_price_keys:
        inferred = _pick_contract_unit_price(contract_pull_price_counter.get(legacy_key) or Counter())
        if inferred > 0:
            inferred_price_count += 1
            inferred_spent_total += inferred
            contract_inferred_count[legacy_key] += 1
            contract_spent_total[legacy_key] += inferred
        else:
            unknown_price_count += 1

    pack_spent_total = pull_spent_total + inferred_spent_total
    total_spent = pack_spent_total + trade_spent_total
    total_earned = buyback_earned_total + trade_earned_total
    net_total = total_earned - total_spent + card_withdraw_total
    trade_volume = trade_spent_total + trade_earned_total

    contract_rows = []
    all_contracts = sorted(set(contract_open_count.keys()) | set(contract_pull_price_counter.keys()))
    for contract in all_contracts:
        price_counter = contract_pull_price_counter.get(contract) or Counter()
        unit_price = _pick_contract_unit_price(price_counter)
        pack_counter = contract_pack_counter.get(contract) or Counter()
        if pack_counter:
            pack_name = pack_counter.most_common(1)[0][0]
        else:
            pack_name = contract_pack_name.get(contract) or f"Contract {_short_hex(contract)}"
        is_legacy_pack = contract.startswith("legacy")
        contract_full = "-" if is_legacy_pack else contract
        contract_short = "-" if is_legacy_pack else _short_hex(contract)
        contract_rows.append(
            {
                "pack_name": pack_name,
                "contract": contract_full,
                "contract_short": contract_short,
                "open_count": int(contract_open_count.get(contract, 0)),
                "direct_count": int(contract_direct_count.get(contract, 0)),
                "inferred_count": int(contract_inferred_count.get(contract, 0)),
                "unit_price": _format_usdt_decimal(unit_price) if unit_price > 0 else "-",
                "spent_total": _format_usdt_decimal(contract_spent_total.get(contract)),
                "spent_total_raw": contract_spent_total.get(contract, Decimal("0")),
            }
        )
    contract_rows.sort(key=lambda x: (x["spent_total_raw"], x["open_count"]), reverse=True)
    for row in contract_rows:
        row.pop("spent_total_raw", None)

    opened_pack_ids = set(release_seen_open_keys) | set(legacy_seen_open_keys)

    ordered_activity_types = [
        "PerpetualReleaseTokenActivity",
        "PerpetualPullActivity",
        "PerpetualBuybackActivity",
        "BuyActivity",
        "SellActivity",
        "SBTMintActivity",
    ]
    hidden_activity_types = {
        "PerpetualPullActivity",
        "SBTMintActivity",
        "BuybackActivity",
        "MintActivity",
        "PullActivity",
        "TransferActivity",
    }
    total_buyback_count = int(type_counts.get("PerpetualBuybackActivity", 0) + type_counts.get("BuybackActivity", 0))
    activity_rows = []
    for t in ordered_activity_types:
        if t in hidden_activity_types:
            continue
        count_val = int(type_counts.get(t, 0))
        if t == "PerpetualReleaseTokenActivity":
            count_val = len(opened_pack_ids)
        elif t == "PerpetualBuybackActivity":
            count_val = total_buyback_count
        if count_val <= 0:
            continue
        activity_rows.append(
            {
                "name": _profile_activity_display_name(t, lang),
                "count": count_val,
                "type": t,
                "highlight": (t == "PerpetualReleaseTokenActivity"),
            }
        )
    known_types = set(ordered_activity_types)
    for t, c in sorted(type_counts.items()):
        if t in known_types or t in hidden_activity_types:
            continue
        activity_rows.append(
            {
                "name": _profile_activity_display_name(t, lang),
                "count": int(c),
                "type": t,
                "highlight": False,
            }
        )

    history_range = "-"
    active_days_count = 0
    if ts_values:
        ts_min = min(ts_values)
        ts_max = max(ts_values)
        dt_min = datetime.utcfromtimestamp(ts_min).strftime("%Y-%m-%d")
        dt_max = datetime.utcfromtimestamp(ts_max).strftime("%Y-%m-%d")
        history_range = dt_min if dt_min == dt_max else f"{dt_min} ~ {dt_max}"
        active_days_count = max(1, int((ts_max - ts_min) // 86400) + 1)

    release_cards = sorted(
        (v for v in release_cards_by_token.values() if isinstance(v, dict)),
        key=lambda x: _parse_int(x.get("timestamp_raw")),
        reverse=True,
    )
    for card in release_cards:
        card.pop("timestamp_raw", None)
    token_value_hints = {k: v[1] for k, v in token_latest_values.items()}

    return {
        "labels": labels,
        "history_range": history_range,
        "wallet_short": f"{wallet_norm[:6]}...{wallet_norm[-4:]}" if wallet_norm and len(wallet_norm) >= 10 else wallet_norm,
        "activity_total_count": len(activities),
        "opened_packs_count": len(opened_pack_ids),
        "direct_price_count": direct_price_count,
        "inferred_price_count": inferred_price_count,
        "unknown_price_count": unknown_price_count,
        "pack_spent_total": pack_spent_total,
        "total_spent": total_spent,
        "total_earned": total_earned,
        "net_total": net_total,
        "trade_volume": trade_volume,
        "buyback_total": buyback_earned_total,
        "market_buy_total": trade_spent_total,
        "market_sell_total": trade_earned_total,
        "card_withdraw_total": card_withdraw_total,
        "active_days_count": active_days_count,
        "contract_rows": contract_rows,
        "activity_rows": activity_rows,
        "release_cards": release_cards,
        "token_value_hints": token_value_hints,
    }


def _build_wallet_extremes_template_context(
    history_data: dict,
    parsed_sorted: list[dict],
    profile_name: str,
    short_wallet: str,
    profile_lang: str = "en",
) -> dict:
    labels = _profile_extreme_labels(_profile_lang_from_locale(profile_lang))
    ui_labels = _profile_ui_labels(_profile_lang_from_locale(profile_lang))
    current_cards_by_token: dict[str, dict] = {}
    for row in parsed_sorted:
        raw = (row or {}).get("raw") or {}
        token_id = str(raw.get("tokenId") or "").strip()
        if not token_id:
            continue
        value = _to_decimal((row or {}).get("fmv")) / Decimal("100")
        current_cards_by_token[token_id] = {
            "token_id": token_id,
            "name": str(raw.get("name") or "Unknown Collectible"),
            "image": str(raw.get("frontImageUrl") or ""),
            "value": value,
        }

    release_cards = (history_data or {}).get("release_cards") or []
    token_value_hints = (history_data or {}).get("token_value_hints") or {}

    seen_tokens: set[str] = set()

    def _candidate_key(c: dict) -> tuple[Decimal, str]:
        return (_to_decimal(c.get("value")), str(c.get("token_id") or ""))

    highest: dict | None = None
    lowest: dict | None = None
    second_lowest: dict | None = None
    candidate_count = 0
    unresolved_release_tokens: list[dict] = []

    def _consider(cand: dict):
        nonlocal highest, lowest, second_lowest, candidate_count
        if not cand:
            return
        value = _to_decimal(cand.get("value"))
        if value <= 0:
            return
        candidate_count += 1
        ck = _candidate_key(cand)
        if highest is None or ck > _candidate_key(highest):
            highest = cand
        if lowest is None or ck < _candidate_key(lowest):
            second_lowest = lowest
            lowest = cand
        elif second_lowest is None or ck < _candidate_key(second_lowest):
            second_lowest = cand

    for card in release_cards:
        if not isinstance(card, dict):
            continue
        token_id = str(card.get("token_id") or "").strip()
        if not token_id or token_id in seen_tokens:
            continue
        seen_tokens.add(token_id)
        hinted_value = _to_decimal(token_value_hints.get(token_id))
        current_info = current_cards_by_token.get(token_id) or {}
        value = hinted_value if hinted_value > 0 else _to_decimal(current_info.get("value"))
        if value <= 0:
            unresolved_release_tokens.append(
                {
                    "token_id": token_id,
                    "name": str(card.get("name") or current_info.get("name") or "Unknown Collectible"),
                    "image": str(current_info.get("image") or ""),
                    "release_image": str(card.get("image") or "").strip(),
                }
            )
            continue
        current_image = str(current_info.get("image") or "").strip()
        release_image = str(card.get("image") or "").strip()
        _consider(
            {
                "token_id": token_id,
                "name": str(card.get("name") or current_info.get("name") or "Unknown Collectible"),
                "image": current_image or release_image,
                "release_image": release_image,
                "need_api_image": bool(not current_image and (not release_image or "graded-cards-renders" not in release_image)),
                "value": value,
            }
        )

    if candidate_count < 2 and unresolved_release_tokens:
        workers = min(8, len(unresolved_release_tokens))
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(_fetch_card_fmv_by_token_id, str(x.get("token_id") or ""), False): x
                    for x in unresolved_release_tokens
                }
                for future in as_completed(futures):
                    base = futures[future]
                    try:
                        value = _to_decimal(future.result())
                    except Exception:
                        value = Decimal("0")
                    if value <= 0:
                        continue
                    image = str(base.get("image") or "").strip()
                    release_image = str(base.get("release_image") or "").strip()
                    _consider(
                        {
                            "token_id": str(base.get("token_id") or ""),
                            "name": str(base.get("name") or "Unknown Collectible"),
                            "image": image or release_image,
                            "release_image": release_image,
                            "need_api_image": bool(not image and (not release_image or "graded-cards-renders" not in release_image)),
                            "value": value,
                        }
                    )
        else:
            for base in unresolved_release_tokens:
                token_id = str(base.get("token_id") or "")
                value = _to_decimal(_fetch_card_fmv_by_token_id(token_id, allow_trade_fallback=False))
                if value <= 0:
                    continue
                image = str(base.get("image") or "").strip()
                release_image = str(base.get("release_image") or "").strip()
                _consider(
                    {
                        "token_id": token_id,
                        "name": str(base.get("name") or "Unknown Collectible"),
                        "image": image or release_image,
                        "release_image": release_image,
                        "need_api_image": bool(not image and (not release_image or "graded-cards-renders" not in release_image)),
                        "value": value,
                    }
                )

    if highest and lowest:
        if str(highest.get("token_id")) == str(lowest.get("token_id")) and second_lowest:
            lowest = second_lowest
        has_data = True
    else:
        highest = {
            "name": labels.get("no_data", "No historical pull data available"),
            "image": _TRANSPARENT_CARD_IMAGE,
            "value": Decimal("0"),
        }
        lowest = dict(highest)
        has_data = False

    def _resolve_extreme_image(cand: dict) -> str:
        token_id = str(cand.get("token_id") or "").strip()
        image = str(cand.get("image") or "").strip()
        if (not image) or bool(cand.get("need_api_image")):
            api_image = _fetch_card_image_by_token_id(token_id) if token_id else ""
            if api_image:
                image = api_image
            elif not image:
                image = str(cand.get("release_image") or "").strip()
        return _prepare_collectible_image_for_poster(image)

    if has_data:
        with ThreadPoolExecutor(max_workers=2) as pool:
            f_hi = pool.submit(_resolve_extreme_image, highest)
            f_lo = pool.submit(_resolve_extreme_image, lowest)
            highest_prepared_image = f_hi.result()
            lowest_prepared_image = f_lo.result()
    else:
        highest_prepared_image = _prepare_collectible_image_for_poster(str(highest.get("image") or ""))
        lowest_prepared_image = _prepare_collectible_image_for_poster(str(lowest.get("image") or ""))

    items = [
        {
            "name": f"{labels.get('high_label', 'Highest Value')} / {str(highest.get('name') or 'Unknown Collectible')}",
            "image": highest_prepared_image,
            "value": _format_usdt_decimal(highest.get("value")),
        },
        {
            "name": f"{labels.get('low_label', 'Lowest Value')} / {str(lowest.get('name') or 'Unknown Collectible')}",
            "image": lowest_prepared_image,
            "value": _format_usdt_decimal(lowest.get("value")),
        },
    ]
    pair_total = _to_decimal(highest.get("value")) + _to_decimal(lowest.get("value"))

    sbt_badges_display = []
    if has_data:
        sbt_badges_display = [
            {"name": labels.get("high_label", "Highest Value"), "label": labels.get("high_label", "High"), "balance": 1, "image": ""},
            {"name": labels.get("low_label", "Lowest Value"), "label": labels.get("low_label", "Low"), "balance": 1, "image": ""},
        ]

    return {
        "collection_name": f"{profile_name} Collection",
        "sbt_total": len(items) if has_data else 0,
        "extreme_mode": True,
        "hide_footer": True,
        "sbt_badges_display": sbt_badges_display,
        "items": items,
        "assets_count": len(release_cards),
        "total_value": _format_usdt_decimal(pair_total),
        "total_value_label": labels.get("top_value_label", "Historical Top/Bottom Pair Value"),
        "items_count_label": labels.get("items_count_label", ui_labels.get("items_count_label", "Items Count")),
        "assets_unit": labels.get("assets_unit", ui_labels.get("assets_unit", "Assets")),
        "sbt_badges_label": labels.get("sbt_badges_label", ui_labels.get("sbt_badges_label", "SBT Badges")),
        "no_sbt_label": labels.get("no_data", ui_labels.get("no_sbt_label", "No data")),
        "owned_prefix": ui_labels.get("owned_prefix", "owned "),
        "brand_name": ui_labels.get("brand_name", "Renaiss"),
        "brand_site": ui_labels.get("brand_site", "renaiss.xyz"),
        "update_date": datetime.now().strftime("%Y-%m-%d"),
        "enable_tilt": False,
        "background_key": "classic",
        "background_image": _profile_background_data_uri("classic"),
        "_meta_wallet_short": short_wallet,
        "_meta_subtitle": labels.get("subtitle", "Highest / Lowest value among historical pulls"),
    }


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
        resp = _http_get(RENAISS_SBT_BADGES_URL, params=params, timeout=25)
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
    wallet_address = _normalize_wallet_address(wallet_address or "") or str(wallet_address or "").strip().lower()
    if not wallet_address:
        return None, None

    def _find_owner_in_collection(collection: list[dict]) -> tuple[str | None, str | None]:
        for item in collection:
            owner_addr = str(item.get("ownerAddress") or "").lower()
            if owner_addr != wallet_address:
                continue
            owner = item.get("owner") or {}
            user_id = owner.get("id")
            username = owner.get("username")
            if user_id:
                return str(user_id), (str(username) if username is not None else None)
        return None, None

    first_payload = {
        "limit": PROFILE_PAGE_LIMIT,
        "offset": 0,
        "sortBy": "mintDate",
        "sortOrder": "desc",
        "includeOpenCardPackRecords": True,
    }
    first_result = _trpc_collectible_list(first_payload)
    first_collection = first_result.get("collection") or []
    found_user_id, found_username = _find_owner_in_collection(first_collection if isinstance(first_collection, list) else [])
    if found_user_id:
        return found_user_id, found_username

    first_pagination = first_result.get("pagination") or {}
    if not first_pagination.get("hasMore"):
        return None, None

    limit_val = _parse_int(first_pagination.get("limit")) or PROFILE_PAGE_LIMIT
    next_offset = max(limit_val, len(first_collection) if isinstance(first_collection, list) else limit_val)
    max_offset = PROFILE_SCAN_MAX_OFFSET
    if next_offset > max_offset:
        return None, None

    offsets = list(range(next_offset, max_offset + 1, limit_val))
    workers = min(PROFILE_RESOLVE_SCAN_WORKERS, len(offsets))
    if workers <= 1:
        workers = 1

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for batch_start in range(0, len(offsets), workers):
            batch = offsets[batch_start : batch_start + workers]
            futures = {}
            for offset in batch:
                payload = {
                    "limit": limit_val,
                    "offset": offset,
                    "sortBy": "mintDate",
                    "sortOrder": "desc",
                    "includeOpenCardPackRecords": True,
                }
                futures[pool.submit(_trpc_collectible_list, payload)] = offset
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception:
                    continue
                collection = result.get("collection") or []
                if not isinstance(collection, list):
                    continue
                found_user_id, found_username = _find_owner_in_collection(collection)
                if found_user_id:
                    return found_user_id, found_username
    return None, None


def _fetch_user_collection(user_id: str) -> list[dict]:
    uid = str(user_id or "").strip()
    if not uid:
        return []

    first_payload = {
        "limit": PROFILE_PAGE_LIMIT,
        "offset": 0,
        "sortBy": "mintDate",
        "sortOrder": "desc",
        "userId": uid,
        "includeOpenCardPackRecords": True,
    }
    first_result = _trpc_collectible_list(first_payload)
    first_collection = first_result.get("collection") or []
    all_items: list[dict] = list(first_collection) if isinstance(first_collection, list) else []

    pagination = first_result.get("pagination") or {}
    if not pagination.get("hasMore"):
        return all_items

    limit_val = _parse_int(pagination.get("limit")) or PROFILE_PAGE_LIMIT
    total_val = _parse_int(pagination.get("total")) or 0

    if total_val <= len(all_items):
        return all_items

    offsets = list(range(limit_val, total_val, limit_val))
    if not offsets:
        return all_items

    workers = min(PROFILE_COLLECTION_FETCH_WORKERS, len(offsets))
    page_rows: dict[int, list[dict]] = {}
    failed_offsets: list[int] = []

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        future_map = {}
        for offset in offsets:
            payload = {
                "limit": limit_val,
                "offset": offset,
                "sortBy": "mintDate",
                "sortOrder": "desc",
                "userId": uid,
                "includeOpenCardPackRecords": True,
            }
            future_map[pool.submit(_trpc_collectible_list, payload)] = offset
        for future in as_completed(future_map):
            offset = future_map[future]
            try:
                result = future.result()
                rows = result.get("collection") or []
                page_rows[offset] = rows if isinstance(rows, list) else []
            except Exception:
                failed_offsets.append(offset)

    # Fallback retry sequentially for failed pages to maximize completeness.
    for offset in sorted(failed_offsets):
        payload = {
            "limit": limit_val,
            "offset": offset,
            "sortBy": "mintDate",
            "sortOrder": "desc",
            "userId": uid,
            "includeOpenCardPackRecords": True,
        }
        try:
            result = _trpc_collectible_list(payload)
            rows = result.get("collection") or []
            page_rows[offset] = rows if isinstance(rows, list) else []
        except Exception:
            page_rows[offset] = []

    for offset in sorted(offsets):
        all_items.extend(page_rows.get(offset, []))
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
    wallet_norm = _normalize_wallet_address(wallet_address) or str(wallet_address or "").strip().lower()
    wallet_short = (
        f"{wallet_norm[:6]}...{wallet_norm[-4:]}" if wallet_norm and len(wallet_norm) >= 10 else (wallet_norm or "Unknown User")
    )
    try:
        user_id, username = _resolve_user_from_wallet(wallet_address)
    except Exception:
        user_id, username = None, None
    if not user_id:
        return {
            "username": wallet_short,
            "user_id": None,
            "collection_count": 0,
            "card_options": [],
            "owned_sbt_count": 0,
            "sbt_options": [],
        }

    try:
        collection = _fetch_user_collection(user_id)
    except Exception:
        collection = []
    if not collection:
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
            "username": str(username or wallet_short),
            "user_id": user_id,
            "collection_count": 0,
            "card_options": [],
            "owned_sbt_count": len(owned_badges),
            "sbt_options": sbt_options,
        }

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
    background_style: str = "classic",
) -> dict:
    profile_lang = _profile_lang_from_locale(profile_lang)
    ui_labels = _profile_ui_labels(profile_lang)
    background_key = _normalize_profile_background_key(background_style)
    background_image = _profile_background_data_uri(background_key)

    # Start history build in background so collection/sbt/image preparation can run in parallel.
    history_holder: dict[str, object] = {"data": None, "error": None}

    def _history_worker():
        try:
            history_holder["data"] = _build_wallet_activity_history(wallet_address, profile_lang=profile_lang)
        except Exception as e:
            history_holder["error"] = e

    history_thread = threading.Thread(target=_history_worker, name="profile-history-worker", daemon=True)
    history_thread.start()

    try:
        user_id, username = _resolve_user_from_wallet(wallet_address)
    except Exception:
        user_id, username = None, None
    wallet_norm = _normalize_wallet_address(wallet_address) or str(wallet_address or "").strip().lower()
    short_wallet = f"{wallet_norm[:6]}...{wallet_norm[-4:]}" if wallet_norm and len(wallet_norm) >= 10 else wallet_norm
    if user_id:
        try:
            collection = _fetch_user_collection(user_id)
        except Exception:
            collection = []
    else:
        collection = []
    has_collection = bool(collection)

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
    hero = parsed_sorted[0]["raw"] if parsed_sorted else {}
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

    hero_name = str(hero.get("name") or "Collection Overview")
    profile_name = str(username or short_wallet or "Unknown User")
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
        badge_image = _prepare_sbt_badge_image_for_poster(str(b.get("image_url") or "").strip())
        display_badges.append(
            {
                "name": name,
                "label": _compact_sbt_label(name),
                "balance": _parse_int(b.get("balance")) or 0,
                "image": badge_image,
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

    history_thread.join()
    history_error = history_holder.get("error")
    history_data = history_holder.get("data") if isinstance(history_holder.get("data"), dict) else None
    if history_error is not None or not isinstance(history_data, dict):
        if history_error is not None:
            print(f"⚠️ activity history build failed: {history_error}", file=sys.stderr)
        history_data = {
            "labels": _profile_history_labels(profile_lang),
            "history_range": "-",
            "wallet_short": short_wallet,
            "activity_total_count": 0,
            "opened_packs_count": 0,
            "direct_price_count": 0,
            "inferred_price_count": 0,
            "unknown_price_count": 0,
            "pack_spent_total": Decimal("0"),
            "total_spent": Decimal("0"),
            "total_earned": Decimal("0"),
            "net_total": Decimal("0"),
            "trade_volume": Decimal("0"),
            "buyback_total": Decimal("0"),
            "market_buy_total": Decimal("0"),
            "market_sell_total": Decimal("0"),
            "card_withdraw_total": Decimal("0"),
            "active_days_count": 0,
            "contract_rows": [],
            "activity_rows": [],
            "release_cards": [],
            "token_value_hints": {},
        }
    history_labels = history_data.get("labels") or _profile_history_labels(profile_lang)
    history_activity_rows = history_data.get("activity_rows") or []
    history_contract_rows = history_data.get("contract_rows") or []
    holdings_value = _to_decimal(total_fmv) / Decimal("100")
    cash_net = _to_decimal(history_data.get("net_total"))
    net_with_holdings = cash_net + holdings_value

    history_template_context = {
        "collection_name": f"{profile_name} Collection",
        "brand_name": ui_labels["brand_name"],
        "brand_site": ui_labels["brand_site"],
        "wallet_short": history_data.get("wallet_short") or short_wallet,
        "update_date": datetime.now().strftime("%Y-%m-%d"),
        "history_title": history_labels.get("title", "Collection History"),
        "history_subtitle": history_labels.get("subtitle", "Pack and trade history overview"),
        "history_range": history_data.get("history_range") or "-",
        "section_contract": history_labels.get("section_contract", "Contract / Pack Breakdown"),
        "section_activity": history_labels.get("section_activity", "Activity Counts"),
        "section_note": history_labels.get("section_note", "Calculation Notes"),
        "head_pack": history_labels.get("head_pack", "Pack"),
        "head_contract": history_labels.get("head_contract", "Contract"),
        "head_open_count": history_labels.get("head_open_count", "Opened"),
        "head_direct_count": history_labels.get("head_direct_count", "Direct"),
        "head_inferred_count": history_labels.get("head_inferred_count", "Inferred"),
        "head_unit_price": history_labels.get("head_unit_price", "Unit (USDT)"),
        "head_spent_total": history_labels.get("head_spent_total", "Total (USDT)"),
        "empty_contract": history_labels.get("empty_contract", "No pack-open data available"),
        "metric_opened_label": history_labels.get("kpi_opened", "Packs Opened"),
        "metric_opened_value": _format_number(history_data.get("opened_packs_count")),
        "metric_pack_spent_label": history_labels.get("kpi_pack_spent", "Pack Spend"),
        "metric_pack_spent_value": _format_usdt_decimal(history_data.get("pack_spent_total")),
        "metric_card_withdraw_label": history_labels.get("kpi_card_withdraw", "Card Withdrawal Value"),
        "metric_card_withdraw_value": _format_usdt_decimal(history_data.get("card_withdraw_total")),
        "metric_total_spent_label": history_labels.get("kpi_total_spent", "Total Spent"),
        "metric_total_spent_note": history_labels.get("kpi_total_spent_note", ""),
        "metric_total_spent_value": _format_usdt_decimal(history_data.get("total_spent")),
        "metric_total_earned_label": history_labels.get("kpi_total_earned", "Total Earned"),
        "metric_total_earned_note": history_labels.get("kpi_total_earned_note", ""),
        "metric_total_earned_value": _format_usdt_decimal(history_data.get("total_earned")),
        "metric_net_label": history_labels.get("kpi_net", "Net PnL"),
        "metric_net_note": history_labels.get("kpi_net_note", ""),
        "metric_net_value": _format_usdt_currency(net_with_holdings, signed=True),
        "metric_trade_volume_label": history_labels.get("kpi_trade_volume", "Trade Volume"),
        "metric_trade_volume_value": _format_usdt_decimal(history_data.get("trade_volume")),
        "metric_assets_value_label": history_labels.get("kpi_assets_value", "Holdings Value"),
        "metric_assets_value_value": _format_usdt_decimal(holdings_value),
        "metric_buyback_label": history_labels.get("kpi_buyback", "Buyback Total"),
        "metric_buyback_value": _format_usdt_decimal(history_data.get("buyback_total")),
        "metric_market_buy_label": history_labels.get("kpi_market_buy", "Market Buy Total"),
        "metric_market_buy_value": _format_usdt_decimal(history_data.get("market_buy_total")),
        "metric_market_sell_label": history_labels.get("kpi_market_sell", "Market Sell Total"),
        "metric_market_sell_value": _format_usdt_decimal(history_data.get("market_sell_total")),
        "active_days_label": history_labels.get("kpi_active_days", "Active Days"),
        "active_days_value": _format_number(history_data.get("active_days_count")),
        "activity_total_label": history_labels.get("activity_total", "Total Activities"),
        "activity_total_value": _format_number(history_data.get("activity_total_count")),
        "chip_direct_label": history_labels.get("chip_direct", "Direct"),
        "chip_inferred_label": history_labels.get("chip_inferred", "Inferred"),
        "chip_unknown_label": history_labels.get("chip_unknown", "Unknown"),
        "direct_price_count": _format_number(history_data.get("direct_price_count")),
        "inferred_price_count": _format_number(history_data.get("inferred_price_count")),
        "unknown_price_count": _format_number(history_data.get("unknown_price_count")),
        "note_line_1": history_labels.get("note_line_1", ""),
        "note_line_2": history_labels.get("note_line_2", ""),
        "contract_rows": history_contract_rows,
        "activity_rows": history_activity_rows,
    }
    extremes_template_context = _build_wallet_extremes_template_context(
        history_data=history_data,
        parsed_sorted=parsed_sorted,
        profile_name=profile_name,
        short_wallet=(history_data.get("wallet_short") or short_wallet),
        profile_lang=profile_lang,
    )

    return {
        "username": profile_name,
        "user_id": user_id,
        "wallet_address": wallet_address,
        "profile_poster_enabled": bool(has_collection and len(poster_items_raw) > 0),
        "count": total_count,
        "total_fmv": total_fmv,
        "shown_count": len(poster_items_raw),
        "shown_fmv": poster_total_fmv,
        "template_context": {
            "collection_name": f"{profile_name} Collection",
            "sbt_total": sbt_total,
            "sbt_badges_display": display_badges,
            "items": poster_items,
            "assets_count": total_count,
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
            "background_key": background_key,
            "background_image": background_image,
        },
        "history_template_context": history_template_context,
        "extreme_template_context": extremes_template_context,
        "history_summary": {
            "opened_packs": history_data.get("opened_packs_count", 0),
            "total_spent": history_data.get("total_spent", Decimal("0")),
            "total_earned": history_data.get("total_earned", Decimal("0")),
            "net_total": net_with_holdings,
            "cash_net": cash_net,
            "holdings_value": holdings_value,
            "buyback_total": history_data.get("buyback_total", Decimal("0")),
            "market_buy_total": history_data.get("market_buy_total", Decimal("0")),
            "market_sell_total": history_data.get("market_sell_total", Decimal("0")),
            "card_withdraw_total": history_data.get("card_withdraw_total", Decimal("0")),
        },
        "replacements": {
            "{{ card_name }}": html_lib.escape(f"{profile_name} Vault"),
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
            "{{ illustrator }}": html_lib.escape(profile_name),
            "{{ release_info }}": html_lib.escape(f"{short_wallet} · TOP: {hero_name[:30]}"),
        },
    }


async def _render_wallet_profile_poster(template_payload: dict, out_dir: str, safe_name: str = "wallet_profile") -> str:
    if isinstance(template_payload, dict):
        replacements = template_payload.get("replacements") or {}
        template_context = template_payload.get("template_context") or {}
    else:
        replacements = template_payload or {}
        template_context = {}

    html_doc = _render_wallet_template_html(
        PROFILE_TEMPLATE_PATH,
        template_context=template_context,
        replacements=replacements,
    )
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


async def _render_wallet_profile_history_poster(template_payload: dict, out_dir: str, safe_name: str = "wallet_profile") -> str:
    if isinstance(template_payload, dict):
        template_context = template_payload.get("history_template_context") or {}
        replacements = template_payload.get("history_replacements") or {}
    else:
        template_context = {}
        replacements = {}

    html_doc = _render_wallet_template_html(
        PROFILE_HISTORY_TEMPLATE_PATH,
        template_context=template_context,
        replacements=replacements,
    )

    os.makedirs(out_dir, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", safe_name).strip("_") or "wallet_profile"
    out_path = os.path.join(out_dir, f"{safe}_profile_history.png")
    await image_generator._render_single_html_poster(
        html_doc,
        out_path,
        width=1200,
        height=900,
        device_scale_factor=2,
    )
    return out_path


async def _render_wallet_profile_extreme_poster(template_payload: dict, out_dir: str, safe_name: str = "wallet_profile") -> str:
    if isinstance(template_payload, dict):
        template_context = template_payload.get("extreme_template_context") or {}
        replacements = template_payload.get("extreme_replacements") or {}
    else:
        template_context = {}
        replacements = {}

    html_doc = _render_wallet_template_html(
        PROFILE_EXTREMES_TEMPLATE_PATH,
        template_context=template_context,
        replacements=replacements,
    )

    os.makedirs(out_dir, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", safe_name).strip("_") or "wallet_profile"
    out_path = os.path.join(out_dir, f"{safe}_profile_extremes.png")
    await image_generator._render_single_html_poster(
        html_doc,
        out_path,
        width=1200,
        height=900,
        device_scale_factor=2,
    )
    return out_path


def _render_wallet_template_html(template_path: str, template_context: dict | None = None, replacements: dict | None = None) -> str:
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"找不到 profile template: {template_path}")

    with open(template_path, "r", encoding="utf-8") as f:
        html_doc = f.read()

    if os.path.exists(PROFILE_LOGO_PATH):
        with open(PROFILE_LOGO_PATH, "rb") as logo_f:
            logo_bytes = logo_f.read()
        logo_bytes = image_generator._strip_white_border_background_png(logo_bytes)
        logo_b64 = base64.b64encode(logo_bytes).decode("utf-8")
        logo_src = f"data:image/png;base64,{logo_b64}"
        html_doc = html_doc.replace('src="logo.png"', f'src="{logo_src}"').replace("src='logo.png'", f"src='{logo_src}'")

    template_context = template_context or {}
    if template_context and ("{%" in html_doc or "{{" in html_doc):
        try:
            from jinja2 import Template
        except ModuleNotFoundError as e:
            raise RuntimeError("缺少套件 jinja2，請先安裝 `pip install -r requirements.txt`") from e
        html_doc = Template(html_doc).render(**template_context)

    for key, value in (replacements or {}).items():
        html_doc = html_doc.replace(key, str(value))

    return html_doc


async def _render_wallet_profile_posters_bundle(
    template_payload: dict,
    out_dir: str,
    safe_name: str = "wallet_profile",
    render_profile: bool = True,
) -> dict:
    if isinstance(template_payload, dict):
        profile_template_context = template_payload.get("template_context") or {}
        profile_replacements = template_payload.get("replacements") or {}
        history_template_context = template_payload.get("history_template_context") or {}
        history_replacements = template_payload.get("history_replacements") or {}
        extreme_template_context = template_payload.get("extreme_template_context") or {}
        extreme_replacements = template_payload.get("extreme_replacements") or {}
    else:
        profile_template_context = {}
        profile_replacements = template_payload or {}
        history_template_context = {}
        history_replacements = {}
        extreme_template_context = {}
        extreme_replacements = {}

    os.makedirs(out_dir, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", safe_name).strip("_") or "wallet_profile"
    profile_out = os.path.join(out_dir, f"{safe}_profile.png") if render_profile else None
    history_out = os.path.join(out_dir, f"{safe}_profile_history.png")
    extremes_out = os.path.join(out_dir, f"{safe}_profile_extremes.png")

    jobs: list[tuple[str, str]] = []
    if render_profile:
        jobs.append(
            (
                _render_wallet_template_html(
                    PROFILE_TEMPLATE_PATH,
                    template_context=profile_template_context,
                    replacements=profile_replacements,
                ),
                profile_out,
            )
        )
    jobs.append(
        (
            _render_wallet_template_html(
                PROFILE_HISTORY_TEMPLATE_PATH,
                template_context=history_template_context,
                replacements=history_replacements,
            ),
            history_out,
        )
    )
    jobs.append(
        (
            _render_wallet_template_html(
                PROFILE_EXTREMES_TEMPLATE_PATH,
                template_context=extreme_template_context,
                replacements=extreme_replacements,
            ),
            extremes_out,
        )
    )

    async with image_generator.RENDER_SEMAPHORE:
        browser = await image_generator.AsyncBrowserManager.get_browser()
        context = await browser.new_context(
            viewport={"width": 1200, "height": 900},
            device_scale_factor=2,
        )
        try:
            for html_doc, out_path in jobs:
                page = await context.new_page()
                await page.set_content(html_doc, wait_until="networkidle")
                await image_generator._screenshot_poster_root(page, out_path)
                await page.close()
        finally:
            await context.close()

    return {
        "profile": profile_out,
        "history": history_out,
        "extremes": extremes_out,
    }


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


def _is_image_attachment(att: discord.Attachment) -> bool:
    filename = str(getattr(att, "filename", "") or "").lower()
    content_type = str(getattr(att, "content_type", "") or "").lower()
    if content_type.startswith("image/"):
        return True
    return any(filename.endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"])


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


_SERIES_CODE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{1,24}$")


def _normalize_series_code_input(text: str) -> str:
    return str(text or "").strip().replace(" ", "").lower()


def _sanitize_cardset_category(raw: str) -> str:
    text = str(raw or "").strip().lower()
    if any(k in text for k in ("one piece", "onepiece", "航海王", "ワンピース", "opc")):
        return "One Piece"
    if any(k in text for k in ("yu-gi-oh", "yugioh", "遊戲王", "游戏王", "ygo")):
        return "Yu-Gi-Oh"
    return "Pokemon"


def _infer_cardset_category_heuristic(series_code: str) -> str:
    code = _normalize_series_code_input(series_code)
    if code.startswith(("op", "st", "eb")) and re.search(r"\d", code):
        return "One Piece"
    if code.startswith(("ygo", "rd", "ocg", "qcc", "db")):
        return "Yu-Gi-Oh"
    return "Pokemon"


async def _infer_cardset_category_with_minimax(series_code: str, lang: str = "zh") -> dict:
    code = _normalize_series_code_input(series_code)
    minimax_key = (os.getenv("MINIMAX_API_KEY") or "").strip().replace('\u2028', '').replace('\n', '').replace('\r', '')
    fallback_category = _infer_cardset_category_heuristic(code)

    if not minimax_key:
        return {
            "series_code": code,
            "category": fallback_category,
            "provider": "heuristic(no_key)",
            "confidence": 0.45,
            "reason": "MINIMAX_API_KEY not configured.",
        }

    url = "https://api.minimax.io/v1/text/chatcompletion_v2"
    model = (os.getenv("MINIMAX_TEXT_MODEL") or "M2-her").strip()
    headers = {
        "Authorization": f"Bearer {minimax_key}",
        "Content-Type": "application/json",
    }
    prompt = (
        "You are a TCG series code classifier. "
        "Given a series code, infer the category among exactly: Pokemon, One Piece, Yu-Gi-Oh. "
        "Return strict JSON only: "
        "{\"category\":\"Pokemon|One Piece|Yu-Gi-Oh\",\"confidence\":0-1,\"reason\":\"short\"}. "
        f"series_code={code}"
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "name": "MiniMax AI", "content": "Output JSON only."},
            {"role": "user", "name": "User", "content": prompt},
        ],
    }

    loop = asyncio.get_running_loop()

    def _do_request():
        for _ in range(3):
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=30)
                response.raise_for_status()
                return response.json()
            except Exception:
                time.sleep(1.0)
        return None

    data = await loop.run_in_executor(None, _do_request)
    if not isinstance(data, dict):
        return {
            "series_code": code,
            "category": fallback_category,
            "provider": "heuristic(minimax_unavailable)",
            "confidence": 0.45,
            "reason": "MiniMax request failed.",
        }

    choices = data.get("choices") or []
    content = ""
    if choices and isinstance(choices[0], dict):
        content = str(((choices[0].get("message") or {}).get("content") or "")).strip()

    if not content:
        base_resp = data.get("base_resp") or {}
        return {
            "series_code": code,
            "category": fallback_category,
            "provider": "heuristic(minimax_empty)",
            "confidence": 0.45,
            "reason": str(base_resp.get("status_msg") or "MiniMax empty response."),
        }

    try:
        parsed = _json_loads_loose(content)
        category = _sanitize_cardset_category(parsed.get("category"))
        confidence_raw = parsed.get("confidence", 0.66)
        try:
            confidence = float(confidence_raw)
        except Exception:
            confidence = 0.66
        confidence = max(0.0, min(1.0, confidence))
        reason = str(parsed.get("reason") or "").strip() or "MiniMax inferred from series code."
        return {
            "series_code": code,
            "category": category,
            "provider": "minimax",
            "confidence": confidence,
            "reason": reason,
        }
    except Exception:
        return {
            "series_code": code,
            "category": fallback_category,
            "provider": "heuristic(minimax_parse_failed)",
            "confidence": 0.45,
            "reason": "MiniMax response parsing failed.",
        }


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


async def _run_nft_sync_script(trigger: str, bootstrap_only: bool = False) -> bool:
    if not NFT_SYNC_ENABLE:
        return True
    if not os.path.exists(NFT_SYNC_SCRIPT_PATH):
        print(f"⚠️ NFT sync script not found: {NFT_SYNC_SCRIPT_PATH}")
        return False

    cmd = [sys.executable, NFT_SYNC_SCRIPT_PATH, "--trigger", trigger]
    if bootstrap_only:
        cmd.append("--bootstrap-only")

    print(f"🕒 NFT sync start trigger={trigger} bootstrap_only={1 if bootstrap_only else 0}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=BASE_DIR,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out_b, err_b = await proc.communicate()
    out = (out_b or b"").decode("utf-8", errors="replace").strip()
    err = (err_b or b"").decode("utf-8", errors="replace").strip()
    if out:
        print(out)
    if err:
        print(err, file=sys.stderr)
    if proc.returncode != 0:
        print(f"❌ NFT sync failed trigger={trigger} rc={proc.returncode}")
        return False
    print(f"✅ NFT sync done trigger={trigger}")
    return True


@tasks.loop(time=NFT_SYNC_RUN_TIME)
async def nft_daily_sync_job():
    await _run_nft_sync_script("daily", bootstrap_only=False)


@nft_daily_sync_job.before_loop
async def _before_nft_daily_sync_job():
    await client.wait_until_ready()


@client.event
async def on_ready():
    global NFT_SYNC_STARTUP_DONE, NFT_SYNC_STARTUP_LOCK
    # Attempt to sync global commands
    try:
        await tree.sync()
        print(f'✅ 機器人已成功登入為 {client.user}')
        print(f"✅ 全域 Slash Commands 同步嘗試完成 (全球更新可能需 1 小時)")
    except Exception as e:
        print(f"⚠️ 全域同步失敗: {e}")

    if NFT_SYNC_ENABLE:
        if NFT_SYNC_STARTUP_LOCK is None:
            NFT_SYNC_STARTUP_LOCK = asyncio.Lock()
        async with NFT_SYNC_STARTUP_LOCK:
            if not NFT_SYNC_STARTUP_DONE:
                await _run_nft_sync_script("startup", bootstrap_only=True)
                NFT_SYNC_STARTUP_DONE = True
        if not nft_daily_sync_job.is_running():
            nft_daily_sync_job.start()

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
            discord.SelectOption(label="Top 4", value="4", default=(default_count == 4)),
            discord.SelectOption(label="Top 5", value="5", default=(default_count == 5)),
            discord.SelectOption(label="Top 7", value="7", default=(default_count == 7)),
            discord.SelectOption(label="Top 10", value="10", default=(default_count == 10)),
        ]
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_template = _clamp_profile_card_count(_parse_int(self.values[0]))
        await self.view.refresh(interaction)


class ProfileBackgroundSelect(discord.ui.Select):
    def __init__(
        self,
        default_key: str = "classic",
        placeholder: str = "Background",
        labels: dict[str, str] | None = None,
    ):
        normalized = _normalize_profile_background_key(default_key)
        labels = labels or {}
        options = [
            discord.SelectOption(label=str(labels.get("classic") or "Classic")[:100], value="classic", default=(normalized == "classic")),
            discord.SelectOption(label=str(labels.get("1") or "1")[:100], value="1", default=(normalized == "1")),
            discord.SelectOption(label=str(labels.get("2") or "2")[:100], value="2", default=(normalized == "2")),
            discord.SelectOption(label=str(labels.get("3") or "3")[:100], value="3", default=(normalized == "3")),
        ]
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, options=options, row=1)

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_background = _normalize_profile_background_key(self.values[0])
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
        super().__init__(timeout=240)
        self.author_id = author_id
        self.wallet = wallet
        self.picker_data = picker_data or {}
        self.username = str((picker_data or {}).get("username") or "Unknown")

        self.selected_lang = _profile_lang_from_locale(selected_lang)
        self.texts = _profile_wizard_texts(self.selected_lang)
        self.selected_template = 10
        self.selected_background = "classic"
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
        # Background selection is intentionally disabled for now.
        # Keep profile posters locked to the classic background preset.
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
        selected_background = "classic"
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
                selected_background,
            )
            safe_name = f"{self.username}_{self.wallet[-6:]}"
            poster_enabled = bool(profile_ctx.get("profile_poster_enabled", True))
            poster_path = None
            extremes_path = None
            history_path = None
            async with POSTER_SEMAPHORE:
                rendered = await _render_wallet_profile_posters_bundle(
                    profile_ctx,
                    out_dir,
                    safe_name=safe_name,
                    render_profile=poster_enabled,
                )
                poster_path = rendered.get("profile")
                history_path = rendered.get("history")
                extremes_path = rendered.get("extremes")
            history_summary = profile_ctx.get("history_summary") or {}
            history_only_hint = ""
            if not poster_enabled:
                history_only_hint = _t(
                    self.selected_lang,
                    "（此地址目前無可展示卡片，僅生成歷史海報）\n",
                    "(No collectible cards found for page 1, history poster only)\n",
                    "(1페이지에 표시할 카드가 없어 히스토리 포스터만 생성)\n",
                    "（该地址目前无可展示卡片，仅生成历史海报）\n",
                )
            summary = (
                f"✅ **{self.username}** · {self.texts['summary_done']}\n"
                f"{history_only_hint}"
                f"{self.texts['wallet_label']}: `{self.wallet}`\n"
                f"{self.texts['shown_label']}: **{profile_ctx.get('shown_count', 0)}** | "
                f"{self.texts['shown_fmv_label']}: **{_format_fmv_usd(_parse_int(profile_ctx.get('shown_fmv')))}**\n"
                f"History · Opened Packs: **{_format_number(history_summary.get('opened_packs'))}** | "
                f"Net: **{_format_usdt_currency(history_summary.get('net_total'), signed=True)}**"
            )
            files = [discord.File(history_path)]
            if extremes_path and os.path.exists(extremes_path):
                files.append(discord.File(extremes_path))
            if poster_path and os.path.exists(poster_path):
                files.insert(0, discord.File(poster_path))
            await interaction.followup.send(
                summary,
                files=files,
            )
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


@tree.command(name="cardset", description="輸入系列代號（如 op15），直接生成卡盒 Top10 報告與海報")
@app_commands.describe(series_code="系列代號，例如 op15 / m4 / loch")
async def cardset(interaction: discord.Interaction, series_code: str):
    series = _normalize_series_code_input(series_code)
    if not _SERIES_CODE_RE.fullmatch(series):
        await interaction.response.send_message(
            "❌ 系列代號格式錯誤。請輸入英數代號（例如 `op15` / `m4` / `loch`）。",
            ephemeral=True,
        )
        return

    # Match /profile UX: choose language first, then open thread.
    lang_view = LanguageSelectView(interaction.user.id, timeout_seconds=20)
    await interaction.response.send_message("🌐 請先選擇語言 / Please choose language", view=lang_view, ephemeral=False)
    resp = await interaction.original_response()
    picked_lang, selected = await lang_view.wait_for_choice()
    lang = picked_lang if selected and picked_lang else "zh"
    lang_view._disable_all()
    try:
        if selected:
            await resp.edit(
                content=_t(
                    lang,
                    f"✅ 語言已選擇：{LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '繁體中文')}**\n"
                    f"📦 series_code: `{series}`，正在建立卡盒分析討論串...",
                    f"✅ Language selected: {LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, 'English')}**\n"
                    f"📦 series_code: `{series}`, creating analysis thread...",
                    f"✅ 언어 선택 완료: {LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '한국어')}**\n"
                    f"📦 series_code: `{series}`, 분석 스레드를 생성합니다...",
                    f"✅ 语言已选择：{LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '简体中文')}**\n"
                    f"📦 series_code: `{series}`，正在创建分析讨论串...",
                ),
                view=lang_view,
            )
        else:
            await resp.edit(content=_profile_wizard_texts(lang)["lang_timeout"], view=lang_view)
    except Exception:
        pass

    thread_name = _t(
        lang,
        f"卡盒分析 {series.upper()}",
        f"Box Analysis {series.upper()}",
        f"박스 분석 {series.upper()}",
        f"卡盒分析 {series.upper()}",
    )
    thread = await resp.create_thread(name=thread_name, auto_archive_duration=60)
    try:
        await thread.add_user(interaction.user)
    except Exception:
        pass

    card_out_dir = tempfile.mkdtemp(prefix=f"tcg_cardset_{interaction.id}_")
    try:
        infer = await _infer_cardset_category_with_minimax(series, lang=lang)
        category = _sanitize_cardset_category(infer.get("category"))

        external_card_info = {
            "name": f"{series.upper()} Booster Box",
            "set_code": series.upper(),
            "number": series.upper(),
            "grade": "Ungraded",
            "jp_name": "",
            "c_name": "",
            "category": category,
            "release_info": f"{series.upper()} Series Box",
            "illustrator": "Unknown",
            "market_heat": "N/A",
            "features": "Series box direct lookup",
            "collection_value": "N/A",
            "competitive_freq": "N/A",
            "is_alt_art": False,
            "language": "Unknown",
            "item_type": "series_box",
            "series_code": series,
            "poster_version": "v3",
        }

        report_text = None
        poster_data = None
        async with REPORT_SEMAPHORE:
            market_report_vision.REPORT_ONLY = True
            result = await market_report_vision.process_single_image(
                image_path=None,
                api_key=(os.getenv("MINIMAX_API_KEY") or "").strip(),
                out_dir=card_out_dir,
                stream_mode=True,
                lang=lang,
                external_card_info=external_card_info,
            )
            for _status in market_report_vision.get_and_clear_notify_msgs():
                await thread.send(_status)

            if isinstance(result, tuple):
                report_text, poster_data = result
            else:
                report_text = result

        if report_text:
            for chunk in smart_split(str(report_text)):
                await thread.send(chunk)
        else:
            await thread.send(
                _t(
                    lang,
                    "❌ 生成失敗：未取得卡盒報告內容。",
                    "❌ Failed: no box report content was returned.",
                    "❌ 생성 실패: 박스 리포트를 받지 못했습니다.",
                    "❌ 生成失败：未取得卡盒报告内容。",
                )
            )
            return

        if poster_data:
            await thread.send(
                _t(
                    lang,
                    "🖼️ 正在生成卡盒 Top10 海報...",
                    "🖼️ Generating box Top10 poster...",
                    "🖼️ 박스 Top10 포스터 생성 중...",
                    "🖼️ 正在生成卡盒 Top10 海报...",
                )
            )
            async with POSTER_SEMAPHORE:
                out_paths = await market_report_vision.generate_posters(poster_data)
            for path in out_paths or []:
                if path and os.path.exists(path):
                    await thread.send(file=discord.File(path))
        else:
            await thread.send(
                _t(
                    lang,
                    "⚠️ 已生成文字報告，但未取得海報資料。",
                    "⚠️ Text report generated, but no poster data returned.",
                    "⚠️ 텍스트 리포트는 생성되었지만 포스터 데이터가 없습니다.",
                    "⚠️ 已生成文字报告，但未取得海报数据。",
                )
            )
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"❌ /cardset 失敗: {e}", file=sys.stderr)
        print(error_trace, file=sys.stderr)
        await thread.send(f"❌ 發生異常：\n```python\n{error_trace[-1900:]}\n```")
    finally:
        shutil.rmtree(card_out_dir, ignore_errors=True)


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
        # Preview-image flow is intentionally disabled for now (copyright risk).
        # Keep /profile in classic-only mode until preview assets are legally cleared.
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


@tree.command(name="sync_status", description="查看 NFT holders 同步狀態")
async def sync_status(interaction: discord.Interaction):
    status_path = _nft_sync_status_path()
    latest_path = os.path.join(
        _nft_sync_data_dir(),
        "snapshots",
        f"nft_{str(os.getenv('NFT_TOKEN_ID', '13')).strip() or '13'}_holders.latest.json",
    )
    if not os.path.exists(status_path):
        await interaction.response.send_message(
            f"⚠️ 尚無同步狀態檔：`{status_path}`\n請先等待啟動 bootstrap 或每日同步執行。",
            ephemeral=True,
        )
        return

    try:
        with open(status_path, "r", encoding="utf-8") as f:
            status = json.load(f)
        if not isinstance(status, dict):
            raise RuntimeError("status json invalid")
    except Exception as e:
        await interaction.response.send_message(
            f"❌ 讀取同步狀態失敗：{e}",
            ephemeral=True,
        )
        return

    success = bool(status.get("success"))
    trigger = str(status.get("trigger") or "unknown")
    updated_at = str(status.get("updated_at") or "unknown")
    message = str(status.get("message") or "").strip()
    extra = status.get("extra") if isinstance(status.get("extra"), dict) else {}
    new_rows = extra.get("new_rows", "n/a")
    pages = extra.get("pages_fetched", "n/a")
    holder_count = extra.get("holder_count", "n/a")
    stop_on_dup = extra.get("stop_on_duplicate", "n/a")
    commit = extra.get("commit", "n/a")

    txt = (
        f"{'✅' if success else '❌'} 同步狀態：**{'成功' if success else '失敗'}**\n"
        f"Trigger: `{trigger}`\n"
        f"Updated At: `{updated_at}`\n"
        f"New Rows: `{new_rows}` | Pages: `{pages}` | Holders: `{holder_count}`\n"
        f"Stop on First Duplicate: `{stop_on_dup}`\n"
        f"Commit: `{commit}`\n"
        f"Status File: `{status_path}`\n"
        f"Latest Snapshot: `{latest_path}`\n"
    )
    if message:
        txt += f"\nMessage: `{message[:1200]}`"
    await interaction.response.send_message(txt, ephemeral=True)


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

    bot_mentioned = client.user in message.mentions

    if bot_mentioned:
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
                if _is_image_attachment(a)
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
