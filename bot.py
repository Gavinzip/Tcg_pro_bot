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
import inspect
import importlib.util
import html as html_lib
import time
import mimetypes
import hashlib
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dt_time, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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


def _load_onchain_metrics_runtime():
    try:
        from scripts.onchain_metrics import OnchainConfig, analyze_wallet, analyze_sbt_wallet  # type: ignore
        return OnchainConfig, analyze_wallet, analyze_sbt_wallet, None
    except Exception:
        pass
    try:
        module_path = os.path.join(BASE_DIR, "scripts", "onchain_metrics.py")
        spec = importlib.util.spec_from_file_location("onchain_metrics_runtime", module_path)
        if spec is None or spec.loader is None:
            return None, None, None, RuntimeError("onchain_metrics spec unavailable")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        OnchainConfig = getattr(module, "OnchainConfig", None)
        analyze_wallet = getattr(module, "analyze_wallet", None)
        analyze_sbt_wallet = getattr(module, "analyze_sbt_wallet", None)
        if OnchainConfig is None or analyze_wallet is None:
            return None, None, None, RuntimeError("onchain_metrics missing OnchainConfig/analyze_wallet")
        return OnchainConfig, analyze_wallet, analyze_sbt_wallet, None
    except Exception as e:
        return None, None, None, e


(
    _ONCHAIN_CONFIG_CLS,
    _ONCHAIN_ANALYZE_WALLET_FN,
    _ONCHAIN_ANALYZE_SBT_WALLET_FN,
    _ONCHAIN_METRICS_IMPORT_ERROR,
) = _load_onchain_metrics_runtime()


def _env_true(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _parse_int_set(raw: str | None) -> set[int]:
    if not raw:
        return set()
    out: set[int] = set()
    for token in re.split(r"[\s,]+", str(raw).strip()):
        if not token:
            continue
        if token.isdigit():
            out.add(int(token))
    return out


def _coerce_thread_auto_archive_minutes(raw: str | int | None, default: int = 1440) -> int:
    allowed = (60, 1440, 4320, 10080)
    try:
        value = int(str(raw).strip()) if raw is not None else default
    except Exception:
        value = default
    return min(allowed, key=lambda x: abs(x - value))


NFT_SYNC_ENABLE = _env_true("NFT_SYNC_ENABLE", True)
NFT_SYNC_SCRIPT_PATH = os.path.join(BASE_DIR, "scripts", "sync_nft13_incremental.py")
NFT_SYNC_STARTUP_DONE = False
NFT_SYNC_STARTUP_LOCK: asyncio.Lock | None = None
NFT_SYNC_TZ = str(os.getenv("NFT_SYNC_TZ", "Asia/Taipei")).strip() or "Asia/Taipei"
NFT_SYNC_COMPARE_ON_STARTUP = _env_true("NFT_SYNC_COMPARE_ON_STARTUP", True)
NFT_SYNC_HOUR = max(0, min(23, int(os.getenv("NFT_SYNC_HOUR", "6"))))
NFT_SYNC_MINUTE = max(0, min(59, int(os.getenv("NFT_SYNC_MINUTE", "0"))))
AUTO_IMAGE_THREAD_MONITOR_ENABLED = _env_true("AUTO_IMAGE_THREAD_MONITOR_ENABLED", False)
AUTO_IMAGE_THREAD_CHANNEL_IDS = _parse_int_set(os.getenv("AUTO_IMAGE_THREAD_CHANNEL_IDS", ""))
AUTO_IMAGE_THREAD_AUTO_ARCHIVE_MIN = _coerce_thread_auto_archive_minutes(
    os.getenv("AUTO_IMAGE_THREAD_AUTO_ARCHIVE_MIN", "1440"),
    default=1440,
)
AUTO_IMAGE_THREAD_NAME_PREFIX = str(os.getenv("AUTO_IMAGE_THREAD_NAME_PREFIX", "圖片討論")).strip() or "圖片討論"
AUTO_IMAGE_THREAD_POST_NOTE = _env_true("AUTO_IMAGE_THREAD_POST_NOTE", False)
# 固定 market 來源設定（依需求不從 .env 讀取）
MARKET_SOURCE_CHANNEL_ID = 1483342992864706675
MARKET_SUMMARY_BOT_ID = 1484044461276139600
MARKET_RECENT_LIMIT = 50
MARKET_SCAN_MESSAGES_PER_THREAD = 25
MARKET_CACHE_IMAGES = True
MARKET_BOOTSTRAP_ARCHIVED_LIMIT = 2000
MARKET_LIVE_SYNC_ON_EVENT = True
MARKET_LIVE_SYNC_DEBOUNCE_SEC = 8.0
MARKET_LIVE_SYNC_LOCK: asyncio.Lock | None = None
MARKET_LIVE_SYNC_LAST_TS = 0.0
MARKET_AUTO_PUSH_ON_NEW = True
MARKET_AUTO_PUSH_COOLDOWN_SEC = 45.0
MARKET_AUTO_PUSH_LOCK: asyncio.Lock | None = None
MARKET_AUTO_PUSH_LAST_TS = 0.0
try:
    ASK_FEEDBACK_CHANNEL_ID = int(
        str(os.getenv("ASK_FEEDBACK_CHANNEL_ID", "1473923458751660063")).strip()
    )
except Exception:
    ASK_FEEDBACK_CHANNEL_ID = 1473923458751660063
ASK_FEEDBACK_SAVE_ENABLED = _env_true("ASK_FEEDBACK_SAVE_ENABLED", False)


def _safe_tzinfo(name: str):
    try:
        return ZoneInfo(name)
    except Exception:
        # Fallback when tzdata package / system zoneinfo is unavailable in container.
        return timezone(timedelta(hours=8))


NFT_SYNC_RUN_TIME = dt_time(hour=NFT_SYNC_HOUR, minute=NFT_SYNC_MINUTE, tzinfo=_safe_tzinfo(NFT_SYNC_TZ))

RANK_SYNC_ENABLE = _env_true("RANK_SYNC_ENABLE", True)
RANK_SYNC_SCRIPT_PATH = os.path.join(BASE_DIR, "scripts", "update_rankings.py")
RANK_SYNC_STARTUP_DONE = False
RANK_SYNC_STARTUP_LOCK: asyncio.Lock | None = None
RANK_SYNC_TZ = str(os.getenv("RANK_SYNC_TZ", "Asia/Taipei")).strip() or "Asia/Taipei"
RANK_SYNC_COMPARE_ON_STARTUP = _env_true("RANK_SYNC_COMPARE_ON_STARTUP", False)
RANK_SYNC_HOUR = max(0, min(23, int(os.getenv("RANK_SYNC_HOUR", "6"))))
RANK_SYNC_MINUTE = max(0, min(59, int(os.getenv("RANK_SYNC_MINUTE", "0"))))
RANK_SYNC_WEEKLY_FULL_ENABLE = _env_true("RANK_SYNC_WEEKLY_FULL_ENABLE", True)
RANK_SYNC_WEEKLY_FULL_WEEKDAY = max(0, min(6, int(os.getenv("RANK_SYNC_WEEKLY_FULL_WEEKDAY", "6"))))
RANK_SYNC_RUN_TIME = dt_time(hour=RANK_SYNC_HOUR, minute=RANK_SYNC_MINUTE, tzinfo=_safe_tzinfo(RANK_SYNC_TZ))


def _nft_sync_data_dir() -> str:
    app_env = str(os.getenv("APP_ENV", "local")).strip().lower() or "local"
    default_dir = "/data/renaiss_sync" if app_env == "server" else "./data/renaiss_sync"
    return str(os.getenv("SYNC_DATA_DIR", default_dir)).strip() or default_dir


def _nft_sync_status_path() -> str:
    token_id = str(os.getenv("NFT_TOKEN_ID", "13")).strip() or "13"
    return os.path.join(_nft_sync_data_dir(), "state", f"nft_{token_id}_status.json")


def _rank_sync_data_dir() -> str:
    app_env = str(os.getenv("APP_ENV", "local")).strip().lower() or "local"
    default_dir = "/data/renaiss_sync/rankings" if app_env == "server" else "./data/renaiss_sync/rankings"
    return str(os.getenv("RANK_SYNC_DATA_DIR", os.getenv("RANKING_DATA_DIR", default_dir))).strip() or default_dir


def _rank_sync_status_path() -> str:
    return os.path.join(_rank_sync_data_dir(), "state", "ranking_status.json")


_USER_SETTINGS_CACHE: dict[str, object] = {
    "path": "",
    "mtime": -1.0,
    "settings": {},
}


def _user_settings_dir() -> str:
    app_env = str(os.getenv("APP_ENV", "local")).strip().lower() or "local"
    default_dir = "/data/renaiss_sync" if app_env == "server" else "./data/renaiss_sync"
    return str(os.getenv("SYNC_DATA_DIR", default_dir)).strip() or default_dir


def _user_settings_path() -> str:
    return os.path.join(_user_settings_dir(), "user_settings.json")


def _load_user_settings() -> dict[str, dict]:
    settings_path = _user_settings_path()
    try:
        mtime = os.path.getmtime(settings_path)
    except OSError:
        os.makedirs(os.path.dirname(settings_path), exist_ok=True)
        return {}

    if (
        _USER_SETTINGS_CACHE.get("path") == settings_path
        and _USER_SETTINGS_CACHE.get("mtime") == mtime
        and isinstance(_USER_SETTINGS_CACHE.get("settings"), dict)
    ):
        return _USER_SETTINGS_CACHE.get("settings")  # type: ignore[return-value]

    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}

    if not isinstance(data, dict):
        return {}

    _USER_SETTINGS_CACHE["path"] = settings_path
    _USER_SETTINGS_CACHE["mtime"] = mtime
    _USER_SETTINGS_CACHE["settings"] = data
    return data


def _save_user_settings(user_id: str, wallet_address: str) -> bool:
    settings_path = _user_settings_path()
    settings = _load_user_settings()
    settings[user_id] = {
        "wallet_address": wallet_address.lower(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        os.makedirs(os.path.dirname(settings_path), exist_ok=True)
        with open(settings_path + ".tmp", "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
        os.replace(settings_path + ".tmp", settings_path)
        _USER_SETTINGS_CACHE["settings"] = settings
        _USER_SETTINGS_CACHE["mtime"] = os.path.getmtime(settings_path)
        return True
    except Exception:
        return False


def _get_user_default_wallet(user_id: str) -> str | None:
    settings = _load_user_settings()
    user_data = settings.get(str(user_id))
    if not user_data:
        return None
    return user_data.get("wallet_address")


_USAGE_STATS_LOCK = threading.Lock()
_ASK_FEEDBACK_LOCK = threading.Lock()


def _bot_usage_stats_path() -> str:
    raw = str(os.getenv("BOT_USAGE_STATS_PATH", "")).strip()
    if raw:
        return raw
    return os.path.join(_user_settings_dir(), "state", "bot_usage_stats.json")


def _ask_feedback_path() -> str:
    raw = str(os.getenv("ASK_FEEDBACK_PATH", "")).strip()
    if raw:
        return raw
    return os.path.join(_user_settings_dir(), "state", "ask_feedback.jsonl")


def _append_ask_feedback(entry: dict[str, object]) -> tuple[bool, str]:
    path = _ask_feedback_path()
    line = json.dumps(entry, ensure_ascii=False)
    with _ASK_FEEDBACK_LOCK:
        try:
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
                f.write("\n")
            return True, path
        except Exception as e:
            return False, str(e)


_ASK_KIND_LABELS = {
    "bug": "Bug 回報",
    "feature": "新功能建議",
    "other": "其他意見",
}

_ASK_KIND_EMBED_COLORS = {
    "bug": 0xE67E22,
    "feature": 0x2ECC71,
    "other": 0x95A5A6,
}


async def _forward_ask_feedback(entry: dict[str, object]) -> tuple[bool, str]:
    channel_id = int(ASK_FEEDBACK_CHANNEL_ID or 0)
    if channel_id <= 0:
        return True, ""

    try:
        channel = client.get_channel(channel_id)
        if channel is None:
            channel = await client.fetch_channel(channel_id)
        if channel is None or not hasattr(channel, "send"):
            return False, f"channel {channel_id} is not sendable"

        kind = str(entry.get("kind") or "other").strip().lower()
        if kind not in _ASK_KIND_LABELS:
            kind = "other"
        kind_label = _ASK_KIND_LABELS.get(kind, _ASK_KIND_LABELS["other"])
        content = str(entry.get("content") or "").strip()
        feedback_id = str(entry.get("feedback_id") or "-")
        display_name = str(entry.get("display_name") or "").strip() or str(entry.get("username") or "unknown")
        user_id = str(entry.get("user_id") or "unknown")
        source_guild = str(entry.get("guild_id") or "DM")
        source_channel = str(entry.get("channel_id") or "DM")
        jump_url = str(entry.get("jump_url") or "").strip()
        screenshot_url = str(entry.get("screenshot_url") or "").strip()

        embed = discord.Embed(
            title=f"📝 /ask 回饋｜{kind_label}",
            description=content[:3800] if content else "(empty)",
            color=int(_ASK_KIND_EMBED_COLORS.get(kind, _ASK_KIND_EMBED_COLORS["other"])),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="ID", value=f"`{feedback_id}`", inline=True)
        embed.add_field(name="User", value=f"{display_name}\n`{user_id}`", inline=True)
        embed.add_field(name="Source", value=f"guild `{source_guild}`\nchannel `{source_channel}`", inline=True)
        if jump_url:
            embed.add_field(name="Jump", value=f"[Open Source]({jump_url})", inline=False)
        if screenshot_url:
            embed.set_image(url=screenshot_url)

        await channel.send(embed=embed)
        return True, ""
    except Exception as e:
        return False, str(e)


def _record_bot_usage(
    user_id: str,
    command_name: str,
    *,
    guild_id: int | None = None,
    channel_id: int | None = None,
) -> None:
    name = str(command_name or "").strip()
    if not name:
        return
    uid = str(user_id or "unknown").strip() or "unknown"
    now_iso = datetime.now(timezone.utc).isoformat()
    path = _bot_usage_stats_path()

    with _USAGE_STATS_LOCK:
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    data = {}
            else:
                data = {}
        except Exception:
            data = {}

        total = _parse_int(data.get("total")) or 0
        data["total"] = total + 1
        data["updated_at"] = now_iso

        commands = data.get("commands")
        if not isinstance(commands, dict):
            commands = {}
        commands[name] = (_parse_int(commands.get(name)) or 0) + 1
        data["commands"] = commands

        users = data.get("users")
        if not isinstance(users, dict):
            users = {}
        user_row = users.get(uid)
        if not isinstance(user_row, dict):
            user_row = {}
        user_total = _parse_int(user_row.get("total")) or 0
        user_row["total"] = user_total + 1
        user_commands = user_row.get("commands")
        if not isinstance(user_commands, dict):
            user_commands = {}
        user_commands[name] = (_parse_int(user_commands.get(name)) or 0) + 1
        user_row["commands"] = user_commands
        user_row["last_command"] = name
        user_row["updated_at"] = now_iso
        if guild_id is not None:
            user_row["last_guild_id"] = str(guild_id)
        if channel_id is not None:
            user_row["last_channel_id"] = str(channel_id)
        users[uid] = user_row
        data["users"] = users

        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp_path = path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.write("\n")
            os.replace(tmp_path, path)
        except Exception:
            return


def _load_bot_usage_stats() -> dict[str, object]:
    primary = _bot_usage_stats_path()
    fallback = os.path.join(_rank_sync_data_dir(), "state", "bot_usage_stats.json")
    paths = []
    for p in (primary, fallback):
        p_norm = str(p).strip()
        if p_norm and p_norm not in paths:
            paths.append(p_norm)
    with _USAGE_STATS_LOCK:
        for path in paths:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data
            except Exception:
                continue
    return {}


_RANKING_LATEST_CACHE: dict[str, object] = {
    "path": "",
    "mtime": -1.0,
    "wallet_map": {},
}
_PACK_PRICE_MAP_CACHE: dict[str, object] = {
    "path": "",
    "mtime": -1.0,
    "data": {},
}


def _load_rankings_wallet_map() -> dict[str, dict]:
    latest_path = os.path.join(_rank_sync_data_dir(), "latest.json")
    try:
        mtime = os.path.getmtime(latest_path)
    except OSError:
        return {}

    if (
        _RANKING_LATEST_CACHE.get("path") == latest_path
        and _RANKING_LATEST_CACHE.get("mtime") == mtime
        and isinstance(_RANKING_LATEST_CACHE.get("wallet_map"), dict)
    ):
        return _RANKING_LATEST_CACHE.get("wallet_map")  # type: ignore[return-value]

    try:
        with open(latest_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}

    rows = data.get("wallets") if isinstance(data, dict) and isinstance(data.get("wallets"), list) else []
    wallet_map: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        addr = _normalize_wallet_address(str(row.get("address") or ""))
        if not addr:
            continue
        wallet_map[addr] = row

    _RANKING_LATEST_CACHE["path"] = latest_path
    _RANKING_LATEST_CACHE["mtime"] = mtime
    _RANKING_LATEST_CACHE["wallet_map"] = wallet_map
    return wallet_map


def _username_from_rankings_wallet(wallet_address: str) -> str | None:
    wallet_norm = _normalize_wallet_address(wallet_address or "") or str(wallet_address or "").strip().lower()
    if not wallet_norm:
        return None
    row = _load_rankings_wallet_map().get(wallet_norm) or {}
    username = str(row.get("username") or "").strip()
    return username or None


def _rank_chip_payload(rank_value: object) -> dict[str, str]:
    rank_int = _parse_int(rank_value)
    if rank_int is None or rank_int <= 0:
        return {"text": "-", "tier": "none"}
    if rank_int <= 10:
        tier = "gold"
    elif rank_int <= 100:
        tier = "silver"
    else:
        tier = "bronze"
    return {"text": str(rank_int), "tier": tier}
# /profile uses an isolated template bundle to avoid impacting normal card posters.
PROFILE_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "profile", "wallet_profile＿beta.html")
PROFILE_HISTORY_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "profile", "wallet_profile_history_beta.html")
PROFILE_EXTREMES_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "profile", "wallet_profile_extremes_beta.html")
PROFILE_HOLDINGS_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "profile", "wallet_profile_holdings_growth.html")
PROFILE_SBT_RANK_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "profile", "wallet_profile_sbt_rank.html")
PROFILE_CARDPACK_PULL_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "profile", "wallet_profile_cardpack_pull.html")
PROFILE_LOGO_PATH = os.path.join(BASE_DIR, "templates", "profile", "logo.png")
PROFILE_BACKGROUND_DIR = os.path.join(BASE_DIR, "templates", "backgorund")
PROFILE_SBT_WREATH_IMAGE_DIR = os.path.abspath(
    os.path.join(BASE_DIR, "..", "..", "other", "bsc_scan", "images")
)
RENAISS_MAIN_URL = "https://www.renaiss.xyz"
RENAISS_COLLECTIBLE_LIST_URL = "https://www.renaiss.xyz/api/trpc/collectible.list"
RENAISS_COLLECTIBLE_BY_TOKEN_URL = "https://www.renaiss.xyz/api/trpc/collectible.getCollectibleByTokenId"
RENAISS_SBT_BADGES_URL = "https://www.renaiss.xyz/api/trpc/sbt.getUserBadges"
RENAISS_ACTIVITY_LIST_URL = "https://www.renaiss.xyz/api/trpc/activity.getSubgraphUserActivities"
RENAISS_TOKEN_ACTIVITY_URL = "https://www.renaiss.xyz/api/trpc/activity.getSubgraphTokenActivities"
RENAISS_SUBPACK_INFO_URL = "https://www.renaiss.xyz/api/trpc/perpetualCardPack.getSubpackInfo"
SUBPACK_MONITOR_ENABLED = _env_true("SUBPACK_MONITOR_ENABLED", True)
SUBPACK_MONITOR_PACK_ID = str(
    os.getenv("SUBPACK_MONITOR_PACK_ID", "a42fe06a-ae67-4f20-a275-6b2668e178fb")
).strip()
try:
    SUBPACK_MONITOR_NOTIFY_CHANNEL_ID = int(
        str(os.getenv("SUBPACK_MONITOR_NOTIFY_CHANNEL_ID", "1479720880845361242")).strip()
    )
except Exception:
    SUBPACK_MONITOR_NOTIFY_CHANNEL_ID = 1479720880845361242
try:
    _subpack_monitor_interval_min = int(str(os.getenv("SUBPACK_MONITOR_INTERVAL_MINUTES", "1")).strip())
except Exception:
    _subpack_monitor_interval_min = 1
SUBPACK_MONITOR_INTERVAL_MINUTES = max(1, _subpack_monitor_interval_min)
try:
    SUBPACK_MONITOR_TIMEOUT_SEC = max(3.0, float(str(os.getenv("SUBPACK_MONITOR_TIMEOUT_SEC", "12")).strip()))
except Exception:
    SUBPACK_MONITOR_TIMEOUT_SEC = 12.0
SUBPACK_MONITOR_TIER_ORDER = ("common", "uncommon", "rare", "epic", "legendary")
SUBPACK_MONITOR_LAST_SNAPSHOT: dict[str, object] | None = None
SUBPACK_MONITOR_LOCK: asyncio.Lock | None = None
LEGENDARY_ALERT_MONITOR_ENABLED = _env_true("LEGENDARY_ALERT_MONITOR_ENABLED", True)
LEGENDARY_ALERT_MONITOR_PACK_SLUG = str(
    os.getenv("LEGENDARY_ALERT_MONITOR_PACK_SLUG", "renacrypt-pack")
).strip() or "renacrypt-pack"
try:
    LEGENDARY_ALERT_MONITOR_CHANNEL_ID = int(
        str(os.getenv("LEGENDARY_ALERT_MONITOR_CHANNEL_ID", "1426092926139633765")).strip()
    )
except Exception:
    LEGENDARY_ALERT_MONITOR_CHANNEL_ID = 1426092926139633765
try:
    _legendary_alert_interval_min = int(str(os.getenv("LEGENDARY_ALERT_MONITOR_INTERVAL_MINUTES", "1")).strip())
except Exception:
    _legendary_alert_interval_min = 1
LEGENDARY_ALERT_MONITOR_INTERVAL_MINUTES = max(1, _legendary_alert_interval_min)
try:
    LEGENDARY_ALERT_MONITOR_TIMEOUT_SEC = max(
        3.0,
        float(str(os.getenv("LEGENDARY_ALERT_MONITOR_TIMEOUT_SEC", "18")).strip()),
    )
except Exception:
    LEGENDARY_ALERT_MONITOR_TIMEOUT_SEC = 18.0
try:
    _legendary_startup_lookback_min = int(
        str(os.getenv("LEGENDARY_ALERT_MONITOR_STARTUP_LOOKBACK_MINUTES", "5")).strip()
    )
except Exception:
    _legendary_startup_lookback_min = 5
LEGENDARY_ALERT_MONITOR_STARTUP_LOOKBACK_MINUTES = max(0, _legendary_startup_lookback_min)
LEGENDARY_ALERT_MONITOR_TEST_ON_STARTUP = _env_true("LEGENDARY_ALERT_MONITOR_TEST_ON_STARTUP", False)
LEGENDARY_ALERT_MONITOR_LOCK: asyncio.Lock | None = None
LEGENDARY_ALERT_MONITOR_SEEN_IDS: set[str] = set()
LEGENDARY_ALERT_MONITOR_TEST_SENT = False
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
PROFILE_HOLDINGS_TOP_GAINERS = max(1, min(8, int(os.getenv("PROFILE_HOLDINGS_TOP_GAINERS", "4"))))
FLEX_PACK_EXTREME_TOKEN_LOOKUP_LIMIT = max(0, int(os.getenv("FLEX_PACK_EXTREME_TOKEN_LOOKUP_LIMIT", "24")))
FLEX_PACK_ALLOW_PREVIEW_IMAGE_FALLBACK = _env_true("FLEX_PACK_ALLOW_PREVIEW_IMAGE_FALLBACK", False)
PROFILE_SBT_BADGE_CACHE_TTL_SEC = max(0, int(os.getenv("PROFILE_SBT_BADGE_CACHE_TTL_SEC", "600")))
PROFILE_SBT_METADATA_CACHE_TTL_SEC = max(300, int(os.getenv("PROFILE_SBT_METADATA_CACHE_TTL_SEC", "21600")))
PROFILE_SBT_BADGE_SOURCE = str(os.getenv("PROFILE_SBT_BADGE_SOURCE", "onchain")).strip().lower() or "onchain"
if PROFILE_SBT_BADGE_SOURCE not in ("onchain", "official", "auto"):
    PROFILE_SBT_BADGE_SOURCE = "onchain"
ONCHAIN_SBT_CONTRACT = str(
    os.getenv("ONCHAIN_SBT_CONTRACT", "0x7d1b7db704d722295fbaa284008f526634673dbf")
).strip().lower()
PROFILE_ENABLE_RUNTIME_CACHE = str(os.getenv("PROFILE_ENABLE_RUNTIME_CACHE", "0")).strip().lower() in ("1", "true", "yes", "on")
PROFILE_ENABLE_DISK_IMAGE_CACHE = str(os.getenv("PROFILE_ENABLE_DISK_IMAGE_CACHE", "0")).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
PROFILE_ENABLE_DISK_FMV_CACHE = _env_true("PROFILE_ENABLE_DISK_FMV_CACHE", True)
PROFILE_DISK_FMV_CACHE_TTL_SEC = max(0, int(os.getenv("PROFILE_DISK_FMV_CACHE_TTL_SEC", "86400")))
PROFILE_DISK_FMV_CACHE_DIR = (
    str(os.getenv("PROFILE_DISK_FMV_CACHE_DIR", os.path.join(_nft_sync_data_dir(), "cache", "fmv"))).strip()
    or os.path.join(_nft_sync_data_dir(), "cache", "fmv")
)
PROFILE_ENABLE_SNKR_TIMELINE = _env_true("PROFILE_ENABLE_SNKR_TIMELINE", True)
PROFILE_ENABLE_HOLDINGS_POSTER = _env_true("PROFILE_ENABLE_HOLDINGS_POSTER", False)
try:
    PROFILE_POSTER_DEVICE_SCALE = max(2.0, float(str(os.getenv("PROFILE_POSTER_DEVICE_SCALE", "3")).strip()))
except Exception:
    PROFILE_POSTER_DEVICE_SCALE = 3.0
PROFILE_SNKR_TIMELINE_STEP_DAYS = max(1, min(30, int(os.getenv("PROFILE_SNKR_TIMELINE_STEP_DAYS", "15"))))
PROFILE_SNKR_WINDOW_DAYS = max(7, min(60, int(os.getenv("PROFILE_SNKR_WINDOW_DAYS", "30"))))
PROFILE_SNKR_HISTORY_MAX_PAGES = max(
    1,
    int(os.getenv("PROFILE_SNKR_HISTORY_MAX_PAGES", os.getenv("SNKR_HISTORY_MAX_PAGES", "20"))),
)
PROFILE_SNKR_CACHE_TTL_SEC = max(0, int(os.getenv("PROFILE_SNKR_CACHE_TTL_SEC", "604800")))
PROFILE_SNKR_CACHE_DIR = (
    str(os.getenv("PROFILE_SNKR_CACHE_DIR", os.path.join(_nft_sync_data_dir(), "cache", "snkr_hist"))).strip()
    or os.path.join(_nft_sync_data_dir(), "cache", "snkr_hist")
)
PROFILE_USE_RANKING_METRICS = _env_true("PROFILE_USE_RANKING_METRICS", True)
PROFILE_REALTIME_RECALC_ON_PROFILE = _env_true("PROFILE_REALTIME_RECALC_ON_PROFILE", True)
PROFILE_REALTIME_METRICS_SOURCE = str(os.getenv("PROFILE_REALTIME_METRICS_SOURCE", "onchain")).strip().lower() or "onchain"
if PROFILE_REALTIME_METRICS_SOURCE not in ("onchain", "ranking", "official"):
    PROFILE_REALTIME_METRICS_SOURCE = "onchain"
DEFAULT_ONCHAIN_PACK_CONTRACTS = (
    "0xaab5f5fa75437a6e9e7004c12c9c56cda4b4885a",
    "0x94e7732b0b2e7c51ffd0d56580067d9c2e2b7910",
    "0xb2891022648c5fad3721c42c05d8d283d4d53080",
)
if PROFILE_REALTIME_RECALC_ON_PROFILE and PROFILE_REALTIME_METRICS_SOURCE == "onchain":
    if _ONCHAIN_ANALYZE_WALLET_FN is None and _ONCHAIN_METRICS_IMPORT_ERROR is not None:
        print(
            f"⚠️ onchain metrics module unavailable; /profile live onchain recalc disabled: {_ONCHAIN_METRICS_IMPORT_ERROR}",
            file=sys.stderr,
        )
if PROFILE_SBT_BADGE_SOURCE in ("onchain", "auto"):
    if _ONCHAIN_ANALYZE_SBT_WALLET_FN is None:
        print(
            "⚠️ onchain SBT metrics unavailable; /profile SBT will fallback to official username path.",
            file=sys.stderr,
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
_PROFILE_SBT_BADGE_CACHE: dict[str, tuple[float, list[dict]]] = {}
_PROFILE_SBT_METADATA_CACHE: dict[str, object] = {"ts": 0.0, "data": {}}
_PROFILE_SBT_METADATA_LOCK = threading.Lock()
_PREPARED_CARD_IMAGE_CACHE: dict[str, str] = {}
_PROFILE_FMV_DISK_LOCK = threading.Lock()
_PROFILE_SNKR_CACHE_LOCK = threading.Lock()
_CARD_SNKR_CACHE: dict[str, dict] = {}
_CARD_PC_CACHE: dict[str, dict] = {}
PROFILE_PREPARED_CARD_CACHE_DIR = os.path.join(BASE_DIR, "templates", "profile", "cache_cards")
PACK_PRICE_MAP_PATH = (
    str(os.getenv("PACK_PRICE_MAP_PATH", os.path.join(BASE_DIR, "data", "pack_price_map.json"))).strip()
    or os.path.join(BASE_DIR, "data", "pack_price_map.json")
)
_HTTP_SESSION_LOCAL = threading.local()
_PROFILE_BACKGROUND_FILES = {
    "classic": None,
    "1": "1.jpg",
    "2": "2.png",
    "3": "3.jpg",
}
_PROFILE_SBT_WREATH_FILES = {
    "gold": "gold.jpg",
    "silver": "silver.jpg",
    "bronze": "copper.jpg",
    "none": "black.jpg",
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


def _format_pct_decimal(value: Decimal | None, signed: bool = True, digits: int = 1) -> str:
    val = _to_decimal(value)
    quant = Decimal("1").scaleb(-max(0, int(digits)))
    val = val.quantize(quant, rounding=ROUND_HALF_UP)
    sign = ""
    if signed and val > 0:
        sign = "+"
    return f"{sign}{val}%" if signed else f"{val}%"


def _to_decimal(value) -> Decimal:
    if value is None or isinstance(value, bool):
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    text = str(value).strip().replace(",", "")
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _parse_address_csv(raw: str | None, *, default_values: tuple[str, ...] = ()) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()
    source = str(raw or "").strip()
    if not source:
        source = ",".join(default_values)
    for token in source.replace("\n", ",").split(","):
        addr = str(token or "").strip().lower()
        if not addr.startswith("0x") or len(addr) != 42:
            continue
        if addr in seen:
            continue
        seen.add(addr)
        values.append(addr)
    return tuple(values)


def _build_profile_onchain_cfg():
    if not PROFILE_REALTIME_RECALC_ON_PROFILE:
        return None
    if PROFILE_REALTIME_METRICS_SOURCE != "onchain":
        return None
    if _ONCHAIN_CONFIG_CLS is None or _ONCHAIN_ANALYZE_WALLET_FN is None:
        return None
    api_key = str(os.getenv("BSCSCAN_API_KEY", "")).strip()
    if not api_key:
        return None
    api_url = str(os.getenv("BSCSCAN_API_URL", "https://api.etherscan.io/v2/api")).strip() or "https://api.etherscan.io/v2/api"
    chain_id = max(1, int(os.getenv("BSCSCAN_CHAIN_ID", "56")))
    usdt_contract = str(
        os.getenv("ONCHAIN_USDT_CONTRACT", "0x55d398326f99059ff775485246999027b3197955")
    ).strip().lower()
    pack_contracts = _parse_address_csv(
        os.getenv("ONCHAIN_PACK_CONTRACTS"),
        default_values=DEFAULT_ONCHAIN_PACK_CONTRACTS,
    )
    marketplace_contract = str(
        os.getenv("ONCHAIN_MARKETPLACE_CONTRACT", "0xae3e7268ef5a062946216a44f58a8f685ffd11d0")
    ).strip().lower()
    page_size = max(100, min(10000, int(os.getenv("ONCHAIN_PAGE_SIZE", "10000"))))
    retries = max(1, int(os.getenv("PROFILE_ONCHAIN_RETRIES", os.getenv("PROFILE_API_MAX_RETRIES", "4"))))
    backoff_sec = max(0.2, float(os.getenv("PROFILE_ONCHAIN_BACKOFF_SEC", os.getenv("PROFILE_API_RETRY_BACKOFF_SEC", "0.8"))))
    if not pack_contracts:
        return None
    return _ONCHAIN_CONFIG_CLS(
        api_url=api_url,
        chain_id=chain_id,
        api_key=api_key,
        usdt_contract=usdt_contract,
        pack_contracts=pack_contracts,
        marketplace_contract=marketplace_contract,
        page_size=page_size,
        retries=retries,
        backoff_sec=backoff_sec,
    )


def _profile_compute_onchain_metrics(wallet_address: str) -> dict[str, Decimal] | None:
    wallet_norm = _normalize_wallet_address(wallet_address or "")
    if not wallet_norm:
        return None
    cfg = _build_profile_onchain_cfg()
    if cfg is None:
        return None
    if _ONCHAIN_ANALYZE_WALLET_FN is None:
        return None
    metrics = _ONCHAIN_ANALYZE_WALLET_FN(cfg, wallet_norm)
    if not isinstance(metrics, dict):
        return None
    return {
        "pack_spent_usdt": _to_decimal(metrics.get("pack_spent_usdt")),
        "trade_volume_usdt": _to_decimal(metrics.get("trade_volume_usdt")),
        "trade_spent_usdt": _to_decimal(metrics.get("trade_spent_usdt")),
        "trade_earned_usdt": _to_decimal(metrics.get("trade_earned_usdt")),
        "buyback_earned_usdt": _to_decimal(metrics.get("buyback_earned_usdt")),
        "total_spent_usdt": _to_decimal(metrics.get("total_spent_usdt")),
        "total_earned_usdt": _to_decimal(metrics.get("total_earned_usdt")),
        "cash_net_usdt": _to_decimal(metrics.get("cash_net_usdt")),
        "open_pack_tx_count": _to_decimal(metrics.get("open_pack_tx_count")),
        "buyback_tx_count": _to_decimal(metrics.get("buyback_tx_count")),
        "trade_buy_tx_count": _to_decimal(metrics.get("trade_buy_tx_count")),
        "trade_sell_tx_count": _to_decimal(metrics.get("trade_sell_tx_count")),
    }


def _subpack_monitor_tier_sort_key(tier: str) -> tuple[int, str]:
    text = str(tier or "").strip().lower()
    try:
        return (SUBPACK_MONITOR_TIER_ORDER.index(text), text)
    except ValueError:
        return (len(SUBPACK_MONITOR_TIER_ORDER) + 10, text)


def _subpack_monitor_fetch_snapshot_sync(pack_id: str) -> dict[str, object]:
    pack_id_text = str(pack_id or "").strip()
    if not pack_id_text:
        raise ValueError("SUBPACK_MONITOR_PACK_ID is empty")

    payload = {"0": {"json": {"packId": pack_id_text}}}
    resp = _http_get(
        RENAISS_SUBPACK_INFO_URL,
        params={"batch": "1", "input": json.dumps(payload, separators=(",", ":"))},
        timeout=SUBPACK_MONITOR_TIMEOUT_SEC,
    )
    resp.raise_for_status()
    raw = resp.json()
    if not isinstance(raw, list) or not raw:
        raise ValueError("Unexpected TRPC payload shape")
    row0 = raw[0] if isinstance(raw[0], dict) else {}
    err_obj = row0.get("error") if isinstance(row0, dict) else None
    if err_obj:
        raise RuntimeError(f"TRPC error: {err_obj}")

    data_json = (
        ((row0.get("result") or {}).get("data") or {}).get("json")
        if isinstance(row0, dict)
        else None
    )
    if not isinstance(data_json, dict):
        raise ValueError("Missing result.data.json")

    ev_raw = str(data_json.get("ev") or "0").strip() or "0"
    tiers_raw = data_json.get("tiers") if isinstance(data_json.get("tiers"), dict) else {}

    tiers: dict[str, str] = {}
    for tier_name, tier_obj in tiers_raw.items():
        if not isinstance(tier_obj, dict):
            continue
        chance_dec = _to_decimal(tier_obj.get("chance"))
        tiers[str(tier_name).strip().lower()] = format(chance_dec, "f")

    return {
        "pack_id": pack_id_text,
        "ev_raw": ev_raw,
        "tiers": tiers,
        "checked_at": int(time.time()),
    }


def _subpack_monitor_snapshot_signature(snapshot: dict[str, object]) -> tuple[str, tuple[tuple[str, str], ...]]:
    ev_raw = str(snapshot.get("ev_raw") or "0")
    tiers_obj = snapshot.get("tiers")
    tiers_dict = tiers_obj if isinstance(tiers_obj, dict) else {}
    pairs: list[tuple[str, str]] = []
    for tier_name in sorted(tiers_dict.keys(), key=_subpack_monitor_tier_sort_key):
        pairs.append((str(tier_name), str(tiers_dict.get(tier_name) or "0")))
    return ev_raw, tuple(pairs)


def _subpack_monitor_pct_text(chance_text: str | None) -> str:
    chance = _to_decimal(chance_text)
    pct = chance * Decimal("100")
    return f"{pct:.4f}%"


def _subpack_monitor_render_change_message(prev: dict[str, object], curr: dict[str, object]) -> str:
    prev_tiers = prev.get("tiers") if isinstance(prev.get("tiers"), dict) else {}
    curr_tiers = curr.get("tiers") if isinstance(curr.get("tiers"), dict) else {}
    all_tiers = sorted({*prev_tiers.keys(), *curr_tiers.keys()}, key=_subpack_monitor_tier_sort_key)

    changed_lines: list[str] = []
    for tier in all_tiers:
        before = str(prev_tiers.get(tier) or "0")
        after = str(curr_tiers.get(tier) or "0")
        if before == after:
            continue
        changed_lines.append(
            f"- `{tier}`: `{_subpack_monitor_pct_text(before)}` -> `{_subpack_monitor_pct_text(after)}`"
        )
    if not changed_lines:
        changed_lines.append("- tier values changed (raw comparison) but no formatted diff")

    ev_before = str(prev.get("ev_raw") or "0")
    ev_after = str(curr.get("ev_raw") or "0")
    ev_line = (
        f"`{ev_before}` -> `{ev_after}`"
        if ev_before != ev_after
        else f"`{ev_after}` (unchanged)"
    )

    checked_at = int(curr.get("checked_at") or int(time.time()))
    pack_id = str(curr.get("pack_id") or "")
    lines = [
        "🔔 **Subpack odds updated**",
        f"packId: `{pack_id}`",
        f"EV(raw): {ev_line}",
        "Changes:",
        *changed_lines,
        f"Checked: <t:{checked_at}:F>",
    ]
    return "\n".join(lines)


def _subpack_monitor_render_initial_message(curr: dict[str, object]) -> str:
    tiers_obj = curr.get("tiers")
    tiers_dict = tiers_obj if isinstance(tiers_obj, dict) else {}
    tier_lines: list[str] = []
    for tier_name in sorted(tiers_dict.keys(), key=_subpack_monitor_tier_sort_key):
        tier_lines.append(f"- `{tier_name}`: `{_subpack_monitor_pct_text(str(tiers_dict.get(tier_name) or '0'))}`")

    checked_at = int(curr.get("checked_at") or int(time.time()))
    pack_id = str(curr.get("pack_id") or "")
    ev_raw = str(curr.get("ev_raw") or "0")
    lines = [
        "🟢 **Subpack odds baseline initialized**",
        f"packId: `{pack_id}`",
        f"EV(raw): `{ev_raw}`",
        "Current tiers:",
        *(tier_lines or ["- (no tiers)"]),
        f"Checked: <t:{checked_at}:F>",
    ]
    return "\n".join(lines)


async def _subpack_monitor_send_message(text: str) -> bool:
    channel_id = int(SUBPACK_MONITOR_NOTIFY_CHANNEL_ID or 0)
    if channel_id <= 0:
        print("⚠️ subpack monitor: notify channel id is invalid")
        return False
    channel = client.get_channel(channel_id)
    if channel is None:
        try:
            channel = await client.fetch_channel(channel_id)
        except Exception as e:
            print(f"⚠️ subpack monitor: cannot fetch channel {channel_id}: {e}")
            return False
    if channel is None or not hasattr(channel, "send"):
        print(f"⚠️ subpack monitor: channel {channel_id} is not messageable")
        return False
    try:
        await channel.send(text)
        return True
    except Exception as e:
        print(f"⚠️ subpack monitor: send failed channel={channel_id} err={e}")
        return False


async def _subpack_monitor_poll_once(trigger: str) -> bool:
    global SUBPACK_MONITOR_LOCK, SUBPACK_MONITOR_LAST_SNAPSHOT

    if not SUBPACK_MONITOR_ENABLED:
        return False
    if not SUBPACK_MONITOR_PACK_ID:
        print("⚠️ subpack monitor: pack id is empty")
        return False
    if SUBPACK_MONITOR_LOCK is None:
        SUBPACK_MONITOR_LOCK = asyncio.Lock()
    if SUBPACK_MONITOR_LOCK.locked():
        return False

    async with SUBPACK_MONITOR_LOCK:
        loop = asyncio.get_running_loop()
        try:
            curr = await loop.run_in_executor(None, _subpack_monitor_fetch_snapshot_sync, SUBPACK_MONITOR_PACK_ID)
        except Exception as e:
            print(f"⚠️ subpack monitor fetch failed trigger={trigger}: {e}")
            return False

        prev = SUBPACK_MONITOR_LAST_SNAPSHOT
        curr_sig = _subpack_monitor_snapshot_signature(curr)
        if not isinstance(prev, dict):
            init_msg = _subpack_monitor_render_initial_message(curr)
            sent = await _subpack_monitor_send_message(init_msg)
            if sent:
                SUBPACK_MONITOR_LAST_SNAPSHOT = curr
                print(
                    "🟢 subpack monitor baseline sent "
                    f"trigger={trigger} packId={SUBPACK_MONITOR_PACK_ID} "
                    f"channel={SUBPACK_MONITOR_NOTIFY_CHANNEL_ID} ev={curr_sig[0]} tiers={len(curr_sig[1])}"
                )
                return True
            return False

        prev_sig = _subpack_monitor_snapshot_signature(prev)
        if prev_sig == curr_sig:
            return True

        msg = _subpack_monitor_render_change_message(prev, curr)
        sent = await _subpack_monitor_send_message(msg)
        if sent:
            SUBPACK_MONITOR_LAST_SNAPSHOT = curr
            print(
                "🔔 subpack monitor change sent "
                f"trigger={trigger} packId={SUBPACK_MONITOR_PACK_ID} channel={SUBPACK_MONITOR_NOTIFY_CHANNEL_ID}"
            )
            return True
        return False


def _legendary_monitor_pack_url(pack_slug: str) -> str:
    slug = str(pack_slug or "").strip().strip("/")
    return f"https://www.renaiss.xyz/gacha/{slug}"


def _legendary_monitor_fmv_to_usd(value: object) -> Decimal:
    raw = str(value or "").strip()
    m = re.fullmatch(r"\$n([0-9]+)", raw)
    if m:
        return _to_decimal(m.group(1)) / Decimal("100")
    return _to_decimal(raw)


def _legendary_monitor_fetch_snapshot_sync(pack_slug: str) -> dict[str, object]:
    slug = str(pack_slug or "").strip().strip("/")
    if not slug:
        raise ValueError("LEGENDARY_ALERT_MONITOR_PACK_SLUG is empty")

    url = _legendary_monitor_pack_url(slug)
    resp = _http_get(url, timeout=LEGENDARY_ALERT_MONITOR_TIMEOUT_SEC)
    resp.raise_for_status()
    html_text = str(resp.text or "")

    pack_id_match = re.search(r'\\"packId\\":\\"([0-9a-fA-F-]{36})\\"', html_text)
    pack_id = str(pack_id_match.group(1) if pack_id_match else "").strip()

    data_match = re.search(
        r'\\"openedPackActivities\\":(\[.*?\]),\\"fetchContent\\":',
        html_text,
        flags=re.S,
    )
    if not data_match:
        raise RuntimeError("openedPackActivities not found in gacha page payload")

    arr_escaped = data_match.group(1)
    arr_json_text = bytes(arr_escaped, "utf-8").decode("unicode_escape")
    arr_obj = json.loads(arr_json_text)
    rows = arr_obj if isinstance(arr_obj, list) else []

    activities: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        token_id = str(row.get("id") or "").strip()
        if not token_id:
            continue
        tier = str(row.get("tier") or "").strip().lower()
        pulled_ts = _parse_int(row.get("pulledAtTimestamp")) or 0
        fmv_raw = str(row.get("fmv") or "").strip()
        fmv_usd = _legendary_monitor_fmv_to_usd(fmv_raw)
        image_url = str(row.get("frontImageUrl") or "").strip()
        activities.append(
            {
                "id": token_id,
                "tier": tier,
                "pulledAtTimestamp": int(pulled_ts),
                "fmv_raw": fmv_raw,
                "fmv_usd": format(fmv_usd, "f"),
                "frontImageUrl": image_url,
            }
        )

    legendary_rows = [x for x in activities if str(x.get("tier") or "") == "legendary"]
    legendary_rows.sort(key=lambda x: int(x.get("pulledAtTimestamp") or 0), reverse=True)
    return {
        "pack_slug": slug,
        "pack_id": pack_id,
        "fetched_at": int(time.time()),
        "activities": activities,
        "legendary_rows": legendary_rows,
    }


def _legendary_monitor_resolve_receiver_sync(token_id: str, pulled_ts: int) -> dict[str, str]:
    tid = str(token_id or "").strip()
    if not tid:
        return {"to": "", "tx_hash": "", "user": ""}
    target_ts = int(pulled_ts or 0)
    cursor: str | None = None

    for _ in range(4):
        page = _trpc_token_activities(tid, cursor=cursor, limit=50)
        activities = page.get("activities") if isinstance(page.get("activities"), list) else []
        for row in activities:
            if not isinstance(row, dict):
                continue
            if str(row.get("__typename") or "") != "TransferActivity":
                continue
            ts = _parse_int(row.get("timestamp")) or 0
            if target_ts > 0 and ts != target_ts:
                continue
            return {
                "to": str(row.get("to") or "").strip().lower(),
                "tx_hash": str(row.get("txHash") or "").strip().lower(),
                "user": str(row.get("user") or "").strip().lower(),
            }
        next_cursor = str(page.get("nextCursor") or "").strip()
        if not next_cursor:
            break
        cursor = next_cursor

    return {"to": "", "tx_hash": "", "user": ""}


def _legendary_monitor_render_alert_text(
    row: dict[str, object],
    snapshot: dict[str, object],
    *,
    is_test: bool = False,
) -> str:
    pack_slug = str(snapshot.get("pack_slug") or "")
    tier = str(row.get("tier") or "").lower()
    pulled_ts = int(_parse_int(row.get("pulledAtTimestamp")) or 0)
    fmv_raw = str(row.get("fmv_raw") or "")
    fmv_usd = _to_decimal(row.get("fmv_usd"))
    fmv_text = f"${fmv_usd:.2f}" if fmv_usd > 0 else fmv_raw

    lines = [
        "🧪 **Legendary Alert Test**" if is_test else "🚨 **LEGENDARY HIT!**",
        "━━━━━━━━━━━━━━",
        f"🎴 Pack: `{pack_slug}`",
        f"💎 Tier: `{tier or 'unknown'}`",
        f"💰 FMV: `{fmv_text}`",
        f"🕒 Pulled: <t:{pulled_ts}:F>" if pulled_ts > 0 else "🕒 Pulled: `N/A`",
    ]
    return "\n".join(lines)


async def _legendary_monitor_send_message(text: str, image_url: str | None = None) -> bool:
    channel_id = int(LEGENDARY_ALERT_MONITOR_CHANNEL_ID or 0)
    if channel_id <= 0:
        print("⚠️ legendary monitor: notify channel id is invalid")
        return False
    channel = client.get_channel(channel_id)
    if channel is None:
        try:
            channel = await client.fetch_channel(channel_id)
        except Exception as e:
            print(f"⚠️ legendary monitor: cannot fetch channel {channel_id}: {e}")
            return False
    if channel is None or not hasattr(channel, "send"):
        print(f"⚠️ legendary monitor: channel {channel_id} is not messageable")
        return False
    try:
        image_text = str(image_url or "").strip()
        embed = None
        if image_text:
            embed = discord.Embed()
            embed.set_image(url=image_text)
        await channel.send(content=text, embed=embed)
        return True
    except Exception as e:
        print(f"⚠️ legendary monitor: send failed channel={channel_id} err={e}")
        return False


async def _legendary_alert_monitor_poll_once(trigger: str, *, send_test: bool = False) -> bool:
    global LEGENDARY_ALERT_MONITOR_LOCK, LEGENDARY_ALERT_MONITOR_TEST_SENT

    if not LEGENDARY_ALERT_MONITOR_ENABLED:
        return False
    if not LEGENDARY_ALERT_MONITOR_PACK_SLUG:
        print("⚠️ legendary monitor: pack slug is empty")
        return False
    if LEGENDARY_ALERT_MONITOR_LOCK is None:
        LEGENDARY_ALERT_MONITOR_LOCK = asyncio.Lock()
    if LEGENDARY_ALERT_MONITOR_LOCK.locked():
        return False

    async with LEGENDARY_ALERT_MONITOR_LOCK:
        loop = asyncio.get_running_loop()
        try:
            snapshot = await loop.run_in_executor(
                None,
                _legendary_monitor_fetch_snapshot_sync,
                LEGENDARY_ALERT_MONITOR_PACK_SLUG,
            )
        except Exception as e:
            print(f"⚠️ legendary monitor fetch failed trigger={trigger}: {e}")
            return False

        legendary_rows_raw = snapshot.get("legendary_rows")
        legendary_rows = (
            [x for x in legendary_rows_raw if isinstance(x, dict)]
            if isinstance(legendary_rows_raw, list)
            else []
        )
        if not LEGENDARY_ALERT_MONITOR_SEEN_IDS:
            now_ts = int(time.time())
            cutoff_ts = now_ts - int(LEGENDARY_ALERT_MONITOR_STARTUP_LOOKBACK_MINUTES * 60)
            baseline_count = 0
            pending_recent_count = 0
            for x in legendary_rows:
                token_id = str(x.get("id") or "").strip()
                if not token_id:
                    continue
                pulled_ts = _parse_int(x.get("pulledAtTimestamp")) or 0
                # On fresh start, suppress old legendary rows to avoid replay spam.
                # Keep recent rows (within lookback window) as pending alerts.
                if pulled_ts > 0 and pulled_ts >= cutoff_ts:
                    pending_recent_count += 1
                    continue
                LEGENDARY_ALERT_MONITOR_SEEN_IDS.add(token_id)
                baseline_count += 1
            print(
                "📦 legendary monitor baseline set "
                f"pack={LEGENDARY_ALERT_MONITOR_PACK_SLUG} "
                f"baseline_old={baseline_count} pending_recent={pending_recent_count} "
                f"lookback_min={LEGENDARY_ALERT_MONITOR_STARTUP_LOOKBACK_MINUTES}"
            )

        if send_test and not LEGENDARY_ALERT_MONITOR_TEST_SENT and legendary_rows:
            test_row = dict(legendary_rows[0])
            pulled_ts = _parse_int(test_row.get("pulledAtTimestamp")) or 0
            token_id = str(test_row.get("id") or "").strip()
            try:
                receiver = await loop.run_in_executor(None, _legendary_monitor_resolve_receiver_sync, token_id, pulled_ts)
            except Exception:
                receiver = {"to": "", "tx_hash": "", "user": ""}
            test_row["winner_wallet"] = str(receiver.get("to") or "")
            test_row["tx_hash"] = str(receiver.get("tx_hash") or "")
            test_row["operator_wallet"] = str(receiver.get("user") or "")
            test_msg = _legendary_monitor_render_alert_text(test_row, snapshot, is_test=True)
            test_image = str(test_row.get("frontImageUrl") or "").strip()
            test_sent = await _legendary_monitor_send_message(test_msg, image_url=test_image)
            if test_sent:
                LEGENDARY_ALERT_MONITOR_TEST_SENT = True
                print(
                    "🧪 legendary monitor test sent "
                    f"pack={LEGENDARY_ALERT_MONITOR_PACK_SLUG} channel={LEGENDARY_ALERT_MONITOR_CHANNEL_ID}"
                )

        new_rows: list[dict[str, object]] = []
        for row in reversed(legendary_rows):
            token_id = str(row.get("id") or "").strip()
            if not token_id:
                continue
            if token_id in LEGENDARY_ALERT_MONITOR_SEEN_IDS:
                continue
            new_rows.append(dict(row))

        if not new_rows:
            return True

        for row in new_rows:
            pulled_ts = _parse_int(row.get("pulledAtTimestamp")) or 0
            token_id = str(row.get("id") or "").strip()
            try:
                receiver = await loop.run_in_executor(None, _legendary_monitor_resolve_receiver_sync, token_id, pulled_ts)
            except Exception:
                receiver = {"to": "", "tx_hash": "", "user": ""}
            row["winner_wallet"] = str(receiver.get("to") or "")
            row["tx_hash"] = str(receiver.get("tx_hash") or "")
            row["operator_wallet"] = str(receiver.get("user") or "")

            msg = _legendary_monitor_render_alert_text(row, snapshot, is_test=False)
            image_url = str(row.get("frontImageUrl") or "").strip()
            sent = await _legendary_monitor_send_message(msg, image_url=image_url)
            if sent:
                LEGENDARY_ALERT_MONITOR_SEEN_IDS.add(token_id)
                print(
                    "🚨 legendary monitor alert sent "
                    f"trigger={trigger} token={token_id} channel={LEGENDARY_ALERT_MONITOR_CHANNEL_ID}"
                )
        return True


def _fmv_disk_cache_path(cache_key: str) -> str:
    digest = hashlib.sha1(str(cache_key or "").encode("utf-8")).hexdigest()
    return os.path.join(PROFILE_DISK_FMV_CACHE_DIR, f"{digest}.json")


def _load_fmv_from_disk_cache(cache_key: str) -> Decimal | None:
    if not PROFILE_ENABLE_DISK_FMV_CACHE:
        return None
    path = _fmv_disk_cache_path(cache_key)
    try:
        with _PROFILE_FMV_DISK_LOCK:
            if not os.path.exists(path):
                return None
            with open(path, "r", encoding="utf-8") as f:
                row = json.load(f)
        if not isinstance(row, dict):
            return None
        updated_at = _parse_int(row.get("updated_at")) or 0
        if PROFILE_DISK_FMV_CACHE_TTL_SEC > 0 and updated_at > 0:
            if (int(time.time()) - updated_at) > PROFILE_DISK_FMV_CACHE_TTL_SEC:
                return None
        return _to_decimal(row.get("value"))
    except Exception:
        return None


def _save_fmv_to_disk_cache(cache_key: str, value: Decimal) -> None:
    if not PROFILE_ENABLE_DISK_FMV_CACHE:
        return
    path = _fmv_disk_cache_path(cache_key)
    payload = {
        "cache_key": str(cache_key),
        "value": format(_to_decimal(value), "f"),
        "updated_at": int(time.time()),
    }
    try:
        with _PROFILE_FMV_DISK_LOCK:
            os.makedirs(PROFILE_DISK_FMV_CACHE_DIR, exist_ok=True)
            tmp_path = f"{path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(tmp_path, path)
    except Exception:
        return


def _snkr_cache_path(search_key: str) -> str:
    digest = hashlib.sha1(str(search_key or "").encode("utf-8")).hexdigest()
    return os.path.join(PROFILE_SNKR_CACHE_DIR, f"{digest}.json")


def _load_snkr_records_from_disk_cache(search_key: str) -> list[dict] | None:
    path = _snkr_cache_path(search_key)
    try:
        with _PROFILE_SNKR_CACHE_LOCK:
            if not os.path.exists(path):
                return None
            with open(path, "r", encoding="utf-8") as f:
                row = json.load(f)
        if not isinstance(row, dict):
            return None
        updated_at = _parse_int(row.get("updated_at")) or 0
        if PROFILE_SNKR_CACHE_TTL_SEC > 0 and updated_at > 0:
            if (int(time.time()) - updated_at) > PROFILE_SNKR_CACHE_TTL_SEC:
                return None
        records = row.get("records")
        if not isinstance(records, list):
            return None
        return [x for x in records if isinstance(x, dict)]
    except Exception:
        return None


def _save_snkr_records_to_disk_cache(search_key: str, records: list[dict]) -> None:
    path = _snkr_cache_path(search_key)
    payload = {
        "search_key": str(search_key),
        "updated_at": int(time.time()),
        "records": [x for x in records if isinstance(x, dict)],
    }
    try:
        with _PROFILE_SNKR_CACHE_LOCK:
            os.makedirs(PROFILE_SNKR_CACHE_DIR, exist_ok=True)
            tmp_path = f"{path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(tmp_path, path)
    except Exception:
        return


def _parse_market_record_date(date_str: str) -> datetime | None:
    text = str(date_str or "").strip()
    if not text:
        return None
    try:
        if re.match(r"^\d{4}/\d{2}/\d{2}$", text):
            return datetime.strptime(text, "%Y/%m/%d")
        if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
            return datetime.strptime(text, "%Y-%m-%d")
        if re.match(r"^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}$", text):
            return datetime.strptime(text, "%b %d, %Y")
    except Exception:
        return None
    return None


def _market_grade_matches(record_grade: str, target_grade: str) -> bool:
    rg = str(record_grade or "").strip()
    tg = str(target_grade or "").strip() or "Unknown"
    if rg == tg:
        return True
    if tg == "Unknown" and rg in ("Ungraded", "裸卡", "A"):
        return True
    return bool(rg and rg.replace(" ", "") == tg.replace(" ", ""))


def _calculate_market_average_window(
    records: list[dict],
    target_grade: str,
    end_at: datetime,
    window_days: int = 30,
) -> tuple[Decimal | None, int]:
    if not isinstance(records, list) or not records:
        return None, 0
    start_dt = end_at - timedelta(days=max(1, int(window_days)))
    prices: list[Decimal] = []
    for row in records:
        if not isinstance(row, dict):
            continue
        if not _market_grade_matches(str(row.get("grade") or ""), target_grade):
            continue
        date_obj = _parse_market_record_date(str(row.get("date") or ""))
        if date_obj is None:
            continue
        if date_obj < start_dt or date_obj > end_at:
            continue
        price = _to_decimal(row.get("price"))
        if price > 0:
            prices.append(price)
    if not prices:
        return None, 0
    count_raw = len(prices)
    values = sorted(prices)
    if len(values) >= 4:
        n = len(values)
        q1 = values[n // 4]
        q3 = values[(n * 3) // 4]
        iqr = q3 - q1
        lower = q1 - (iqr * Decimal("1.5"))
        upper = q3 + (iqr * Decimal("1.5"))
        filtered = [p for p in values if lower <= p <= upper]
        if filtered:
            values = filtered
    avg = sum(values, Decimal("0")) / Decimal(len(values))
    return avg, count_raw


def _extract_set_code_from_name(full_name: str) -> str:
    text = str(full_name or "")
    m = re.search(r"\b(SV-P|S-P|SM-P|XY-P|BW-P|DP-P|L-P|ADV-P|SV-G|S8a-G)\b", text, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"\b(SV|S|SM|XY|BW|DP|L|ADV)\s+Promo\b", text, re.IGNORECASE)
    if m:
        return f"{m.group(1).upper()}-P"
    m = re.search(r"\b(OP\d+|ST\d+|EB\d+)\b", text, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"\b(SV\d+[A-Za-z]*)\b", text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\b(S\d+[A-Za-z]?)\b", text)
    if m and re.match(r"^S\d", m.group(1)):
        return m.group(1)
    return ""


def _parse_renaiss_name_for_market(full_name: str) -> tuple[str, str, str, str, str]:
    text = str(full_name or "").strip()
    grade_m = re.search(r"(PSA|BGS|CGC|SGC)\s+(\d+(?:\.\d+)?)", text)
    grade_tag = f"{grade_m.group(1)} {grade_m.group(2)}" if grade_m else "Unknown"
    set_code = _extract_set_code_from_name(text)

    number = "0"
    set_name_candidate = text
    card_name_candidate = text
    num_m = re.search(r"#([A-Za-z0-9]+(?:/[A-Za-z0-9-]+)?)", text)
    if num_m:
        number = num_m.group(1)
        set_name_candidate = text[: num_m.start()]
        card_name_candidate = text[num_m.end() :]

    if grade_m:
        set_name_candidate = set_name_candidate.replace(grade_m.group(0), "")
    if set_code:
        set_name_candidate = re.sub(re.escape(set_code) + r"[^\s]*", "", set_name_candidate, flags=re.IGNORECASE).strip()
    set_name_candidate = re.sub(r"\b20\d{2}\b", "", set_name_candidate).strip()
    for kw in ("Pokemon", "Japanese", "English", "Korean", "Gem Mint", "Mint", "One Piece"):
        set_name_candidate = re.sub(rf"\b{re.escape(kw)}\b", "", set_name_candidate, flags=re.IGNORECASE).strip()
    set_name = " ".join(set_name_candidate.split())

    if grade_m:
        card_name_candidate = card_name_candidate.replace(grade_m.group(0), "")
    if set_code:
        card_name_candidate = re.sub(re.escape(set_code) + r"[^\s]*", "", card_name_candidate, flags=re.IGNORECASE).strip()
    card_name_candidate = re.sub(r"\b20\d{2}\b", "", card_name_candidate).strip()
    for kw in ("Pokemon", "Japanese", "English", "Korean", "One Piece"):
        card_name_candidate = re.sub(rf"\b{re.escape(kw)}\b", "", card_name_candidate, flags=re.IGNORECASE).strip()
    card_name = " ".join(card_name_candidate.split()) or text
    return card_name, number, set_code, set_name, grade_tag


def _normalize_market_card_language(raw_value: str, full_name: str = "") -> str:
    value = str(raw_value or "").strip().lower()
    if value in ("jp", "ja", "jpn", "japanese", "日文", "日語", "日本語", "日版"):
        return "JP"
    if value in ("en", "eng", "english", "英文", "英語", "usa", "us"):
        return "EN"
    if value in ("kr", "ko", "kor", "korean", "韓文", "韓語"):
        return "KR"
    name_lower = str(full_name or "").lower()
    if "japanese" in name_lower:
        return "JP"
    if "korean" in name_lower:
        return "KR"
    if "english" in name_lower:
        return "EN"
    return "UNKNOWN"


def _build_snkr_search_spec_from_collectible(raw: dict) -> dict:
    full_name = str((raw or {}).get("name") or "").strip()
    card_name, number, set_code, set_name, grade_tag = _parse_renaiss_name_for_market(full_name)
    attributes = (raw or {}).get("attributes")
    attr_number = ""
    attr_set_name = ""
    attr_language = ""
    attr_category = ""
    if isinstance(attributes, list):
        for attr in attributes:
            if not isinstance(attr, dict):
                continue
            trait = str(attr.get("trait") or "").strip().lower()
            value = str(attr.get("value") or "").strip()
            if not value:
                continue
            if trait == "card number":
                attr_number = value.replace("#", "").strip()
            elif trait == "set":
                attr_set_name = value
            elif trait == "language":
                attr_language = value
            elif trait == "category":
                attr_category = value
    if attr_number:
        number = attr_number
    if attr_set_name:
        set_name = attr_set_name
        if not set_code:
            set_code = _extract_set_code_from_name(attr_set_name)

    category = "One Piece" if re.match(r"^(OP|ST|EB)\d", set_code or "", re.IGNORECASE) else "Pokemon"
    if attr_category:
        category = attr_category
    elif any(x in full_name for x in ("One Piece", "WANTED")):
        category = "One Piece"

    company = str((raw or {}).get("gradingCompany") or "").strip().upper()
    grade_text = str((raw or {}).get("grade") or "").strip()
    if company:
        grade_num_m = re.search(r"(\d+(?:\.\d+)?)", grade_text)
        if grade_num_m:
            grade_tag = f"{company} {grade_num_m.group(1)}"

    lang_code = _normalize_market_card_language(attr_language, full_name)
    variant_map = {
        "manga": ["コミパラ", "manga"],
        "parallel": ["パラレル"],
        "wanted": ["wanted"],
        "-sp": ["sp", "-sp"],
        "l-p": ["l-p"],
        "sr-p": ["sr-p"],
        "flagship": ["flagship", "フラッグシップ", "フラシ"],
    }
    name_lower = full_name.lower()
    snkr_variant_kws: list[str] = []
    for kws in variant_map.values():
        if any(kw in name_lower for kw in kws):
            snkr_variant_kws.append(kws[0])
    is_alt_art = bool(snkr_variant_kws) or any(x in name_lower for x in ("special card", "alt art", "alternative"))

    return {
        "name": card_name or full_name or "Unknown",
        "number": number or "0",
        "set_code": set_code or "",
        "set_name": set_name or str((raw or {}).get("setName") or ""),
        "target_grade": grade_tag or "Unknown",
        "is_alt_art": bool(is_alt_art),
        "category": category,
        "card_language": lang_code,
        "snkr_variant_kws": snkr_variant_kws,
    }


def _fetch_snkr_records_by_spec(search_spec: dict, required_start_date: date | None = None) -> list[dict]:
    spec = dict(search_spec or {})
    req_start_text = required_start_date.isoformat() if isinstance(required_start_date, date) else ""
    cache_scope = {"spec": spec, "required_start_date": req_start_text}
    cache_key = json.dumps(cache_scope, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if PROFILE_ENABLE_RUNTIME_CACHE:
        cached = _CARD_SNKR_CACHE.get(cache_key)
        if isinstance(cached, dict):
            updated_at = _parse_int(cached.get("updated_at")) or 0
            if PROFILE_SNKR_CACHE_TTL_SEC <= 0 or updated_at <= 0 or (int(time.time()) - updated_at) <= PROFILE_SNKR_CACHE_TTL_SEC:
                records = cached.get("records")
                if isinstance(records, list):
                    return [x for x in records if isinstance(x, dict)]

    disk_cached = _load_snkr_records_from_disk_cache(cache_key)
    if isinstance(disk_cached, list):
        if PROFILE_ENABLE_RUNTIME_CACHE:
            _CARD_SNKR_CACHE[cache_key] = {"updated_at": int(time.time()), "records": disk_cached}
        return disk_cached

    records: list[dict] = []
    try:
        result = market_report_vision.search_snkrdunk(
            en_name=str(spec.get("name") or ""),
            jp_name="",
            number=str(spec.get("number") or ""),
            set_code=str(spec.get("set_code") or ""),
            target_grade=str(spec.get("target_grade") or "Unknown"),
            is_alt_art=bool(spec.get("is_alt_art")),
            card_language=str(spec.get("card_language") or "UNKNOWN"),
            snkr_variant_kws=list(spec.get("snkr_variant_kws") or []),
            set_name=str(spec.get("set_name") or ""),
            history_start_date=req_start_text,
            history_max_pages=PROFILE_SNKR_HISTORY_MAX_PAGES if req_start_text else 1,
        )
        if isinstance(result, tuple) and len(result) >= 1 and isinstance(result[0], list):
            records = [x for x in result[0] if isinstance(x, dict)]
    except Exception:
        records = []

    if PROFILE_ENABLE_RUNTIME_CACHE:
        _CARD_SNKR_CACHE[cache_key] = {"updated_at": int(time.time()), "records": records}
    _save_snkr_records_to_disk_cache(cache_key, records)
    return records


def _fetch_pc_records_by_spec(search_spec: dict, required_start_date: date | None = None) -> list[dict]:
    spec = dict(search_spec or {})
    req_start_text = required_start_date.isoformat() if isinstance(required_start_date, date) else ""
    cache_scope = {"spec": spec, "required_start_date": req_start_text}
    cache_key = json.dumps(cache_scope, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if PROFILE_ENABLE_RUNTIME_CACHE:
        cached = _CARD_PC_CACHE.get(cache_key)
        if isinstance(cached, dict):
            updated_at = _parse_int(cached.get("updated_at")) or 0
            if PROFILE_SNKR_CACHE_TTL_SEC <= 0 or updated_at <= 0 or (int(time.time()) - updated_at) <= PROFILE_SNKR_CACHE_TTL_SEC:
                records = cached.get("records")
                if isinstance(records, list):
                    return [x for x in records if isinstance(x, dict)]

    records: list[dict] = []
    try:
        result = market_report_vision.search_pricecharting(
            name=str(spec.get("name") or ""),
            number=str(spec.get("number") or ""),
            set_code=str(spec.get("set_code") or ""),
            target_grade=str(spec.get("target_grade") or "Unknown"),
            is_alt_art=bool(spec.get("is_alt_art")),
            category=str(spec.get("category") or "Pokemon"),
            set_name=str(spec.get("set_name") or ""),
            jp_name="",
            is_flagship=any(
                "flagship" in str(x).lower()
                for x in (spec.get("snkr_variant_kws") or [])
            ),
        )
        if isinstance(result, tuple) and len(result) >= 1 and isinstance(result[0], list):
            records = [x for x in result[0] if isinstance(x, dict)]
    except Exception:
        records = []

    if PROFILE_ENABLE_RUNTIME_CACHE:
        _CARD_PC_CACHE[cache_key] = {"updated_at": int(time.time()), "records": records}
    return records


def _select_snkr_average_usd(records: list[dict], target_grade: str, end_at: datetime, jpy_rate: Decimal) -> Decimal:
    avg_jpy, _ = _calculate_market_average_window(records, target_grade, end_at=end_at, window_days=PROFILE_SNKR_WINDOW_DAYS)
    if avg_jpy is None or avg_jpy <= 0 or jpy_rate <= 0:
        return Decimal("0")
    return avg_jpy / jpy_rate


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


def _looks_like_preview_image_url(url: str | None) -> bool:
    text = str(url or "").strip().lower()
    if not text:
        return True
    return (
        "graded-cards-renders" in text
        or "nft_image" in text
        or "/renders/" in text
        or "preview" in text
    )


def _pick_non_preview_image(*urls: str) -> str:
    for raw in urls:
        u = str(raw or "").strip()
        if u and not _looks_like_preview_image_url(u):
            return u
    return ""


def _looks_like_pack_image_url(url: str | None) -> bool:
    text = str(url or "").strip().lower()
    if not text:
        return False
    if "/packs/" in text:
        return True
    if "pack%20rotate" in text or "pack_rotate" in text:
        return True
    return False


def _clamp_profile_card_count(value: int | None) -> int:
    if value in (1, 3, 4, 5, 7, 10):
        return int(value)
    return 10


def _clamp_flex_pack_card_count(value: int | None) -> int:
    if value in (1, 2, 3, 4, 7, 10):
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
    return _file_to_data_uri(img_path)


def _file_to_data_uri(file_path: str | None) -> str:
    path = str(file_path or "").strip()
    if not path or not os.path.exists(path):
        return ""
    try:
        with open(path, "rb") as f:
            raw = f.read()
        mime, _ = mimetypes.guess_type(path)
        if not mime:
            ext = os.path.splitext(path)[1].lower()
            mime = "image/png" if ext == ".png" else "image/jpeg"
        b64 = base64.b64encode(raw).decode("utf-8")
        return f"data:{mime};base64,{b64}"
    except Exception:
        return ""


def _profile_sbt_rank_background_data_uri(rank_tier: str | None) -> str:
    tier = str(rank_tier or "").strip().lower()
    if tier not in ("gold", "silver", "bronze"):
        tier = "none"
    filename = _PROFILE_SBT_WREATH_FILES.get(tier) or _PROFILE_SBT_WREATH_FILES["none"]
    return _file_to_data_uri(os.path.join(PROFILE_SBT_WREATH_IMAGE_DIR, filename))


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


def _profile_holdings_labels(lang: str) -> dict[str, str]:
    if lang == "zh":
        return {
            "title": "持倉資產時間軸",
            "subtitle": "依卡片取得時間累積目前 FMV 資產",
            "total_current": "目前持倉總值",
            "total_change": "期間增長",
            "holdings_count": "持倉張數",
            "timeline_title": "資產累積曲線",
            "timeline_empty": "目前沒有足夠資料可生成時間軸",
            "timeline_min": "最低",
            "timeline_max": "最高",
            "event_title": "近期資產增加事件",
            "event_empty": "尚無可顯示的資產增加事件",
            "event_cards": "張卡片",
            "note": "以目前持倉卡片 FMV 計算，按取得時間做累積曲線。",
        }
    if lang == "zhs":
        return {
            "title": "持仓资产时间轴",
            "subtitle": "按卡片获取时间累积当前 FMV 资产",
            "total_current": "当前持仓总值",
            "total_change": "期间增长",
            "holdings_count": "持仓张数",
            "timeline_title": "资产累积曲线",
            "timeline_empty": "当前没有足够数据可生成时间轴",
            "timeline_min": "最低",
            "timeline_max": "最高",
            "event_title": "近期资产增加事件",
            "event_empty": "暂无可显示的资产增加事件",
            "event_cards": "张卡片",
            "note": "以当前持仓卡片 FMV 计算，并按获取时间生成累积曲线。",
        }
    if lang == "ko":
        return {
            "title": "보유 자산 타임라인",
            "subtitle": "카드 획득 시점 기준 현재 FMV 누적",
            "total_current": "현재 보유 총가치",
            "total_change": "기간 증가",
            "holdings_count": "보유 카드 수",
            "timeline_title": "자산 누적 곡선",
            "timeline_empty": "타임라인을 만들 데이터가 부족합니다",
            "timeline_min": "최저",
            "timeline_max": "최고",
            "event_title": "최근 자산 증가 이벤트",
            "event_empty": "표시할 자산 증가 이벤트가 없습니다",
            "event_cards": "장",
            "note": "현재 보유 카드 FMV를 획득 시점 순서로 누적해 표시합니다.",
        }
    return {
        "title": "Holdings Timeline",
        "subtitle": "Cumulative current FMV by acquired time",
        "total_current": "Current Holdings Value",
        "total_change": "Period Growth",
        "holdings_count": "Holdings Count",
        "timeline_title": "Asset Accumulation Curve",
        "timeline_empty": "Not enough data to generate timeline",
        "timeline_min": "Low",
        "timeline_max": "High",
        "event_title": "Recent Asset Increase Events",
        "event_empty": "No asset increase events available",
        "event_cards": "cards",
        "note": "Calculated from current holdings FMV and accumulated by acquired time.",
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


def _upsert_activity_count_row(
    rows: list[dict],
    *,
    row_types: tuple[str, ...],
    count: int,
    lang: str,
    highlight: bool = False,
) -> list[dict]:
    if not isinstance(rows, list):
        rows = []
    normalized_types = tuple(str(t or "").strip() for t in row_types if str(t or "").strip())
    if not normalized_types:
        return list(rows)

    out: list[dict] = []
    replaced = False
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_type = str(row.get("type") or "").strip()
        if row_type in normalized_types:
            if replaced:
                continue
            if count > 0:
                new_row = dict(row)
                new_row["count"] = int(count)
                new_row["highlight"] = bool(highlight)
                new_row["name"] = _profile_activity_display_name(normalized_types[0], lang)
                new_row["type"] = normalized_types[0]
                out.append(new_row)
            replaced = True
            continue
        out.append(row)

    if not replaced and count > 0:
        out.append(
            {
                "name": _profile_activity_display_name(normalized_types[0], lang),
                "count": int(count),
                "type": normalized_types[0],
                "highlight": bool(highlight),
            }
        )
    return out


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
    cached_disk_value = _load_fmv_from_disk_cache(cache_key)
    if cached_disk_value is not None:
        if PROFILE_ENABLE_RUNTIME_CACHE:
            _CARD_FMV_CACHE[cache_key] = cached_disk_value
        return cached_disk_value

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
    _save_fmv_to_disk_cache(cache_key, value)
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


def _normalize_pack_label_key(name: str | None) -> str:
    return re.sub(r"\s+", " ", str(name or "").strip().lower())


def _default_pack_price_map() -> dict:
    return {
        "version": 1,
        "updated_at": "",
        "by_contract": {},
        "by_pack_name": {},
    }


def _load_pack_price_map() -> dict:
    path = PACK_PRICE_MAP_PATH
    default_data = _default_pack_price_map()
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return default_data

    if (
        _PACK_PRICE_MAP_CACHE.get("path") == path
        and _PACK_PRICE_MAP_CACHE.get("mtime") == mtime
        and isinstance(_PACK_PRICE_MAP_CACHE.get("data"), dict)
    ):
        data_cached = _PACK_PRICE_MAP_CACHE.get("data") or {}
        if isinstance(data_cached.get("by_contract"), dict) and isinstance(data_cached.get("by_pack_name"), dict):
            return data_cached  # type: ignore[return-value]
        return default_data

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return default_data
    if not isinstance(data, dict):
        return default_data
    if not isinstance(data.get("by_contract"), dict):
        data["by_contract"] = {}
    if not isinstance(data.get("by_pack_name"), dict):
        data["by_pack_name"] = {}
    _PACK_PRICE_MAP_CACHE["path"] = path
    _PACK_PRICE_MAP_CACHE["mtime"] = mtime
    _PACK_PRICE_MAP_CACHE["data"] = data
    return data


def _save_pack_price_map(pack_map: dict) -> None:
    if not isinstance(pack_map, dict):
        return
    by_contract = pack_map.get("by_contract") if isinstance(pack_map.get("by_contract"), dict) else {}
    by_pack_name = pack_map.get("by_pack_name") if isinstance(pack_map.get("by_pack_name"), dict) else {}
    payload = {
        "version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "by_contract": by_contract,
        "by_pack_name": by_pack_name,
    }
    path = PACK_PRICE_MAP_PATH
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
        _PACK_PRICE_MAP_CACHE["path"] = path
        _PACK_PRICE_MAP_CACHE["mtime"] = os.path.getmtime(path)
        _PACK_PRICE_MAP_CACHE["data"] = payload
    except Exception:
        return


def _pack_price_entry_to_decimal(entry) -> Decimal:
    if isinstance(entry, dict):
        return _to_decimal(entry.get("unit_price"))
    return _to_decimal(entry)


def _lookup_pack_unit_price(pack_map: dict, contract_key: str | None = None, pack_name: str | None = None) -> Decimal:
    if not isinstance(pack_map, dict):
        return Decimal("0")
    by_contract = pack_map.get("by_contract") if isinstance(pack_map.get("by_contract"), dict) else {}
    by_pack_name = pack_map.get("by_pack_name") if isinstance(pack_map.get("by_pack_name"), dict) else {}

    key = str(contract_key or "").strip().lower()
    if key:
        v = _pack_price_entry_to_decimal(by_contract.get(key))
        if v > 0:
            return v

    name_key = _normalize_pack_label_key(pack_name)
    if name_key:
        v = _pack_price_entry_to_decimal(by_pack_name.get(name_key))
        if v > 0:
            return v
    return Decimal("0")


def _record_pack_unit_price(pack_map: dict, contract_key: str | None, pack_name: str | None, unit_price: Decimal) -> bool:
    if not isinstance(pack_map, dict):
        return False
    price = _to_decimal(unit_price)
    if price <= 0:
        return False
    if not isinstance(pack_map.get("by_contract"), dict):
        pack_map["by_contract"] = {}
    if not isinstance(pack_map.get("by_pack_name"), dict):
        pack_map["by_pack_name"] = {}
    by_contract = pack_map["by_contract"]
    by_pack_name = pack_map["by_pack_name"]
    now_iso = datetime.now(timezone.utc).isoformat()
    changed = False
    price_text = _format_usdt_decimal(price)

    key = str(contract_key or "").strip().lower()
    label = str(pack_name or "").strip()
    if key:
        prev = by_contract.get(key) if isinstance(by_contract.get(key), dict) else {}
        next_row = {
            "unit_price": price_text,
            "pack_name": label or str(prev.get("pack_name") or ""),
            "updated_at": now_iso,
        }
        if prev != next_row:
            by_contract[key] = next_row
            changed = True

    name_key = _normalize_pack_label_key(label)
    if name_key:
        prev = by_pack_name.get(name_key) if isinstance(by_pack_name.get(name_key), dict) else {}
        next_row = {
            "unit_price": price_text,
            "pack_name": label,
            "contract_key": key,
            "updated_at": now_iso,
        }
        if prev != next_row:
            by_pack_name[name_key] = next_row
            changed = True
    return changed


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
    legacy_pull_by_checkout: dict[str, dict] = {}
    buyback_by_checkout: defaultdict[str, list[dict]] = defaultdict(list)
    seen_withdraw_events: set[str] = set()
    withdraw_token_ids: set[str] = set()
    release_cards_by_token: dict[str, dict] = {}
    token_latest_values: dict[str, tuple[int, Decimal]] = {}
    token_buy_cost_by_ts: dict[str, tuple[int, Decimal]] = {}
    token_pull_cost_by_ts: dict[str, tuple[int, Decimal]] = {}
    pack_price_map = _load_pack_price_map()
    pack_price_map_dirty = False

    def _remember_token_value(token_id: str, value: Decimal, ts_value: int):
        tid = str(token_id or "").strip()
        if not tid or value <= 0:
            return
        ts_norm = int(_parse_int(ts_value) or 0)
        prev = token_latest_values.get(tid)
        if prev is None or ts_norm >= prev[0]:
            token_latest_values[tid] = (ts_norm, value)

    def _remember_token_cost(cost_map: dict[str, tuple[int, Decimal]], token_id: str, value: Decimal, ts_value: int):
        tid = str(token_id or "").strip()
        if not tid or value <= 0:
            return
        ts_norm = int(_parse_int(ts_value) or 0)
        prev = cost_map.get(tid)
        if prev is None or ts_norm >= prev[0]:
            cost_map[tid] = (ts_norm, value)

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
                    release_contract = str(row.get("contractAddress") or "").strip().lower()
                    release_checkout_id = str(row.get("checkoutId") or "").strip()
                    release_cards_by_token[token_hint] = {
                        "token_id": token_hint,
                        "name": str(row_item.get("collectibleName") or row_item.get("title") or "Unknown Collectible").strip(),
                        "market_image": str(row_item.get("imageUrl") or "").strip(),
                        "preview_image": str(row_item.get("collectibleImageUrl") or "").strip(),
                        "image": str(row_item.get("imageUrl") or row_item.get("collectibleImageUrl") or "").strip(),
                        "timestamp_raw": ts,
                        "pack_contract": release_contract,
                        "checkout_id": release_checkout_id,
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
            if token_hint and price > 0:
                _remember_token_cost(token_pull_cost_by_ts, token_hint, price, ts)
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
            legacy_pull_by_checkout[legacy_event_key] = {
                "legacy_key": legacy_key,
                "pack_name": pack_label,
                "checkout_id": checkout_id,
                "token_id": token_hint,
                "market_image": str(row_item.get("imageUrl") or "").strip(),
                "preview_image": str(row_item.get("collectibleImageUrl") or "").strip(),
                "image": str(row_item.get("imageUrl") or row_item.get("collectibleImageUrl") or "").strip(),
                "timestamp_raw": _parse_int(row.get("timestamp")) or 0,
                "event_key": legacy_event_key,
            }
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
                if token_hint:
                    _remember_token_cost(token_pull_cost_by_ts, token_hint, price, ts)
            else:
                legacy_missing_price_keys.append(legacy_key)
        elif row_type in ("PerpetualBuybackActivity", "BuybackActivity"):
            checkout_id = str(row.get("checkoutId") or "").strip()
            buyback_price = _wei_to_usdt(row.get("priceInUsdt"))
            if buyback_price <= 0:
                buyback_price = _wei_to_usdt(row.get("amount"))
            if buyback_price > 0:
                buyback_earned_total += buyback_price
            fmv_hint = _card_price_to_usd(row.get("fmvPriceInUsd"))
            _remember_token_value(token_hint, fmv_hint if fmv_hint > 0 else buyback_price, ts)
            if checkout_id:
                item_obj = row.get("item") if isinstance(row.get("item"), dict) else {}
                buyback_by_checkout[checkout_id].append(
                    {
                        "token_id": token_hint or str(item_obj.get("tokenId") or "").strip(),
                        "name": str(
                            item_obj.get("collectibleName")
                            or item_obj.get("title")
                            or item_obj.get("name")
                            or "Unknown Collectible"
                        ).strip(),
                        "market_image": str(item_obj.get("imageUrl") or "").strip(),
                        "preview_image": str(item_obj.get("collectibleImageUrl") or "").strip(),
                        "image": str(item_obj.get("imageUrl") or item_obj.get("collectibleImageUrl") or "").strip(),
                        "value": fmv_hint if fmv_hint > 0 else buyback_price,
                        "timestamp_raw": ts,
                    }
                )
        elif row_type in ("BuyActivity", "SellActivity"):
            amount = _wei_to_usdt(row.get("amount"))
            if amount <= 0:
                continue
            bidder = str(row.get("bidder") or "").strip().lower()
            asker = str(row.get("asker") or "").strip().lower()
            if bidder == wallet_norm:
                trade_spent_total += amount
                if token_hint:
                    _remember_token_cost(token_buy_cost_by_ts, token_hint, amount, ts)
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
            if inferred <= 0:
                row_item = row.get("item") if isinstance(row.get("item"), dict) else {}
                mapped_pack_name = contract_pack_name.get(contract) or _pack_label_from_pull_item(row_item)
                inferred = _lookup_pack_unit_price(
                    pack_price_map,
                    contract_key=contract,
                    pack_name=mapped_pack_name,
                )
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
        if inferred <= 0:
            inferred = _lookup_pack_unit_price(
                pack_price_map,
                contract_key=legacy_key,
                pack_name=contract_pack_name.get(legacy_key),
            )
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
        direct_unit_price = _pick_contract_unit_price(price_counter)
        pack_counter = contract_pack_counter.get(contract) or Counter()
        if pack_counter:
            pack_name = pack_counter.most_common(1)[0][0]
        else:
            pack_name = contract_pack_name.get(contract) or f"Contract {_short_hex(contract)}"
        unit_price = direct_unit_price if direct_unit_price > 0 else _lookup_pack_unit_price(
            pack_price_map,
            contract_key=contract,
            pack_name=pack_name,
        )
        if direct_unit_price > 0:
            pack_price_map_dirty = _record_pack_unit_price(
                pack_price_map,
                contract_key=contract,
                pack_name=pack_name,
                unit_price=direct_unit_price,
            ) or pack_price_map_dirty
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
    if pack_price_map_dirty:
        _save_pack_price_map(pack_price_map)

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
        now_ts = int(datetime.now(timezone.utc).timestamp())
        active_days_count = max(1, int((max(now_ts, ts_min) - ts_min) // 86400) + 1)

    legacy_release_cards: list[dict] = []
    for pull in legacy_pull_by_checkout.values():
        if not isinstance(pull, dict):
            continue
        checkout_id = str(pull.get("checkout_id") or "").strip()
        legacy_key = str(pull.get("legacy_key") or "").strip().lower()
        if not legacy_key:
            continue
        candidates = buyback_by_checkout.get(checkout_id) or []
        best = None
        if candidates:
            best = max(candidates, key=lambda x: int(_parse_int(x.get("timestamp_raw")) or 0))
        token_id = str((best or {}).get("token_id") or pull.get("token_id") or "").strip()
        if not token_id:
            token_id = f"{legacy_key}:{checkout_id or str(pull.get('event_key') or '')}"
        value = _to_decimal((best or {}).get("value"))
        ts_raw = int(_parse_int(pull.get("timestamp_raw")) or 0)
        if value > 0:
            prev = token_latest_values.get(token_id)
            if prev is None or ts_raw >= prev[0]:
                token_latest_values[token_id] = (ts_raw, value)
        pull_market_image = str(pull.get("market_image") or "").strip()
        pull_preview_image = str(pull.get("preview_image") or "").strip()
        market_image = str((best or {}).get("market_image") or pull_market_image).strip()
        preview_image = str((best or {}).get("preview_image") or pull_preview_image).strip()
        image = str((best or {}).get("image") or market_image or preview_image).strip()
        legacy_release_cards.append(
            {
                "token_id": token_id,
                "name": str((best or {}).get("name") or pull.get("pack_name") or "Unknown Collectible"),
                "market_image": market_image,
                "preview_image": preview_image,
                "image": image,
                "timestamp_raw": ts_raw,
                "pack_contract": legacy_key,
                "checkout_id": checkout_id,
            }
        )

    release_cards_source = [v for v in release_cards_by_token.values() if isinstance(v, dict)] + legacy_release_cards
    release_cards = sorted(
        release_cards_source,
        key=lambda x: _parse_int(x.get("timestamp_raw")),
        reverse=True,
    )
    pack_release_counts: defaultdict[str, int] = defaultdict(int)
    pack_latest_ts: defaultdict[str, int] = defaultdict(int)
    for card in release_cards:
        ts_raw = _parse_int(card.get("timestamp_raw")) or 0
        card["timestamp"] = ts_raw
        card.pop("timestamp_raw", None)
        contract = str(card.get("pack_contract") or "").strip().lower()
        if contract:
            pack_release_counts[contract] += 1
            if ts_raw > int(pack_latest_ts.get(contract, 0)):
                pack_latest_ts[contract] = ts_raw
            pack_name = (
                contract_pack_name.get(contract)
                or f"Contract {_short_hex(contract)}"
            )
            card["pack_name"] = pack_name
        else:
            card["pack_name"] = "Unknown Pack"

    pack_options = []
    for contract, count in pack_release_counts.items():
        if not contract or count <= 0:
            continue
        pack_name = contract_pack_name.get(contract) or f"Contract {_short_hex(contract)}"
        pack_options.append(
            {
                "contract": contract,
                "pack_name": pack_name,
                "release_count": int(count),
                "latest_ts": int(pack_latest_ts.get(contract, 0)),
            }
        )
    pack_options.sort(
        key=lambda x: (
            int(x.get("latest_ts") or 0),
            int(x.get("release_count") or 0),
            str(x.get("pack_name") or ""),
        ),
        reverse=True,
    )

    pack_spent_map: dict[str, str] = {}
    for key, value in contract_spent_total.items():
        norm_key = str(key or "").strip().lower()
        if not norm_key:
            continue
        pack_spent_map[norm_key] = _format_usdt_decimal(_to_decimal(value))

    for card in release_cards_source:
        if not isinstance(card, dict):
            continue
        token_id = str(card.get("token_id") or "").strip()
        checkout_id = str(card.get("checkout_id") or "").strip()
        ts_raw = int(_parse_int(card.get("timestamp_raw")) or 0)
        if not token_id or not checkout_id:
            continue
        pull_price = _to_decimal(pull_price_by_checkout.get(checkout_id))
        if pull_price > 0:
            _remember_token_cost(token_pull_cost_by_ts, token_id, pull_price, ts_raw)

    token_acquire_cost_hints: dict[str, dict[str, object]] = {}
    for token_id, row in token_pull_cost_by_ts.items():
        token_acquire_cost_hints[token_id] = {
            "cost": _to_decimal(row[1]),
            "timestamp": int(row[0]),
            "source": "pull",
        }
    for token_id, row in token_buy_cost_by_ts.items():
        prev = token_acquire_cost_hints.get(token_id)
        ts_raw = int(row[0])
        if prev is None or ts_raw >= int(_parse_int(prev.get("timestamp")) or 0):
            token_acquire_cost_hints[token_id] = {
                "cost": _to_decimal(row[1]),
                "timestamp": ts_raw,
                "source": "buy",
            }

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
        "pack_options": pack_options,
        "pack_spent_map": pack_spent_map,
        "token_value_hints": token_value_hints,
        "token_acquire_cost_hints": token_acquire_cost_hints,
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


def _build_wallet_holdings_growth_template_context(
    history_data: dict,
    parsed_sorted: list[dict],
    profile_name: str,
    short_wallet: str,
    profile_lang: str = "en",
) -> dict:
    lang = _profile_lang_from_locale(profile_lang)
    labels = _profile_holdings_labels(lang)
    ui_labels = _profile_ui_labels(lang)
    token_cost_hints = (history_data or {}).get("token_acquire_cost_hints") or {}
    if not isinstance(token_cost_hints, dict):
        token_cost_hints = {}
    release_rows = (history_data or {}).get("release_cards") or []
    token_acquire_ts: dict[str, int] = {}

    for token_id, info in token_cost_hints.items():
        tid = str(token_id or "").strip()
        if not tid or not isinstance(info, dict):
            continue
        ts = int(_parse_int(info.get("timestamp")) or 0)
        if ts > 0 and (tid not in token_acquire_ts or ts < token_acquire_ts[tid]):
            token_acquire_ts[tid] = ts

    for row in release_rows:
        if not isinstance(row, dict):
            continue
        tid = str(row.get("token_id") or "").strip()
        if not tid:
            continue
        ts = int(_parse_int(row.get("timestamp")) or _parse_int(row.get("timestamp_raw")) or 0)
        if ts > 0 and (tid not in token_acquire_ts or ts < token_acquire_ts[tid]):
            token_acquire_ts[tid] = ts

    holdings_rows: list[dict] = []
    now_ts = int(time.time())
    for row in parsed_sorted:
        if not isinstance(row, dict):
            continue
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        token_id = str(raw.get("tokenId") or "").strip()
        if not token_id:
            continue
        current_value = _to_decimal(row.get("fmv")) / Decimal("100")
        acquire_ts = int(token_acquire_ts.get(token_id) or 0)
        if acquire_ts <= 0:
            acquire_ts = int(
                _parse_int(raw.get("mintDate"))
                or _parse_int(raw.get("mintedAt"))
                or _parse_int(raw.get("createdAt"))
                or _parse_int(raw.get("timestamp"))
                or now_ts
            )
        acquire_date = datetime.utcfromtimestamp(acquire_ts).date()
        holdings_rows.append(
            {
                "token_id": token_id,
                "raw": raw,
                "name": str(raw.get("name") or "Unknown Collectible"),
                "image": _prepare_collectible_image_for_poster(raw.get("frontImageUrl") or ""),
                "current_value_raw": current_value,
                "acquire_ts": acquire_ts,
                "acquire_date": acquire_date,
            }
        )

    holdings_rows.sort(key=lambda x: (int(x.get("acquire_ts") or 0), str(x.get("token_id") or "")))
    holdings_count = len(holdings_rows)

    cumulative_points: list[dict] = []
    total_current = Decimal("0")
    rising_count = holdings_count
    baseline_total_for_pct = Decimal("0")

    if PROFILE_ENABLE_SNKR_TIMELINE and holdings_rows:
        jpy_rate = _to_decimal(market_report_vision.get_exchange_rate())
        if jpy_rate <= 0:
            jpy_rate = Decimal("150")
        start_day = min((x.get("acquire_date") for x in holdings_rows if isinstance(x.get("acquire_date"), date)), default=None)
        required_start_date = None
        if isinstance(start_day, date):
            required_start_date = start_day - timedelta(days=PROFILE_SNKR_WINDOW_DAYS)

        spec_cache: dict[str, tuple[dict, list[dict]]] = {}
        for row in holdings_rows:
            raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
            spec = _build_snkr_search_spec_from_collectible(raw)
            spec_key = json.dumps(spec, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            if spec_key not in spec_cache:
                spec_cache[spec_key] = (spec, [])
            row["spec_key"] = spec_key
            row["target_grade"] = str(spec.get("target_grade") or "Unknown")

        keys = list(spec_cache.keys())
        if keys:
            workers = min(8, len(keys))
            if workers > 1:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    future_map = {
                        pool.submit(
                            _fetch_snkr_records_by_spec,
                            dict(spec_cache[k][0]),
                            required_start_date,
                        ): k
                        for k in keys
                    }
                    for future in as_completed(future_map):
                        key = future_map[future]
                        try:
                            records = future.result()
                        except Exception:
                            records = []
                        spec_cache[key] = (spec_cache[key][0], [x for x in records if isinstance(x, dict)])
            else:
                for key in keys:
                    records = _fetch_snkr_records_by_spec(dict(spec_cache[key][0]), required_start_date=required_start_date)
                    spec_cache[key] = (spec_cache[key][0], [x for x in records if isinstance(x, dict)])

        pc_records_cache: dict[str, list[dict]] = {}

        def _resolve_market_value_for_day(row_obj: dict, end_dt: datetime) -> Decimal:
            spec_key = str(row_obj.get("spec_key") or "")
            target_grade = str(row_obj.get("target_grade") or "Unknown")
            snkr_records = spec_cache.get(spec_key, ({}, []))[1] if spec_key else []
            v = _select_snkr_average_usd(snkr_records, target_grade=target_grade, end_at=end_dt, jpy_rate=jpy_rate)
            if v > 0:
                return v
            # SNKR has no usable price at this checkpoint -> fallback to PriceCharting history.
            pc_records = pc_records_cache.get(spec_key)
            if pc_records is None:
                spec_obj = dict(spec_cache.get(spec_key, ({}, []))[0] or {})
                fetched_pc = _fetch_pc_records_by_spec(spec_obj, required_start_date=required_start_date)
                pc_records = [x for x in fetched_pc if isinstance(x, dict)]
                pc_records_cache[spec_key] = pc_records
            v = _select_snkr_average_usd(pc_records, target_grade=target_grade, end_at=end_dt, jpy_rate=jpy_rate)
            if v > 0:
                return v
            return _to_decimal(row_obj.get("current_value_raw"))

        end_day = datetime.utcnow().date()
        if isinstance(start_day, date):
            baseline_total = Decimal("0")
            for row in holdings_rows:
                acquire_date = row.get("acquire_date")
                if not isinstance(acquire_date, date):
                    continue
                base_end_dt = datetime.combine(acquire_date, dt_time(23, 59, 59))
                base_val = _resolve_market_value_for_day(row, base_end_dt)
                row["base_value_raw"] = base_val
                row["last_checkpoint_value_raw"] = base_val
                row["ever_up"] = False
                baseline_total += base_val

            checkpoints: list[date] = []
            cursor = start_day
            step_days = max(1, PROFILE_SNKR_TIMELINE_STEP_DAYS)
            while cursor <= end_day:
                checkpoints.append(cursor)
                cursor = cursor + timedelta(days=step_days)
            if checkpoints[-1] != end_day:
                checkpoints.append(end_day)

            prev_total_val = Decimal("0")
            for day in checkpoints:
                end_dt = datetime.combine(day, dt_time(23, 59, 59))
                total_val = Decimal("0")
                active_count = 0
                for row in holdings_rows:
                    acquire_date = row.get("acquire_date")
                    if not isinstance(acquire_date, date) or acquire_date > day:
                        continue
                    active_count += 1
                    snkr_val = _resolve_market_value_for_day(row, end_dt)
                    prev_checkpoint_val = _to_decimal(row.get("last_checkpoint_value_raw"))
                    if snkr_val > prev_checkpoint_val:
                        row["ever_up"] = True
                    row["last_checkpoint_value_raw"] = snkr_val
                    total_val += snkr_val
                delta = total_val - prev_total_val
                cumulative_points.append(
                    {
                        "date": day.strftime("%Y-%m-%d"),
                        "delta": delta,
                        "count": active_count,
                        "cumulative": total_val,
                    }
                )
                prev_total_val = total_val

            end_dt = datetime.combine(end_day, dt_time(23, 59, 59))
            current_total = Decimal("0")
            rising_count = 0
            for row in holdings_rows:
                acquire_date = row.get("acquire_date")
                if not isinstance(acquire_date, date) or acquire_date > end_day:
                    continue
                snkr_val = _resolve_market_value_for_day(row, end_dt)
                current_total += snkr_val
                if bool(row.get("ever_up")):
                    rising_count += 1
            total_current = current_total
            baseline_total_for_pct = baseline_total

    if not cumulative_points:
        total_current = sum((_to_decimal(x.get("current_value_raw")) for x in holdings_rows), Decimal("0"))
        daily_events: dict[str, dict] = {}
        for row in holdings_rows:
            acquire_date = row.get("acquire_date")
            if not isinstance(acquire_date, date):
                continue
            d = acquire_date.strftime("%Y-%m-%d")
            slot = daily_events.get(d)
            if slot is None:
                slot = {"date": d, "delta": Decimal("0"), "count": 0}
                daily_events[d] = slot
            slot["delta"] = _to_decimal(slot.get("delta")) + _to_decimal(row.get("current_value_raw"))
            slot["count"] = int(slot.get("count") or 0) + 1

        ordered_days = sorted(daily_events.keys())
        running = Decimal("0")
        for day in ordered_days:
            slot = daily_events.get(day) or {}
            delta = _to_decimal(slot.get("delta"))
            running += delta
            cumulative_points.append(
                {
                    "date": day,
                    "delta": delta,
                    "count": int(slot.get("count") or 0),
                    "cumulative": running,
                }
            )
        today_key = datetime.utcnow().strftime("%Y-%m-%d")
        if cumulative_points and cumulative_points[-1]["date"] != today_key:
            cumulative_points.append(
                {
                    "date": today_key,
                    "delta": Decimal("0"),
                    "count": holdings_count,
                    "cumulative": running,
                }
            )
        if not cumulative_points:
            cumulative_points.append(
                {
                    "date": today_key,
                    "delta": Decimal("0"),
                    "count": 0,
                    "cumulative": Decimal("0"),
                }
            )

    values = [_to_decimal(x.get("cumulative")) for x in cumulative_points]
    val_min_raw = min(values) if values else Decimal("0")
    val_max_raw = max(values) if values else Decimal("0")
    spread = val_max_raw - val_min_raw
    pad = spread * Decimal("0.12")
    if spread <= 0:
        pad = Decimal("1")
    chart_min = val_min_raw - pad
    chart_max = val_max_raw + pad
    if chart_max <= chart_min:
        chart_max = chart_min + Decimal("1")

    view_width = Decimal("1040")
    view_height = Decimal("360")
    chart_left = Decimal("74")
    chart_top = Decimal("14")
    chart_width = Decimal("948")
    chart_height = Decimal("318")
    denom = chart_max - chart_min

    def _x_of(idx: int) -> Decimal:
        n = len(cumulative_points)
        if n <= 1:
            return chart_left
        return chart_left + (chart_width * Decimal(idx) / Decimal(n - 1))

    def _y_of(v: Decimal) -> Decimal:
        ratio = (_to_decimal(v) - chart_min) / denom
        return chart_top + (Decimal("1") - ratio) * chart_height

    svg_points: list[tuple[Decimal, Decimal]] = []
    for i, point in enumerate(cumulative_points):
        svg_points.append((_x_of(i), _y_of(_to_decimal(point.get("cumulative")))))

    if not svg_points:
        svg_points = [(chart_left, chart_top + chart_height)]

    def _build_smooth_path(points: list[tuple[Decimal, Decimal]]) -> str:
        if len(points) <= 2:
            return " ".join(
                [f"M {points[0][0]:.2f} {points[0][1]:.2f}"]
                + [f"L {x:.2f} {y:.2f}" for x, y in points[1:]]
            )
        path_parts = [f"M {points[0][0]:.2f} {points[0][1]:.2f}"]
        for i in range(len(points) - 1):
            p0 = points[i - 1] if i > 0 else points[i]
            p1 = points[i]
            p2 = points[i + 1]
            p3 = points[i + 2] if (i + 2) < len(points) else p2
            c1x = p1[0] + (p2[0] - p0[0]) / Decimal("6")
            c1y = p1[1] + (p2[1] - p0[1]) / Decimal("6")
            c2x = p2[0] - (p3[0] - p1[0]) / Decimal("6")
            c2y = p2[1] - (p3[1] - p1[1]) / Decimal("6")
            path_parts.append(
                f"C {c1x:.2f} {c1y:.2f}, {c2x:.2f} {c2y:.2f}, {p2[0]:.2f} {p2[1]:.2f}"
            )
        return " ".join(path_parts)

    line_path = _build_smooth_path(svg_points)
    baseline_y = chart_top + chart_height
    area_path = (
        f"{line_path} "
        f"L {svg_points[-1][0]:.2f} {baseline_y:.2f} "
        f"L {svg_points[0][0]:.2f} {baseline_y:.2f} Z"
    )
    latest_x, latest_y = svg_points[-1]

    axis_labels = []
    seen_idx: set[int] = set()
    n_points = len(cumulative_points)
    tick_count = max(6, min(10, n_points))
    if n_points <= 1:
        tick_indices = [0]
    else:
        tick_indices = sorted(
            {
                max(0, min(n_points - 1, int(round(i * (n_points - 1) / max(1, tick_count - 1)))))
                for i in range(tick_count)
            }
        )

    start_date_obj = None
    end_date_obj = None
    try:
        start_date_obj = datetime.strptime(str(cumulative_points[0].get("date") or ""), "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(str(cumulative_points[-1].get("date") or ""), "%Y-%m-%d").date()
    except Exception:
        start_date_obj = None
        end_date_obj = None
    total_days = (end_date_obj - start_date_obj).days if (start_date_obj and end_date_obj) else 0
    if total_days >= 365:
        label_fmt = "%Y/%m"
    else:
        label_fmt = "%m/%d"

    for idx in tick_indices:
        if idx in seen_idx:
            continue
        seen_idx.add(idx)
        point = cumulative_points[idx]
        raw_day = str(point.get("date") or "")
        label_text = raw_day[5:] if raw_day else "-"
        try:
            day_obj = datetime.strptime(raw_day, "%Y-%m-%d").date()
            if idx == 0 or idx == (n_points - 1):
                label_text = day_obj.strftime("%Y/%m/%d")
            else:
                label_text = day_obj.strftime(label_fmt)
        except Exception:
            pass
        axis_labels.append(
            {
                "left_pct": f"{(float(_x_of(idx) / view_width) * 100):.2f}%",
                "label": label_text,
                "row": len(axis_labels) % 2,
            }
        )

    y_grid_lines = []
    y_steps = 6
    for i in range(y_steps):
        ratio = Decimal(i) / Decimal(max(1, y_steps - 1))
        val = chart_max - ((chart_max - chart_min) * ratio)
        y = _y_of(_to_decimal(val))
        y_grid_lines.append(
            {
                "y": f"{y:.2f}",
                "label": _format_usdt_decimal(val),
            }
        )

    first_val = _to_decimal(values[0] if values else Decimal("0"))
    last_val = _to_decimal(values[-1] if values else Decimal("0"))
    total_change = last_val - first_val
    if PROFILE_ENABLE_SNKR_TIMELINE and baseline_total_for_pct > 0:
        total_change = last_val - baseline_total_for_pct
        total_change_pct = (total_change / baseline_total_for_pct) * Decimal("100")
    else:
        total_change_pct = (total_change / first_val * Decimal("100")) if first_val > 0 else Decimal("0")

    if PROFILE_ENABLE_SNKR_TIMELINE:
        if lang == "zh":
            count_label = "價格上漲張數"
        elif lang == "zhs":
            count_label = "价格上涨张数"
        elif lang == "ko":
            count_label = "가격 상승 카드 수"
        else:
            count_label = "Rising Cards"
        count_value = _format_number(rising_count)
    else:
        count_label = labels.get("holdings_count", "Holdings Count")
        count_value = _format_number(holdings_count)

    return {
        "collection_name": f"{profile_name} Collection",
        "brand_name": ui_labels.get("brand_name", "Renaiss"),
        "brand_site": ui_labels.get("brand_site", "renaiss.xyz"),
        "wallet_short": short_wallet,
        "update_date": datetime.now().strftime("%Y-%m-%d"),
        "title": labels.get("title", "Holdings Timeline"),
        "subtitle": labels.get("subtitle", "Cumulative current FMV by acquired time"),
        "history_range": str((history_data or {}).get("history_range") or "-"),
        "total_current_label": labels.get("total_current", "Current Holdings Value"),
        "total_current_value": _format_usdt_decimal(total_current),
        "total_change_label": labels.get("total_change", "Period Growth"),
        "total_change_value": _format_usdt_currency(total_change, signed=True),
        "total_change_pct": _format_pct_decimal(total_change_pct, signed=True, digits=2),
        "holdings_count_label": count_label,
        "holdings_count_value": count_value,
        "timeline_title": labels.get("timeline_title", "Asset Accumulation Curve"),
        "timeline_empty": labels.get("timeline_empty", "Not enough data to generate timeline"),
        "view_width": f"{view_width:.0f}",
        "view_height": f"{view_height:.0f}",
        "chart_left": f"{chart_left:.2f}",
        "chart_right": f"{(chart_left + chart_width):.2f}",
        "chart_line_path": line_path,
        "chart_area_path": area_path,
        "chart_latest_x": f"{latest_x:.2f}",
        "chart_latest_y": f"{latest_y:.2f}",
        "axis_labels": axis_labels,
        "y_grid_lines": y_grid_lines,
        "calc_note": (
            f"SNKR {PROFILE_SNKR_WINDOW_DAYS}D Avg · 每 {PROFILE_SNKR_TIMELINE_STEP_DAYS} 天估值一次"
            if PROFILE_ENABLE_SNKR_TIMELINE
            else labels.get("note", "")
        ),
    }


def _resolve_flex_pack_layout_count(requested: int, available: int) -> int:
    req = _clamp_flex_pack_card_count(requested)
    if available <= 0:
        return 0
    if available >= req:
        return req
    for candidate in (10, 7, 4, 3, 2, 1):
        if candidate <= available and candidate <= req:
            return candidate
    for candidate in (10, 7, 4, 3, 2, 1):
        if candidate <= available:
            return candidate
    return 1


def _build_wallet_flex_pack_picker_data(wallet_address: str, profile_lang: str = "en") -> dict:
    lang = _profile_lang_from_locale(profile_lang)
    wallet_norm = _normalize_wallet_address(wallet_address) or str(wallet_address or "").strip().lower()
    wallet_short = f"{wallet_norm[:6]}...{wallet_norm[-4:]}" if wallet_norm and len(wallet_norm) >= 10 else wallet_norm
    try:
        user_id, username = _resolve_user_from_wallet(wallet_address)
    except Exception:
        user_id, username = None, None
    if not username:
        username = _username_from_rankings_wallet(wallet_norm)

    history_data = _build_wallet_activity_history(wallet_address, profile_lang=lang)
    release_cards = [x for x in (history_data.get("release_cards") or []) if isinstance(x, dict)]
    pack_options = [x for x in (history_data.get("pack_options") or []) if isinstance(x, dict)]

    pack_cards_map: defaultdict[str, list[dict]] = defaultdict(list)
    for card in release_cards:
        contract = str(card.get("pack_contract") or "").strip().lower()
        if not contract:
            continue
        pack_cards_map[contract].append(card)
    for contract in list(pack_cards_map.keys()):
        pack_cards_map[contract].sort(key=lambda x: int(_parse_int(x.get("timestamp")) or 0), reverse=True)

    if not pack_options:
        for contract, rows in pack_cards_map.items():
            if not rows:
                continue
            pack_options.append(
                {
                    "contract": contract,
                    "pack_name": str(rows[0].get("pack_name") or f"Contract {_short_hex(contract)}"),
                    "release_count": len(rows),
                }
            )
        pack_options.sort(key=lambda x: (int(x.get("release_count") or 0), str(x.get("pack_name") or "")), reverse=True)

    pack_options_ui = []
    for row in pack_options[:25]:
        contract = str(row.get("contract") or "").strip().lower()
        if not contract:
            continue
        pack_name = str(row.get("pack_name") or f"Contract {_short_hex(contract)}").strip()
        release_count = int(_parse_int(row.get("release_count")) or len(pack_cards_map.get(contract) or []))
        label = f"{pack_name} · {release_count}"
        pack_options_ui.append(
            {
                "value": contract,
                "label": label[:100],
                "full_label": label[:220],
                "description": _short_hex(contract)[:100],
            }
        )

    ui_labels = _profile_ui_labels(lang)
    return {
        "wallet": wallet_norm,
        "wallet_short": wallet_short,
        "username": str(username or wallet_short or "Unknown User"),
        "user_id": user_id,
        "history_data": history_data,
        "pack_cards_map": dict(pack_cards_map),
        "pack_options": pack_options_ui,
        "brand_name": ui_labels.get("brand_name", "Renaiss"),
        "brand_site": ui_labels.get("brand_site", "renaiss.xyz"),
    }


def _build_wallet_flex_pack_template_context(
    wallet_address: str,
    picker_data: dict,
    *,
    selected_pack_contract: str,
    mode: str,
    card_count: int,
    profile_lang: str = "en",
) -> dict:
    lang = _profile_lang_from_locale(profile_lang)
    pack_contract = str(selected_pack_contract or "").strip().lower()
    mode_name = "extreme" if str(mode or "").strip().lower() == "extreme" else "picked"
    requested_count = _clamp_flex_pack_card_count(card_count)

    history_data = (picker_data or {}).get("history_data") if isinstance(picker_data, dict) else {}
    if not isinstance(history_data, dict):
        history_data = {}
    pack_cards_map = (picker_data or {}).get("pack_cards_map") if isinstance(picker_data, dict) else {}
    if not isinstance(pack_cards_map, dict):
        pack_cards_map = {}
    pack_cards = [x for x in (pack_cards_map.get(pack_contract) or []) if isinstance(x, dict)]
    if not pack_cards:
        raise RuntimeError("此卡包目前沒有可用的抽卡紀錄。")

    profile_name = str((picker_data or {}).get("username") or "Unknown User")
    token_value_hints = (history_data or {}).get("token_value_hints") or {}

    current_cards_by_token: dict[str, dict] = {}
    user_id = str((picker_data or {}).get("user_id") or "").strip()
    if user_id:
        try:
            collection = _fetch_user_collection(user_id)
        except Exception:
            collection = []
        for item in collection:
            token_id = str((item or {}).get("tokenId") or "").strip()
            if not token_id:
                continue
            current_cards_by_token[token_id] = {
                "token_id": token_id,
                "name": str((item or {}).get("name") or "Unknown Collectible"),
                "image": str((item or {}).get("frontImageUrl") or ""),
                "value": _to_decimal(_parse_int((item or {}).get("fmvPriceInUSD"))) / Decimal("100"),
            }

    def _resolve_card_candidate(
        card: dict,
        allow_trade_fallback: bool = True,
        fetch_missing: bool = True,
        prepare_image: bool = True,
    ) -> dict:
        token_id = str(card.get("token_id") or "").strip()
        current_info = current_cards_by_token.get(token_id) or {}
        value = _to_decimal(token_value_hints.get(token_id))
        if value <= 0:
            value = _to_decimal(current_info.get("value"))
        if value <= 0 and token_id and fetch_missing:
            value = _to_decimal(_fetch_card_fmv_by_token_id(token_id, allow_trade_fallback=allow_trade_fallback))

        # flex_pack poster uses historical transaction images only.
        market_image = str(card.get("market_image") or "").strip()
        activity_image = str(card.get("image") or "").strip()
        preview_image = str(card.get("preview_image") or "").strip()
        image = market_image or activity_image
        used_token_image_fallback = False
        if image and _looks_like_pack_image_url(image) and token_id:
            token_image = _fetch_card_image_by_token_id(token_id)
            if token_image:
                image = token_image
                used_token_image_fallback = True
        if not image and FLEX_PACK_ALLOW_PREVIEW_IMAGE_FALLBACK:
            image = preview_image
        if not image and token_id:
            token_image = _fetch_card_image_by_token_id(token_id)
            if token_image:
                image = token_image
                used_token_image_fallback = True

        prepared = _prepare_collectible_image_for_poster(image) if prepare_image else image

        return {
            "token_id": token_id,
            "name": str(card.get("name") or current_info.get("name") or "Unknown Collectible"),
            "value": value,
            "image": prepared,
            "used_token_image_fallback": used_token_image_fallback,
        }

    ui_labels = _profile_ui_labels(lang)
    pack_name = str(pack_cards[0].get("pack_name") or f"Contract {_short_hex(pack_contract)}")

    def _pack_title_short(name: str) -> str:
        text = str(name or "").strip()
        if "|" in text:
            text = text.split("|", 1)[0].strip()
        return text or str(name or "").strip()

    pack_title = _pack_title_short(pack_name)
    wallet_norm = _normalize_wallet_address(wallet_address) or str(wallet_address or "").strip().lower()
    wallet_short = f"{wallet_norm[:6]}...{wallet_norm[-4:]}" if wallet_norm and len(wallet_norm) >= 10 else wallet_norm
    subtitle = f"{pack_name} · {wallet_short}"

    def _parse_usdt_text(raw_value) -> Decimal:
        text = str(raw_value or "").strip().replace(",", "")
        try:
            return _to_decimal(text)
        except Exception:
            return Decimal("0")

    pack_spent_total = Decimal("0")
    pack_spent_map = history_data.get("pack_spent_map") if isinstance(history_data, dict) else {}
    if isinstance(pack_spent_map, dict):
        mapped_spent = pack_spent_map.get(pack_contract)
        if mapped_spent is not None:
            pack_spent_total = _to_decimal(mapped_spent)

    for row in (history_data.get("contract_rows") or []):
        if not isinstance(row, dict):
            continue
        contract = str(row.get("contract") or "").strip().lower()
        if contract != pack_contract:
            continue
        pack_spent_total = _parse_usdt_text(row.get("spent_total"))
        break

    unique_cards: dict[str, dict] = {}
    for card in pack_cards:
        token_id = str(card.get("token_id") or "").strip()
        if not token_id or token_id in unique_cards:
            continue
        unique_cards[token_id] = card

    resolved_all = [
        _resolve_card_candidate(card, allow_trade_fallback=False, fetch_missing=False, prepare_image=False)
        for card in unique_cards.values()
    ]
    unresolved = [x for x in resolved_all if _to_decimal(x.get("value")) <= 0 and str(x.get("token_id") or "").strip()]
    # In picked mode, resolve all unresolved token values for accurate whole-pack PnL.
    # In extreme mode, keep optional lookup cap for speed.
    lookup_limit = FLEX_PACK_EXTREME_TOKEN_LOOKUP_LIMIT if mode_name == "extreme" else 0
    if lookup_limit > 0 and len(unresolved) > lookup_limit:
        unresolved = unresolved[:lookup_limit]
    if unresolved:
        workers = min(8, len(unresolved))
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                future_map = {
                    pool.submit(_fetch_card_fmv_by_token_id, str(row.get("token_id") or ""), False): row
                    for row in unresolved
                }
                for future in as_completed(future_map):
                    row = future_map[future]
                    try:
                        v = _to_decimal(future.result())
                    except Exception:
                        v = Decimal("0")
                    if v > 0:
                        row["value"] = v
        else:
            for row in unresolved:
                tid = str(row.get("token_id") or "")
                v = _to_decimal(_fetch_card_fmv_by_token_id(tid, allow_trade_fallback=False))
                if v > 0:
                    row["value"] = v

    value_map = {str(x.get("token_id") or ""): _to_decimal(x.get("value")) for x in resolved_all}
    total_pack_card_value = sum((_to_decimal(x.get("value")) for x in resolved_all if _to_decimal(x.get("value")) > 0), Decimal("0"))
    pack_pnl_total = total_pack_card_value - pack_spent_total
    pnl_tone = "gain" if pack_pnl_total > 0 else "loss" if pack_pnl_total < 0 else "neutral"

    if mode_name == "picked":
        selected_count = _resolve_flex_pack_layout_count(requested_count, len(pack_cards))
        picked_rows = pack_cards[:selected_count]
        resolved = []
        for card in picked_rows:
            item = _resolve_card_candidate(card, allow_trade_fallback=True, fetch_missing=False, prepare_image=True)
            token_id = str(item.get("token_id") or "")
            mapped = _to_decimal(value_map.get(token_id))
            if mapped > 0:
                item["value"] = mapped
            resolved.append(item)

        items = [
            {
                "name": str(x.get("name") or "Unknown Collectible"),
                "image": str(x.get("image") or _TRANSPARENT_CARD_IMAGE),
                "value": _format_usdt_decimal(x.get("value")),
                "price_up": bool(x.get("used_token_image_fallback")),
            }
            for x in resolved
        ]
        template_context = {
            "collection_name": pack_title,
            "sbt_total": len(pack_cards),
            "extreme_mode": False,
            "hide_footer": False,
            "sbt_badges_display": [],
            "items": items,
            "assets_count": len(unique_cards),
            "total_value": _format_usdt_decimal(pack_pnl_total, signed=True),
            "total_value_label": "Pack PnL",
            "items_count_label": "Cards Pulled",
            "assets_unit": "Cards",
            "sbt_badges_label": "Pack Name",
            "no_sbt_label": "No data",
            "owned_prefix": ui_labels.get("owned_prefix", "owned "),
            "brand_name": str((picker_data or {}).get("brand_name") or ui_labels.get("brand_name", "Renaiss")),
            "brand_site": str((picker_data or {}).get("brand_site") or ui_labels.get("brand_site", "renaiss.xyz")),
            "update_date": datetime.now().strftime("%Y-%m-%d"),
            "enable_tilt": False,
            "background_key": "classic",
            "background_image": _profile_background_data_uri("classic"),
            "meta_counter_label": "PULL",
            "pack_name_display": pack_title,
            "pnl_tone": pnl_tone,
            "_meta_subtitle": subtitle,
        }
        return {
            "template_context": template_context,
            "mode": mode_name,
            "pack_name": pack_name,
            "requested_count": requested_count,
            "selected_count": selected_count,
            "pack_total_count": len(unique_cards),
            "pack_spent_total": pack_spent_total,
            "pack_value_total": total_pack_card_value,
            "pack_pnl_total": pack_pnl_total,
        }

    resolved_positive = [x for x in resolved_all if _to_decimal(x.get("value")) > 0]
    if len(resolved_positive) < 2:
        raise RuntimeError("此卡包可定價的卡片不足 2 張，無法生成天堂地獄版本。")

    sorted_cards = sorted(
        resolved_positive,
        key=lambda x: (_to_decimal(x.get("value")), str(x.get("token_id") or "")),
        reverse=True,
    )
    highest = sorted_cards[0]
    lowest = sorted_cards[-1]
    if str(highest.get("token_id")) == str(lowest.get("token_id")) and len(sorted_cards) > 1:
        lowest = sorted_cards[-2]

    def _prepare_final_extreme_image(row: dict) -> str:
        image = str(row.get("image") or "").strip()
        token_id = str(row.get("token_id") or "").strip()
        if image and _looks_like_pack_image_url(image) and token_id:
            token_image = _fetch_card_image_by_token_id(token_id)
            if token_image:
                image = token_image
        if not image:
            if token_id:
                image = _fetch_card_image_by_token_id(token_id)
        return _prepare_collectible_image_for_poster(image)

    highest["image"] = _prepare_final_extreme_image(highest)
    lowest["image"] = _prepare_final_extreme_image(lowest)

    items = [
        {
            "name": f"Heaven / {str(highest.get('name') or 'Unknown Collectible')}",
            "image": str(highest.get("image") or _TRANSPARENT_CARD_IMAGE),
            "value": _format_usdt_decimal(highest.get("value")),
            "price_up": bool(highest.get("used_token_image_fallback")),
        },
        {
            "name": f"Hell / {str(lowest.get('name') or 'Unknown Collectible')}",
            "image": str(lowest.get("image") or _TRANSPARENT_CARD_IMAGE),
            "value": _format_usdt_decimal(lowest.get("value")),
            "price_up": bool(lowest.get("used_token_image_fallback")),
        },
    ]
    pair_total = _to_decimal(highest.get("value")) + _to_decimal(lowest.get("value"))
    template_context = {
        "collection_name": pack_title,
        "sbt_total": len(pack_cards),
        "extreme_mode": True,
        "hide_footer": False,
        "sbt_badges_display": [],
        "items": items,
        "assets_count": len(unique_cards),
        "total_value": _format_usdt_decimal(pack_pnl_total, signed=True),
        "total_value_label": "Pack PnL",
        "items_count_label": "Cards Pulled",
        "assets_unit": "Cards",
        "sbt_badges_label": "Pack Name",
        "no_sbt_label": "No data",
        "owned_prefix": ui_labels.get("owned_prefix", "owned "),
        "brand_name": str((picker_data or {}).get("brand_name") or ui_labels.get("brand_name", "Renaiss")),
        "brand_site": str((picker_data or {}).get("brand_site") or ui_labels.get("brand_site", "renaiss.xyz")),
        "update_date": datetime.now().strftime("%Y-%m-%d"),
        "enable_tilt": False,
        "background_key": "classic",
        "background_image": _profile_background_data_uri("classic"),
        "meta_counter_label": "PULL",
        "pack_name_display": pack_title,
        "pnl_tone": pnl_tone,
        "_meta_subtitle": subtitle,
    }
    return {
        "template_context": template_context,
        "mode": mode_name,
        "pack_name": pack_name,
        "requested_count": requested_count,
        "selected_count": 2,
        "pack_total_count": len(unique_cards),
        "pack_spent_total": pack_spent_total,
        "pack_value_total": total_pack_card_value,
        "pack_pnl_total": pack_pnl_total,
    }


def _fetch_user_sbt_badges_official(username: str | None) -> list[dict]:
    uname = str(username or "").strip()
    if not uname:
        return []
    cache_key = f"official:{uname.lower()}"
    now_ts = time.time()
    cached = _PROFILE_SBT_BADGE_CACHE.get(cache_key)
    if cached and PROFILE_SBT_BADGE_CACHE_TTL_SEC > 0 and (now_ts - cached[0]) <= PROFILE_SBT_BADGE_CACHE_TTL_SEC:
        return [dict(x) for x in cached[1]]
    input_payload = {"0": {"json": {"username": uname}}}
    params = {
        "batch": "1",
        "input": json.dumps(input_payload, separators=(",", ":"), ensure_ascii=False),
    }
    last_err = None
    for attempt in range(1, PROFILE_API_MAX_RETRIES + 1):
        try:
            resp = _http_get(RENAISS_SBT_BADGES_URL, params=params, timeout=25)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError("sbt.getUserBadges 回傳格式異常")
            row = data[0]
            if row.get("error"):
                err = row.get("error", {}).get("json", {}).get("message") or "unknown error"
                raise RuntimeError(f"sbt.getUserBadges error: {err}")
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
            _PROFILE_SBT_BADGE_CACHE[cache_key] = (time.time(), [dict(x) for x in out])
            return out
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
    if cached:
        print(f"⚠️ sbt.getUserBadges failed for {uname}, fallback stale cache: {last_err}", file=sys.stderr)
        return [dict(x) for x in cached[1]]
    return []


def _fetch_sbt_metadata_map() -> dict[str, dict[str, str]]:
    now_ts = time.time()
    with _PROFILE_SBT_METADATA_LOCK:
        cached_ts = float(_PROFILE_SBT_METADATA_CACHE.get("ts") or 0.0)
        cached_data = _PROFILE_SBT_METADATA_CACHE.get("data")
        if (
            isinstance(cached_data, dict)
            and PROFILE_SBT_METADATA_CACHE_TTL_SEC > 0
            and (now_ts - cached_ts) <= PROFILE_SBT_METADATA_CACHE_TTL_SEC
        ):
            return {str(k): dict(v) for k, v in cached_data.items() if isinstance(v, dict)}

    sbt_blob = "SBT/minified/"
    chunk_name_re = re.compile(r"([a-f0-9]{16})\.js")
    entry_re = re.compile(
        r'\{id:(\d+),name:"([^"]+)"[^}]*?imageUrl:"(https://[^"]*' + re.escape(sbt_blob) + r'[^"]+)"'
    )

    def _get_chunk_names(url: str) -> set[str]:
        try:
            resp = _http_get(url, timeout=15)
            resp.raise_for_status()
            return {str(x) for x in chunk_name_re.findall(str(resp.text or ""))}
        except Exception:
            return set()

    chunk_names = _get_chunk_names(RENAISS_MAIN_URL) | _get_chunk_names(f"{RENAISS_MAIN_URL}/profile/achievements")
    out: dict[str, dict[str, str]] = {}
    for chunk_name in sorted(chunk_names):
        try:
            js_resp = _http_get(f"{RENAISS_MAIN_URL}/_next/static/chunks/{chunk_name}.js", timeout=15)
            js_resp.raise_for_status()
            js_text = str(js_resp.text or "")
        except Exception:
            continue
        if sbt_blob not in js_text:
            continue
        for m in entry_re.finditer(js_text):
            token_id = str(m.group(1) or "").strip()
            if not token_id:
                continue
            if token_id in out:
                continue
            out[token_id] = {
                "name": str(m.group(2) or "").strip(),
                "image_url": str(m.group(3) or "").strip(),
            }

    with _PROFILE_SBT_METADATA_LOCK:
        _PROFILE_SBT_METADATA_CACHE["ts"] = time.time()
        _PROFILE_SBT_METADATA_CACHE["data"] = {str(k): dict(v) for k, v in out.items()}
    return out


def _fetch_wallet_sbt_badges_onchain(wallet_address: str | None) -> list[dict]:
    wallet_norm = _normalize_wallet_address(wallet_address or "")
    if not wallet_norm:
        return []
    cache_key = f"onchain:{wallet_norm}"
    now_ts = time.time()
    cached = _PROFILE_SBT_BADGE_CACHE.get(cache_key)
    if cached and PROFILE_SBT_BADGE_CACHE_TTL_SEC > 0 and (now_ts - cached[0]) <= PROFILE_SBT_BADGE_CACHE_TTL_SEC:
        return [dict(x) for x in cached[1]]
    if _ONCHAIN_ANALYZE_SBT_WALLET_FN is None:
        return []
    cfg = _build_profile_onchain_cfg()
    if cfg is None:
        return []
    if not ONCHAIN_SBT_CONTRACT.startswith("0x") or len(ONCHAIN_SBT_CONTRACT) != 42:
        return []

    try:
        balances = _ONCHAIN_ANALYZE_SBT_WALLET_FN(cfg, wallet_norm, ONCHAIN_SBT_CONTRACT)
    except Exception as e:
        if cached:
            print(f"⚠️ onchain sbt failed for {wallet_norm}, fallback stale cache: {e}", file=sys.stderr)
            return [dict(x) for x in cached[1]]
        return []
    if not isinstance(balances, dict):
        return []

    metadata = _fetch_sbt_metadata_map()
    out: list[dict] = []
    for token_id_raw, amount_raw in balances.items():
        token_id = str(token_id_raw or "").strip()
        amount = _parse_int(amount_raw) or 0
        if not token_id or amount <= 0:
            continue
        meta = metadata.get(token_id) or {}
        name = str(meta.get("name") or "").strip() or f"SBT #{token_id}"
        image_url = str(meta.get("image_url") or "").strip()
        out.append(
            {
                "name": name,
                "balance": amount,
                "is_owned": True,
                "sbt_id": token_id,
                "image_url": image_url,
            }
        )
    out.sort(key=lambda x: (_parse_int(x.get("balance")) or 0, str(x.get("sbt_id") or "")), reverse=True)
    _PROFILE_SBT_BADGE_CACHE[cache_key] = (time.time(), [dict(x) for x in out])
    return out


def _fetch_user_sbt_badges(username: str | None, wallet_address: str | None = None) -> list[dict]:
    source = PROFILE_SBT_BADGE_SOURCE
    badges_onchain: list[dict] = []
    if source in ("onchain", "auto"):
        badges_onchain = _fetch_wallet_sbt_badges_onchain(wallet_address)
        if badges_onchain or source == "onchain":
            return badges_onchain
    if source in ("official", "auto"):
        badges_official = _fetch_user_sbt_badges_official(username)
        if badges_official or source == "official":
            return badges_official
        if badges_onchain:
            return badges_onchain
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
            if user_id or username:
                return (str(user_id) if user_id is not None else None), (str(username) if username is not None else None)
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
    if found_user_id or found_username:
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
                if found_user_id or found_username:
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
    if not username:
        username = _username_from_rankings_wallet(wallet_norm)
    if not user_id:
        sbt_badges = _fetch_user_sbt_badges(username, wallet_norm)
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
            "user_id": None,
            "collection_count": 0,
            "card_options": [],
            "owned_sbt_count": len(owned_badges),
            "sbt_options": sbt_options,
        }

    try:
        collection = _fetch_user_collection(user_id)
    except Exception:
        collection = []
    if not collection:
        sbt_badges = _fetch_user_sbt_badges(username, wallet_norm)
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

    sbt_badges = _fetch_user_sbt_badges(username, wallet_norm)
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
    wallet_norm = _normalize_wallet_address(wallet_address) or str(wallet_address or "").strip().lower()

    # Start history build in background so collection/sbt/image preparation can run in parallel.
    history_holder: dict[str, object] = {"data": None, "error": None}
    live_onchain_holder: dict[str, object] = {"data": None, "error": None}

    def _history_worker():
        try:
            history_holder["data"] = _build_wallet_activity_history(wallet_address, profile_lang=profile_lang)
        except Exception as e:
            history_holder["error"] = e

    history_thread = threading.Thread(target=_history_worker, name="profile-history-worker", daemon=True)
    history_thread.start()
    live_onchain_thread: threading.Thread | None = None
    if PROFILE_REALTIME_RECALC_ON_PROFILE and PROFILE_REALTIME_METRICS_SOURCE == "onchain" and wallet_norm:
        def _live_onchain_worker():
            try:
                live_onchain_holder["data"] = _profile_compute_onchain_metrics(wallet_norm)
            except Exception as e:
                live_onchain_holder["error"] = e
        live_onchain_thread = threading.Thread(
            target=_live_onchain_worker,
            name="profile-live-onchain-worker",
            daemon=True,
        )
        live_onchain_thread.start()

    try:
        user_id, username = _resolve_user_from_wallet(wallet_address)
    except Exception:
        user_id, username = None, None
    if not username:
        username = _username_from_rankings_wallet(wallet_norm)
    short_wallet = f"{wallet_norm[:6]}...{wallet_norm[-4:]}" if wallet_norm and len(wallet_norm) >= 10 else wallet_norm
    ranking_row = _load_rankings_wallet_map().get(wallet_norm, {}) if wallet_norm else {}
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
    sbt_badges = _fetch_user_sbt_badges(username, wallet_norm)
    sbt_total = sum((b.get("balance") or 0) for b in sbt_badges)
    sbt_live_total = int(sbt_total)
    volume_rank_chip = _rank_chip_payload((ranking_row or {}).get("volume_rank"))
    total_spent_rank_chip = _rank_chip_payload((ranking_row or {}).get("total_spent_rank"))
    holdings_rank_chip = _rank_chip_payload((ranking_row or {}).get("holdings_rank"))
    pnl_rank_chip = _rank_chip_payload((ranking_row or {}).get("pnl_rank"))
    participation_rank_chip = _rank_chip_payload((ranking_row or {}).get("participation_days_rank"))
    sbt_rank_chip = _rank_chip_payload((ranking_row or {}).get("sbt_rank"))
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
    if live_onchain_thread is not None:
        live_onchain_thread.join()
    history_error = history_holder.get("error")
    history_data = history_holder.get("data") if isinstance(history_holder.get("data"), dict) else None
    live_onchain_error = live_onchain_holder.get("error")
    live_onchain_metrics = live_onchain_holder.get("data") if isinstance(live_onchain_holder.get("data"), dict) else None
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
            "token_acquire_cost_hints": {},
        }
    if live_onchain_error is not None:
        print(f"⚠️ onchain profile recalc failed for {wallet_norm}: {live_onchain_error}", file=sys.stderr)
    history_labels = history_data.get("labels") or _profile_history_labels(profile_lang)
    history_activity_rows = list(history_data.get("activity_rows") or [])
    history_contract_rows = history_data.get("contract_rows") or []
    metric_opened_count = _parse_int(history_data.get("opened_packs_count")) or 0
    rank_metrics_ready = bool(ranking_row) and any(
        k in ranking_row
        for k in (
            "pack_spent_usdt",
            "total_spent_usdt",
            "total_earned_usdt",
            "trade_volume_usdt",
            "cash_net_usdt",
            "total_pnl_usdt",
        )
    )
    use_rank_metrics = bool(PROFILE_USE_RANKING_METRICS and rank_metrics_ready)
    use_live_onchain_metrics = bool(
        PROFILE_REALTIME_RECALC_ON_PROFILE
        and PROFILE_REALTIME_METRICS_SOURCE == "onchain"
        and isinstance(live_onchain_metrics, dict)
        and live_onchain_metrics
    )

    if use_live_onchain_metrics:
        metric_pack_spent = _to_decimal(live_onchain_metrics.get("pack_spent_usdt"))
        metric_total_spent = _to_decimal(live_onchain_metrics.get("total_spent_usdt"))
        metric_total_earned = _to_decimal(live_onchain_metrics.get("total_earned_usdt"))
        metric_trade_volume = _to_decimal(live_onchain_metrics.get("trade_volume_usdt"))
        metric_buyback_total = _to_decimal(live_onchain_metrics.get("buyback_earned_usdt"))
        metric_market_buy_total = _to_decimal(live_onchain_metrics.get("trade_spent_usdt"))
        metric_market_sell_total = _to_decimal(live_onchain_metrics.get("trade_earned_usdt"))
        metric_card_withdraw_total = _to_decimal(history_data.get("card_withdraw_total"))
        holdings_from_ranking = _to_decimal(ranking_row.get("holdings_value_usdt"))
        holdings_value = holdings_from_ranking if holdings_from_ranking > 0 else (_to_decimal(total_fmv) / Decimal("100"))
        # Net follows profile definition: cash flow + withdraw-card value.
        cash_net = _to_decimal(live_onchain_metrics.get("cash_net_usdt")) + metric_card_withdraw_total
        net_with_holdings = cash_net + holdings_value
        metric_active_days_count = (
            _parse_int(history_data.get("active_days_count"))
            or _parse_int(ranking_row.get("participation_days_count"))
            or 0
        )
        onchain_opened_count = _parse_int(live_onchain_metrics.get("open_pack_tx_count")) or 0
        onchain_buyback_count = _parse_int(live_onchain_metrics.get("buyback_tx_count")) or 0
        onchain_trade_buy_count = _parse_int(live_onchain_metrics.get("trade_buy_tx_count")) or 0
        onchain_trade_sell_count = _parse_int(live_onchain_metrics.get("trade_sell_tx_count")) or 0
        if onchain_opened_count > 0:
            metric_opened_count = onchain_opened_count
        history_activity_rows = _upsert_activity_count_row(
            history_activity_rows,
            row_types=("PerpetualReleaseTokenActivity",),
            count=onchain_opened_count,
            lang=profile_lang,
            highlight=True,
        )
        history_activity_rows = _upsert_activity_count_row(
            history_activity_rows,
            row_types=("PerpetualBuybackActivity", "BuybackActivity"),
            count=onchain_buyback_count,
            lang=profile_lang,
        )
        history_activity_rows = _upsert_activity_count_row(
            history_activity_rows,
            row_types=("BuyActivity",),
            count=onchain_trade_buy_count,
            lang=profile_lang,
        )
        history_activity_rows = _upsert_activity_count_row(
            history_activity_rows,
            row_types=("SellActivity",),
            count=onchain_trade_sell_count,
            lang=profile_lang,
        )
    elif use_rank_metrics:
        metric_pack_spent = _to_decimal(ranking_row.get("pack_spent_usdt"))
        metric_total_spent = _to_decimal(ranking_row.get("total_spent_usdt"))
        metric_total_earned = _to_decimal(ranking_row.get("total_earned_usdt"))
        metric_trade_volume = _to_decimal(ranking_row.get("trade_volume_usdt"))
        metric_buyback_total = _to_decimal(ranking_row.get("buyback_earned_usdt"))
        metric_market_buy_total = _to_decimal(ranking_row.get("trade_spent_usdt"))
        metric_market_sell_total = _to_decimal(ranking_row.get("trade_earned_usdt"))
        metric_card_withdraw_total = _to_decimal(ranking_row.get("card_withdraw_total_usdt"))
        holdings_value = _to_decimal(ranking_row.get("holdings_value_usdt"))
        cash_net = _to_decimal(ranking_row.get("cash_net_usdt"))
        net_with_holdings = _to_decimal(ranking_row.get("total_pnl_usdt"))
        metric_active_days_count = _parse_int(ranking_row.get("participation_days_count")) or (
            _parse_int(history_data.get("active_days_count")) or 0
        )
    else:
        metric_pack_spent = _to_decimal(history_data.get("pack_spent_total"))
        metric_total_spent = _to_decimal(history_data.get("total_spent"))
        metric_total_earned = _to_decimal(history_data.get("total_earned"))
        metric_trade_volume = _to_decimal(history_data.get("trade_volume"))
        metric_buyback_total = _to_decimal(history_data.get("buyback_total"))
        metric_market_buy_total = _to_decimal(history_data.get("market_buy_total"))
        metric_market_sell_total = _to_decimal(history_data.get("market_sell_total"))
        metric_card_withdraw_total = _to_decimal(history_data.get("card_withdraw_total"))
        holdings_value = _to_decimal(total_fmv) / Decimal("100")
        cash_net = _to_decimal(history_data.get("net_total"))
        net_with_holdings = cash_net + holdings_value
        metric_active_days_count = _parse_int(history_data.get("active_days_count")) or 0

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
        "metric_opened_value": _format_number(metric_opened_count),
        "metric_pack_spent_label": history_labels.get("kpi_pack_spent", "Pack Spend"),
        "metric_pack_spent_value": _format_usdt_decimal(metric_pack_spent),
        "metric_card_withdraw_label": history_labels.get("kpi_card_withdraw", "Card Withdrawal Value"),
        "metric_card_withdraw_value": _format_usdt_decimal(metric_card_withdraw_total),
        "metric_total_spent_label": history_labels.get("kpi_total_spent", "Total Spent"),
        "metric_total_spent_note": history_labels.get("kpi_total_spent_note", ""),
        "metric_total_spent_value": _format_usdt_decimal(metric_total_spent),
        "metric_total_spent_rank": total_spent_rank_chip["text"],
        "metric_total_spent_rank_tier": total_spent_rank_chip["tier"],
        "metric_total_earned_label": history_labels.get("kpi_total_earned", "Total Earned"),
        "metric_total_earned_note": history_labels.get("kpi_total_earned_note", ""),
        "metric_total_earned_value": _format_usdt_decimal(metric_total_earned),
        "metric_net_label": history_labels.get("kpi_net", "Net PnL"),
        "metric_net_note": history_labels.get("kpi_net_note", ""),
        "metric_net_value": _format_usdt_currency(net_with_holdings, signed=True),
        "metric_net_rank": pnl_rank_chip["text"],
        "metric_net_rank_tier": pnl_rank_chip["tier"],
        "metric_trade_volume_label": history_labels.get("kpi_trade_volume", "Trade Volume"),
        "metric_trade_volume_value": _format_usdt_decimal(metric_trade_volume),
        "metric_trade_volume_rank": volume_rank_chip["text"],
        "metric_trade_volume_rank_tier": volume_rank_chip["tier"],
        "metric_assets_value_label": history_labels.get("kpi_assets_value", "Holdings Value"),
        "metric_assets_value_value": _format_usdt_decimal(holdings_value),
        "metric_assets_value_rank": holdings_rank_chip["text"],
        "metric_assets_value_rank_tier": holdings_rank_chip["tier"],
        "metric_sbt_label": "SBT",
        # SBT quantity must always reflect live badges; ranking snapshot is rank-only.
        "metric_sbt_value": _format_number(sbt_live_total),
        "metric_sbt_rank": sbt_rank_chip["text"],
        "metric_sbt_rank_tier": sbt_rank_chip["tier"],
        "metric_buyback_label": history_labels.get("kpi_buyback", "Buyback Total"),
        "metric_buyback_value": _format_usdt_decimal(metric_buyback_total),
        "metric_market_buy_label": history_labels.get("kpi_market_buy", "Market Buy Total"),
        "metric_market_buy_value": _format_usdt_decimal(metric_market_buy_total),
        "metric_market_sell_label": history_labels.get("kpi_market_sell", "Market Sell Total"),
        "metric_market_sell_value": _format_usdt_decimal(metric_market_sell_total),
        "active_days_label": history_labels.get("kpi_active_days", "Active Days"),
        "active_days_value": _format_number(metric_active_days_count),
        "active_days_rank": participation_rank_chip["text"],
        "active_days_rank_tier": participation_rank_chip["tier"],
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
    holdings_template_context = {}
    if PROFILE_ENABLE_HOLDINGS_POSTER:
        holdings_template_context = _build_wallet_holdings_growth_template_context(
            history_data=history_data,
            parsed_sorted=parsed_sorted,
            profile_name=profile_name,
            short_wallet=(history_data.get("wallet_short") or short_wallet),
            profile_lang=profile_lang,
        )
    sbt_rank_tier = sbt_rank_chip["tier"] if sbt_rank_chip["tier"] in ("gold", "silver", "bronze") else "none"
    sbt_rank_value = sbt_rank_chip["text"]
    sbt_owned_total_snapshot = _parse_int((ranking_row or {}).get("sbt_owned_total")) or 0
    sbt_owned_badge_snapshot = _parse_int((ranking_row or {}).get("sbt_owned_badge_count")) or 0
    sbt_owned_badge_live = len(owned_badges)
    sbt_total_display = sbt_live_total if sbt_live_total > 0 else sbt_owned_total_snapshot
    sbt_badge_count_display = sbt_owned_badge_live if sbt_owned_badge_live > 0 else sbt_owned_badge_snapshot
    sbt_badges_sorted = sorted(
        owned_badges,
        key=lambda x: (_parse_int(x.get("balance")) or 0),
        reverse=True,
    )
    sbt_badges_for_rank: list[dict[str, object]] = []
    for b in sbt_badges_sorted:
        name = str(b.get("name") or "").strip()
        if not name:
            continue
        sbt_badges_for_rank.append(
            {
                "name": _compact_sbt_label(name),
                "balance": _parse_int(b.get("balance")) or 0,
                "image": _prepare_sbt_badge_image_for_poster(str(b.get("image_url") or "").strip()),
            }
        )
    if not sbt_badges_for_rank:
        for b in display_badges:
            sbt_badges_for_rank.append(
                {
                    "name": str(b.get("label") or b.get("name") or "").strip(),
                    "balance": _parse_int(b.get("balance")) or 0,
                    "image": str(b.get("image") or _TRANSPARENT_CARD_IMAGE),
                }
            )
    sbt_rank_template_context = {
        "collection_name": f"{profile_name} Collection",
        "brand_name": ui_labels["brand_name"],
        "brand_site": ui_labels["brand_site"],
        "update_date": datetime.now().strftime("%Y-%m-%d"),
        "title": _t(profile_lang, "SBT 排名", "SBT Ranking", "SBT 랭킹", "SBT 排名"),
        "subtitle": _t(
            profile_lang,
            "鏈上持有徽章統計",
            "On-chain badge leaderboard snapshot",
            "온체인 배지 리더보드 스냅샷",
            "链上徽章排行榜快照",
        ),
        "wallet_label": _t(profile_lang, "錢包", "Wallet", "지갑", "钱包"),
        "holders_label": _t(profile_lang, "總持有人", "Total Holders", "총 보유자", "总持有人"),
        "sbt_total_label": _t(profile_lang, "SBT 總數", "Total SBT", "총 SBT", "SBT 总数"),
        "sbt_badge_label": _t(profile_lang, "持有徽章數", "Owned Badge Types", "보유 배지 수", "持有徽章数"),
        "badges_title": _t(profile_lang, "持有徽章", "Owned Badges", "보유 배지", "持有徽章"),
        "badges_empty": _t(profile_lang, "目前無可顯示 SBT", "No SBT badges found", "표시할 SBT 없음", "暂无可显示 SBT"),
        "rank_label": "SBT Rank",
        "holders_suffix": _t(profile_lang, "位", "holders", "명", "位"),
        "wallet_short": history_data.get("wallet_short") or short_wallet or wallet_norm,
        "username": profile_name,
        "sbt_total": _format_number(sbt_total_display),
        "sbt_badge_count": _format_number(sbt_badge_count_display),
        "sbt_rank": sbt_rank_value,
        "sbt_rank_tier": sbt_rank_tier,
        "holders_total": _format_number(len(_load_rankings_wallet_map())),
        "background_image": _profile_sbt_rank_background_data_uri(sbt_rank_tier),
        "sbt_badges": sbt_badges_for_rank,
    }

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
        "holdings_template_context": holdings_template_context,
        "sbt_rank_template_context": sbt_rank_template_context,
        "history_summary": {
            "opened_packs": history_data.get("opened_packs_count", 0),
            "total_spent": metric_total_spent,
            "total_earned": metric_total_earned,
            "net_total": net_with_holdings,
            "cash_net": cash_net,
            "holdings_value": holdings_value,
            "buyback_total": metric_buyback_total,
            "market_buy_total": metric_market_buy_total,
            "market_sell_total": metric_market_sell_total,
            "card_withdraw_total": metric_card_withdraw_total,
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


async def _render_wallet_profile_holdings_poster(template_payload: dict, out_dir: str, safe_name: str = "wallet_profile") -> str:
    if isinstance(template_payload, dict):
        template_context = template_payload.get("holdings_template_context") or {}
        replacements = template_payload.get("holdings_replacements") or {}
    else:
        template_context = {}
        replacements = {}

    html_doc = _render_wallet_template_html(
        PROFILE_HOLDINGS_TEMPLATE_PATH,
        template_context=template_context,
        replacements=replacements,
    )

    os.makedirs(out_dir, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", safe_name).strip("_") or "wallet_profile"
    out_path = os.path.join(out_dir, f"{safe}_profile_holdings.png")
    await image_generator._render_single_html_poster(
        html_doc,
        out_path,
        width=1200,
        height=900,
        device_scale_factor=2,
    )
    return out_path


async def _render_wallet_profile_cardpack_pull_poster(
    template_payload: dict,
    out_dir: str,
    safe_name: str = "wallet_pack_flex",
) -> str:
    if isinstance(template_payload, dict):
        template_context = template_payload.get("template_context") or {}
        replacements = template_payload.get("replacements") or {}
    else:
        template_context = {}
        replacements = {}

    html_doc = _render_wallet_template_html(
        PROFILE_CARDPACK_PULL_TEMPLATE_PATH,
        template_context=template_context,
        replacements=replacements,
    )
    os.makedirs(out_dir, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", safe_name).strip("_") or "wallet_pack_flex"
    out_path = os.path.join(out_dir, f"{safe}_cardpack_pull.png")
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
    render_holdings: bool = False,
) -> dict:
    if isinstance(template_payload, dict):
        profile_template_context = template_payload.get("template_context") or {}
        profile_replacements = template_payload.get("replacements") or {}
        history_template_context = template_payload.get("history_template_context") or {}
        history_replacements = template_payload.get("history_replacements") or {}
        extreme_template_context = template_payload.get("extreme_template_context") or {}
        extreme_replacements = template_payload.get("extreme_replacements") or {}
        holdings_template_context = template_payload.get("holdings_template_context") or {}
        holdings_replacements = template_payload.get("holdings_replacements") or {}
        sbt_rank_template_context = template_payload.get("sbt_rank_template_context") or {}
        sbt_rank_replacements = template_payload.get("sbt_rank_replacements") or {}
    else:
        profile_template_context = {}
        profile_replacements = template_payload or {}
        history_template_context = {}
        history_replacements = {}
        extreme_template_context = {}
        extreme_replacements = {}
        holdings_template_context = {}
        holdings_replacements = {}
        sbt_rank_template_context = {}
        sbt_rank_replacements = {}

    os.makedirs(out_dir, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", safe_name).strip("_") or "wallet_profile"
    profile_out = os.path.join(out_dir, f"{safe}_profile.png") if render_profile else None
    history_out = os.path.join(out_dir, f"{safe}_profile_history.png")
    extremes_out = os.path.join(out_dir, f"{safe}_profile_extremes.png")
    sbt_rank_out = os.path.join(out_dir, f"{safe}_profile_sbt_rank.png")
    holdings_out = os.path.join(out_dir, f"{safe}_profile_holdings.png") if render_holdings else None

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
    jobs.append(
        (
            _render_wallet_template_html(
                PROFILE_SBT_RANK_TEMPLATE_PATH,
                template_context=sbt_rank_template_context,
                replacements=sbt_rank_replacements,
            ),
            sbt_rank_out,
        )
    )
    if render_holdings:
        jobs.append(
            (
                _render_wallet_template_html(
                    PROFILE_HOLDINGS_TEMPLATE_PATH,
                    template_context=holdings_template_context,
                    replacements=holdings_replacements,
                ),
                holdings_out,
            )
        )

    async with image_generator.RENDER_SEMAPHORE:
        context = await image_generator._new_browser_context(
            viewport={"width": 1200, "height": 900},
            device_scale_factor=PROFILE_POSTER_DEVICE_SCALE,
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
        "sbt_rank": sbt_rank_out,
        "holdings": holdings_out,
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


_MARKET_LISTING_RE = re.compile(r"^\s*(WTS|WTB)\b(?:\s*[·•\-:：|]\s*)?(.*)$", re.IGNORECASE)
_MARKET_PRICE_RE = re.compile(r"(\d+(?:[.,]\d{1,2})?)\s*(USD|USDT)\b", re.IGNORECASE)


def _market_cache_root_dir() -> str:
    # 對齊 rankings 的 server/local 路徑策略：
    # 直接掛在 _rank_sync_data_dir() 的同層，讓 server 外掛磁碟與本地測試都跟著切換。
    rank_dir = os.path.abspath(_rank_sync_data_dir())
    return os.path.join(os.path.dirname(rank_dir), "market_cache")


def _market_cache_images_dir() -> str:
    return os.path.join(_market_cache_root_dir(), "images")


def _market_index_path() -> str:
    return os.path.join(_market_cache_root_dir(), "market_index.json")


def _market_bootstrap_state_path() -> str:
    return os.path.join(_market_cache_root_dir(), "state", "bootstrap_once.json")


def _market_load_bootstrap_state() -> dict[str, object]:
    path = _market_bootstrap_state_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _market_save_bootstrap_state(payload: dict[str, object]) -> None:
    path = _market_bootstrap_state_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(path + ".tmp", path)
    except Exception:
        pass


def _market_write_index_file(
    *,
    listings: list[dict],
    source_channel_id: int,
    include_archived: bool,
    scanned_thread_count: int,
    active_thread_count: int,
    recent_limit: int | str,
) -> tuple[bool, str, str, str]:
    market_index_path = _market_index_path()
    updated_at = datetime.now(timezone.utc).isoformat()
    payload: dict[str, object] = {
        "updated_at": updated_at,
        "source_channel_id": int(source_channel_id),
        "summary_bot_id": int(MARKET_SUMMARY_BOT_ID),
        "recent_limit": recent_limit,
        "include_archived": bool(include_archived),
        "scanned_thread_count": int(scanned_thread_count),
        "active_thread_count": int(active_thread_count),
        "listing_count": len(listings),
        "items": list(listings or []),
    }
    try:
        os.makedirs(os.path.dirname(market_index_path), exist_ok=True)
        with open(market_index_path + ".tmp", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(market_index_path + ".tmp", market_index_path)
        return True, "", market_index_path, updated_at
    except Exception as e:
        return False, str(e), market_index_path, updated_at


def _market_load_cached_index(limit: int | None = MARKET_RECENT_LIMIT) -> tuple[list[dict], dict]:
    path = _market_index_path()
    if not os.path.exists(path):
        return [], {
            "from_cache": False,
            "cache_path": path,
            "error": f"market cache 不存在：{path}",
        }
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        return [], {
            "from_cache": False,
            "cache_path": path,
            "error": f"market cache 讀取失敗：{e}",
        }
    if not isinstance(payload, dict):
        return [], {
            "from_cache": False,
            "cache_path": path,
            "error": "market cache 格式錯誤",
        }

    items_raw = payload.get("items")
    items = [x for x in items_raw if isinstance(x, dict)] if isinstance(items_raw, list) else []
    items.sort(key=lambda item: int(item.get("created_at_ts") or 0), reverse=True)
    if isinstance(limit, int) and limit > 0:
        items = items[:limit]
        limit_meta: int | str = int(limit)
    else:
        limit_meta = "all"

    meta = {
        "from_cache": True,
        "cache_path": path,
        "source_channel_id": int(_parse_int(payload.get("source_channel_id")) or MARKET_SOURCE_CHANNEL_ID),
        "active_thread_count": int(_parse_int(payload.get("active_thread_count")) or 0),
        "scanned_thread_count": int(_parse_int(payload.get("scanned_thread_count")) or 0),
        "listing_count": len(items),
        "include_archived": bool(payload.get("include_archived")),
        "recent_limit": limit_meta,
        "index_path": path,
        "images_dir": _market_cache_images_dir(),
        "updated_at": str(payload.get("updated_at") or ""),
    }
    return items, meta


async def _market_collect_source_threads(
    channel: discord.TextChannel | discord.ForumChannel,
    guild: discord.Guild,
    *,
    include_archived: bool = False,
    archived_limit: int | None = MARKET_BOOTSTRAP_ARCHIVED_LIMIT,
) -> list[discord.Thread]:
    thread_map: dict[int, discord.Thread] = {}

    try:
        active_threads = await guild.active_threads()
    except Exception:
        active_threads = []

    for t in active_threads:
        if isinstance(t, discord.Thread) and t.parent_id == channel.id and not bool(getattr(t, "archived", False)):
            thread_map[int(t.id)] = t

    if include_archived:
        archived_count = 0
        try:
            async for t in channel.archived_threads(limit=None):
                if not isinstance(t, discord.Thread):
                    continue
                if t.parent_id != channel.id:
                    continue
                thread_map[int(t.id)] = t
                archived_count += 1
                if isinstance(archived_limit, int) and archived_limit > 0 and archived_count >= archived_limit:
                    break
        except Exception:
            pass

    threads = list(thread_map.values())
    threads.sort(key=lambda t: int(getattr(t, "last_message_id", 0) or t.id or 0), reverse=True)
    return threads


async def _market_parse_thread_listing(thread: discord.Thread, guild: discord.Guild) -> dict[str, object] | None:
    summary_msg: discord.Message | None = None
    parsed_side = None
    parsed_title = ""
    try:
        async for msg in thread.history(limit=MARKET_SCAN_MESSAGES_PER_THREAD, oldest_first=False):
            author_id = int(getattr(getattr(msg, "author", None), "id", 0) or 0)
            if MARKET_SUMMARY_BOT_ID > 0 and author_id != MARKET_SUMMARY_BOT_ID:
                continue
            side, title = _market_parse_summary(str(getattr(msg, "content", "") or ""))
            if side in ("WTS", "WTB"):
                summary_msg = msg
                parsed_side = side
                parsed_title = title
                break
    except Exception:
        return None

    if summary_msg is None or parsed_side not in ("WTS", "WTB"):
        return None

    image_url = _market_extract_image_url(summary_msg)
    if not image_url:
        image_url = await _market_find_fallback_image(thread)
    cache_path = ""
    if image_url:
        cache_path = _market_image_cache_path(int(thread.id), int(summary_msg.id), image_url)

    summary_text = str(getattr(summary_msg, "content", "") or "").strip()[:500]
    price_amount, price_currency = _market_parse_price(summary_text)
    if not price_amount:
        price_amount, price_currency = _market_parse_price(parsed_title)
    price_text = f"{price_amount} {price_currency}".strip() if price_amount and price_currency else ""

    created_at = getattr(summary_msg, "created_at", None) or getattr(thread, "created_at", datetime.now(timezone.utc))
    return {
        "key": f"{thread.id}:{summary_msg.id}",
        "side": parsed_side,
        "title": parsed_title,
        "summary": summary_text,
        "thread_id": int(thread.id),
        "message_id": int(summary_msg.id),
        "thread_name": str(getattr(thread, "name", "") or ""),
        "thread_url": str(getattr(summary_msg, "jump_url", "") or f"https://discord.com/channels/{guild.id}/{thread.id}"),
        "created_at_ts": int(created_at.timestamp()) if created_at else 0,
        "image_url": image_url,
        "local_image_path": cache_path,
        "price_amount": price_amount,
        "price_currency": price_currency,
        "price_text": price_text,
    }


def _market_parse_summary(content: str) -> tuple[str | None, str]:
    first_line = ""
    for raw_line in str(content or "").splitlines():
        if raw_line.strip():
            first_line = raw_line.strip()
            break
    if not first_line:
        return None, ""
    matched = _MARKET_LISTING_RE.match(first_line)
    if not matched:
        return None, first_line[:120]
    side = str(matched.group(1) or "").upper()
    title = str(matched.group(2) or "").strip()
    if not title:
        title = first_line
    return side, title[:160]


def _market_parse_price(content: str) -> tuple[str, str]:
    text = str(content or "")
    best_match = None
    for m in _MARKET_PRICE_RE.finditer(text):
        best_match = m
    if best_match is None:
        return "", ""
    amount_raw = str(best_match.group(1) or "").replace(",", "").strip()
    currency = str(best_match.group(2) or "").upper().strip()
    if not amount_raw or not currency:
        return "", ""
    try:
        d = Decimal(amount_raw)
        amount = format(d.normalize(), "f")
        if "." in amount:
            amount = amount.rstrip("0").rstrip(".")
        amount = amount or "0"
    except Exception:
        amount = amount_raw
    return amount, currency


def _market_is_public_image_url(url: str | None) -> bool:
    u = str(url or "").strip()
    return u.startswith("https://") or u.startswith("http://")


def _market_item_price_text(item: dict) -> str:
    direct = str(item.get("price_text") or "").strip()
    if direct:
        return direct
    for field in ("summary", "title"):
        amount, currency = _market_parse_price(str(item.get(field) or ""))
        if amount and currency:
            return f"{amount} {currency}"
    return ""


def _market_side_label(side: str | None, lang: str = "zh") -> str:
    s = str(side or "").upper().strip()
    if s == "WTS":
        return _t(lang, "賣", "Sell", "판매", "卖")
    if s == "WTB":
        return _t(lang, "買", "Buy", "구매", "买")
    return s or "-"


def _market_item_image_url(item: dict) -> str:
    url = str(item.get("image_url") or "").strip()
    if _market_is_public_image_url(url):
        return url
    return ""


def _market_item_local_image_path(item: dict) -> str:
    path = str(item.get("local_image_path") or "").strip()
    if path and os.path.exists(path):
        return path
    return ""


def _market_local_image_filename(item: dict, local_path: str) -> str:
    allowed_ext = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
    src_url = str(item.get("image_url") or "").strip()
    url_path = re.split(r"[?#]", src_url, maxsplit=1)[0]
    ext = os.path.splitext(url_path)[1].lower()
    if ext not in allowed_ext:
        local_ext = os.path.splitext(local_path)[1].lower()
        ext = local_ext if local_ext in allowed_ext else ".jpg"
    return f"market_{item.get('thread_id', 'x')}_{item.get('message_id', 'x')}{ext}"


def _market_extract_image_url(message: discord.Message | None) -> str:
    if message is None:
        return ""
    attachments = list(getattr(message, "attachments", []) or [])
    for att in attachments:
        if _is_image_attachment(att):
            url = str(getattr(att, "url", "") or "").strip()
            if _market_is_public_image_url(url):
                return url
    for emb in list(getattr(message, "embeds", []) or []):
        image = getattr(emb, "image", None)
        image_url = str(getattr(image, "url", "") or "").strip()
        image_proxy = str(getattr(image, "proxy_url", "") or "").strip()
        for cand in (image_url, image_proxy):
            if _market_is_public_image_url(cand):
                return cand
            if cand.startswith("attachment://"):
                filename = cand[len("attachment://"):].strip()
                if not filename:
                    continue
                for att in attachments:
                    if str(getattr(att, "filename", "") or "").strip() == filename and _is_image_attachment(att):
                        att_url = str(getattr(att, "url", "") or "").strip()
                        if _market_is_public_image_url(att_url):
                            return att_url
        thumb = getattr(emb, "thumbnail", None)
        thumb_url = str(getattr(thumb, "url", "") or "").strip()
        thumb_proxy = str(getattr(thumb, "proxy_url", "") or "").strip()
        for cand in (thumb_url, thumb_proxy):
            if _market_is_public_image_url(cand):
                return cand
    return ""


def _market_image_cache_path(thread_id: int, message_id: int, image_url: str) -> str:
    key = hashlib.sha1(f"{thread_id}:{message_id}:{image_url}".encode("utf-8")).hexdigest()
    return os.path.join(_market_cache_images_dir(), f"{key}.img")


def _market_cache_image_sync(image_url: str, output_path: str):
    if not image_url:
        return
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        return
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    tmp_path = f"{output_path}.tmp"
    try:
        resp = requests.get(image_url, timeout=12)
        resp.raise_for_status()
        content_type = str(resp.headers.get("Content-Type") or "").lower()
        if content_type and not content_type.startswith("image/"):
            return
        with open(tmp_path, "wb") as f:
            f.write(resp.content)
        os.replace(tmp_path, output_path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


async def _market_cache_image_async(image_url: str, output_path: str):
    if not image_url or not output_path:
        return
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _market_cache_image_sync, image_url, output_path)


async def _market_find_fallback_image(thread: discord.Thread) -> str:
    starter = getattr(thread, "starter_message", None)
    if starter:
        image = _market_extract_image_url(starter)
        if image:
            return image

    try:
        starter_msg = await thread.fetch_message(thread.id)
        image = _market_extract_image_url(starter_msg)
        if image:
            return image
    except Exception:
        pass

    parent = getattr(thread, "parent", None)
    if parent is not None:
        try:
            starter_msg = await parent.fetch_message(thread.id)
            image = _market_extract_image_url(starter_msg)
            if image:
                return image
        except Exception:
            pass

    try:
        async for msg in thread.history(limit=20, oldest_first=True):
            image = _market_extract_image_url(msg)
            if image:
                return image
    except Exception:
        pass
    return ""


async def _market_collect_listings(
    *,
    include_archived: bool = False,
    limit: int | None = MARKET_RECENT_LIMIT,
    archived_limit: int | None = MARKET_BOOTSTRAP_ARCHIVED_LIMIT,
) -> tuple[list[dict], dict]:
    def _fallback(error_message: str) -> tuple[list[dict], dict]:
        cached_items, cached_meta = _market_load_cached_index(limit=limit)
        fallback_meta = dict(cached_meta or {})
        fallback_meta["error"] = error_message
        fallback_meta["used_cache_fallback"] = bool(fallback_meta.get("from_cache"))
        return cached_items, fallback_meta

    if MARKET_SOURCE_CHANNEL_ID <= 0:
        return _fallback("MARKET_SOURCE_CHANNEL_ID 未設定。")

    channel = client.get_channel(MARKET_SOURCE_CHANNEL_ID)
    if channel is None:
        try:
            channel = await client.fetch_channel(MARKET_SOURCE_CHANNEL_ID)
        except Exception as e:
            return _fallback(f"無法取得來源頻道：{e}")

    if not isinstance(channel, (discord.TextChannel, discord.ForumChannel)):
        return _fallback(f"來源頻道型別不支援：{type(channel).__name__}")

    guild = channel.guild
    if guild is None:
        return _fallback("來源頻道不在 guild 中。")

    try:
        source_threads = await _market_collect_source_threads(
            channel,
            guild,
            include_archived=include_archived,
            archived_limit=archived_limit,
        )
    except Exception as e:
        return _fallback(f"無法讀取來源討論串：{e}")

    listings: list[dict] = []
    image_cache_tasks: list[asyncio.Task] = []
    active_thread_count = len([t for t in source_threads if not bool(getattr(t, "archived", False))])
    for thread in source_threads:
        item = await _market_parse_thread_listing(thread, guild)
        if not item:
            continue
        listings.append(item)

        image_url = str(item.get("image_url") or "").strip()
        cache_path = str(item.get("local_image_path") or "").strip()
        if image_url and cache_path and MARKET_CACHE_IMAGES and not os.path.exists(cache_path):
            image_cache_tasks.append(asyncio.create_task(_market_cache_image_async(image_url, cache_path)))

    listings.sort(key=lambda item: int(item.get("created_at_ts") or 0), reverse=True)
    if isinstance(limit, int) and limit > 0:
        listings = listings[:limit]
        limit_meta: int | str = int(limit)
    else:
        limit_meta = "all"

    index_write_ok, index_write_error, market_index_path, index_updated_at = _market_write_index_file(
        listings=listings,
        source_channel_id=int(channel.id),
        include_archived=bool(include_archived),
        scanned_thread_count=len(source_threads),
        active_thread_count=int(active_thread_count),
        recent_limit=limit_meta,
    )
    if not index_write_ok:
        print(f"⚠️ market index write failed: {index_write_error}", file=sys.stderr)
    else:
        print(
            "🗂️ market index written "
            f"path={market_index_path} listings={len(listings)} updated_at={index_updated_at}"
        )

    if image_cache_tasks:
        # 不阻塞 /market 互動流程，讓快取背景下載即可。
        for task in image_cache_tasks:
            task.add_done_callback(lambda _: None)

    return listings, {
        "source_channel_id": int(channel.id),
        "active_thread_count": int(active_thread_count),
        "scanned_thread_count": len(source_threads),
        "listing_count": len(listings),
        "recent_limit": limit_meta,
        "include_archived": bool(include_archived),
        "index_path": market_index_path,
        "images_dir": _market_cache_images_dir(),
        "updated_at": index_updated_at,
        "index_write_ok": bool(index_write_ok),
        "index_write_error": str(index_write_error or ""),
    }


async def _market_live_sync_refresh(reason: str, force: bool = False) -> bool:
    if not MARKET_LIVE_SYNC_ON_EVENT:
        return False
    global MARKET_LIVE_SYNC_LOCK, MARKET_LIVE_SYNC_LAST_TS
    if MARKET_LIVE_SYNC_LOCK is None:
        MARKET_LIVE_SYNC_LOCK = asyncio.Lock()

    now = time.time()
    if (not force) and (now - MARKET_LIVE_SYNC_LAST_TS < MARKET_LIVE_SYNC_DEBOUNCE_SEC):
        return False
    if MARKET_LIVE_SYNC_LOCK.locked() and not force:
        return False

    async with MARKET_LIVE_SYNC_LOCK:
        now2 = time.time()
        if (not force) and (now2 - MARKET_LIVE_SYNC_LAST_TS < MARKET_LIVE_SYNC_DEBOUNCE_SEC):
            return False
        before_items, _before_meta = _market_load_cached_index(limit=None)
        before_keys = {
            str(it.get("key") or "").strip()
            for it in list(before_items or [])
            if str(it.get("key") or "").strip()
        }
        listings, meta = await _market_collect_listings()
        MARKET_LIVE_SYNC_LAST_TS = time.time()
        err = str(meta.get("error") or "").strip()
        if err:
            print(f"⚠️ market live sync failed reason={reason} error={err}")
            return False
        after_keys = {
            str(it.get("key") or "").strip()
            for it in list(listings or [])
            if str(it.get("key") or "").strip()
        }
        new_keys = after_keys - before_keys
        print(
            "✅ market live sync "
            f"reason={reason} listings={len(listings)} active_threads={int(meta.get('active_thread_count') or 0)}"
        )
        if new_keys:
            asyncio.create_task(_market_auto_push_market_cache(reason=f"{reason}:new={len(new_keys)}"))
        return True


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


async def _process_single_image_compat(**kwargs):
    """
    Call market_report_vision.process_single_image with runtime-compatible kwargs.
    Older deployments may not support newer args like vision_provider_override.
    """
    try:
        sig = inspect.signature(market_report_vision.process_single_image)
        supported = set(sig.parameters.keys())
        filtered = {k: v for k, v in kwargs.items() if k in supported}
        dropped = [k for k in kwargs.keys() if k not in supported]
        if dropped:
            print(f"⚠️ process_single_image 不支援參數，已略過: {dropped}")
    except Exception:
        filtered = dict(kwargs)
    return await market_report_vision.process_single_image(**filtered)


def _build_auto_image_thread_name(attachment: discord.Attachment) -> str:
    base_name = os.path.splitext(str(getattr(attachment, "filename", "") or ""))[0].strip()
    safe_base = re.sub(r"\s+", "-", base_name)
    safe_base = re.sub(r"[^\w\u4e00-\u9fff-]+", "", safe_base)
    safe_base = safe_base.strip("-_") or "image"
    suffix = str(getattr(attachment, "id", ""))[-4:] or "img"
    thread_name = f"{AUTO_IMAGE_THREAD_NAME_PREFIX}-{safe_base}-{suffix}"
    return thread_name[:100]


async def choose_language_for_thread(thread: discord.Thread, author_id: int, timeout_seconds: int = 15) -> str:
    prompt = "🌐 請在此討論串選擇語言（15 秒內未選擇將預設 🇹🇼 繁體中文）"
    view = LanguageSelectView(author_id, timeout_seconds=timeout_seconds)
    choose_msg = await thread.send(prompt, view=view)
    lang, selected = await view.wait_for_choice()
    if not selected:
        lang = "zh"
        view._disable_all()
        try:
            await choose_msg.edit(content="⏱️ 15 秒未選擇，已使用預設語言：🇹🇼 **繁體中文**。", view=view)
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


async def _handle_image_impl(
    attachment,
    message,
    lang="zh",
    existing_thread: discord.Thread | None = None,
    vision_provider_override: str | None = None,
    google_model_override: str | None = None,
    openai_model_override: str | None = None,
    echo_original_image: bool = False,
):
    """
    ** 並發核心函數（stream 模式）**

    流程：
    1. 建立討論串並加入使用者
    2. 下載圖片
    3. AI 分析 + 爬蟲 → 立即傳送文字報告
    4. （非同步）生成海報 → 生成完成後補傳
    """
    thread = existing_thread
    if thread is None:
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
    else:
        await thread.send(
            _t(
                lang,
                f"🃏 收到圖片，分析語言：{LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '繁體中文')}**...",
                f"🃏 Image received. Analysis language: {LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, 'English')}**...",
                f"🃏 이미지를 받았습니다. 분석 언어: {LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '한국어')}**...",
                f"🃏 收到图片，分析语言：{LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '简体中文')}**..."
            )
        )

    # 立即傳送第一則訊息，提供即時回饋
    analyzing_msg = _t(
        lang,
        "🔍 正在分析圖片中，請稍候...",
        "🔍 Analyzing image, please wait...",
        "🔍 이미지를 분석 중입니다. 잠시만 기다려주세요...",
        "🔍 正在分析图片，请稍候..."
    )
    await thread.send(analyzing_msg)

    # 僅在特定模式（如 /mega）回貼原始圖片
    if echo_original_image:
        try:
            original_file = await attachment.to_file(use_cached=True)
            await thread.send(
                _t(
                    lang,
                    "🖼️ 你上傳的原始圖片：",
                    "🖼️ Your uploaded image:",
                    "🖼️ 업로드한 원본 이미지:",
                    "🖼️ 你上传的原始图片："
                ),
                file=original_file
            )
        except Exception as e:
            print(f"⚠️ 無法回貼原始圖片到討論串: {e}")

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
            result = await _process_single_image_compat(
                image_path=img_path,
                api_key=api_key,
                out_dir=card_out_dir,
                stream_mode=True,
                lang="zh",
                vision_provider_override=vision_provider_override,
                google_model_override=google_model_override,
                openai_model_override=openai_model_override,
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


async def handle_images_from_monitored_channel(message: discord.Message, attachments: list[discord.Attachment]):
    for idx, attachment in enumerate(attachments):
        thread: discord.Thread | None = None
        try:
            thread_name = _build_auto_image_thread_name(attachment)
            seed_message: discord.Message = message
            if idx == 0:
                thread = await message.create_thread(
                    name=thread_name,
                    auto_archive_duration=AUTO_IMAGE_THREAD_AUTO_ARCHIVE_MIN,
                )
            else:
                starter = await message.reply(f"🧵 正在為 `{attachment.filename}` 建立討論串...")
                seed_message = starter
                thread = await starter.create_thread(
                    name=thread_name,
                    auto_archive_duration=AUTO_IMAGE_THREAD_AUTO_ARCHIVE_MIN,
                )
                if not AUTO_IMAGE_THREAD_POST_NOTE:
                    try:
                        await seed_message.delete()
                    except Exception:
                        pass

            await thread.add_user(message.author)
            if idx == 0 and AUTO_IMAGE_THREAD_POST_NOTE:
                try:
                    await message.reply(f"🧵 已建立討論串：{thread.mention}")
                except Exception:
                    pass

            lang = await choose_language_for_thread(thread, message.author.id, timeout_seconds=15)
            await _handle_image_impl(attachment, message, lang=lang, existing_thread=thread)
        except Exception as e:
            print(f"❌ 自動監控流程失敗 (message={message.id}, attachment={getattr(attachment, 'filename', 'unknown')}): {e}", file=sys.stderr)
            if thread is not None:
                try:
                    await thread.send(
                        "❌ 自動建立討論串分析失敗，請稍後重試或使用 @bot 重新觸發。"
                    )
                except Exception:
                    pass


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


async def _run_ranking_sync_script(
    trigger: str,
    bootstrap_only: bool = False,
    full_rebuild: bool = False,
    push_only: bool = False,
    market_only: bool = False,
) -> bool:
    if not RANK_SYNC_ENABLE:
        return True
    if not os.path.exists(RANK_SYNC_SCRIPT_PATH):
        print(f"⚠️ Ranking sync script not found: {RANK_SYNC_SCRIPT_PATH}")
        return False

    rank_data_dir = _rank_sync_data_dir()
    cmd = [
        sys.executable,
        RANK_SYNC_SCRIPT_PATH,
        "--trigger",
        trigger,
        "--data-dir",
        rank_data_dir,
    ]
    if bootstrap_only:
        cmd.append("--bootstrap-only")
    if full_rebuild:
        cmd.append("--full-rebuild")
    if push_only:
        cmd.append("--push-only")
    if market_only:
        cmd.append("--market-only")

    print(
        "🕒 Ranking sync start "
        f"trigger={trigger} bootstrap_only={1 if bootstrap_only else 0} "
        f"full_rebuild={1 if full_rebuild else 0} push_only={1 if push_only else 0} market_only={1 if market_only else 0} "
        f"data_dir={rank_data_dir}"
    )
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
        print(f"❌ Ranking sync failed trigger={trigger} rc={proc.returncode}")
        return False
    print(f"✅ Ranking sync done trigger={trigger}")
    return True


def _rank_sync_should_weekly_full_rebuild_today() -> bool:
    if not RANK_SYNC_WEEKLY_FULL_ENABLE:
        return False
    now_local = datetime.now(_safe_tzinfo(RANK_SYNC_TZ))
    return int(now_local.weekday()) == int(RANK_SYNC_WEEKLY_FULL_WEEKDAY)


async def _market_auto_push_market_cache(reason: str, force: bool = False) -> bool:
    if not MARKET_AUTO_PUSH_ON_NEW:
        return False
    global MARKET_AUTO_PUSH_LOCK, MARKET_AUTO_PUSH_LAST_TS
    if MARKET_AUTO_PUSH_LOCK is None:
        MARKET_AUTO_PUSH_LOCK = asyncio.Lock()
    now = time.time()
    if (not force) and (now - MARKET_AUTO_PUSH_LAST_TS < MARKET_AUTO_PUSH_COOLDOWN_SEC):
        return False
    if MARKET_AUTO_PUSH_LOCK.locked() and not force:
        return False

    async with MARKET_AUTO_PUSH_LOCK:
        now2 = time.time()
        if (not force) and (now2 - MARKET_AUTO_PUSH_LAST_TS < MARKET_AUTO_PUSH_COOLDOWN_SEC):
            return False
        ok = await _run_ranking_sync_script(
            "market_auto_push",
            bootstrap_only=False,
            full_rebuild=False,
            push_only=True,
            market_only=True,
        )
        MARKET_AUTO_PUSH_LAST_TS = time.time()
        if ok:
            print(f"✅ market auto push done reason={reason}")
        else:
            print(f"⚠️ market auto push failed reason={reason}")
        return ok


@tasks.loop(time=RANK_SYNC_RUN_TIME)
async def ranking_daily_sync_job():
    full_rebuild_today = _rank_sync_should_weekly_full_rebuild_today()
    trigger = "weekly_full" if full_rebuild_today else "daily"
    await _run_ranking_sync_script(trigger, bootstrap_only=False, full_rebuild=full_rebuild_today)


@ranking_daily_sync_job.before_loop
async def _before_ranking_daily_sync_job():
    await client.wait_until_ready()


@tasks.loop(minutes=SUBPACK_MONITOR_INTERVAL_MINUTES)
async def subpack_odds_monitor_job():
    await _subpack_monitor_poll_once("interval")


@subpack_odds_monitor_job.before_loop
async def _before_subpack_odds_monitor_job():
    await client.wait_until_ready()


@tasks.loop(minutes=LEGENDARY_ALERT_MONITOR_INTERVAL_MINUTES)
async def legendary_alert_monitor_job():
    await _legendary_alert_monitor_poll_once("interval", send_test=False)


@legendary_alert_monitor_job.before_loop
async def _before_legendary_alert_monitor_job():
    await client.wait_until_ready()


@client.event
async def on_ready():
    global NFT_SYNC_STARTUP_DONE, NFT_SYNC_STARTUP_LOCK, RANK_SYNC_STARTUP_DONE, RANK_SYNC_STARTUP_LOCK
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
                bootstrap_ok = await _run_nft_sync_script("startup", bootstrap_only=True)
                if NFT_SYNC_COMPARE_ON_STARTUP:
                    compare_ok = await _run_nft_sync_script("startup_compare", bootstrap_only=False)
                    print(
                        "🧪 NFT startup compare done "
                        f"bootstrap_ok={1 if bootstrap_ok else 0} compare_ok={1 if compare_ok else 0}"
                    )
                NFT_SYNC_STARTUP_DONE = True
        if not nft_daily_sync_job.is_running():
            nft_daily_sync_job.start()

    if RANK_SYNC_ENABLE:
        if RANK_SYNC_STARTUP_LOCK is None:
            RANK_SYNC_STARTUP_LOCK = asyncio.Lock()
        async with RANK_SYNC_STARTUP_LOCK:
            if not RANK_SYNC_STARTUP_DONE:
                bootstrap_ok = await _run_ranking_sync_script("startup", bootstrap_only=True, full_rebuild=False)
                if RANK_SYNC_COMPARE_ON_STARTUP:
                    compare_ok = await _run_ranking_sync_script("startup_compare", bootstrap_only=False, full_rebuild=False)
                    print(
                        "🧪 Ranking startup compare done "
                        f"bootstrap_ok={1 if bootstrap_ok else 0} compare_ok={1 if compare_ok else 0}"
                    )
                RANK_SYNC_STARTUP_DONE = True
        if not ranking_daily_sync_job.is_running():
            ranking_daily_sync_job.start()

    if MARKET_LIVE_SYNC_ON_EVENT and MARKET_SOURCE_CHANNEL_ID > 0:
        asyncio.create_task(_market_live_sync_refresh("startup", force=True))
    if SUBPACK_MONITOR_ENABLED and SUBPACK_MONITOR_PACK_ID:
        asyncio.create_task(_subpack_monitor_poll_once("startup"))
        if not subpack_odds_monitor_job.is_running():
            subpack_odds_monitor_job.start()
            print(
                "✅ subpack monitor started "
                f"packId={SUBPACK_MONITOR_PACK_ID} interval={SUBPACK_MONITOR_INTERVAL_MINUTES}m "
                f"channel={SUBPACK_MONITOR_NOTIFY_CHANNEL_ID}"
            )
    if LEGENDARY_ALERT_MONITOR_ENABLED and LEGENDARY_ALERT_MONITOR_PACK_SLUG:
        asyncio.create_task(
            _legendary_alert_monitor_poll_once(
                "startup",
                send_test=LEGENDARY_ALERT_MONITOR_TEST_ON_STARTUP,
            )
        )
        if not legendary_alert_monitor_job.is_running():
            legendary_alert_monitor_job.start()
            print(
                "✅ legendary monitor started "
                f"pack={LEGENDARY_ALERT_MONITOR_PACK_SLUG} interval={LEGENDARY_ALERT_MONITOR_INTERVAL_MINUTES}m "
                f"channel={LEGENDARY_ALERT_MONITOR_CHANNEL_ID} test_on_startup={1 if LEGENDARY_ALERT_MONITOR_TEST_ON_STARTUP else 0}"
            )

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
            holdings_path = None
            sbt_rank_path = None
            async with POSTER_SEMAPHORE:
                rendered = await _render_wallet_profile_posters_bundle(
                    profile_ctx,
                    out_dir,
                    safe_name=safe_name,
                    render_profile=poster_enabled,
                    render_holdings=PROFILE_ENABLE_HOLDINGS_POSTER,
                )
                poster_path = rendered.get("profile")
                history_path = rendered.get("history")
                extremes_path = rendered.get("extremes")
                holdings_path = rendered.get("holdings")
                sbt_rank_path = rendered.get("sbt_rank")
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
            if holdings_path and os.path.exists(holdings_path):
                files.append(discord.File(holdings_path))
            if extremes_path and os.path.exists(extremes_path):
                files.append(discord.File(extremes_path))
            if sbt_rank_path and os.path.exists(sbt_rank_path):
                files.append(discord.File(sbt_rank_path))
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


class FlexPackModeSelect(discord.ui.Select):
    def __init__(self, selected_mode: str, lang: str):
        mode = "extreme" if selected_mode == "extreme" else "picked"
        options = [
            discord.SelectOption(
                label=_t(lang, "剛抽卡排版", "Recent Pull Layout", "최근 뽑기 레이아웃", "刚抽卡排版"),
                value="picked",
                default=(mode == "picked"),
            ),
            discord.SelectOption(
                label=_t(lang, "天堂與地獄", "Heaven and Hell", "천국과 지옥", "天堂与地狱"),
                value="extreme",
                default=(mode == "extreme"),
            ),
        ]
        super().__init__(
            placeholder=_t(lang, "1) 選擇模式", "1) Choose Mode", "1) 모드 선택", "1) 选择模式"),
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_mode = str(self.values[0] or "picked")
        await self.view.refresh(interaction)


class FlexPackPackSelect(discord.ui.Select):
    def __init__(self, options_data: list[dict], selected_value: str | None, lang: str):
        options = []
        selected_norm = str(selected_value or "").strip().lower()
        for item in options_data[:25]:
            val = str(item.get("value") or "").strip().lower()
            label = str(item.get("label") or "Pack")[:100]
            desc = str(item.get("description") or "")[:100] or None
            if not val:
                continue
            options.append(
                discord.SelectOption(
                    label=label,
                    value=val,
                    description=desc,
                    default=(val == selected_norm),
                )
            )
        if not options:
            options = [discord.SelectOption(label="No Pack Option", value="__none__", default=True)]
        super().__init__(
            placeholder=_t(lang, "2) 選擇卡包", "2) Choose Pack", "2) 팩 선택", "2) 选择卡包"),
            min_values=1,
            max_values=1,
            options=options,
            row=1,
        )
        if options and options[0].value == "__none__":
            self.disabled = True

    async def callback(self, interaction: discord.Interaction):
        value = str(self.values[0] or "").strip().lower()
        if value != "__none__":
            self.view.selected_pack_contract = value
        await self.view.refresh(interaction)


class FlexPackTemplateSelect(discord.ui.Select):
    def __init__(self, selected_count: int, lang: str):
        default_count = _clamp_flex_pack_card_count(selected_count)
        options = [
            discord.SelectOption(label="1", value="1", default=(default_count == 1)),
            discord.SelectOption(label="2", value="2", default=(default_count == 2)),
            discord.SelectOption(label="3", value="3", default=(default_count == 3)),
            discord.SelectOption(label="4", value="4", default=(default_count == 4)),
            discord.SelectOption(label="7", value="7", default=(default_count == 7)),
            discord.SelectOption(label="10", value="10", default=(default_count == 10)),
        ]
        super().__init__(
            placeholder=_t(lang, "3) 選擇排版數量", "3) Choose Layout Count", "3) 레이아웃 수 선택", "3) 选择排版数量"),
            min_values=1,
            max_values=1,
            options=options,
            row=2,
        )

    async def callback(self, interaction: discord.Interaction):
        self.view.selected_template = _clamp_flex_pack_card_count(_parse_int(self.values[0]))
        await self.view.refresh(interaction)


class FlexPackConfigView(discord.ui.View):
    def __init__(self, author_id: int, wallet: str, picker_data: dict, selected_lang: str):
        super().__init__(timeout=240)
        self.author_id = author_id
        self.wallet = wallet
        self.picker_data = picker_data or {}
        self.selected_lang = _profile_lang_from_locale(selected_lang)
        self.username = str((picker_data or {}).get("username") or "Unknown")

        self.selected_mode = "picked"
        self.selected_template = 10
        self.pack_options = list((picker_data or {}).get("pack_options") or [])
        self.pack_label_map = {
            str(x.get("value") or "").strip().lower(): str(x.get("full_label") or x.get("label") or "")
            for x in self.pack_options
        }
        first_pack = str((self.pack_options[0] or {}).get("value") or "").strip().lower() if self.pack_options else ""
        self.selected_pack_contract = first_pack
        self.bound_message: discord.Message | None = None

        self._rebuild_components()

    def _rebuild_components(self):
        self.clear_items()
        self.add_item(FlexPackModeSelect(self.selected_mode, self.selected_lang))
        self.add_item(FlexPackPackSelect(self.pack_options, self.selected_pack_contract, self.selected_lang))
        layout_select = FlexPackTemplateSelect(self.selected_template, self.selected_lang)
        # Keep layout selector visible/clickable in all modes to avoid UI controls disappearing after mode switch.
        # In extreme mode the selected layout value is ignored by generator logic.
        layout_select.disabled = False
        self.add_item(layout_select)
        self.generate.label = _t(self.selected_lang, "生成海報", "Generate Poster", "포스터 생성", "生成海报")[:80]
        self.add_item(self.generate)

    def bind_message(self, message: discord.Message):
        self.bound_message = message

    def _pack_count(self) -> int:
        pack_map = (self.picker_data or {}).get("pack_cards_map") if isinstance(self.picker_data, dict) else {}
        if not isinstance(pack_map, dict):
            return 0
        return len(pack_map.get(self.selected_pack_contract) or [])

    def render_message(self) -> str:
        pack_label = self.pack_label_map.get(self.selected_pack_contract) or _short_hex(self.selected_pack_contract)
        mode_label = (
            _t(self.selected_lang, "天堂與地獄", "Heaven and Hell", "천국과 지옥", "天堂与地狱")
            if self.selected_mode == "extreme"
            else _t(self.selected_lang, "剛抽卡排版", "Recent Pull Layout", "최근 뽑기 레이아웃", "刚抽卡排版")
        )
        layout_value = "-" if self.selected_mode == "extreme" else str(self.selected_template)
        return (
            f"🎛️ **{self.username}** · {_t(self.selected_lang, '卡包海報設定', 'Pack Poster Setup', '팩 포스터 설정', '卡包海报设置')}\n"
            f"{_t(self.selected_lang, '錢包', 'Wallet', '지갑', '钱包')}: `{self.wallet}`\n"
            f"{_t(self.selected_lang, '卡包', 'Pack', '팩', '卡包')}: **{pack_label}**\n"
            f"{_t(self.selected_lang, '模式', 'Mode', '모드', '模式')}: **{mode_label}**\n"
            f"{_t(self.selected_lang, '排版數量', 'Layout Count', '레이아웃 수', '排版数量')}: **{layout_value}**\n"
            f"{_t(self.selected_lang, '該包卡片數', 'Cards in Pack', '해당 팩 카드 수', '该包卡片数')}: **{self._pack_count()}**\n\n"
            f"{_t(self.selected_lang, '剛抽卡排版提供 1/2/3/4/7/10，卡片不足會自動 fallback。', 'Recent Pull Layout supports 1/2/3/4/7/10 and auto-fallback on insufficient cards.', '최근 뽑기 레이아웃은 1/2/3/4/7/10 지원, 카드 부족 시 자동 fallback.', '刚抽卡排版提供 1/2/3/4/7/10，卡片不足自动 fallback。')}"
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                _t(self.selected_lang, "只有建立者可以操作此面板。", "Only the creator can use this panel.", "생성자만 이 패널을 사용할 수 있습니다.", "只有创建者可以操作此面板。"),
                ephemeral=True,
            )
            return False
        return True

    async def refresh(self, interaction: discord.Interaction):
        self._rebuild_components()
        await interaction.response.edit_message(content=self.render_message(), view=self)

    def _disable_all(self):
        for child in self.children:
            child.disabled = True

    async def on_timeout(self):
        self._disable_all()
        if self.bound_message:
            try:
                await self.bound_message.edit(
                    content=_t(self.selected_lang, "⏰ 設定面板已逾時，請重新輸入 `/flex_pack`。", "⏰ Setup panel timed out. Run `/flex_pack` again.", "⏰ 설정 시간이 만료되었습니다. `/flex_pack`을 다시 실행하세요.", "⏰ 设置面板已超时，请重新输入 `/flex_pack`。"),
                    view=self,
                )
            except Exception:
                pass

    async def _run_generate(self, interaction: discord.Interaction):
        self._disable_all()
        await interaction.response.edit_message(
            content=_t(self.selected_lang, "⏳ 生成中...", "⏳ Generating...", "⏳ 생성 중...", "⏳ 生成中..."),
            view=self,
        )

        out_dir = tempfile.mkdtemp(prefix=f"tcg_flex_pack_{interaction.id}_")
        loop = asyncio.get_running_loop()
        try:
            payload = await loop.run_in_executor(
                None,
                lambda: _build_wallet_flex_pack_template_context(
                    self.wallet,
                    self.picker_data,
                    selected_pack_contract=self.selected_pack_contract,
                    mode=self.selected_mode,
                    card_count=self.selected_template,
                    profile_lang=self.selected_lang,
                ),
            )
            safe_name = f"{self.username}_{self.wallet[-6:]}_flex_pack"
            async with POSTER_SEMAPHORE:
                poster_path = await _render_wallet_profile_cardpack_pull_poster(payload, out_dir, safe_name=safe_name)

            requested = int(_parse_int(payload.get("requested_count")) or 0)
            selected = int(_parse_int(payload.get("selected_count")) or 0)
            pack_total = int(_parse_int(payload.get("pack_total_count")) or 0)
            pack_name = str(payload.get("pack_name") or _short_hex(self.selected_pack_contract))
            pack_pnl = _to_decimal(payload.get("pack_pnl_total"))
            pack_spent = _to_decimal(payload.get("pack_spent_total"))
            pack_value = _to_decimal(payload.get("pack_value_total"))
            mode_label = (
                _t(self.selected_lang, "天堂與地獄", "Heaven and Hell", "천국과 지옥", "天堂与地狱")
                if self.selected_mode == "extreme"
                else _t(self.selected_lang, "剛抽卡排版", "Recent Pull Layout", "최근 뽑기 레이아웃", "刚抽卡排版")
            )
            fallback_note = ""
            if self.selected_mode == "picked" and selected < requested:
                fallback_note = _t(
                    self.selected_lang,
                    f"\n⚠️ 排版 fallback：要求 {requested}，實際使用 {selected}",
                    f"\n⚠️ Layout fallback: requested {requested}, used {selected}",
                    f"\n⚠️ 레이아웃 fallback: 요청 {requested}, 실제 {selected}",
                    f"\n⚠️ 排版 fallback：要求 {requested}，实际使用 {selected}",
                )

            summary = (
                f"✅ **{self.username}** · {_t(self.selected_lang, '卡包海報已生成', 'Pack poster generated', '팩 포스터 생성 완료', '卡包海报已生成')}\n"
                f"{_t(self.selected_lang, '卡包', 'Pack', '팩', '卡包')}: **{pack_name}**\n"
                f"{_t(self.selected_lang, '模式', 'Mode', '모드', '模式')}: **{mode_label}**\n"
                f"{_t(self.selected_lang, '該包卡片數', 'Cards in Pack', '해당 팩 카드 수', '该包卡片数')}: **{pack_total}**\n"
                f"PnL: **{_format_usdt_currency(pack_pnl, signed=True)}** | "
                f"Spent: **{_format_usdt_currency(pack_spent)}** | "
                f"Value: **{_format_usdt_currency(pack_value)}**"
                f"{fallback_note}"
            )
            await interaction.followup.send(summary, file=discord.File(poster_path))
        except Exception as e:
            print(f"❌ FlexPackConfigView 生成失敗: {e}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            await interaction.followup.send(f"❌ {_t(self.selected_lang, '生成失敗', 'Generation failed', '생성 실패', '生成失败')}: {e}")
        finally:
            shutil.rmtree(out_dir, ignore_errors=True)
            self.stop()

    @discord.ui.button(label="生成海報", style=discord.ButtonStyle.primary, row=3)
    async def generate(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run_generate(interaction)


class MarketListingSelect(discord.ui.Select):
    def __init__(self, parent_view: "MarketBrowserView", items: list[dict]):
        self.parent_view = parent_view
        lang = self.parent_view.lang
        options = []
        for item in items[:25]:
            title = str(item.get("title") or "Untitled").strip() or "Untitled"
            label = title[:100]
            side = str(item.get("side") or "-")
            price = _market_item_price_text(item)
            side_label = _market_side_label(side, lang)
            description = f"{side_label}｜💵{price}" if price else side_label
            description = description[:100]
            options.append(
                discord.SelectOption(
                    label=label,
                    value=str(item.get("key") or "")[:100],
                    description=description or None,
                )
            )
        super().__init__(
            placeholder=_t(
                lang,
                "選擇卡片查看圖片與直達連結",
                "Select a card to view image and direct link",
                "카드를 선택해 이미지와 바로가기 확인",
                "选择卡片查看图片与直达链接",
            ),
            min_values=1,
            max_values=1,
            options=options or [discord.SelectOption(label=_t(lang, "目前無卡片", "No cards", "카드 없음", "暂无卡片"), value="__none__")],
            row=2,
        )
        if not options:
            self.disabled = True

    async def callback(self, interaction: discord.Interaction):
        lang = self.parent_view.lang
        selected = self.values[0]
        if selected == "__none__":
            await interaction.response.send_message(
                _t(lang, "目前這頁沒有可選擇的卡片。", "No selectable cards on this page.", "이 페이지에는 선택 가능한 카드가 없습니다.", "当前页没有可选择的卡片。"),
                ephemeral=True,
            )
            return

        item = self.parent_view.key_to_item.get(selected)
        if not item:
            await interaction.response.send_message(
                _t(lang, "找不到該卡片資料，請按「重新整理」。", "Card data not found. Please click Refresh.", "카드 데이터를 찾을 수 없습니다. 새로고침을 눌러주세요.", "找不到该卡片数据，请按“刷新”。"),
                ephemeral=True,
            )
            return

        side = str(item.get("side") or "-")
        side_label = _market_side_label(side, lang)
        title = str(item.get("title") or "Untitled").strip() or "Untitled"
        thread_url = str(item.get("thread_url") or "").strip()
        image_url = _market_item_image_url(item)
        local_image_path = _market_item_local_image_path(item)
        summary = str(item.get("summary") or "").strip()
        price_text = _market_item_price_text(item)
        ts = int(item.get("created_at_ts") or 0)

        if price_text:
            desc_lines = [f"💵 **{price_text}**"]
        else:
            desc_lines = [f"💵 **{_t(lang, '未標示', 'N/A', '미표시', '未标示')}**"]
        desc_lines.append(f"{_t(lang, '類型', 'Type', '유형', '类型')}：**{side_label}**")
        if ts > 0:
            desc_lines.append(f"{_t(lang, '時間', 'Time', '시간', '时间')}：<t:{ts}:f>（<t:{ts}:R>）")
        if thread_url:
            desc_lines.append(f"[{_t(lang, '直達討論串', 'Open Thread', '스레드 바로가기', '直达讨论串')}]({thread_url})")
        if summary:
            desc_lines.append("")
            desc_lines.append(f"```text\n{summary[:600]}\n```")

        embed = discord.Embed(
            title=title[:256],
            description="\n".join(desc_lines)[:4000],
            color=0x2ECC71 if side == "WTS" else 0x3498DB,
        )
        if image_url:
            embed.set_image(url=image_url)

        detail_view = discord.ui.View()
        if thread_url:
            detail_view.add_item(discord.ui.Button(label="直達討論串", url=thread_url))
        if image_url:
            await interaction.response.send_message(embed=embed, view=detail_view, ephemeral=True)
            return
        if local_image_path:
            filename = _market_local_image_filename(item, local_image_path)
            file = discord.File(local_image_path, filename=filename)
            embed.set_image(url=f"attachment://{filename}")
            await interaction.response.send_message(embed=embed, view=detail_view, file=file, ephemeral=True)
            return
        await interaction.response.send_message(embed=embed, view=detail_view, ephemeral=True)


class MarketBrowserView(discord.ui.View):
    def __init__(self, author_id: int, listings: list[dict], meta: dict | None = None, lang: str = "zh"):
        super().__init__(timeout=300)
        self.author_id = int(author_id)
        self.listings = [item for item in list(listings or []) if _market_item_image_url(item)]
        self.meta = dict(meta or {})
        self.lang = str(lang or "zh")
        self.selected_side = "WTS"
        self.page = 0
        self.page_size = 10
        self.bound_message: discord.Message | None = None
        self.key_to_item: dict[str, dict] = {}
        self._rebuild_components()

    def bind_message(self, message: discord.Message):
        self.bound_message = message

    def _count_by_side(self, side: str) -> int:
        target = str(side or "").upper()
        return sum(1 for item in self.listings if str(item.get("side") or "").upper() == target)

    def _filtered(self) -> list[dict]:
        target = str(self.selected_side or "").upper()
        return [item for item in self.listings if str(item.get("side") or "").upper() == target]

    def _total_pages(self) -> int:
        total = len(self._filtered())
        return max(1, (total + self.page_size - 1) // self.page_size)

    def _page_items(self) -> list[dict]:
        rows = self._filtered()
        total_pages = self._total_pages()
        if self.page >= total_pages:
            self.page = total_pages - 1
        if self.page < 0:
            self.page = 0
        start = self.page * self.page_size
        end = start + self.page_size
        return rows[start:end]

    def render_message(self) -> str:
        active_threads = int(self.meta.get("active_thread_count") or 0)
        source_channel_id = int(self.meta.get("source_channel_id") or MARKET_SOURCE_CHANNEL_ID)
        filtered = self._filtered()
        total_pages = self._total_pages()

        lines = [
            "📦 **Market Browser**",
            _t(self.lang, f"來源頻道：<#{source_channel_id}>（只看未封存討論串）", f"Source: <#{source_channel_id}> (unarchived threads only)", f"소스 채널: <#{source_channel_id}> (미보관 스레드만)", f"来源频道：<#{source_channel_id}>（仅看未封存讨论串）"),
            _t(self.lang, f"目前索引：**{len(self.listings)}** 筆（最近 {MARKET_RECENT_LIMIT}）｜ Active Threads：**{active_threads}**", f"Indexed: **{len(self.listings)}** (latest {MARKET_RECENT_LIMIT}) | Active Threads: **{active_threads}**", f"인덱스: **{len(self.listings)}** (최근 {MARKET_RECENT_LIMIT}) | 활성 스레드: **{active_threads}**", f"当前索引：**{len(self.listings)}** 笔（最近 {MARKET_RECENT_LIMIT}）｜Active Threads：**{active_threads}**"),
            _t(self.lang, f"篩選：**{_market_side_label(self.selected_side, self.lang)}**（賣={self._count_by_side('WTS')} / 買={self._count_by_side('WTB')}）｜ 第 **{self.page + 1}/{total_pages}** 頁", f"Filter: **{_market_side_label(self.selected_side, self.lang)}** (Sell={self._count_by_side('WTS')} / Buy={self._count_by_side('WTB')}) | Page **{self.page + 1}/{total_pages}**", f"필터: **{_market_side_label(self.selected_side, self.lang)}** (판매={self._count_by_side('WTS')} / 구매={self._count_by_side('WTB')}) | **{self.page + 1}/{total_pages}** 페이지", f"筛选：**{_market_side_label(self.selected_side, self.lang)}**（卖={self._count_by_side('WTS')} / 买={self._count_by_side('WTB')}）｜第 **{self.page + 1}/{total_pages}** 页"),
            "",
        ]

        if not filtered:
            lines.append(_t(self.lang, "目前沒有符合的卡片。", "No matching cards.", "조건에 맞는 카드가 없습니다.", "目前没有符合的卡片。"))
        else:
            lines.append(_t(self.lang, "卡片已用嵌入卡片顯示於下方（小圖預覽 + 價格 + 連結）。", "Cards are shown below as embeds (thumbnail + price + link).", "아래에 임베드 카드로 표시됩니다 (썸네일 + 가격 + 링크).", "卡片已以下方嵌入卡片显示（小图预览 + 价格 + 链接）。"))

        lines.append("")
        lines.append(_t(self.lang, "提示：按「重新整理」可即時更新列表；點嵌入圖片可放大檢視。", "Tip: Click Refresh to update the list now; click an embedded image to zoom in.", "팁: 새로고침으로 목록을 즉시 갱신할 수 있고, 임베드 이미지를 눌러 확대해서 볼 수 있습니다.", "提示：按“刷新”可即时更新列表；点击嵌入图片可放大查看。"))
        text = "\n".join(lines)
        # Discord message content hard limit is 2000.
        if len(text) > 1950:
            text = text[:1930].rstrip() + "\n…"
        return text

    def render_embeds(self) -> list[discord.Embed]:
        embeds: list[discord.Embed] = []
        start_idx = self.page * self.page_size
        for idx, item in enumerate(self._page_items(), start=start_idx + 1):
            side = str(item.get("side") or "-")
            side_label = _market_side_label(side, self.lang)
            title = str(item.get("title") or "Untitled").strip() or "Untitled"
            thread_url = str(item.get("thread_url") or "").strip()
            ts = int(item.get("created_at_ts") or 0)
            price_text = _market_item_price_text(item) or "未標示"
            top_line = f"💵 {price_text}｜{side_label}"
            desc_lines = [f"{idx}. {title[:220]}"]
            if ts > 0:
                desc_lines.append(f"<t:{ts}:R>")
            if thread_url:
                desc_lines.append(f"[{_t(self.lang, '直達討論串', 'Open Thread', '스레드 바로가기', '直达讨论串')}]({thread_url})")
            embed = discord.Embed(
                title=top_line[:256],
                description="\n".join(desc_lines)[:1024],
                color=0x2ECC71 if side == "WTS" else 0x3498DB,
            )
            image_url = _market_item_image_url(item)
            if image_url:
                # Medium preview size: larger than icon, smaller than full image.
                embed.set_thumbnail(url=image_url)
            embeds.append(embed)
            if len(embeds) >= 10:
                break
        return embeds

    def _rebuild_components(self):
        self.clear_items()
        self.key_to_item = {}
        page_items = self._page_items()
        for item in page_items:
            key = str(item.get("key") or "").strip()
            if key:
                self.key_to_item[key] = item

        wts_btn = discord.ui.Button(
            label=f"{_market_side_label('WTS', self.lang)} ({self._count_by_side('WTS')})",
            style=discord.ButtonStyle.primary if self.selected_side == "WTS" else discord.ButtonStyle.secondary,
            row=0,
        )
        wtb_btn = discord.ui.Button(
            label=f"{_market_side_label('WTB', self.lang)} ({self._count_by_side('WTB')})",
            style=discord.ButtonStyle.primary if self.selected_side == "WTB" else discord.ButtonStyle.secondary,
            row=0,
        )

        prev_btn = discord.ui.Button(
            label=_t(self.lang, "上一頁", "Prev", "이전", "上一页"),
            style=discord.ButtonStyle.secondary,
            row=1,
            disabled=(self.page <= 0),
        )
        next_btn = discord.ui.Button(
            label=_t(self.lang, "下一頁", "Next", "다음", "下一页"),
            style=discord.ButtonStyle.secondary,
            row=1,
            disabled=(self.page + 1 >= self._total_pages()),
        )
        refresh_btn = discord.ui.Button(label=_t(self.lang, "重新整理", "Refresh", "새로고침", "刷新"), style=discord.ButtonStyle.success, row=1)

        async def _on_wts(interaction: discord.Interaction):
            self.selected_side = "WTS"
            self.page = 0
            self._rebuild_components()
            await interaction.response.edit_message(content=self.render_message(), embeds=self.render_embeds(), view=self)

        async def _on_wtb(interaction: discord.Interaction):
            self.selected_side = "WTB"
            self.page = 0
            self._rebuild_components()
            await interaction.response.edit_message(content=self.render_message(), embeds=self.render_embeds(), view=self)

        async def _on_prev(interaction: discord.Interaction):
            self.page = max(0, self.page - 1)
            self._rebuild_components()
            await interaction.response.edit_message(content=self.render_message(), embeds=self.render_embeds(), view=self)

        async def _on_next(interaction: discord.Interaction):
            self.page = min(self._total_pages() - 1, self.page + 1)
            self._rebuild_components()
            await interaction.response.edit_message(content=self.render_message(), embeds=self.render_embeds(), view=self)

        async def _on_refresh(interaction: discord.Interaction):
            await interaction.response.defer()
            try:
                # Force one live fetch attempt, then render from newest cache snapshot.
                await _market_live_sync_refresh("manual_refresh", force=True)
                cached_items, cached_meta = _market_load_cached_index(limit=MARKET_RECENT_LIMIT)
                if bool(cached_meta.get("from_cache")) and (cached_items or os.path.exists(_market_index_path())):
                    listings = cached_items
                    meta = cached_meta
                else:
                    listings, meta = await _market_collect_listings()
                self.listings = [item for item in list(listings or []) if _market_item_image_url(item)]
                self.meta = dict(meta or {})
            except Exception as e:
                print(f"⚠️ market refresh failed: {e}", file=sys.stderr)
            self.page = 0
            if self.selected_side not in ("WTS", "WTB"):
                self.selected_side = "WTS"
            self._rebuild_components()
            if self.bound_message:
                await self.bound_message.edit(content=self.render_message(), embeds=self.render_embeds(), view=self)
            else:
                await interaction.edit_original_response(content=self.render_message(), embeds=self.render_embeds(), view=self)

        wts_btn.callback = _on_wts
        wtb_btn.callback = _on_wtb
        prev_btn.callback = _on_prev
        next_btn.callback = _on_next
        refresh_btn.callback = _on_refresh

        self.add_item(wts_btn)
        self.add_item(wtb_btn)
        self.add_item(prev_btn)
        self.add_item(next_btn)
        self.add_item(refresh_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                _t(self.lang, "只有 `/market` 指令發起者可以操作這個面板。", "Only the `/market` requester can use this panel.", "`/market` 요청자만 이 패널을 사용할 수 있습니다.", "只有 `/market` 指令发起者可以操作这个面板。"),
                ephemeral=True,
            )
            return False
        return True

    def _disable_all(self):
        for child in self.children:
            child.disabled = True

    async def on_timeout(self):
        self._disable_all()
        if self.bound_message:
            try:
                await self.bound_message.edit(
                    content=_t(self.lang, "⏰ `/market` 面板已逾時，請重新輸入 `/market`。", "⏰ `/market` panel timed out. Please run `/market` again.", "⏰ `/market` 패널이 만료되었습니다. `/market`을 다시 실행하세요.", "⏰ `/market` 面板已超时，请重新输入 `/market`。"),
                    view=self,
                )
            except Exception:
                pass


@tree.command(name="flex_pack", description="指定卡包生成 Flex 海報（剛抽排版 / 天堂地獄）")
@app_commands.describe(address="錢包地址（可留空使用已儲存的預設地址）")
async def flex_pack(interaction: discord.Interaction, address: str = None):
    if not address:
        address = _get_user_default_wallet(str(interaction.user.id))
        if not address:
            await interaction.response.send_message(
                "❌ 請輸入錢包地址，或先使用 `/settings wallet:0x...` 設定預設地址。",
                ephemeral=True,
            )
            return
    
    wallet = _normalize_wallet_address(address)
    if not wallet:
        await interaction.response.send_message(
            "❌ 錢包地址格式錯誤，請輸入 `0x` 開頭且長度 42 的地址。",
            ephemeral=True,
        )
        return

    try:
        await interaction.response.send_message("🎛️ 正在建立卡包 Flex 海報設定討論串...", ephemeral=False)
        resp = await interaction.original_response()
    except discord.NotFound:
        channel = interaction.channel
        if channel is None:
            print("❌ /flex_pack 互動已失效且找不到可用頻道。", file=sys.stderr)
            return
        resp = await channel.send("🎛️ 正在建立卡包 Flex 海報設定討論串...")

    thread = await resp.create_thread(name="卡包 Flex 海報設定", auto_archive_duration=60)
    await thread.add_user(interaction.user)

    default_lang = "zh"
    lang_view = LanguageSelectView(interaction.user.id, timeout_seconds=20)
    lang_msg = await thread.send(_profile_wizard_texts(default_lang)["lang_prompt"], view=lang_view)
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
            _build_wallet_flex_pack_picker_data,
            wallet,
            profile_lang,
        )
        if not (picker_data.get("pack_options") or []):
            await thread.send("⚠️ 這個地址目前找不到可用的卡包抽卡紀錄。")
            return
        view = FlexPackConfigView(interaction.user.id, wallet, picker_data, profile_lang)
        msg = await thread.send(view.render_message(), view=view)
        view.bind_message(msg)
    except Exception as e:
        print(f"❌ /flex_pack 失敗: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        await thread.send(f"❌ 生成失敗：{e}")


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


@tree.command(name="mega", description="使用 OpenAI GPT-5.4 直接分析卡片並生成報告")
async def mega(interaction: discord.Interaction, image: discord.Attachment, lang: str = "zh"):
    if not any(image.filename.lower().endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".webp"]):
        await interaction.response.send_message("❌ 請上傳有效的圖片", ephemeral=True)
        return

    openai_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not openai_key:
        await interaction.response.send_message(
            "❌ `/mega` 需要 `OPENAI_API_KEY`。請先在伺服器環境變數設定。",
            ephemeral=True,
        )
        return

    await interaction.response.send_message("🔍 收到圖片，正在建立 /mega 分析討論串...", ephemeral=False)
    resp = await interaction.original_response()
    thread_name = "卡片分析 (/mega)" if lang == "zh" else "Card Analysis (/mega)"
    thread = await resp.create_thread(name=thread_name, auto_archive_duration=60)
    await thread.add_user(interaction.user)

    model_name = (os.getenv("MEGA_OPENAI_MODEL") or "gpt-5.4").strip()
    await thread.send(f"🧠 `/mega` 固定模型：`{model_name}`")
    await _handle_image_impl(
        image,
        resp,
        lang=lang,
        existing_thread=thread,
        vision_provider_override="openai",
        openai_model_override=model_name,
        echo_original_image=True,
    )


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
            result = await _process_single_image_compat(
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


@tree.command(name="market", description="瀏覽市場討論串（WTS / WTB）")
async def market(interaction: discord.Interaction):
    thread: discord.Thread | None = None
    lang = "zh"
    try:
        lang_view = LanguageSelectView(interaction.user.id, timeout_seconds=20)
        await interaction.response.send_message(
            "🌐 請先選擇語言 / Please choose language",
            view=lang_view,
            ephemeral=False,
        )
        starter = await interaction.original_response()
        picked_lang, selected = await lang_view.wait_for_choice()
        lang = picked_lang if selected and picked_lang else "zh"
        lang_view._disable_all()
        try:
            if selected:
                await starter.edit(
                    content=_t(
                        lang,
                        f"✅ 語言已選擇：{LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '繁體中文')}**\n📦 正在載入 Market Browser...",
                        f"✅ Language selected: {LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, 'English')}**\n📦 Loading Market Browser...",
                        f"✅ 언어 선택 완료: {LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '한국어')}**\n📦 Market Browser를 불러오는 중...",
                        f"✅ 语言已选择：{LANG_FLAGS.get(lang, '🌐')} **{LANG_LABELS.get(lang, '简体中文')}**\n📦 正在载入 Market Browser...",
                    ),
                    view=lang_view,
                )
            else:
                await starter.edit(
                    content=_t(
                        lang,
                        "⏱️ 20 秒未選擇，已使用預設語言：🇹🇼 **繁體中文**。\n📦 正在載入 Market Browser...",
                        "⏱️ No selection in 20s. Default language set to 🇹🇼 **Traditional Chinese**.\n📦 Loading Market Browser...",
                        "⏱️ 20초 내 미선택으로 기본 언어 🇹🇼 **번체 중국어**를 사용합니다.\n📦 Market Browser를 불러오는 중...",
                        "⏱️ 20 秒未选择，已使用默认语言：🇹🇼 **繁体中文**。\n📦 正在载入 Market Browser...",
                    ),
                    view=lang_view,
                )
        except Exception:
            pass

        if isinstance(interaction.channel, discord.Thread):
            thread = interaction.channel
        else:
            channel = interaction.channel
            if channel is None:
                print("❌ /market 互動已失效且找不到可用頻道。", file=sys.stderr)
                return

            thread_name = f"market-{datetime.now().strftime('%m%d-%H%M')}"
            try:
                thread = await starter.create_thread(name=thread_name, auto_archive_duration=60)
            except Exception as e:
                print(f"❌ /market 建立討論串失敗: {e}", file=sys.stderr)
                await starter.reply(f"❌ 建立討論串失敗：{e}")
                return
            try:
                await thread.add_user(interaction.user)
            except Exception:
                pass

        if thread is None:
            raise RuntimeError("market thread is not available")

        cached_items, cached_meta = _market_load_cached_index(limit=MARKET_RECENT_LIMIT)
        if bool(cached_meta.get("from_cache")) and (cached_items or os.path.exists(_market_index_path())):
            listings = list(cached_items or [])
            meta = dict(cached_meta or {})
            error = str(meta.get("error") or "").strip()
            asyncio.create_task(_market_live_sync_refresh("market_open"))
        else:
            listings, meta = await _market_collect_listings()
            error = str(meta.get("error") or "").strip()

        if error:
            if bool(meta.get("used_cache_fallback")):
                updated_at = str(meta.get("updated_at") or "").strip() or "-"
                await thread.send(
                    _t(
                        lang,
                        "⚠️ 讀取 market 來源失敗，已改用本地快取。\n"
                        f"Error: `{error}`\n"
                        f"Cache Updated: `{updated_at}`\n"
                        f"Cache Path: `{meta.get('index_path') or _market_index_path()}`",
                        "⚠️ Failed to read market source. Using local cache.\n"
                        f"Error: `{error}`\n"
                        f"Cache Updated: `{updated_at}`\n"
                        f"Cache Path: `{meta.get('index_path') or _market_index_path()}`",
                        "⚠️ 마켓 소스 읽기에 실패하여 로컬 캐시를 사용합니다.\n"
                        f"Error: `{error}`\n"
                        f"Cache Updated: `{updated_at}`\n"
                        f"Cache Path: `{meta.get('index_path') or _market_index_path()}`",
                        "⚠️ 读取 market 来源失败，已改用本地缓存。\n"
                        f"Error: `{error}`\n"
                        f"Cache Updated: `{updated_at}`\n"
                        f"Cache Path: `{meta.get('index_path') or _market_index_path()}`",
                    )
                )
            else:
                await thread.send(
                    _t(
                        lang,
                        f"⚠️ 讀取 market 來源時發生問題：`{error}`",
                        f"⚠️ Error reading market source: `{error}`",
                        f"⚠️ 마켓 소스 읽기 오류: `{error}`",
                        f"⚠️ 读取 market 来源时发生问题：`{error}`",
                    )
                )

        view = MarketBrowserView(interaction.user.id, listings, meta=meta, lang=lang)
        panel_msg = await thread.send(view.render_message(), embeds=view.render_embeds(), view=view)
        view.bind_message(panel_msg)
    except Exception as e:
        print(f"❌ /market 失敗: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        err_text = _t(
            lang,
            f"❌ `/market` 執行失敗：{e}",
            f"❌ `/market` failed: {e}",
            f"❌ `/market` 실행 실패: {e}",
            f"❌ `/market` 执行失败：{e}",
        )
        if thread is not None:
            try:
                await thread.send(err_text)
                return
            except Exception:
                pass
        try:
            if interaction.response.is_done():
                await interaction.followup.send(err_text, ephemeral=True)
            else:
                await interaction.response.send_message(err_text, ephemeral=True)
            return
        except Exception:
            pass
        channel = interaction.channel
        if channel is not None:
            try:
                await channel.send(err_text)
            except Exception:
                pass


@tree.command(name="market_bootstrap", description="一次性全頻道掃描 market thread（含封存）")
@app_commands.describe(
    force="保留參數（目前每次都會完整重掃，可忽略）",
    push_backup="完成後是否立即觸發一次備份推送到資料 Git（預設 true）",
)
async def market_bootstrap(
    interaction: discord.Interaction,
    force: bool = False,
    push_backup: bool = True,
):
    await interaction.response.send_message(
        "⏳ 正在執行 market 全掃（每次都完整重掃，含封存討論串），完成後會回報索引路徑...",
        ephemeral=True,
    )
    started_at = datetime.now(timezone.utc)
    listings, meta = await _market_collect_listings(
        include_archived=True,
        limit=None,
        archived_limit=None,
    )
    error = str(meta.get("error") or "").strip()
    if error:
        await interaction.followup.send(
            f"❌ `market_bootstrap` 失敗：`{error}`",
            ephemeral=True,
        )
        return

    # 強制再寫一次索引，確保這次全掃內容一定落地到 market_index.json。
    index_write_ok, index_write_error, forced_index_path, forced_updated_at = _market_write_index_file(
        listings=listings,
        source_channel_id=int(meta.get("source_channel_id") or MARKET_SOURCE_CHANNEL_ID),
        include_archived=True,
        scanned_thread_count=int(meta.get("scanned_thread_count") or 0),
        active_thread_count=int(meta.get("active_thread_count") or 0),
        recent_limit="all",
    )
    if not index_write_ok:
        await interaction.followup.send(
            f"❌ `market_bootstrap` 失敗：索引寫入失敗 `{index_write_error}`\nPath: `{forced_index_path}`",
            ephemeral=True,
        )
        return

    state_payload: dict[str, object] = {
        "done": True,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at.isoformat(),
        "forced": bool(force),
        "mode": "always_rescan",
        "source_channel_id": int(meta.get("source_channel_id") or MARKET_SOURCE_CHANNEL_ID),
        "include_archived": True,
        "scanned_thread_count": int(meta.get("scanned_thread_count") or 0),
        "listing_count": len(listings),
        "index_path": str(forced_index_path or meta.get("index_path") or _market_index_path()),
        "images_dir": str(meta.get("images_dir") or _market_cache_images_dir()),
        "index_updated_at": str(forced_updated_at or meta.get("updated_at") or ""),
    }
    _market_save_bootstrap_state(state_payload)

    summary_msg = (
        "✅ `market_bootstrap` 完成（本指令每次都會完整重掃）。\n"
        f"Scanned Threads: `{state_payload['scanned_thread_count']}`\n"
        f"Matched Listings: `{state_payload['listing_count']}`\n"
        f"Index: `{state_payload['index_path']}`\n"
        f"Images: `{state_payload['images_dir']}`"
    )
    if not push_backup:
        await interaction.followup.send(summary_msg, ephemeral=True)
        return

    await interaction.followup.send(summary_msg + "\n⏳ 正在觸發備份推送...", ephemeral=True)
    backup_ok = await _run_ranking_sync_script(
        "market_bootstrap",
        bootstrap_only=False,
        full_rebuild=False,
        push_only=True,
        market_only=True,
    )
    await interaction.followup.send(
        (
            "✅ 備份推送流程已完成（請到資料 repo 檢查最新 commit）。"
            if backup_ok
            else "⚠️ 備份推送失敗，請檢查 BACKUP_GIT_ENABLED / BACKUP_GIT_REPO 與伺服器網路。"
        ),
        ephemeral=True,
    )


@tree.command(name="market_push_backup", description="僅推送 market 快取到資料 Git（不重掃 market）")
async def market_push_backup(interaction: discord.Interaction):
    await interaction.response.send_message(
        "⏳ 正在執行 market 備份推送（push-only）...",
        ephemeral=True,
    )
    backup_ok = await _run_ranking_sync_script(
        "market_manual_push",
        bootstrap_only=False,
        full_rebuild=False,
        push_only=True,
        market_only=True,
    )
    await interaction.followup.send(
        (
            "✅ market 備份推送完成。"
            if backup_ok
            else "⚠️ 備份推送失敗，請檢查 BACKUP_GIT_ENABLED / BACKUP_GIT_REPO 與伺服器網路。"
        ),
        ephemeral=True,
    )


@tree.command(name="ask", description="提交意見回饋（Bug / 新功能）")
@app_commands.describe(
    kind="回饋類型",
    content="請輸入 bug 描述或功能需求（最多 1200 字）",
    screenshot="可選：附上圖片截圖",
)
@app_commands.choices(
    kind=[
        app_commands.Choice(name="🐞 Bug 回報", value="bug"),
        app_commands.Choice(name="✨ 新功能建議", value="feature"),
        app_commands.Choice(name="💬 其他意見", value="other"),
    ]
)
async def ask(
    interaction: discord.Interaction,
    kind: app_commands.Choice[str],
    content: str,
    screenshot: discord.Attachment | None = None,
):
    feedback_text = str(content or "").strip()
    if len(feedback_text) < 3:
        await interaction.response.send_message(
            "❌ 內容太短，請至少輸入 3 個字。",
            ephemeral=True,
        )
        return
    if len(feedback_text) > 1200:
        await interaction.response.send_message(
            "❌ 內容太長，請控制在 1200 字以內。",
            ephemeral=True,
        )
        return

    screenshot_url = ""
    if screenshot is not None:
        ctype = str(getattr(screenshot, "content_type", "") or "").strip().lower()
        if ctype and not ctype.startswith("image/"):
            await interaction.response.send_message(
                "❌ `screenshot` 只支援圖片檔案。",
                ephemeral=True,
            )
            return
        screenshot_url = str(getattr(screenshot, "url", "") or "").strip()

    kind_value = str(getattr(kind, "value", "other") or "other").strip().lower()
    if kind_value not in _ASK_KIND_LABELS:
        kind_value = "other"
    kind_label = _ASK_KIND_LABELS.get(kind_value, _ASK_KIND_LABELS["other"])

    now = datetime.now(timezone.utc)
    feedback_id = f"ask_{now.strftime('%Y%m%d%H%M%S')}_{int(interaction.id)}"
    guild_id = getattr(interaction, "guild_id", None)
    channel_id = getattr(interaction, "channel_id", None)
    jump_url = ""
    if guild_id and channel_id:
        jump_url = f"https://discord.com/channels/{guild_id}/{channel_id}"

    entry: dict[str, object] = {
        "feedback_id": feedback_id,
        "created_at": now.isoformat(),
        "kind": kind_value,
        "content": feedback_text,
        "screenshot_url": screenshot_url,
        "user_id": str(getattr(interaction.user, "id", "unknown")),
        "username": str(getattr(interaction.user, "name", "") or ""),
        "display_name": str(getattr(interaction.user, "display_name", "") or ""),
        "guild_id": str(guild_id) if guild_id is not None else "",
        "channel_id": str(channel_id) if channel_id is not None else "",
        "jump_url": jump_url,
    }

    if ASK_FEEDBACK_SAVE_ENABLED:
        save_ok, save_info = _append_ask_feedback(entry)
        if not save_ok:
            await interaction.response.send_message(
                f"❌ 儲存回饋失敗：`{save_info}`",
                ephemeral=True,
            )
            return

    forward_ok, forward_err = await _forward_ask_feedback(entry)
    if (
        not forward_ok
        and int(ASK_FEEDBACK_CHANNEL_ID or 0) > 0
        and not ASK_FEEDBACK_SAVE_ENABLED
    ):
        await interaction.response.send_message(
            f"❌ 回饋送出失敗：`{str(forward_err)[:180]}`",
            ephemeral=True,
        )
        return

    lines = [
        "✅ 已收到你的回饋，感謝幫忙改善 tcg value。",
        f"ID: `{feedback_id}`",
        f"類型: `{kind_label}`",
        "儲存: `已關閉`" if not ASK_FEEDBACK_SAVE_ENABLED else f"儲存: `{save_info}`",
    ]
    if int(ASK_FEEDBACK_CHANNEL_ID or 0) > 0:
        if not forward_ok:
            if ASK_FEEDBACK_SAVE_ENABLED:
                lines.append(f"⚠️ 已儲存成功，但推送頻道失敗：`{str(forward_err)[:180]}`")
            else:
                lines.append(f"⚠️ 推送頻道失敗：`{str(forward_err)[:180]}`")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)


@tree.command(name="settings", description="設定你的預設錢包地址")
@app_commands.describe(wallet="錢包地址（0x 開頭），留空則顯示目前設定")
async def settings(interaction: discord.Interaction, wallet: str = None):
    user_id = str(interaction.user.id)
    
    if wallet is None:
        current = _get_user_default_wallet(user_id)
        if current:
            await interaction.response.send_message(
                f"✅ 你的預設錢包地址：\n`{current}`\n\n要更新請重新輸入 `/settings wallet:0x...`",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "📝 你尚未設定預設錢包地址。\n\n請使用 `/settings wallet:0x...` 來設定。",
                ephemeral=True,
            )
        return
    
    addr = _normalize_wallet_address(wallet)
    if not addr:
        await interaction.response.send_message(
            "❌ 錢包地址格式錯誤，請輸入 `0x` 開頭且長度 42 的地址。",
            ephemeral=True,
        )
        return
    
    if _save_user_settings(user_id, addr):
        await interaction.response.send_message(
            f"✅ 已儲存你的預設錢包地址：\n`{addr}`",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            "❌ 儲存失敗，請稍後再試。",
            ephemeral=True,
        )


@tree.command(name="profile", description="輸入錢包地址，開啟互動式海報設定面板")
@app_commands.describe(address="錢包地址（可留空使用已儲存的預設地址）")
async def profile(interaction: discord.Interaction, address: str = None):
    if not address:
        address = _get_user_default_wallet(str(interaction.user.id))
        if not address:
            await interaction.response.send_message(
                "❌ 請輸入錢包地址，或先使用 `/settings wallet:0x...` 設定預設地址。",
                ephemeral=True,
            )
            return
    
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


@tree.interaction_check
async def _global_interaction_usage_check(interaction: discord.Interaction) -> bool:
    # Keep as a permissive global check.
    # Usage counting is handled in on_interaction for reliability.
    return True


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


@tree.command(name="ranking_sync_status", description="查看 rankings 同步狀態")
async def ranking_sync_status(interaction: discord.Interaction):
    status_path = _rank_sync_status_path()
    latest_path = os.path.join(_rank_sync_data_dir(), "latest.json")
    if not os.path.exists(status_path):
        await interaction.response.send_message(
            f"⚠️ 尚無 rankings 同步狀態檔：`{status_path}`\n請先等待啟動 bootstrap / compare 或整點同步執行。",
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
            f"❌ 讀取 rankings 同步狀態失敗：{e}",
            ephemeral=True,
        )
        return

    success = bool(status.get("success"))
    trigger = str(status.get("trigger") or "unknown")
    updated_at = str(status.get("updated_at") or "unknown")
    message = str(status.get("message") or "").strip()
    extra = status.get("extra") if isinstance(status.get("extra"), dict) else {}
    wallet_count = extra.get("wallet_count", "n/a")
    pages = extra.get("collectible_pages", "n/a")
    changed_wallets = extra.get("changed_wallets", "n/a")
    refreshed_wallets = extra.get("refreshed_wallets", "n/a")
    full_rebuild = extra.get("full_rebuild", "n/a")
    backup_status = extra.get("backup_status", "n/a")
    commit = extra.get("commit", "n/a")
    duration_sec = extra.get("duration_sec", "n/a")

    txt = (
        f"{'✅' if success else '❌'} Rankings 同步狀態：**{'成功' if success else '失敗'}**\n"
        f"Trigger: `{trigger}`\n"
        f"Updated At: `{updated_at}`\n"
        f"Wallets: `{wallet_count}` | Pages: `{pages}`\n"
        f"Changed: `{changed_wallets}` | Refreshed: `{refreshed_wallets}` | Full Rebuild: `{full_rebuild}`\n"
        f"Backup: `{backup_status}` | Commit: `{commit}` | Duration: `{duration_sec}`\n"
        f"Status File: `{status_path}`\n"
        f"Latest Snapshot: `{latest_path}`\n"
    )
    if message:
        txt += f"\nMessage: `{message[:1200]}`"
    await interaction.response.send_message(txt, ephemeral=True)


@tree.command(name="ranking", description="查看各項排名 Top 10（文字版）")
async def ranking(interaction: discord.Interaction):
    latest_path = os.path.join(_rank_sync_data_dir(), "latest.json")
    if not os.path.exists(latest_path):
        await interaction.response.send_message(
            f"⚠️ 尚無排名資料：`{latest_path}`\n請先執行一次 ranking 同步。",
            ephemeral=True,
        )
        return

    try:
        with open(latest_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            raise RuntimeError("latest.json 格式錯誤")
    except Exception as e:
        await interaction.response.send_message(f"❌ 讀取排名資料失敗：{e}", ephemeral=True)
        return

    wallets_raw = payload.get("wallets")
    wallets = wallets_raw if isinstance(wallets_raw, list) else []
    top_map = payload.get("top") if isinstance(payload.get("top"), dict) else {}

    def _label(row: dict) -> str:
        addr = _short_hex(str(row.get("address") or ""))
        username = str(row.get("username") or "").strip()
        if not username:
            return addr or "unknown"
        username = username[:28]
        return f"{username} · {addr}" if addr else username

    def _money_text(row: dict, field: str, signed: bool = False) -> str:
        return _format_usdt_currency(_to_decimal(row.get(field)), signed=signed)

    def _int_text(row: dict, field: str) -> str:
        return _format_number(_parse_int(row.get(field)) or 0)

    def _rank_text(row: dict, field: str) -> str:
        val = _parse_int(row.get(field))
        return str(val) if (val is not None and val > 0) else "-"

    def _fallback_top(
        metric_field: str,
        *,
        numeric_type: str,
        descending: bool = True,
        require_negative: bool = False,
    ) -> list[dict]:
        rows: list[dict] = []
        for w in wallets:
            if isinstance(w, dict):
                if require_negative:
                    if numeric_type == "int":
                        if (_parse_int(w.get(metric_field)) or 0) >= 0:
                            continue
                    else:
                        if _to_decimal(w.get(metric_field)) >= 0:
                            continue
                rows.append(w)
        if numeric_type == "int":
            rows.sort(
                key=lambda x: (_parse_int(x.get(metric_field)) or 0, str(x.get("address") or "").lower()),
                reverse=descending,
            )
        else:
            rows.sort(
                key=lambda x: (_to_decimal(x.get(metric_field)), str(x.get("address") or "").lower()),
                reverse=descending,
            )
        return rows[:10]

    sections = [
        ("volume", "交易量", "trade_volume_usdt", "volume_rank", "money", True, False),
        ("total_spent", "總花費", "total_spent_usdt", "total_spent_rank", "money", True, False),
        ("holdings", "持有價值", "holdings_value_usdt", "holdings_rank", "money", True, False),
        ("pnl", "總盈虧", "total_pnl_usdt", "pnl_rank", "money_signed", True, False),
        ("loss", "總虧損", "total_pnl_usdt", "", "money_signed", False, True),
        ("participation_days", "參與天數", "participation_days_count", "participation_days_rank", "int", True, False),
        ("sbt", "SBT", "sbt_owned_total", "sbt_rank", "int", True, False),
    ]

    blocks: list[str] = []
    for top_key, title, value_field, rank_field, value_type, descending, require_negative in sections:
        top_rows_raw = top_map.get(top_key) if isinstance(top_map, dict) else None
        if isinstance(top_rows_raw, list) and top_rows_raw:
            rows = [x for x in top_rows_raw if isinstance(x, dict)][:10]
        else:
            rows = _fallback_top(
                value_field,
                numeric_type=("int" if value_type == "int" else "decimal"),
                descending=descending,
                require_negative=require_negative,
            )

        section_lines: list[str] = [f"**{title} Top 10**"]
        if not rows:
            section_lines.append("無資料")
            blocks.append("\n".join(section_lines))
            continue

        for idx, row in enumerate(rows, start=1):
            if value_type == "money":
                value_txt = _money_text(row, value_field, signed=False)
            elif value_type == "money_signed":
                value_txt = _money_text(row, value_field, signed=True)
            else:
                value_txt = _int_text(row, value_field)
            section_lines.append(f"{idx}. `{_rank_text(row, rank_field)}` {_label(row)} | {value_txt}")
        blocks.append("\n".join(section_lines))

    updated_at = "-"
    meta = payload.get("meta")
    if isinstance(meta, dict):
        updated_at = str(meta.get("updated_at") or "-")
    blocks.append(f"Updated: `{updated_at}`")

    if not blocks:
        blocks = ["無資料"]

    # Keep each outgoing message safely below Discord message limit.
    normalized_blocks: list[str] = []
    for block in blocks:
        text = str(block or "").strip()
        if not text:
            continue
        if len(text) <= 1900:
            normalized_blocks.append(text)
            continue
        start = 0
        while start < len(text):
            normalized_blocks.append(text[start : start + 1800])
            start += 1800
    if not normalized_blocks:
        normalized_blocks = ["無資料"]

    if isinstance(interaction.channel, discord.Thread):
        await interaction.response.send_message(normalized_blocks[0], ephemeral=False)
        for block in normalized_blocks[1:]:
            await interaction.followup.send(block, ephemeral=False)
        return

    try:
        await interaction.response.send_message("📊 正在建立 ranking 討論串...", ephemeral=False)
        starter = await interaction.original_response()
    except discord.NotFound:
        channel = interaction.channel
        if channel is None:
            print("❌ /ranking 互動已失效且找不到可用頻道。", file=sys.stderr)
            return
        starter = await channel.send("📊 正在建立 ranking 討論串...")

    thread_name = f"ranking-{datetime.now().strftime('%m%d-%H%M')}"
    try:
        thread = await starter.create_thread(name=thread_name, auto_archive_duration=60)
    except Exception as e:
        print(f"❌ /ranking 建立討論串失敗: {e}", file=sys.stderr)
        await starter.reply(f"❌ 建立討論串失敗：{e}")
        return

    try:
        await thread.add_user(interaction.user)
    except Exception:
        pass
    for block in normalized_blocks:
        await thread.send(block)


@tree.command(name="bot_usage", description="查看機器人使用次數（總次數 + 各指令）")
async def bot_usage(interaction: discord.Interaction):
    path = _bot_usage_stats_path()
    data = _load_bot_usage_stats()
    if not data:
        app_env = str(os.getenv("APP_ENV", "local")).strip() or "local"
        sync_data_dir = str(os.getenv("SYNC_DATA_DIR", "")).strip() or "(unset)"
        rank_data_dir = str(os.getenv("RANK_SYNC_DATA_DIR", os.getenv("RANKING_DATA_DIR", ""))).strip() or "(unset)"
        fallback_path = os.path.join(_rank_sync_data_dir(), "state", "bot_usage_stats.json")
        await interaction.response.send_message(
            "⚠️ 尚無使用統計資料。\n"
            f"usage path: `{path}`\n"
            f"fallback path: `{fallback_path}`\n"
            f"APP_ENV: `{app_env}`\n"
            f"SYNC_DATA_DIR: `{sync_data_dir}`\n"
            f"RANK_SYNC_DATA_DIR: `{rank_data_dir}`\n"
            "先執行幾次 `/profile`、`/ranking`、`/bot_usage` 後再查看。",
            ephemeral=True,
        )
        return

    total = _parse_int(data.get("total")) or 0
    updated_at = str(data.get("updated_at") or "-")
    commands_raw = data.get("commands")
    commands = commands_raw if isinstance(commands_raw, dict) else {}
    items: list[tuple[str, int]] = []
    for name, count_raw in commands.items():
        name_s = str(name or "").strip()
        if not name_s:
            continue
        count = _parse_int(count_raw) or 0
        items.append((name_s, count))
    items.sort(key=lambda x: (-x[1], x[0].lower()))

    lines = [
        f"**總使用次數**：`{_format_number(total)}`",
        "",
        "**各指令使用次數**",
    ]
    if not items:
        lines.append("無資料")
    else:
        max_len = 1850
        rendered = 0
        for idx, (name, count) in enumerate(items, start=1):
            line = f"{idx}. `{name}`: `{_format_number(count)}`"
            draft = "\n".join(lines + [line])
            if len(draft) > max_len:
                remain = len(items) - rendered
                lines.append(f"... 其餘 `{_format_number(remain)}` 個指令略過")
                break
            lines.append(line)
            rendered += 1
    lines.append("")
    lines.append(f"Updated: `{updated_at}`")
    lines.append(f"Path: `{path}`")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)


@tree.command(name="clear_stale_commands", description="[Owner] 清除所有全域斜線指令並重置 (建議改用 !sync)")
async def clear_stale(interaction: discord.Interaction):
    await interaction.response.send_message("⚙️ 正在清除全域指令並重新同步，請稍候...", ephemeral=True)
    # Note: Global sync can be slow. 
    await tree.sync()
    await interaction.followup.send("✅ 已發送同步請求。若指令未更新，請嘗試使用文字指令 `!sync` (需標註機器人)。", ephemeral=True)


@client.event
async def on_interaction(interaction: discord.Interaction):
    try:
        if getattr(interaction, "type", None) == discord.InteractionType.application_command:
            raw = interaction.data if isinstance(interaction.data, dict) else {}
            cmd_name = str(raw.get("name") or "").strip()
            if not cmd_name:
                cmd = getattr(interaction, "command", None)
                cmd_name = str(
                    getattr(cmd, "qualified_name", None) or getattr(cmd, "name", None) or "unknown"
                ).strip()
            _record_bot_usage(
                str(getattr(interaction.user, "id", "unknown")),
                f"/{cmd_name}",
                guild_id=getattr(interaction, "guild_id", None),
                channel_id=getattr(interaction, "channel_id", None),
            )
    except Exception as e:
        print(f"⚠️ usage stats record failed in on_interaction: {e}", file=sys.stderr)


@client.event
async def on_thread_create(thread: discord.Thread):
    try:
        parent_id = int(getattr(thread, "parent_id", 0) or 0)
        if parent_id != MARKET_SOURCE_CHANNEL_ID:
            return
        if bool(getattr(thread, "archived", False)):
            return
        asyncio.create_task(_market_live_sync_refresh("thread_create"))
    except Exception as e:
        print(f"⚠️ market thread_create hook failed: {e}", file=sys.stderr)


@client.event
async def on_thread_update(before: discord.Thread, after: discord.Thread):
    try:
        parent_id = int(getattr(after, "parent_id", 0) or 0)
        if parent_id != MARKET_SOURCE_CHANNEL_ID:
            return
        if bool(getattr(before, "archived", False)) != bool(getattr(after, "archived", False)):
            asyncio.create_task(_market_live_sync_refresh("thread_archive_state_changed"))
    except Exception as e:
        print(f"⚠️ market thread_update hook failed: {e}", file=sys.stderr)


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    try:
        ch = message.channel
        if isinstance(ch, discord.Thread):
            parent_id = int(getattr(ch, "parent_id", 0) or 0)
            if parent_id == MARKET_SOURCE_CHANNEL_ID:
                author_id = int(getattr(message.author, "id", 0) or 0)
                if MARKET_SUMMARY_BOT_ID <= 0 or author_id == MARKET_SUMMARY_BOT_ID:
                    side, _title = _market_parse_summary(str(getattr(message, "content", "") or ""))
                    if side in ("WTS", "WTB"):
                        asyncio.create_task(_market_live_sync_refresh("summary_message"))
    except Exception as e:
        print(f"⚠️ market summary hook failed: {e}", file=sys.stderr)

    bot_mentioned = client.user in message.mentions
    content_lower = message.content.lower().strip()

    if bot_mentioned and "!sync" in content_lower:
        _record_bot_usage(
            str(getattr(message.author, "id", "unknown")),
            "!sync",
            guild_id=getattr(message.guild, "id", None),
            channel_id=getattr(message.channel, "id", None),
        )
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

    auto_monitor_handled = False
    if (
        AUTO_IMAGE_THREAD_MONITOR_ENABLED
        and AUTO_IMAGE_THREAD_CHANNEL_IDS
        and not isinstance(message.channel, discord.Thread)
        and message.channel.id in AUTO_IMAGE_THREAD_CHANNEL_IDS
        and message.attachments
    ):
        valid_attachments = [a for a in message.attachments if _is_image_attachment(a)]
        if valid_attachments:
            for _ in valid_attachments:
                _record_bot_usage(
                    str(getattr(message.author, "id", "unknown")),
                    "image:auto_monitor",
                    guild_id=getattr(message.guild, "id", None),
                    channel_id=getattr(message.channel, "id", None),
                )
            auto_monitor_handled = True
            asyncio.create_task(handle_images_from_monitored_channel(message, valid_attachments))

    if bot_mentioned:
        # 原本的圖片處理邏輯
        if message.attachments and not auto_monitor_handled:
            content_lower = message.content.lower()
            valid_attachments = [
                a for a in message.attachments
                if _is_image_attachment(a)
            ]
            if not valid_attachments:
                return

            for _ in valid_attachments:
                _record_bot_usage(
                    str(getattr(message.author, "id", "unknown")),
                    "image:mention",
                    guild_id=getattr(message.guild, "id", None),
                    channel_id=getattr(message.channel, "id", None),
                )

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
