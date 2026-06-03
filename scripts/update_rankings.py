#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

import requests
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
for _path in (PROJECT_ROOT, SCRIPT_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from onchain_metrics import (
    OnchainConfig,
    analyze_wallet as analyze_wallet_onchain,
    fetch_latest_usdt_tx_hash,
    scan_erc1155_contract_balances,
    scan_wallet_open_counts_by_time_incremental,
)
from runtime.wallet_migration import load_wallet_migration_map, wallet_migration_map_path
from runtime.wallet_usernames import (
    load_wallet_username_map,
    wallet_username_map_path as default_wallet_username_map_path,
)

RENAISS_COLLECTIBLE_LIST_URL = "https://www.renaiss.xyz/api/trpc/collectible.list"
RENAISS_COLLECTIBLE_BY_TOKEN_URL = "https://www.renaiss.xyz/api/trpc/collectible.getCollectibleByTokenId"
RENAISS_ACTIVITY_LIST_URL = "https://www.renaiss.xyz/api/trpc/activity.getSubgraphUserActivities"
RENAISS_TOKEN_ACTIVITY_URL = "https://www.renaiss.xyz/api/trpc/activity.getSubgraphTokenActivities"
RENAISS_SBT_BADGES_URL = "https://www.renaiss.xyz/api/trpc/sbt.getUserBadges"

WEI_DECIMAL = Decimal("1000000000000000000")
PROFILE_CARD_WITHDRAW_ADDRESS = str(
    os.getenv("PROFILE_CARD_WITHDRAW_ADDRESS", "0x341Edb3EdC1E45612E5704F29eC8d26fBb4072b4")
).strip().lower()
PROFILE_CARD_WITHDRAW_ADDRESS_EXTRA = str(
    os.getenv("PROFILE_CARD_WITHDRAW_ADDRESS_EXTRA", "0x72a004654Cef4694a6377f5b019d0489bA8A6c9E")
).strip().lower()
API_MAX_RETRIES = max(1, int(os.getenv("PROFILE_API_MAX_RETRIES", "4")))
API_RETRY_BACKOFF_SEC = max(0.2, float(os.getenv("PROFILE_API_RETRY_BACKOFF_SEC", "0.8")))
HTTP_POOL_MAXSIZE = max(8, int(os.getenv("PROFILE_HTTP_POOL_MAXSIZE", "48")))
TOKEN_ACTIVITY_PAGE_LIMIT = max(1, min(50, int(os.getenv("PROFILE_TOKEN_ACTIVITY_PAGE_LIMIT", "50"))))
TOKEN_ACTIVITY_MAX_PAGES = max(1, int(os.getenv("PROFILE_TOKEN_ACTIVITY_MAX_PAGES", "20")))
DEFAULT_MONTHLY_PACK_LAUNCH_START = "2026-05-01T00:00:00+08:00"

_HTTP_SESSION_LOCAL = threading.local()
_WITHDRAW_VALUE_CACHE: dict[str, Decimal] = {}
_WITHDRAW_VALUE_CACHE_LOCK = threading.Lock()


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


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


def _normalize_wallet_address(address: str | None) -> str:
    text = str(address or "").strip().lower()
    if text.startswith("0x") and len(text) == 42:
        return text
    return ""


DEFAULT_ONCHAIN_PACK_CONTRACTS = (
    "0xaab5f5fa75437a6e9e7004c12c9c56cda4b4885a",
    "0x94e7732b0b2e7c51ffd0d56580067d9c2e2b7910",
    "0xb2891022648c5fad3721c42c05d8d283d4d53080",
    "0xfda4a907d23d9f24271bc47483c5b983831e325e",
)


def _ranking_onchain_pack_contracts() -> tuple[str, ...]:
    return _merge_address_tuples(
        DEFAULT_ONCHAIN_PACK_CONTRACTS,
        _parse_address_csv(os.getenv("ONCHAIN_PACK_CONTRACTS"), default_values=()),
        _parse_address_csv(os.getenv("ONCHAIN_PACK_CONTRACTS_EXTRA"), default_values=()),
        _parse_address_csv(os.getenv("RANK_ONCHAIN_PACK_CONTRACTS"), default_values=()),
        _parse_address_csv(os.getenv("RANK_ONCHAIN_PACK_CONTRACTS_EXTRA"), default_values=()),
    )


def _withdraw_target_set() -> set[str]:
    out = set(
        _merge_address_tuples(
            (
                PROFILE_CARD_WITHDRAW_ADDRESS,
                PROFILE_CARD_WITHDRAW_ADDRESS_EXTRA,
            ),
            _parse_address_csv(os.getenv("PROFILE_CARD_WITHDRAW_ADDRESSES"), default_values=()),
        )
    )
    return {x for x in out if x.startswith("0x") and len(x) == 42}


def _merge_address_tuples(*groups: tuple[str, ...]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for addr in group:
            a = str(addr or "").strip().lower()
            if not a.startswith("0x") or len(a) != 42:
                continue
            if a in seen:
                continue
            seen.add(a)
            out.append(a)
    return tuple(out)


def _default_holders_file(app_env: str) -> Path:
    token_id = str(os.getenv("NFT_TOKEN_ID", "13")).strip() or "13"
    default_sync_dir = "/data/renaiss_sync" if app_env == "server" else "./data/renaiss_sync"
    sync_data_dir = Path(os.getenv("SYNC_DATA_DIR", default_sync_dir)).expanduser().resolve()
    return sync_data_dir / "snapshots" / f"nft_{token_id}_holders.latest.json"


def _safe_tzinfo(name: str):
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    # Fallback when container does not ship tzdata.
    return timezone(timedelta(hours=8))


def _run(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=False, text=True, capture_output=True)


def _http_session() -> requests.Session:
    sess = getattr(_HTTP_SESSION_LOCAL, "session", None)
    if sess is not None:
        return sess
    sess = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=HTTP_POOL_MAXSIZE,
        pool_maxsize=HTTP_POOL_MAXSIZE,
        max_retries=0,
    )
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    _HTTP_SESSION_LOCAL.session = sess
    return sess


def _http_get(url: str, **kwargs):
    return _http_session().get(url, **kwargs)


def _to_decimal(value: Any) -> Decimal:
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


def _wei_to_usdt(value: Any) -> Decimal:
    wei = _to_decimal(value)
    if wei == 0:
        return Decimal("0")
    return wei / WEI_DECIMAL


def _quantize_2(value: Decimal) -> Decimal:
    return _to_decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _decimal_to_str(value: Decimal) -> str:
    return format(_quantize_2(value), "f")


def _parse_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.startswith("-"):
        return int(text[1:]) * -1 if text[1:].isdigit() else None
    return int(text) if text.isdigit() else None


def _card_price_to_usd(value: Any) -> Decimal:
    amount = _to_decimal(value)
    if amount <= 0:
        return Decimal("0")
    # Upstream card price may be cent-like integer, keep parity with /profile logic.
    if amount == amount.to_integral_value() and amount >= Decimal("1000"):
        return amount / Decimal("100")
    return amount


def _pick_contract_unit_price(price_counter: Counter) -> Decimal:
    if not price_counter:
        return Decimal("0")
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


def _day_key_from_ts(ts: int) -> str:
    if not isinstance(ts, int) or ts <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _month_start_local(now_dt: datetime) -> datetime:
    return now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _monthly_pack_rank_window(cfg: "RankingConfig", now_dt: datetime) -> tuple[datetime, datetime]:
    month_start = _month_start_local(now_dt)
    launch_raw = str(os.getenv("PACK_RANK_LAUNCH_START", DEFAULT_MONTHLY_PACK_LAUNCH_START)).strip()
    launch_dt = _parse_iso_dt(launch_raw)
    if launch_dt is not None:
        if launch_dt.tzinfo is None:
            launch_dt = launch_dt.replace(tzinfo=cfg.tzinfo)
        else:
            launch_dt = launch_dt.astimezone(cfg.tzinfo)
        if launch_dt.year == now_dt.year and launch_dt.month == now_dt.month and launch_dt > month_start:
            month_start = launch_dt
    return month_start, now_dt


def _participation_days_since_join(day_keys: set[str], now_dt: datetime) -> int:
    normalized = sorted({str(x or "").strip() for x in (day_keys or set()) if str(x or "").strip()})
    if not normalized:
        return 0
    try:
        first_day = datetime.strptime(normalized[0], "%Y-%m-%d").date()
    except Exception:
        return len(normalized)
    today = now_dt.date()
    if today < first_day:
        return 1
    return int((today - first_day).days) + 1


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp_path, path)


def _json_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def _json_load_any(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _progress_every() -> int:
    return max(1, int(os.getenv("RANK_SYNC_PROGRESS_EVERY", "50")))


def _print_progress(stage: str, completed: int, total: int) -> None:
    total = max(0, int(total))
    completed = max(0, int(completed))
    percent = 100.0 if total <= 0 else min(100.0, (completed / total) * 100.0)
    print(f"[PROGRESS] {stage} {completed}/{total} ({percent:.1f}%)", flush=True)


def _maybe_print_progress(stage: str, completed: int, total: int, every: int) -> None:
    if total <= 0:
        return
    if completed == 1 or completed % every == 0 or completed >= total:
        _print_progress(stage, completed, total)


def _trpc_collectible_list(payload: dict[str, Any]) -> dict[str, Any]:
    params = {
        "batch": "1",
        "input": json.dumps({"0": {"json": payload}}, separators=(",", ":"), ensure_ascii=False),
    }
    last_err: Exception | None = None
    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            resp = _http_get(RENAISS_COLLECTIBLE_LIST_URL, params=params, timeout=30)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError("collectible.list invalid response")
            row = data[0]
            if row.get("error"):
                err = row.get("error", {}).get("json", {}).get("message") or "unknown error"
                raise RuntimeError(f"collectible.list error: {err}")

            result = (((row.get("result") or {}).get("data") or {}).get("json") or {})
            if not isinstance(result, dict):
                raise RuntimeError("collectible.list missing result")
            return result
        except Exception as e:  # noqa: BLE001
            last_err = e
            status = None
            if isinstance(e, requests.RequestException) and getattr(e, "response", None) is not None:
                status = int(e.response.status_code or 0)
            retryable = status in (408, 409, 425, 429) or (status is not None and status >= 500) or status is None
            if retryable and attempt < API_MAX_RETRIES:
                wait_sec = API_RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                time.sleep(wait_sec)
                continue
            break

    raise RuntimeError(f"collectible.list request failed: {last_err}")


def _trpc_user_activities(address: str, cursor: str | None, limit: int) -> dict[str, Any]:
    row: dict[str, Any] = {
        "json": {
            "address": address.lower(),
            "filter": "all",
            "mode": "private",
            "limit": int(limit),
            "cursor": cursor,
        }
    }
    if cursor is None:
        row["meta"] = {"values": {"cursor": ["undefined"]}}

    params = {
        "batch": "1",
        "input": json.dumps({"0": row}, separators=(",", ":"), ensure_ascii=False),
    }

    last_err: Exception | None = None
    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            resp = _http_get(RENAISS_ACTIVITY_LIST_URL, params=params, timeout=30)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError("activity.getSubgraphUserActivities invalid response")
            root = data[0]
            if root.get("error"):
                err = root.get("error", {}).get("json", {}).get("message") or "unknown error"
                raise RuntimeError(f"activity.getSubgraphUserActivities error: {err}")

            result = (((root.get("result") or {}).get("data") or {}).get("json") or {})
            if not isinstance(result, dict):
                raise RuntimeError("activity.getSubgraphUserActivities missing result")
            return result
        except Exception as e:  # noqa: BLE001
            last_err = e
            status = None
            if isinstance(e, requests.RequestException) and getattr(e, "response", None) is not None:
                status = int(e.response.status_code or 0)
            retryable = status in (408, 409, 425, 429) or (status is not None and status >= 500) or status is None
            if retryable and attempt < API_MAX_RETRIES:
                wait_sec = API_RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                time.sleep(wait_sec)
                continue
            break

    raise RuntimeError(f"activity.getSubgraphUserActivities request failed: {last_err}")


def _trpc_user_badges(username: str) -> list[dict[str, Any]]:
    params = {
        "batch": "1",
        "input": json.dumps({"0": {"json": {"username": username}}}, separators=(",", ":"), ensure_ascii=False),
    }
    last_err: Exception | None = None
    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            resp = _http_get(RENAISS_SBT_BADGES_URL, params=params, timeout=30)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list) or not data:
                return []
            root = data[0]
            if root.get("error"):
                err = root.get("error", {}).get("json", {}).get("message") or "unknown error"
                raise RuntimeError(f"sbt.getUserBadges error: {err}")
            badges = (((root.get("result") or {}).get("data") or {}).get("json") or {}).get("badges") or []
            return badges if isinstance(badges, list) else []
        except Exception as e:  # noqa: BLE001
            last_err = e
            status = None
            if isinstance(e, requests.RequestException) and getattr(e, "response", None) is not None:
                status = int(e.response.status_code or 0)
            retryable = status in (408, 409, 425, 429) or (status is not None and status >= 500) or status is None
            if retryable and attempt < API_MAX_RETRIES:
                wait_sec = API_RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                time.sleep(wait_sec)
                continue
            break
    raise RuntimeError(f"sbt.getUserBadges request failed: {last_err}")


def _trpc_collectible_by_token(token_id: str) -> dict[str, Any]:
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
    last_err: Exception | None = None
    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            resp = _http_get(RENAISS_COLLECTIBLE_BY_TOKEN_URL, params=params, timeout=30)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError("collectible.getCollectibleByTokenId invalid response")
            root = data[0]
            if root.get("error"):
                err_data = ((root.get("error") or {}).get("json") or {}).get("data") or {}
                if int(err_data.get("httpStatus") or 0) == 404:
                    return {}
                err = ((root.get("error") or {}).get("json") or {}).get("message") or "unknown error"
                raise RuntimeError(f"collectible.getCollectibleByTokenId error: {err}")

            result = (((root.get("result") or {}).get("data") or {}).get("json") or {})
            if not isinstance(result, dict):
                raise RuntimeError("collectible.getCollectibleByTokenId missing result")
            return result
        except Exception as e:  # noqa: BLE001
            last_err = e
            status = None
            if isinstance(e, requests.RequestException) and getattr(e, "response", None) is not None:
                status = int(e.response.status_code or 0)
            retryable = status in (408, 409, 425, 429) or (status is not None and status >= 500) or status is None
            if retryable and attempt < API_MAX_RETRIES:
                wait_sec = API_RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                time.sleep(wait_sec)
                continue
            break
    raise RuntimeError(f"collectible.getCollectibleByTokenId request failed: {last_err}")


def _trpc_token_activities(token_id: str, cursor: str | None, limit: int = TOKEN_ACTIVITY_PAGE_LIMIT) -> dict[str, Any]:
    token = str(token_id or "").strip()
    if not token:
        return {"activities": [], "nextCursor": None}
    row: dict[str, Any] = {
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
    last_err: Exception | None = None
    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            resp = _http_get(RENAISS_TOKEN_ACTIVITY_URL, params=params, timeout=30)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError("activity.getSubgraphTokenActivities invalid response")
            root = data[0]
            if root.get("error"):
                err = ((root.get("error") or {}).get("json") or {}).get("message") or "unknown error"
                raise RuntimeError(f"activity.getSubgraphTokenActivities error: {err}")
            result = (((root.get("result") or {}).get("data") or {}).get("json") or {})
            if not isinstance(result, dict):
                raise RuntimeError("activity.getSubgraphTokenActivities missing result")
            return result
        except Exception as e:  # noqa: BLE001
            last_err = e
            status = None
            if isinstance(e, requests.RequestException) and getattr(e, "response", None) is not None:
                status = int(e.response.status_code or 0)
            retryable = status in (408, 409, 425, 429) or (status is not None and status >= 500) or status is None
            if retryable and attempt < API_MAX_RETRIES:
                wait_sec = API_RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                time.sleep(wait_sec)
                continue
            break
    raise RuntimeError(f"activity.getSubgraphTokenActivities request failed: {last_err}")


def _fetch_token_activities(token_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    tid = str(token_id or "").strip()
    if not tid:
        return rows
    cursor: str | None = None
    seen_cursors: set[str] = set()
    for _ in range(TOKEN_ACTIVITY_MAX_PAGES):
        page = _trpc_token_activities(tid, cursor=cursor, limit=TOKEN_ACTIVITY_PAGE_LIMIT)
        page_rows = page.get("activities") or []
        if isinstance(page_rows, list):
            rows.extend([x for x in page_rows if isinstance(x, dict)])
        next_cursor = page.get("nextCursor")
        if not next_cursor:
            break
        next_cursor = str(next_cursor)
        if next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    return rows


def _fetch_card_trade_fallback_by_token_id(token_id: str) -> Decimal:
    tid = str(token_id or "").strip()
    if not tid:
        return Decimal("0")
    try:
        activities = _fetch_token_activities(tid)
    except Exception:
        return Decimal("0")

    best_value = Decimal("0")
    best_ts = -1
    for row in activities:
        row_type = str(row.get("__typename") or "").strip()
        ts = _parse_int(row.get("timestamp")) or 0
        value = Decimal("0")
        if row_type in ("SellActivity", "BuyActivity"):
            value = _wei_to_usdt(row.get("amount"))
        elif row_type in ("PerpetualBuybackActivity", "BuybackActivity"):
            value = _wei_to_usdt(row.get("priceInUsdt"))
            if value <= 0:
                value = _wei_to_usdt(row.get("amount"))
        if value > 0 and ts >= best_ts:
            best_ts = ts
            best_value = value
    return best_value if best_value > 0 else Decimal("0")


def _fetch_card_withdraw_value_by_token_id(token_id: str) -> Decimal:
    tid = str(token_id or "").strip()
    if not tid:
        return Decimal("0")
    with _WITHDRAW_VALUE_CACHE_LOCK:
        cached = _WITHDRAW_VALUE_CACHE.get(tid)
    if cached is not None:
        return cached

    value = Decimal("0")
    try:
        collectible = _trpc_collectible_by_token(tid)
        for candidate in (
            _card_price_to_usd(collectible.get("fmvPriceInUSD")),
            _card_price_to_usd(collectible.get("buybackBaseValueInUSD")),
            _card_price_to_usd(collectible.get("askPriceInUSDT")),
        ):
            if candidate > 0:
                value = candidate
                break
    except Exception:
        value = Decimal("0")

    if value <= 0:
        value = _fetch_card_trade_fallback_by_token_id(tid)

    with _WITHDRAW_VALUE_CACHE_LOCK:
        _WITHDRAW_VALUE_CACHE[tid] = value
    return value


def _card_withdraw_total_from_token_ids(token_ids: Any) -> Decimal:
    if not isinstance(token_ids, (list, tuple, set)):
        return Decimal("0")
    seen: set[str] = set()
    total = Decimal("0")
    for raw in token_ids:
        tid = str(raw or "").strip()
        if not tid or tid in seen:
            continue
        seen.add(tid)
        total += _to_decimal(_fetch_card_withdraw_value_by_token_id(tid))
    return total


@dataclass
class WalletRecord:
    address: str
    username: str | None
    holdings_value_usdt: Decimal
    collectible_count: int
    pack_spent_usdt: Decimal = Decimal("0")
    trade_volume_usdt: Decimal = Decimal("0")
    trade_spent_usdt: Decimal = Decimal("0")
    trade_earned_usdt: Decimal = Decimal("0")
    buyback_earned_usdt: Decimal = Decimal("0")
    card_withdraw_total_usdt: Decimal = Decimal("0")
    total_spent_usdt: Decimal = Decimal("0")
    total_earned_usdt: Decimal = Decimal("0")
    cash_net_usdt: Decimal = Decimal("0")
    total_pnl_usdt: Decimal = Decimal("0")
    participation_days_count: int = 0
    sbt_owned_total: int = 0
    sbt_owned_badge_count: int = 0
    monthly_gacha_open_count: int = 0
    volume_rank: int | None = None
    total_spent_rank: int | None = None
    holdings_rank: int | None = None
    pnl_rank: int | None = None
    participation_days_rank: int | None = None
    sbt_rank: int | None = None
    monthly_gacha_open_rank: int | None = None
    monthly_gacha_level: str = "none"


@dataclass
class RankingConfig:
    app_env: str
    test_mode: bool
    trigger: str
    full_rebuild_requested: bool
    data_dir: Path
    workers: int
    activity_page_limit: int
    activity_max_pages: int
    failed_retry_rounds: int
    failed_retry_sleep_sec: float
    max_wallets: int | None
    tz_name: str
    tzinfo: Any
    full_rebuild_days: int
    full_rebuild_active_only: bool
    metrics_source: str
    monthly_pack_source: str
    sbt_source: str
    onchain_api_url: str
    onchain_chain_id: int
    onchain_api_key: str
    onchain_usdt_contract: str
    onchain_sbt_contract: str
    onchain_pack_contracts: tuple[str, ...]
    onchain_marketplace_contract: str
    onchain_page_size: int
    bootstrap_from_git: bool
    backup_git_enabled: bool
    backup_git_repo: str
    backup_git_branch: str
    backup_git_dir: Path
    webhook_url: str
    progress_every: int
    checkpoint_flush_every: int
    wallet_source: str
    holders_file: Path | None
    wallet_migration_map_path: Path | None
    wallet_username_map_path: Path | None

    @property
    def latest_path(self) -> Path:
        return self.data_dir / "latest.json"

    @property
    def state_path(self) -> Path:
        return self.data_dir / "state" / "ranking_state.json"

    @property
    def status_path(self) -> Path:
        return self.data_dir / "state" / "ranking_status.json"

    @property
    def market_status_path(self) -> Path:
        return self.data_dir / "state" / "market_push_status.json"

    @property
    def checkpoint_path(self) -> Path:
        return self.data_dir / "state" / "activity_checkpoints.json"

    @property
    def pack_rank_state_path(self) -> Path:
        return self.data_dir / "state" / "pack_rank_state.json"

    @property
    def pack_rank_latest_path(self) -> Path:
        return self.data_dir / "pack_rank_latest.json"

    @property
    def monthly_pack_scan_state_path(self) -> Path:
        return self.data_dir / "state" / "ranking_monthly_pack_state.json"

    @property
    def sbt_state_path(self) -> Path:
        return self.data_dir / "state" / "ranking_sbt_state.json"

    @property
    def source_wallet_state_path(self) -> Path:
        return self.data_dir / "state" / "ranking_source_wallets_state.json"

    def history_path(self, now_dt: datetime) -> Path:
        key = now_dt.strftime("%Y-%m-%d_%H")
        return self.data_dir / "history" / f"{key}.json"

    @property
    def repo_dataset_dir(self) -> Path:
        return self.backup_git_dir / "rankings"

    @property
    def market_cache_dir(self) -> Path:
        return self.data_dir.parent / "market_cache"

    @property
    def repo_market_cache_dir(self) -> Path:
        return self.backup_git_dir / "market_cache"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incremental ranking sync with startup bootstrap and git backup")
    parser.add_argument("--trigger", default="manual")
    parser.add_argument("--bootstrap-only", action="store_true")
    parser.add_argument("--push-only", action="store_true", help="Skip sync and only push current rankings/market_cache to backup git")
    parser.add_argument("--market-only", action="store_true", help="With --push-only, push only market_cache to backup git")
    parser.add_argument("--full-rebuild", action="store_true")
    parser.add_argument(
        "--full-rebuild-all",
        action="store_true",
        help="With --full-rebuild, recompute every wallet instead of active-only rebuild.",
    )
    parser.add_argument("--data-dir", default=os.getenv("RANKING_DATA_DIR", ""))
    parser.add_argument("--workers", type=int, default=max(2, int(os.getenv("RANKING_WORKERS", "8"))))
    parser.add_argument(
        "--activity-page-limit",
        type=int,
        default=max(10, min(50, int(os.getenv("RANKING_ACTIVITY_PAGE_LIMIT", "50")))),
    )
    parser.add_argument("--activity-max-pages", type=int, default=max(1, int(os.getenv("RANKING_ACTIVITY_MAX_PAGES", os.getenv("PROFILE_ACTIVITY_MAX_PAGES", "120")))))
    parser.add_argument("--max-wallets", type=int, default=None, help="Only for testing; limit wallets to process")
    return parser.parse_args()


def load_config(args: argparse.Namespace) -> RankingConfig:
    load_dotenv()
    app_env = str(os.getenv("APP_ENV", "local")).strip().lower() or "local"
    default_data_dir = "/data/renaiss_sync/rankings" if app_env == "server" else "./data/renaiss_sync/rankings"
    data_dir_raw = str(args.data_dir or os.getenv("RANK_SYNC_DATA_DIR", default_data_dir)).strip() or default_data_dir

    tz_name = str(os.getenv("RANK_SYNC_TZ", "Asia/Taipei")).strip() or "Asia/Taipei"
    tzinfo = _safe_tzinfo(tz_name)
    test_mode = _env_bool("SYNC_TEST_MODE", False)

    backup_git_enabled = _env_bool("BACKUP_GIT_ENABLED", False)
    bootstrap_from_git = _env_bool("BOOTSTRAP_FROM_GIT", app_env == "server")
    progress_every = _progress_every()
    checkpoint_flush_every = max(1, int(os.getenv("RANK_SYNC_CHECKPOINT_FLUSH_EVERY", "50")))

    holders_file_raw = str(os.getenv("RANK_SYNC_HOLDERS_FILE", "")).strip()
    holders_file = Path(holders_file_raw).expanduser() if holders_file_raw else _default_holders_file(app_env)
    migration_map_raw = str(os.getenv("WALLET_MIGRATION_MAP_PATH", "")).strip()
    migration_map_path = (
        Path(migration_map_raw).expanduser().resolve()
        if migration_map_raw
        else Path(wallet_migration_map_path()).expanduser().resolve()
    )
    username_map_raw = str(os.getenv("WALLET_USERNAME_MAP_PATH", "")).strip()
    username_map_path = (
        Path(username_map_raw).expanduser().resolve()
        if username_map_raw
        else Path(default_wallet_username_map_path()).expanduser().resolve()
    )

    wallet_source = str(os.getenv("RANK_SYNC_WALLET_SOURCE", "holders_file")).strip().lower() or "holders_file"
    if wallet_source not in ("holders_file", "collectible", "auto"):
        raise RuntimeError("RANK_SYNC_WALLET_SOURCE must be holders_file, collectible, or auto")
    metrics_source = str(os.getenv("RANK_METRICS_SOURCE", "hybrid")).strip().lower() or "hybrid"
    if metrics_source not in ("official", "onchain", "hybrid"):
        raise RuntimeError("RANK_METRICS_SOURCE must be official, onchain, or hybrid")
    monthly_pack_source = str(os.getenv("RANK_MONTHLY_PACK_SOURCE", "pack_rank_latest")).strip().lower()
    if monthly_pack_source not in ("pack_rank_latest", "onchain", "none"):
        raise RuntimeError("RANK_MONTHLY_PACK_SOURCE must be pack_rank_latest, onchain, or none")
    sbt_source = str(os.getenv("RANK_SBT_SOURCE", "onchain_contract")).strip().lower() or "onchain_contract"
    if sbt_source not in ("onchain_contract", "official"):
        raise RuntimeError("RANK_SBT_SOURCE must be onchain_contract or official")

    onchain_api_url = str(os.getenv("BSCSCAN_API_URL", "https://api.etherscan.io/v2/api")).strip() or "https://api.etherscan.io/v2/api"
    onchain_chain_id = max(1, int(os.getenv("BSCSCAN_CHAIN_ID", "56")))
    onchain_api_key = str(os.getenv("BSCSCAN_API_KEY", "")).strip()
    onchain_usdt_contract = str(
        os.getenv("ONCHAIN_USDT_CONTRACT", "0x55d398326f99059ff775485246999027b3197955")
    ).strip().lower()
    onchain_sbt_contract = str(
        os.getenv("ONCHAIN_SBT_CONTRACT", "0x7d1b7db704d722295fbaa284008f526634673dbf")
    ).strip().lower()
    onchain_pack_contracts = _ranking_onchain_pack_contracts()
    onchain_marketplace_contract = str(
        os.getenv("ONCHAIN_MARKETPLACE_CONTRACT", "0xae3e7268ef5a062946216a44f58a8f685ffd11d0")
    ).strip().lower()
    onchain_page_size = max(100, min(10000, int(os.getenv("ONCHAIN_PAGE_SIZE", "10000"))))

    selected_holders_file: Path | None
    if wallet_source == "collectible":
        selected_holders_file = None
    elif wallet_source == "holders_file":
        selected_holders_file = holders_file
        if not (selected_holders_file.exists() and selected_holders_file.is_file() and os.access(selected_holders_file, os.R_OK)):
            raise RuntimeError(f"holders file not readable: {selected_holders_file}")
    else:
        if holders_file.exists() and holders_file.is_file() and os.access(holders_file, os.R_OK):
            selected_holders_file = holders_file
            wallet_source = "holders_file"
        else:
            raise RuntimeError(
                f"holders file not readable: {holders_file}; "
                "ranking wallet universe must come from NFT holder snapshot. "
                "Run scripts/sync_nft13_incremental.py first or set RANK_SYNC_WALLET_SOURCE=collectible only for debug."
            )

    return RankingConfig(
        app_env=app_env,
        test_mode=test_mode,
        trigger=str(args.trigger or "manual"),
        full_rebuild_requested=bool(args.full_rebuild),
        data_dir=Path(data_dir_raw).expanduser().resolve(),
        workers=max(1, int(args.workers)),
        activity_page_limit=max(1, min(50, int(args.activity_page_limit))),
        activity_max_pages=max(1, int(args.activity_max_pages)),
        failed_retry_rounds=max(0, int(os.getenv("RANK_SYNC_FAILED_RETRY_ROUNDS", "3"))),
        failed_retry_sleep_sec=max(0.5, float(os.getenv("RANK_SYNC_FAILED_RETRY_SLEEP_SEC", "3"))),
        max_wallets=int(args.max_wallets) if args.max_wallets else None,
        tz_name=tz_name,
        tzinfo=tzinfo,
        full_rebuild_days=max(1, int(os.getenv("RANK_SYNC_FULL_REBUILD_DAYS", "7"))),
        full_rebuild_active_only=False
        if bool(getattr(args, "full_rebuild_all", False))
        else _env_bool("RANK_SYNC_FULL_REBUILD_ACTIVE_ONLY", True),
        metrics_source=metrics_source,
        monthly_pack_source=monthly_pack_source,
        sbt_source=sbt_source,
        onchain_api_url=onchain_api_url,
        onchain_chain_id=onchain_chain_id,
        onchain_api_key=onchain_api_key,
        onchain_usdt_contract=onchain_usdt_contract,
        onchain_sbt_contract=onchain_sbt_contract,
        onchain_pack_contracts=onchain_pack_contracts,
        onchain_marketplace_contract=onchain_marketplace_contract,
        onchain_page_size=onchain_page_size,
        bootstrap_from_git=bootstrap_from_git,
        backup_git_enabled=backup_git_enabled,
        backup_git_repo=str(os.getenv("BACKUP_GIT_REPO", "")).strip(),
        backup_git_branch=str(os.getenv("BACKUP_GIT_BRANCH", "main")).strip() or "main",
        backup_git_dir=Path(os.getenv("BACKUP_GIT_DIR", str(Path(data_dir_raw).expanduser().resolve().parent / "backup_repo"))).expanduser().resolve(),
        webhook_url=str(
            os.getenv("RANK_SYNC_WEBHOOK_URL")
            or os.getenv("SYNC_WEBHOOK_URL")
            or os.getenv("DISCORD_SYNC_WEBHOOK_URL")
            or os.getenv("DISCORD_WEBHOOK_URL")
            or ""
        ).strip(),
        progress_every=progress_every,
        checkpoint_flush_every=checkpoint_flush_every,
        wallet_source=wallet_source,
        holders_file=selected_holders_file,
        wallet_migration_map_path=migration_map_path,
        wallet_username_map_path=username_map_path,
    )


def validate_config(cfg: RankingConfig) -> None:
    if cfg.backup_git_enabled and not cfg.backup_git_repo:
        raise RuntimeError("BACKUP_GIT_REPO is required when BACKUP_GIT_ENABLED=1")
    if cfg.metrics_source in ("onchain", "hybrid"):
        if not cfg.onchain_api_key:
            raise RuntimeError("BSCSCAN_API_KEY is required when RANK_METRICS_SOURCE is onchain/hybrid")
    if cfg.sbt_source == "onchain_contract":
        if not cfg.onchain_api_key:
            raise RuntimeError("BSCSCAN_API_KEY is required when RANK_SBT_SOURCE=onchain_contract")
        if not (cfg.onchain_sbt_contract.startswith("0x") and len(cfg.onchain_sbt_contract) == 42):
            raise RuntimeError("ONCHAIN_SBT_CONTRACT is required when RANK_SBT_SOURCE=onchain_contract")


def send_webhook(cfg: RankingConfig, message: str, success: bool = True) -> None:
    if not cfg.webhook_url:
        return
    title = "Ranking Sync Success" if success else "Ranking Sync Failed"
    color = 0x2ECC71 if success else 0xE74C3C
    if cfg.test_mode:
        title = f"[TEST] {title}"
    payload = {
        "embeds": [
            {
                "title": title,
                "description": message[:4000],
                "color": color,
                "timestamp": datetime.now(tz=cfg.tzinfo).isoformat(),
            }
        ]
    }
    try:
        requests.post(cfg.webhook_url, json=payload, timeout=12)
    except Exception:
        pass


def _write_status_to_path(
    cfg: RankingConfig,
    path: Path,
    *,
    success: bool,
    message: str,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "updated_at": datetime.now(tz=cfg.tzinfo).isoformat(),
        "success": bool(success),
        "trigger": cfg.trigger,
        "message": message,
        "app_env": cfg.app_env,
        "test_mode": cfg.test_mode,
    }
    if extra:
        payload["extra"] = extra
    _atomic_write_json(path, payload)


def write_status(
    cfg: RankingConfig,
    *,
    success: bool,
    message: str,
    extra: dict[str, Any] | None = None,
) -> None:
    _write_status_to_path(cfg, cfg.status_path, success=success, message=message, extra=extra)


def write_market_status(
    cfg: RankingConfig,
    *,
    success: bool,
    message: str,
    extra: dict[str, Any] | None = None,
) -> None:
    _write_status_to_path(cfg, cfg.market_status_path, success=success, message=message, extra=extra)


def ensure_repo(cfg: RankingConfig) -> Path:
    repo_dir = cfg.backup_git_dir
    if (repo_dir / ".git").exists():
        return repo_dir
    repo_dir.parent.mkdir(parents=True, exist_ok=True)
    res = _run(["git", "clone", "--branch", cfg.backup_git_branch, cfg.backup_git_repo, str(repo_dir)])
    if res.returncode != 0:
        raise RuntimeError(f"git clone failed: {res.stderr.strip() or res.stdout.strip()}")
    return repo_dir


def git_pull(cfg: RankingConfig, repo_dir: Path) -> None:
    _run(["git", "fetch", "--all"], cwd=repo_dir)
    res = _run(["git", "pull", "--rebase", "origin", cfg.backup_git_branch], cwd=repo_dir)
    if res.returncode != 0:
        raise RuntimeError(f"git pull failed: {res.stderr.strip() or res.stdout.strip()}")


def git_push_rankings(cfg: RankingConfig, now_dt: datetime, commit_message: str) -> str:
    if cfg.test_mode:
        return "test-mode-skip-push"
    repo_dir = ensure_repo(cfg)
    git_pull(cfg, repo_dir)

    dataset_dir = cfg.repo_dataset_dir
    (dataset_dir / "history").mkdir(parents=True, exist_ok=True)
    (dataset_dir / "state").mkdir(parents=True, exist_ok=True)

    history_path = cfg.history_path(now_dt)
    if cfg.latest_path.exists():
        shutil.copy2(cfg.latest_path, dataset_dir / "latest.json")
    if history_path.exists():
        shutil.copy2(history_path, dataset_dir / "history" / history_path.name)
    if cfg.state_path.exists():
        shutil.copy2(cfg.state_path, dataset_dir / "state" / cfg.state_path.name)
    if cfg.status_path.exists():
        shutil.copy2(cfg.status_path, dataset_dir / "state" / cfg.status_path.name)
    if cfg.checkpoint_path.exists():
        shutil.copy2(cfg.checkpoint_path, dataset_dir / "state" / cfg.checkpoint_path.name)
    if cfg.source_wallet_state_path.exists():
        shutil.copy2(cfg.source_wallet_state_path, dataset_dir / "state" / cfg.source_wallet_state_path.name)

    had_repo_market_cache = cfg.repo_market_cache_dir.exists()
    if cfg.market_cache_dir.exists():
        if cfg.repo_market_cache_dir.exists():
            shutil.rmtree(cfg.repo_market_cache_dir, ignore_errors=True)
        shutil.copytree(cfg.market_cache_dir, cfg.repo_market_cache_dir)
    elif had_repo_market_cache:
        shutil.rmtree(cfg.repo_market_cache_dir, ignore_errors=True)

    _run(["git", "config", "user.name", os.getenv("BACKUP_GIT_USER_NAME", "tcg-pro-bot")], cwd=repo_dir)
    _run(["git", "config", "user.email", os.getenv("BACKUP_GIT_USER_EMAIL", "tcg-pro-bot@example.com")], cwd=repo_dir)
    _run(["git", "add", "-A", "rankings"], cwd=repo_dir)
    if cfg.market_cache_dir.exists() or had_repo_market_cache:
        _run(["git", "add", "-A", "market_cache"], cwd=repo_dir)
    status = _run(["git", "status", "--porcelain"], cwd=repo_dir)
    if status.returncode != 0:
        raise RuntimeError(f"git status failed: {status.stderr.strip() or status.stdout.strip()}")
    if not status.stdout.strip():
        return "no-change"

    commit = _run(["git", "commit", "-m", commit_message], cwd=repo_dir)
    if commit.returncode != 0:
        raise RuntimeError(f"git commit failed: {commit.stderr.strip() or commit.stdout.strip()}")
    push = _run(["git", "push", "origin", cfg.backup_git_branch], cwd=repo_dir)
    if push.returncode != 0:
        raise RuntimeError(f"git push failed: {push.stderr.strip() or push.stdout.strip()}")
    head = _run(["git", "rev-parse", "--short", "HEAD"], cwd=repo_dir)
    return head.stdout.strip() or "unknown"


def git_push_market_cache(cfg: RankingConfig, commit_message: str) -> str:
    if cfg.test_mode:
        return "test-mode-skip-push"
    repo_dir = ensure_repo(cfg)
    git_pull(cfg, repo_dir)

    had_repo_market_cache = cfg.repo_market_cache_dir.exists()
    if cfg.market_cache_dir.exists():
        if cfg.repo_market_cache_dir.exists():
            shutil.rmtree(cfg.repo_market_cache_dir, ignore_errors=True)
        shutil.copytree(cfg.market_cache_dir, cfg.repo_market_cache_dir)
    elif had_repo_market_cache:
        shutil.rmtree(cfg.repo_market_cache_dir, ignore_errors=True)

    _run(["git", "config", "user.name", os.getenv("BACKUP_GIT_USER_NAME", "tcg-pro-bot")], cwd=repo_dir)
    _run(["git", "config", "user.email", os.getenv("BACKUP_GIT_USER_EMAIL", "tcg-pro-bot@example.com")], cwd=repo_dir)
    _run(["git", "add", "-A", "market_cache"], cwd=repo_dir)
    status = _run(["git", "status", "--porcelain"], cwd=repo_dir)
    if status.returncode != 0:
        raise RuntimeError(f"git status failed: {status.stderr.strip() or status.stdout.strip()}")
    if not status.stdout.strip():
        return "no-change"

    commit = _run(["git", "commit", "-m", commit_message], cwd=repo_dir)
    if commit.returncode != 0:
        raise RuntimeError(f"git commit failed: {commit.stderr.strip() or commit.stdout.strip()}")
    push = _run(["git", "push", "origin", cfg.backup_git_branch], cwd=repo_dir)
    if push.returncode != 0:
        raise RuntimeError(f"git push failed: {push.stderr.strip() or push.stdout.strip()}")
    head = _run(["git", "rev-parse", "--short", "HEAD"], cwd=repo_dir)
    return head.stdout.strip() or "unknown"


def _payload_updated_at_ts(path: Path) -> float | None:
    data = _json_load(path)
    if not isinstance(data, dict) or not data:
        return None
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    raw = data.get("updated_at") or meta.get("updated_at")
    dt = _parse_iso_dt(str(raw or ""))
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return float(dt.timestamp())


def _should_bootstrap_ranking_payload(local_latest: Path, repo_latest: Path) -> bool:
    if not repo_latest.exists():
        return False
    if not local_latest.exists():
        return True

    repo_ts = _payload_updated_at_ts(repo_latest)
    local_ts = _payload_updated_at_ts(local_latest)
    if repo_ts is not None and local_ts is not None:
        return repo_ts > local_ts
    if repo_ts is not None and local_ts is None:
        return True
    if repo_ts is None and local_ts is not None:
        return False

    try:
        return repo_latest.stat().st_mtime > local_latest.stat().st_mtime
    except OSError:
        return False


def bootstrap_from_git(cfg: RankingConfig) -> bool:
    if not cfg.backup_git_enabled:
        return False
    repo_dir = ensure_repo(cfg)
    if not cfg.test_mode:
        git_pull(cfg, repo_dir)
    repo_latest = cfg.repo_dataset_dir / "latest.json"
    if not repo_latest.exists():
        return False
    if not _should_bootstrap_ranking_payload(cfg.latest_path, repo_latest):
        print(
            "[INFO] bootstrap_from_git skipped: local ranking latest is newer or same "
            f"local={cfg.latest_path} repo={repo_latest}",
            flush=True,
        )
        return False

    cfg.latest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(repo_latest, cfg.latest_path)

    repo_history_dir = cfg.repo_dataset_dir / "history"
    if repo_history_dir.exists():
        target_history_dir = cfg.data_dir / "history"
        target_history_dir.mkdir(parents=True, exist_ok=True)
        for p in repo_history_dir.glob("*.json"):
            shutil.copy2(p, target_history_dir / p.name)

    repo_state = cfg.repo_dataset_dir / "state" / cfg.state_path.name
    if repo_state.exists():
        cfg.state_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repo_state, cfg.state_path)

    repo_status = cfg.repo_dataset_dir / "state" / cfg.status_path.name
    if repo_status.exists():
        cfg.status_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repo_status, cfg.status_path)

    repo_checkpoint = cfg.repo_dataset_dir / "state" / cfg.checkpoint_path.name
    if repo_checkpoint.exists():
        cfg.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repo_checkpoint, cfg.checkpoint_path)

    repo_source_wallet_state = cfg.repo_dataset_dir / "state" / cfg.source_wallet_state_path.name
    if repo_source_wallet_state.exists():
        cfg.source_wallet_state_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repo_source_wallet_state, cfg.source_wallet_state_path)

    if cfg.repo_market_cache_dir.exists():
        if cfg.market_cache_dir.exists():
            shutil.rmtree(cfg.market_cache_dir, ignore_errors=True)
        shutil.copytree(cfg.repo_market_cache_dir, cfg.market_cache_dir)
    return True


def _from_wallet_row(row: dict[str, Any]) -> WalletRecord | None:
    if not isinstance(row, dict):
        return None
    address = str(row.get("address") or "").strip().lower()
    if not address:
        return None
    rec = WalletRecord(
        address=address,
        username=str(row.get("username") or "").strip() or None,
        holdings_value_usdt=_to_decimal(row.get("holdings_value_usdt")),
        collectible_count=int(_to_decimal(row.get("collectible_count"))),
        pack_spent_usdt=_to_decimal(row.get("pack_spent_usdt")),
        trade_volume_usdt=_to_decimal(row.get("trade_volume_usdt")),
        trade_spent_usdt=_to_decimal(row.get("trade_spent_usdt")),
        trade_earned_usdt=_to_decimal(row.get("trade_earned_usdt")),
        buyback_earned_usdt=_to_decimal(row.get("buyback_earned_usdt")),
        card_withdraw_total_usdt=_to_decimal(row.get("card_withdraw_total_usdt")),
        total_spent_usdt=_to_decimal(row.get("total_spent_usdt")),
        total_earned_usdt=_to_decimal(row.get("total_earned_usdt")),
        cash_net_usdt=_to_decimal(row.get("cash_net_usdt")),
        total_pnl_usdt=_to_decimal(row.get("total_pnl_usdt")),
        participation_days_count=int(_to_decimal(row.get("participation_days_count"))),
        sbt_owned_total=int(_to_decimal(row.get("sbt_owned_total"))),
        sbt_owned_badge_count=int(_to_decimal(row.get("sbt_owned_badge_count"))),
        monthly_gacha_open_count=int(_to_decimal(row.get("monthly_gacha_open_count"))),
        total_spent_rank=(
            int(_to_decimal(row.get("total_spent_rank")))
            if str(row.get("total_spent_rank") or "").strip()
            else None
        ),
        monthly_gacha_open_rank=(
            int(_to_decimal(row.get("monthly_gacha_open_rank")))
            if str(row.get("monthly_gacha_open_rank") or "").strip()
            else None
        ),
        monthly_gacha_level=str(row.get("monthly_gacha_level") or "none").strip().lower() or "none",
    )
    return rec


def load_previous_wallets(cfg: RankingConfig) -> dict[str, WalletRecord]:
    data = _json_load(cfg.latest_path)
    rows = data.get("wallets") if isinstance(data.get("wallets"), list) else []
    out: dict[str, WalletRecord] = {}
    for row in rows:
        rec = _from_wallet_row(row)
        if rec is None:
            continue
        out[rec.address] = rec
    return out


def _wallet_record_to_dict(rec: WalletRecord) -> dict[str, Any]:
    return {
        "address": rec.address,
        "username": rec.username,
        "collectible_count": rec.collectible_count,
        "pack_spent_usdt": _decimal_to_str(rec.pack_spent_usdt),
        "trade_volume_usdt": _decimal_to_str(rec.trade_volume_usdt),
        "trade_spent_usdt": _decimal_to_str(rec.trade_spent_usdt),
        "trade_earned_usdt": _decimal_to_str(rec.trade_earned_usdt),
        "buyback_earned_usdt": _decimal_to_str(rec.buyback_earned_usdt),
        "card_withdraw_total_usdt": _decimal_to_str(rec.card_withdraw_total_usdt),
        "total_spent_usdt": _decimal_to_str(rec.total_spent_usdt),
        "total_earned_usdt": _decimal_to_str(rec.total_earned_usdt),
        "cash_net_usdt": _decimal_to_str(rec.cash_net_usdt),
        "holdings_value_usdt": _decimal_to_str(rec.holdings_value_usdt),
        "total_pnl_usdt": _decimal_to_str(rec.total_pnl_usdt),
        "participation_days_count": rec.participation_days_count,
        "sbt_owned_total": rec.sbt_owned_total,
        "sbt_owned_badge_count": rec.sbt_owned_badge_count,
        "monthly_gacha_open_count": rec.monthly_gacha_open_count,
        "volume_rank": rec.volume_rank,
        "total_spent_rank": rec.total_spent_rank,
        "holdings_rank": rec.holdings_rank,
        "pnl_rank": rec.pnl_rank,
        "participation_days_rank": rec.participation_days_rank,
        "sbt_rank": rec.sbt_rank,
        "monthly_gacha_open_rank": rec.monthly_gacha_open_rank,
        "monthly_gacha_level": rec.monthly_gacha_level,
    }


def load_previous_source_wallets(cfg: RankingConfig) -> tuple[dict[str, WalletRecord], bool]:
    data = _json_load(cfg.source_wallet_state_path)
    rows = data.get("wallets") if isinstance(data.get("wallets"), list) else []
    out: dict[str, WalletRecord] = {}
    for row in rows:
        rec = _from_wallet_row(row)
        if rec is None:
            continue
        out[rec.address] = rec
    if out:
        return out, True
    return load_previous_wallets(cfg), False


def _clone_wallet_records(rows: dict[str, WalletRecord]) -> dict[str, WalletRecord]:
    return {addr: replace(rec) for addr, rec in rows.items()}


def load_activity_checkpoints(cfg: RankingConfig) -> dict[str, dict[str, Any]]:
    data = _json_load(cfg.checkpoint_path)
    out: dict[str, dict[str, Any]] = {}
    for addr, row in (data.items() if isinstance(data, dict) else []):
        address = str(addr or "").strip().lower()
        if not address.startswith("0x"):
            continue
        if not isinstance(row, dict):
            continue
        cp = str(row.get("last_seen_activity_id") or "").strip()
        usdt_tx_hash = str(row.get("last_seen_usdt_tx_hash") or "").strip().lower()
        updated_at = str(row.get("updated_at") or "").strip()
        if not cp and not usdt_tx_hash:
            continue
        day_keys_raw = row.get("activity_day_keys")
        day_keys: list[str] = []
        if isinstance(day_keys_raw, list):
            for x in day_keys_raw:
                s = str(x or "").strip()
                if s:
                    day_keys.append(s)
        out[address] = {
            "last_seen_activity_id": cp,
            "last_seen_usdt_tx_hash": usdt_tx_hash,
            "updated_at": updated_at,
            "activity_day_keys": sorted(set(day_keys)),
        }
    return out


def load_pack_rank_state(cfg: RankingConfig) -> dict[str, Any]:
    data = _json_load(cfg.pack_rank_state_path)
    return data if isinstance(data, dict) else {}


def load_monthly_pack_counts_from_latest(
    cfg: RankingConfig,
    *,
    expected_window_start: str,
) -> tuple[dict[str, int], dict[str, Any], str]:
    path = cfg.pack_rank_latest_path
    payload = _json_load(path)
    if not isinstance(payload, dict) or not payload:
        return (
            {},
            {
                "scan_mode": "pack_rank_latest_unavailable",
                "api_calls": 0,
                "rows_scanned": 0,
                "reset_applied": False,
                "source_latest_path": str(path),
            },
            f"pack_rank_latest not readable: {path}",
        )

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    source_window_start = str(meta.get("monthly_gacha_window_start") or "").strip()
    if expected_window_start and source_window_start != expected_window_start:
        return (
            {},
            {
                "scan_mode": "pack_rank_latest_window_mismatch",
                "api_calls": 0,
                "rows_scanned": 0,
                "reset_applied": False,
                "source_latest_path": str(path),
                "source_window_start": source_window_start,
            },
            (
                "pack_rank_latest monthly window mismatch: "
                f"expected={expected_window_start} actual={source_window_start or '-'}"
            ),
        )

    rows = payload.get("wallets") if isinstance(payload.get("wallets"), list) else []
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        addr = str(row.get("address") or "").strip().lower()
        if not addr.startswith("0x") or len(addr) != 42:
            continue
        counts[addr] = max(0, int(_to_decimal(row.get("monthly_gacha_open_count"))))

    stats = {
        "scan_mode": "pack_rank_latest",
        "api_calls": 0,
        "rows_scanned": len(rows),
        "reset_applied": False,
        "source_latest_path": str(path),
        "source_updated_at": str(meta.get("updated_at") or ""),
        "source_wallet_count": int(_to_decimal(meta.get("wallet_count")) or len(rows)),
        "source_total_monthly_opens": int(_to_decimal(meta.get("total_monthly_opens")) or 0),
        "source_wallet_migration_pairs": int(_to_decimal(meta.get("wallet_migration_pairs")) or 0),
    }
    return counts, stats, ""


def build_checkpoint_dump(
    current_addrs: set[str],
    checkpoint_next: dict[str, dict[str, Any]],
    checkpoint_time: str,
) -> dict[str, Any]:
    checkpoint_dump: dict[str, Any] = {}
    for addr in sorted(current_addrs):
        row = checkpoint_next.get(addr) or {}
        cp = str(row.get("last_seen_activity_id") or "").strip()
        usdt_tx_hash = str(row.get("last_seen_usdt_tx_hash") or "").strip().lower()
        if not cp and not usdt_tx_hash:
            continue
        keys_raw = row.get("activity_day_keys")
        day_keys = (
            sorted({str(x or "").strip() for x in keys_raw if str(x or "").strip()})
            if isinstance(keys_raw, list)
            else []
        )
        checkpoint_dump[addr] = {
            "last_seen_activity_id": cp,
            "last_seen_usdt_tx_hash": usdt_tx_hash,
            "updated_at": checkpoint_time,
            "activity_day_keys": day_keys,
        }
    return checkpoint_dump


def load_holders_wallets(path: Path) -> dict[str, WalletRecord]:
    data = _json_load_any(path)
    rows: list[Any] = []
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        for k in ("holders", "result", "rows", "wallets"):
            v = data.get(k)
            if isinstance(v, list):
                rows = v
                break
    if not rows:
        return {}

    wallets: dict[str, WalletRecord] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        address = ""
        for field in ("owner_of", "ownerAddress", "owner_address", "address", "wallet"):
            address = str(row.get(field) or "").strip().lower()
            if address:
                break
        if not address:
            continue
        if address not in wallets:
            wallets[address] = WalletRecord(
                address=address,
                username=None,
                holdings_value_usdt=Decimal("0"),
                collectible_count=0,
            )
    print(f"[INFO] loaded holders wallets={len(wallets)} from {path}", flush=True)
    return wallets


def _empty_wallet_record(address: str) -> WalletRecord:
    return WalletRecord(
        address=address,
        username=None,
        holdings_value_usdt=Decimal("0"),
        collectible_count=0,
    )


def _canonical_wallet_address(address: str, old_to_new: dict[str, str]) -> str:
    current = _normalize_wallet_address(address)
    if not current:
        return ""
    seen: set[str] = set()
    while current in old_to_new and current not in seen:
        seen.add(current)
        next_addr = _normalize_wallet_address(old_to_new.get(current))
        if not next_addr:
            break
        current = next_addr
    return current


def _expand_wallets_for_migration(
    wallets: dict[str, WalletRecord],
    old_to_new: dict[str, str],
) -> int:
    if not old_to_new or not wallets:
        return 0
    linked_base = set(wallets.keys())
    added = 0
    for old_addr, new_addr in old_to_new.items():
        old_norm = _normalize_wallet_address(old_addr)
        new_norm = _normalize_wallet_address(new_addr)
        if not old_norm or not new_norm or old_norm == new_norm:
            continue
        if old_norm not in linked_base and new_norm not in linked_base:
            continue
        for addr in (old_norm, new_norm):
            if addr in wallets:
                continue
            wallets[addr] = _empty_wallet_record(addr)
            added += 1
    return added


def _canonicalize_int_counts(counts: dict[str, int], old_to_new: dict[str, str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for raw_addr, raw_count in (counts or {}).items():
        addr = _normalize_wallet_address(raw_addr)
        if not addr:
            continue
        canonical = _canonical_wallet_address(addr, old_to_new) or addr
        out[canonical] = int(out.get(canonical, 0)) + max(0, int(_to_decimal(raw_count) or 0))
    return out


def _apply_wallet_username_map(records: Any, username_map: dict[str, str]) -> int:
    if not username_map:
        return 0
    applied = 0
    for rec in records or []:
        if not isinstance(rec, WalletRecord):
            continue
        addr = _normalize_wallet_address(rec.address)
        if not addr:
            continue
        username = str(username_map.get(addr) or "").strip()
        if not username:
            continue
        if rec.username != username:
            rec.username = username
            applied += 1
    return applied


def fetch_latest_activity_id_for_wallet(address: str) -> tuple[str | None, bool]:
    wallet_norm = str(address or "").strip().lower()
    if not wallet_norm:
        return "", True
    try:
        page = _trpc_user_activities(wallet_norm, cursor=None, limit=1)
        rows = page.get("activities") or []
        if not isinstance(rows, list) or not rows:
            return "", True
        first = rows[0] if isinstance(rows[0], dict) else {}
        return str(first.get("id") or "").strip(), True
    except Exception:
        return None, False


def fetch_legacy_open_pack_hints_for_wallet(address: str, page_limit: int, max_pages: int) -> list[dict[str, Any]]:
    wallet_norm = str(address or "").strip().lower()
    if not wallet_norm:
        return []
    hints: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    for _ in range(max(1, int(max_pages or 1))):
        page = _trpc_user_activities(wallet_norm, cursor=cursor, limit=page_limit)
        rows = page.get("activities") or []
        if not isinstance(rows, list):
            rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_type = str(row.get("__typename") or "").strip()
            if row_type not in ("PullActivity", "PerpetualPullActivity"):
                continue
            price = _wei_to_usdt(row.get("priceInUsdt"))
            ts = _parse_int(row.get("timestamp")) or 0
            if price <= 0 or ts <= 0:
                continue
            row_item = row.get("item") if isinstance(row.get("item"), dict) else {}
            pack_id = str(row.get("packId") or "").strip()
            pack_label = _pack_label_from_pull_item(row_item)
            hints.append(
                {
                    "timestamp": int(ts),
                    "price": price,
                    "pack_key": (
                        str(row.get("contractAddress") or "").strip().lower()
                        if row_type == "PerpetualPullActivity"
                        and str(row.get("contractAddress") or "").strip().lower().startswith("0x")
                        and len(str(row.get("contractAddress") or "").strip().lower()) == 42
                        else f"legacy:{pack_id}" if pack_id else f"legacy-name:{pack_label}"
                    ),
                    "contract_address": str(row.get("contractAddress") or "").strip().lower(),
                }
            )
        next_cursor = page.get("nextCursor")
        if not next_cursor:
            break
        next_cursor = str(next_cursor)
        if next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    return hints


def _build_onchain_cfg(cfg: RankingConfig) -> OnchainConfig:
    return OnchainConfig(
        api_url=cfg.onchain_api_url,
        chain_id=cfg.onchain_chain_id,
        api_key=cfg.onchain_api_key,
        usdt_contract=cfg.onchain_usdt_contract,
        pack_contracts=cfg.onchain_pack_contracts,
        marketplace_contract=cfg.onchain_marketplace_contract,
        page_size=cfg.onchain_page_size,
        retries=API_MAX_RETRIES,
        backoff_sec=API_RETRY_BACKOFF_SEC,
        withdraw_addresses=tuple(sorted(_withdraw_target_set())),
        card_contract=str(
            os.getenv(
                "PROFILE_CHAIN_CARD_CONTRACT",
                os.getenv("WALLET_MIGRATION_CARD_CONTRACT", "0xf8646a3ca093e97bb404c3b25e675c0394dd5b30"),
            )
        ).strip().lower(),
    )


def fetch_latest_usdt_tx_hash_for_wallet(onchain_cfg: OnchainConfig, address: str) -> tuple[str | None, bool]:
    wallet_norm = str(address or "").strip().lower()
    if not wallet_norm:
        return "", True
    try:
        return fetch_latest_usdt_tx_hash(onchain_cfg, wallet_norm), True
    except Exception:
        return None, False


def fetch_all_wallets(
    cfg: RankingConfig,
    max_wallets: int | None = None,
    fallback_wallets: dict[str, WalletRecord] | None = None,
    wallet_migration_map: dict[str, str] | None = None,
) -> tuple[dict[str, WalletRecord], int, str]:
    limit = 100
    offset = 0
    total_pages = 0
    wallets: dict[str, WalletRecord] = {}
    if cfg.wallet_source == "holders_file" and cfg.holders_file is not None:
        wallets.update(load_holders_wallets(cfg.holders_file))
    migration_added = _expand_wallets_for_migration(wallets, wallet_migration_map or {})
    if migration_added:
        print(
            "[INFO] migration source wallets added "
            f"added={migration_added} base_wallets={len(wallets) - migration_added} source_wallets={len(wallets)}",
            flush=True,
        )
    holders_only_mode = cfg.wallet_source == "holders_file"
    print(
        f"[INFO] wallet_source={cfg.wallet_source} base_wallets={len(wallets)} "
        f"holders_file={str(cfg.holders_file) if cfg.holders_file is not None else '-'}",
        flush=True,
    )

    collection_fallback_reason = ""
    try:
        while True:
            payload = {
                "limit": limit,
                "offset": offset,
                "sortBy": "mintDate",
                "sortOrder": "desc",
                "includeOpenCardPackRecords": True,
            }
            result = _trpc_collectible_list(payload)
            total_pages += 1
            if total_pages == 1 or total_pages % 10 == 0:
                print(f"[PROGRESS] collectible_pages fetched={total_pages} wallets={len(wallets)}", flush=True)

            collection = result.get("collection") or []
            if not isinstance(collection, list):
                collection = []

            for item in collection:
                if not isinstance(item, dict):
                    continue
                address = str(item.get("ownerAddress") or "").strip().lower()
                if not address:
                    continue
                if holders_only_mode and address not in wallets:
                    # In holders-only mode, keep ranking universe strictly from NFT holders list.
                    continue

                owner = item.get("owner") if isinstance(item.get("owner"), dict) else {}
                username = str(owner.get("username") or "").strip() or None
                fmv_raw = _to_decimal(item.get("fmvPriceInUSD"))
                holdings_delta = fmv_raw / Decimal("100") if fmv_raw > 0 else Decimal("0")

                if address not in wallets:
                    wallets[address] = WalletRecord(
                        address=address,
                        username=username,
                        holdings_value_usdt=Decimal("0"),
                        collectible_count=0,
                    )

                rec = wallets[address]
                if (not rec.username) and username:
                    rec.username = username
                rec.holdings_value_usdt += holdings_delta
                rec.collectible_count += 1

            pagination = result.get("pagination") or {}
            has_more = bool(pagination.get("hasMore"))
            if not has_more:
                break

            next_limit = int(pagination.get("limit") or limit)
            next_offset = int(pagination.get("offset") or offset) + next_limit
            if next_offset <= offset:
                break
            offset = next_offset
    except Exception as e:  # noqa: BLE001
        collection_fallback_reason = f"{type(e).__name__}: {e}"
        fallback_source = fallback_wallets if isinstance(fallback_wallets, dict) else {}
        if fallback_source:
            wallets = _clone_wallet_records(fallback_source)
            fallback_migration_added = _expand_wallets_for_migration(wallets, wallet_migration_map or {})
            print(
                "[WARN] collectible.list failed; using previous latest wallet snapshot "
                f"wallets={len(wallets)} migration_added={fallback_migration_added} "
                f"pages_before_failure={total_pages} error={collection_fallback_reason}",
                flush=True,
            )
        elif wallets:
            print(
                "[WARN] collectible.list failed; using currently loaded holder wallet base "
                f"wallets={len(wallets)} pages_before_failure={total_pages} error={collection_fallback_reason}",
                flush=True,
            )
        else:
            raise

    if max_wallets is not None and len(wallets) > max_wallets:
        ordered_addrs = sorted(wallets.keys())[:max_wallets]
        wallets = {addr: wallets[addr] for addr in ordered_addrs}
    print(f"[INFO] final wallet_count={len(wallets)} collectible_pages={total_pages}", flush=True)

    return wallets, total_pages, collection_fallback_reason


def compute_activity_metrics_for_wallet(
    address: str,
    page_limit: int,
    max_pages: int,
    *,
    stop_activity_id: str | None = None,
) -> dict[str, Any]:
    cursor: str | None = None
    seen_cursors: set[str] = set()
    wallet_norm = str(address or "").strip().lower()

    pull_price_by_checkout: dict[str, Decimal] = {}
    contract_pull_price_counter: defaultdict[str, Counter] = defaultdict(Counter)
    legacy_missing_price_keys: list[str] = []
    release_rows: list[dict[str, Any]] = []

    pull_spent_total = Decimal("0")
    inferred_spent_total = Decimal("0")
    buyback_earned_total = Decimal("0")
    trade_spent_total = Decimal("0")
    trade_earned_total = Decimal("0")

    seen_withdraw_events: set[str] = set()
    withdraw_token_ids: set[str] = set()
    token_latest_values: dict[str, tuple[int, Decimal]] = {}
    day_keys: set[str] = set()

    def _remember_token_value(token_id: str, value: Decimal, ts_value: int):
        tid = str(token_id or "").strip()
        if not tid or value <= 0:
            return
        prev = token_latest_values.get(tid)
        if prev is None or ts_value >= prev[0]:
            token_latest_values[tid] = (ts_value, value)

    stop_id = str(stop_activity_id or "").strip()
    stop_reached = False
    latest_activity_id = ""

    for _ in range(max_pages):
        page = _trpc_user_activities(wallet_norm, cursor=cursor, limit=page_limit)
        rows = page.get("activities") or []
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            row_id = str(row.get("id") or "").strip()
            if not latest_activity_id and row_id:
                latest_activity_id = row_id
            if stop_id and row_id == stop_id:
                stop_reached = True
                break

            row_type = str(row.get("__typename") or "").strip()
            ts = _parse_int(row.get("timestamp")) or 0
            day_key = _day_key_from_ts(ts)
            if day_key:
                day_keys.add(day_key)
            row_item = row.get("item") if isinstance(row.get("item"), dict) else {}
            token_hint = str(row.get("nftTokenId") or row.get("tokenId") or row_item.get("tokenId") or "").strip()

            if row_type == "PerpetualPullActivity":
                checkout_id = str(row.get("checkoutId") or "").strip()
                contract = str(row.get("contractAddress") or "").strip().lower()
                price = _wei_to_usdt(row.get("priceInUsdt"))
                if checkout_id and price > 0:
                    pull_price_by_checkout[checkout_id] = price
                if contract and price > 0:
                    contract_pull_price_counter[contract][price] += 1
                    pull_spent_total += price
            elif row_type == "PullActivity":
                checkout_id = str(row.get("checkoutId") or "").strip()
                pack_id = str(row.get("packId") or "").strip()
                pack_label = _pack_label_from_pull_item(row_item)
                legacy_key = f"legacy:{pack_id}" if pack_id else f"legacy-name:{pack_label}"
                price = _wei_to_usdt(row.get("priceInUsdt"))
                if checkout_id and price > 0:
                    pull_price_by_checkout[checkout_id] = price
                if price > 0:
                    contract_pull_price_counter[legacy_key][price] += 1
                    pull_spent_total += price
                else:
                    legacy_missing_price_keys.append(legacy_key)
            elif row_type == "PerpetualReleaseTokenActivity":
                release_rows.append(row)
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
                    trade_earned_total += amount
            elif row_type == "TransferActivity":
                target = str(row.get("to") or "").strip().lower()
                if target not in _withdraw_target_set():
                    continue
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

        if stop_reached:
            break

        next_cursor = page.get("nextCursor")
        if not next_cursor:
            break
        next_cursor = str(next_cursor)
        if next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    for row in release_rows:
        contract = str(row.get("contractAddress") or "").strip().lower()
        checkout_id = str(row.get("checkoutId") or "").strip()
        price = Decimal("0")
        if checkout_id and checkout_id in pull_price_by_checkout:
            price = pull_price_by_checkout[checkout_id]
        if price <= 0:
            inferred = _pick_contract_unit_price(contract_pull_price_counter.get(contract) or Counter())
            if inferred > 0:
                inferred_spent_total += inferred

    for legacy_key in legacy_missing_price_keys:
        inferred = _pick_contract_unit_price(contract_pull_price_counter.get(legacy_key) or Counter())
        if inferred > 0:
            inferred_spent_total += inferred

    card_withdraw_total = Decimal("0")
    unresolved_withdraw_tokens: list[str] = []
    for token_id in withdraw_token_ids:
        hinted_value = _to_decimal((token_latest_values.get(token_id) or (0, Decimal("0")))[1])
        if hinted_value > 0:
            card_withdraw_total += hinted_value
        else:
            unresolved_withdraw_tokens.append(token_id)

    for token_id in unresolved_withdraw_tokens:
        fallback_value = _to_decimal(_fetch_card_withdraw_value_by_token_id(token_id))
        if fallback_value > 0:
            card_withdraw_total += fallback_value

    pack_spent_total = pull_spent_total + inferred_spent_total
    total_spent = pack_spent_total + trade_spent_total
    total_earned = buyback_earned_total + trade_earned_total
    cash_net = total_earned - total_spent + card_withdraw_total
    trade_volume = trade_spent_total + trade_earned_total

    return {
        "pack_spent_usdt": pack_spent_total,
        "trade_volume_usdt": trade_volume,
        "trade_spent_usdt": trade_spent_total,
        "trade_earned_usdt": trade_earned_total,
        "buyback_earned_usdt": buyback_earned_total,
        "card_withdraw_total_usdt": card_withdraw_total,
        "total_spent_usdt": total_spent,
        "total_earned_usdt": total_earned,
        "cash_net_usdt": cash_net,
        "latest_activity_id": latest_activity_id,
        "stop_reached": stop_reached,
        "activity_day_keys": sorted(day_keys),
    }


def compute_sbt_for_wallet_official(username: str | None) -> tuple[int, int]:
    if not username:
        return 0, 0
    badges = _trpc_user_badges(username)
    owned_total = 0
    owned_badge_count = 0
    for badge in badges:
        if not isinstance(badge, dict):
            continue
        if not bool(badge.get("isOwned")):
            continue
        balance = int(_to_decimal(badge.get("balance")))
        if balance <= 0:
            continue
        owned_total += balance
        owned_badge_count += 1
    return owned_total, owned_badge_count


def assign_rank(records: list[WalletRecord], value_getter, attr_name: str, *, require_positive: bool = False) -> None:
    for rec in records:
        setattr(rec, attr_name, None)
    if require_positive:
        sorted_records = [rec for rec in records if _to_decimal(value_getter(rec)) > 0]
        sorted_records.sort(key=lambda x: (value_getter(x), x.address), reverse=True)
    else:
        sorted_records = sorted(records, key=lambda x: (value_getter(x), x.address), reverse=True)
    prev_val = None
    current_rank = 0
    for idx, rec in enumerate(sorted_records, start=1):
        val = value_getter(rec)
        if prev_val is None or val != prev_val:
            current_rank = idx
            prev_val = val
        setattr(rec, attr_name, current_rank)


def _wallet_changed(prev: WalletRecord | None, curr: WalletRecord) -> bool:
    if prev is None:
        return True
    if (prev.username or "") != (curr.username or ""):
        return True
    if int(prev.collectible_count) != int(curr.collectible_count):
        return True
    return _quantize_2(prev.holdings_value_usdt) != _quantize_2(curr.holdings_value_usdt)


def _wallet_has_activity_history(rec: WalletRecord | None) -> bool:
    if rec is None:
        return False
    return any(
        _to_decimal(v) > 0
        for v in (
            rec.trade_volume_usdt,
            rec.pack_spent_usdt,
            rec.trade_spent_usdt,
            rec.trade_earned_usdt,
            rec.buyback_earned_usdt,
            rec.card_withdraw_total_usdt,
            rec.total_spent_usdt,
            rec.total_earned_usdt,
        )
    )


def _merge_wallet_records_by_migration(
    records: list[WalletRecord],
    old_to_new: dict[str, str],
    *,
    prev_day_keys_map: dict[str, set[str]],
    monthly_pack_counts_map: dict[str, int],
    monthly_pack_scan_ok: bool,
    finished_at: datetime,
) -> tuple[list[WalletRecord], dict[str, Any], dict[str, str]]:
    canonical_records: dict[str, WalletRecord] = {}
    canonical_day_keys: dict[str, set[str]] = defaultdict(set)
    canonical_participation_fallback: dict[str, int] = defaultdict(int)
    source_counts: dict[str, int] = defaultdict(int)
    source_to_canonical: dict[str, str] = {}

    for rec in records:
        source_addr = _normalize_wallet_address(rec.address)
        if not source_addr:
            continue
        canonical_addr = _canonical_wallet_address(source_addr, old_to_new) or source_addr
        source_to_canonical[source_addr] = canonical_addr
        source_counts[canonical_addr] += 1

        target = canonical_records.get(canonical_addr)
        if target is None:
            target = _empty_wallet_record(canonical_addr)
            canonical_records[canonical_addr] = target

        if source_addr == canonical_addr and rec.username:
            target.username = rec.username
        elif not target.username and rec.username:
            target.username = rec.username

        target.holdings_value_usdt += rec.holdings_value_usdt
        target.collectible_count += int(rec.collectible_count or 0)
        target.pack_spent_usdt += rec.pack_spent_usdt
        target.trade_volume_usdt += rec.trade_volume_usdt
        target.trade_spent_usdt += rec.trade_spent_usdt
        target.trade_earned_usdt += rec.trade_earned_usdt
        target.buyback_earned_usdt += rec.buyback_earned_usdt
        target.card_withdraw_total_usdt += rec.card_withdraw_total_usdt
        target.total_spent_usdt += rec.total_spent_usdt
        target.total_earned_usdt += rec.total_earned_usdt
        target.cash_net_usdt += rec.cash_net_usdt
        target.sbt_owned_total += int(rec.sbt_owned_total or 0)
        target.sbt_owned_badge_count += int(rec.sbt_owned_badge_count or 0)
        if not monthly_pack_scan_ok:
            target.monthly_gacha_open_count += int(rec.monthly_gacha_open_count or 0)

        day_keys = prev_day_keys_map.get(source_addr) or set()
        if day_keys:
            canonical_day_keys[canonical_addr].update(day_keys)
        canonical_participation_fallback[canonical_addr] = max(
            int(canonical_participation_fallback.get(canonical_addr) or 0),
            int(rec.participation_days_count or 0),
        )

    if monthly_pack_scan_ok:
        for canonical_addr, target in canonical_records.items():
            target.monthly_gacha_open_count = int(monthly_pack_counts_map.get(canonical_addr, 0))

    for canonical_addr, target in canonical_records.items():
        day_keys = canonical_day_keys.get(canonical_addr) or set()
        if day_keys:
            target.participation_days_count = _participation_days_since_join(day_keys, finished_at)
        else:
            target.participation_days_count = int(canonical_participation_fallback.get(canonical_addr) or 0)
        target.total_pnl_usdt = target.cash_net_usdt + target.holdings_value_usdt

    merged_records = sorted(canonical_records.values(), key=lambda x: x.address)
    migrated_source_wallets = sum(1 for source, canonical in source_to_canonical.items() if source != canonical)
    stats = {
        "wallet_migration_pairs": len(old_to_new),
        "source_wallet_count": len(records),
        "canonical_wallet_count": len(merged_records),
        "migrated_source_wallets": migrated_source_wallets,
        "canonical_wallets_from_multiple_sources": sum(1 for count in source_counts.values() if int(count) > 1),
    }
    return merged_records, stats, source_to_canonical


def build_source_wallet_state(
    records: list[WalletRecord],
    *,
    source_to_canonical: dict[str, str],
    cfg: RankingConfig,
    finished_at: datetime,
    migration_stats: dict[str, Any],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for rec in sorted(records, key=lambda x: x.address):
        row = _wallet_record_to_dict(rec)
        row["canonical_address"] = source_to_canonical.get(rec.address, rec.address)
        rows.append(row)
    return {
        "version": 1,
        "updated_at": finished_at.isoformat(),
        "trigger": cfg.trigger,
        "metrics_source": cfg.metrics_source,
        "wallet_migration_map_path": str(cfg.wallet_migration_map_path or wallet_migration_map_path()),
        "wallet_migration_pairs": int(_to_decimal(migration_stats.get("wallet_migration_pairs")) or 0),
        "source_wallet_count": int(_to_decimal(migration_stats.get("source_wallet_count")) or len(records)),
        "canonical_wallet_count": int(_to_decimal(migration_stats.get("canonical_wallet_count")) or 0),
        "migrated_source_wallets": int(_to_decimal(migration_stats.get("migrated_source_wallets")) or 0),
        "canonical_wallets_from_multiple_sources": int(
            _to_decimal(migration_stats.get("canonical_wallets_from_multiple_sources")) or 0
        ),
        "wallets": rows,
    }


def _parse_iso_dt(text: str | None) -> datetime | None:
    t = str(text or "").strip()
    if not t:
        return None
    try:
        return datetime.fromisoformat(t)
    except Exception:
        return None


def _should_full_rebuild(cfg: RankingConfig, now_dt: datetime, state: dict[str, Any]) -> tuple[bool, str]:
    if cfg.full_rebuild_requested:
        return True, "forced"
    last_full = _parse_iso_dt(state.get("last_full_rebuild_at") if isinstance(state, dict) else None)
    if last_full is None:
        return True, "no-last-full-rebuild"
    delta = now_dt - last_full
    if delta >= timedelta(days=cfg.full_rebuild_days):
        return True, f"interval-{cfg.full_rebuild_days}d"
    return False, "incremental"


def build_payload(
    records: list[WalletRecord],
    started_at: datetime,
    finished_at: datetime,
    *,
    collectible_pages: int,
    collection_fallback_reason: str,
    full_rebuild: bool,
    full_rebuild_reason: str,
    refreshed_wallets: int,
    changed_wallets: int,
    removed_wallets: int,
    wallet_source: str,
    holders_file: Path | None,
    monthly_pack_window_start: datetime,
    monthly_pack_window_end: datetime,
    monthly_pack_scan_stats: dict[str, Any] | None = None,
    sbt_scan_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    def _gacha_level(rank_value: int | None) -> str:
        rv = int(rank_value or 0)
        if rv <= 0:
            return "none"
        if rv <= 10:
            return "master"
        if rv <= 50:
            return "hunter"
        if rv <= 200:
            return "seeker"
        return "none"

    for rec in records:
        rec.monthly_gacha_open_count = max(0, int(rec.monthly_gacha_open_count or 0))

    assign_rank(records, lambda r: r.trade_volume_usdt, "volume_rank")
    assign_rank(records, lambda r: r.total_spent_usdt, "total_spent_rank")
    assign_rank(records, lambda r: r.holdings_value_usdt, "holdings_rank")
    assign_rank(records, lambda r: r.total_pnl_usdt, "pnl_rank")
    assign_rank(records, lambda r: r.participation_days_count, "participation_days_rank")
    assign_rank(records, lambda r: r.sbt_owned_total, "sbt_rank", require_positive=True)
    assign_rank(records, lambda r: r.monthly_gacha_open_count, "monthly_gacha_open_rank")

    for rec in records:
        if rec.monthly_gacha_open_count <= 0:
            rec.monthly_gacha_open_rank = None
            rec.monthly_gacha_level = "none"
            continue
        rec.monthly_gacha_level = _gacha_level(rec.monthly_gacha_open_rank)

    by_volume = sorted(records, key=lambda x: (x.trade_volume_usdt, x.address), reverse=True)
    by_total_spent = sorted(records, key=lambda x: (x.total_spent_usdt, x.address), reverse=True)
    by_holdings = sorted(records, key=lambda x: (x.holdings_value_usdt, x.address), reverse=True)
    by_pnl = sorted(records, key=lambda x: (x.total_pnl_usdt, x.address), reverse=True)
    by_participation = sorted(records, key=lambda x: (x.participation_days_count, x.address), reverse=True)
    by_sbt = sorted(
        (x for x in records if x.sbt_owned_total > 0),
        key=lambda x: (x.sbt_owned_total, x.address),
        reverse=True,
    )
    by_monthly_gacha = sorted(
        (x for x in records if x.monthly_gacha_open_count > 0),
        key=lambda x: (x.monthly_gacha_open_count, x.address),
        reverse=True,
    )

    return {
        "meta": {
            "timezone": "Asia/Taipei",
            "started_at": started_at.isoformat(),
            "updated_at": finished_at.isoformat(),
            "collectible_pages": collectible_pages,
            "collection_snapshot_fallback": bool(collection_fallback_reason),
            "collection_snapshot_fallback_reason": collection_fallback_reason,
            "wallet_count": len(records),
            "version": 4,
            "full_rebuild": bool(full_rebuild),
            "full_rebuild_reason": full_rebuild_reason,
            "refreshed_wallets": refreshed_wallets,
            "changed_wallets": changed_wallets,
            "removed_wallets": removed_wallets,
            "trigger": None,
            "wallet_source": wallet_source,
            "holders_file": str(holders_file) if holders_file is not None else None,
            "monthly_gacha_window_start": monthly_pack_window_start.isoformat(),
            "monthly_gacha_window_end": monthly_pack_window_end.isoformat(),
            "monthly_gacha_level_rules": {
                "master": "top10",
                "hunter": "top50",
                "seeker": "top200",
            },
            "monthly_gacha_scan_mode": str((monthly_pack_scan_stats or {}).get("scan_mode") or "none"),
            "monthly_gacha_scan_api_calls": int(_to_decimal((monthly_pack_scan_stats or {}).get("api_calls")) or 0),
            "monthly_gacha_scan_rows_scanned": int(_to_decimal((monthly_pack_scan_stats or {}).get("rows_scanned")) or 0),
            "monthly_gacha_scan_reset": bool((monthly_pack_scan_stats or {}).get("reset_applied")),
            "sbt_source": str((sbt_scan_stats or {}).get("source") or ""),
            "sbt_scan_mode": str((sbt_scan_stats or {}).get("scan_mode") or ""),
            "sbt_scan_api_calls": int(_to_decimal((sbt_scan_stats or {}).get("api_calls")) or 0),
            "sbt_scan_rows_scanned": int(_to_decimal((sbt_scan_stats or {}).get("rows_scanned")) or 0),
            "sbt_scan_reset": bool((sbt_scan_stats or {}).get("reset_applied")),
            "sbt_refresh_wallets": int(_to_decimal((sbt_scan_stats or {}).get("refresh_wallets")) or 0),
            "sbt_fallback_wallets": int(_to_decimal((sbt_scan_stats or {}).get("fallback_wallets")) or 0),
            "sbt_failed_wallets": int(_to_decimal((sbt_scan_stats or {}).get("failed_wallets")) or 0),
        },
        "top": {
            "volume": [_wallet_record_to_dict(x) for x in by_volume[:100]],
            "total_spent": [_wallet_record_to_dict(x) for x in by_total_spent[:100]],
            "holdings": [_wallet_record_to_dict(x) for x in by_holdings[:100]],
            "pnl": [_wallet_record_to_dict(x) for x in by_pnl[:100]],
            "participation_days": [_wallet_record_to_dict(x) for x in by_participation[:100]],
            "sbt": [_wallet_record_to_dict(x) for x in by_sbt[:100]],
            "monthly_gacha": [_wallet_record_to_dict(x) for x in by_monthly_gacha[:300]],
        },
        "wallets": [_wallet_record_to_dict(x) for x in sorted(records, key=lambda x: x.address)],
    }


def run_sync(cfg: RankingConfig) -> dict[str, Any]:
    started_at = datetime.now(tz=cfg.tzinfo)
    monthly_pack_window_start, monthly_pack_window_end = _monthly_pack_rank_window(cfg, started_at)
    monthly_window_start_iso = monthly_pack_window_start.isoformat()
    monthly_pack_window_start_ts = int(monthly_pack_window_start.astimezone(timezone.utc).timestamp())
    monthly_pack_window_end_ts = int(monthly_pack_window_end.astimezone(timezone.utc).timestamp())
    migration_map_path_text = str(cfg.wallet_migration_map_path or wallet_migration_map_path())
    old_to_new = load_wallet_migration_map(migration_map_path_text)
    if old_to_new:
        print(
            f"[INFO] wallet migration map loaded pairs={len(old_to_new)} path={migration_map_path_text}",
            flush=True,
        )
    else:
        print(
            f"[WARN] wallet migration map unavailable; ranking will not merge old/new wallets path={migration_map_path_text}",
            flush=True,
        )
    username_map_path_text = str(cfg.wallet_username_map_path or default_wallet_username_map_path())
    wallet_username_map = load_wallet_username_map(username_map_path_text)
    if wallet_username_map:
        print(
            f"[INFO] wallet username map loaded wallets={len(wallet_username_map)} path={username_map_path_text}",
            flush=True,
        )
    else:
        print(
            f"[WARN] wallet username map unavailable; ranking usernames will use collectible/latest only path={username_map_path_text}",
            flush=True,
        )
    prev_latest_payload = _json_load(cfg.latest_path)
    prev_meta = prev_latest_payload.get("meta") if isinstance(prev_latest_payload.get("meta"), dict) else {}
    prev_monthly_window_start = str(prev_meta.get("monthly_gacha_window_start") or "").strip()
    monthly_window_changed = bool(prev_monthly_window_start and prev_monthly_window_start != monthly_window_start_iso)
    prev_wallets, prev_source_state_loaded = load_previous_source_wallets(cfg)
    prev_state = _json_load(cfg.state_path)
    prev_sbt_state = _json_load(cfg.sbt_state_path)
    prev_checkpoints = load_activity_checkpoints(cfg)

    current_wallets, collectible_pages, collection_fallback_reason = fetch_all_wallets(
        cfg,
        max_wallets=cfg.max_wallets,
        fallback_wallets=prev_wallets,
        wallet_migration_map=old_to_new,
    )
    username_map_applied = _apply_wallet_username_map(current_wallets.values(), wallet_username_map)
    if wallet_username_map:
        print(
            f"[INFO] wallet username map applied source_wallets={username_map_applied}",
            flush=True,
        )
    current_addrs = set(current_wallets.keys())
    prev_addrs = set(prev_wallets.keys())
    removed_wallets = len(prev_addrs - current_addrs)
    changed_by_collection = {a for a in current_addrs if _wallet_changed(prev_wallets.get(a), current_wallets[a])}

    full_rebuild, full_reason = _should_full_rebuild(cfg, started_at, prev_state)
    if old_to_new and not prev_source_state_loaded and not full_rebuild:
        full_rebuild = True
        full_reason = "migration-source-state-bootstrap"
    if not full_rebuild and prev_checkpoints:
        for row in prev_checkpoints.values():
            keys = row.get("activity_day_keys")
            if not isinstance(keys, list):
                full_rebuild = True
                full_reason = "checkpoint-upgrade-participation-days"
                break
    records = list(current_wallets.values())
    # Preserve previous username when current collectible snapshot does not provide one.
    for rec in records:
        if rec.username:
            continue
        prev = prev_wallets.get(rec.address)
        if prev is not None and prev.username:
            rec.username = prev.username
    for rec in records:
        prev = prev_wallets.get(rec.address)
        if prev is not None and not monthly_window_changed:
            rec.monthly_gacha_open_count = int(prev.monthly_gacha_open_count or 0)
        else:
            rec.monthly_gacha_open_count = 0

    username_changed_addrs: set[str] = set()
    for rec in records:
        curr_username = str(rec.username or "").strip().lower()
        if not curr_username:
            continue
        prev = prev_wallets.get(rec.address)
        prev_username = str((prev.username if prev is not None else "") or "").strip().lower()
        if curr_username != prev_username:
            username_changed_addrs.add(rec.address)
    workers = max(1, min(cfg.workers, len(records))) if records else 1

    use_onchain_metrics = cfg.metrics_source in ("onchain", "hybrid")
    onchain_cfg = _build_onchain_cfg(cfg) if use_onchain_metrics else None
    pack_rank_onchain_cfg = _build_onchain_cfg(cfg) if bool(cfg.onchain_api_key) else None
    monthly_pack_counts_map: dict[str, int] = {}
    monthly_pack_scan_stats: dict[str, Any] | None = None
    monthly_pack_scan_ok = False
    if cfg.monthly_pack_source == "pack_rank_latest":
        monthly_pack_counts_map, monthly_pack_scan_stats, pack_latest_reason = load_monthly_pack_counts_from_latest(
            cfg,
            expected_window_start=monthly_window_start_iso,
        )
        if pack_latest_reason:
            print(f"[WARN] monthly pack latest unavailable: {pack_latest_reason}", flush=True)
        else:
            monthly_pack_scan_ok = True
            if old_to_new:
                monthly_pack_counts_map = _canonicalize_int_counts(monthly_pack_counts_map, old_to_new)
                if monthly_pack_scan_stats is not None:
                    monthly_pack_scan_stats["wallet_migration_pairs"] = len(old_to_new)
            print(
                "[INFO] monthly pack counts loaded from pack_rank_latest "
                f"wallets={len(monthly_pack_counts_map)} path={cfg.pack_rank_latest_path}",
                flush=True,
            )
    elif cfg.monthly_pack_source == "onchain" and pack_rank_onchain_cfg is not None:
        try:
            pack_rank_prev_state = {} if full_rebuild else _json_load(cfg.monthly_pack_scan_state_path)
            pack_scan = scan_wallet_open_counts_by_time_incremental(
                pack_rank_onchain_cfg,
                wallets=sorted(current_addrs),
                window_start_ts=monthly_pack_window_start_ts,
                window_end_ts=monthly_pack_window_end_ts,
                prev_state=pack_rank_prev_state,
            )
            pack_state = pack_scan.get("state") if isinstance(pack_scan.get("state"), dict) else {}
            if pack_state:
                _atomic_write_json(cfg.monthly_pack_scan_state_path, pack_state)
            monthly_pack_scan_stats = pack_scan.get("stats") if isinstance(pack_scan.get("stats"), dict) else {}
            raw_counts = pack_scan.get("wallet_counts")
            if isinstance(raw_counts, dict):
                for addr, raw in raw_counts.items():
                    key = str(addr or "").strip().lower()
                    if not key.startswith("0x") or len(key) != 42:
                        continue
                    monthly_pack_counts_map[key] = max(0, int(_to_decimal(raw)))
            if old_to_new:
                monthly_pack_counts_map = _canonicalize_int_counts(monthly_pack_counts_map, old_to_new)
                if monthly_pack_scan_stats is not None:
                    monthly_pack_scan_stats["wallet_migration_pairs"] = len(old_to_new)
            monthly_pack_scan_ok = True
        except Exception as e:
            print(f"[WARN] wallet-time monthly pack scan failed: {e}", flush=True)
    elif cfg.monthly_pack_source == "onchain":
        monthly_pack_scan_stats = {
            "scan_mode": "onchain_unavailable",
            "api_calls": 0,
            "rows_scanned": 0,
            "reset_applied": False,
        }
        print("[WARN] monthly pack onchain scan skipped: missing BSCSCAN_API_KEY", flush=True)
    else:
        monthly_pack_scan_stats = {
            "scan_mode": "disabled",
            "api_calls": 0,
            "rows_scanned": 0,
            "reset_applied": False,
        }
    latest_activity_map: dict[str, str] = {}
    latest_usdt_tx_map: dict[str, str] = {}
    changed_by_activity: set[str] = set()
    probe_failed_addrs: set[str] = set()
    activity_probe_failed = 0
    if not full_rebuild and current_addrs:
        # Participation-day updates always follow the original activity API semantics
        # (not on-chain tx-hash probing), so change detection for activity is unified.
        probe_addrs = set(current_addrs)

        probe_total = len(probe_addrs)
        probe_completed = 0
        _print_progress("latest_activity", 0, probe_total if probe_total > 0 else 1)
        with ThreadPoolExecutor(max_workers=max(1, min(workers, len(probe_addrs) if probe_addrs else 1))) as pool:
            future_map = {pool.submit(fetch_latest_activity_id_for_wallet, addr): addr for addr in probe_addrs}
            for future in as_completed(future_map):
                addr = future_map[future]
                try:
                    marker, ok = future.result()
                except Exception:
                    marker, ok = None, False
                probe_completed += 1
                _maybe_print_progress("latest_activity", probe_completed, probe_total, cfg.progress_every)
                if not ok:
                    probe_failed_addrs.add(addr)
                    continue
                latest_activity_map[addr] = str(marker or "")

        # Retry probe failures in delayed rounds (for transient 502/5xx).
        unresolved_probe = set(probe_failed_addrs)
        for retry_idx in range(cfg.failed_retry_rounds):
            if not unresolved_probe:
                break
            wait_sec = cfg.failed_retry_sleep_sec * (2 ** retry_idx)
            time.sleep(wait_sec)
            retry_batch = sorted(unresolved_probe)
            retry_total = len(retry_batch)
            retry_completed = 0
            _print_progress(f"latest_activity retry{retry_idx + 1}", 0, retry_total)
            with ThreadPoolExecutor(max_workers=max(1, min(workers, len(retry_batch)))) as pool:
                future_map = {pool.submit(fetch_latest_activity_id_for_wallet, addr): addr for addr in retry_batch}
                for future in as_completed(future_map):
                    addr = future_map[future]
                    try:
                        marker, ok = future.result()
                    except Exception:
                        marker, ok = None, False
                    retry_completed += 1
                    _maybe_print_progress(
                        f"latest_activity retry{retry_idx + 1}",
                        retry_completed,
                        retry_total,
                        cfg.progress_every,
                    )
                    if not ok:
                        continue
                    latest_activity_map[addr] = str(marker or "")
                    unresolved_probe.discard(addr)

        activity_probe_failed = len(unresolved_probe)

        for addr in probe_addrs:
            if addr not in prev_wallets:
                changed_by_activity.add(addr)
                continue
            prev_cp = str((prev_checkpoints.get(addr) or {}).get("last_seen_activity_id") or "")
            latest_id = latest_activity_map.get(addr)
            if latest_id is None:
                continue
            if latest_id and latest_id != prev_cp:
                changed_by_activity.add(addr)

    changed_addrs = changed_by_collection | changed_by_activity
    if full_rebuild:
        if cfg.full_rebuild_active_only:
            refresh_addrs = {
                a
                for a in current_addrs
                if (a not in prev_wallets) or _wallet_has_activity_history(prev_wallets.get(a))
            }
            print(
                "[INFO] full rebuild active-only enabled "
                f"refresh_addrs={len(refresh_addrs)} wallet_count={len(current_addrs)}",
                flush=True,
            )
        else:
            refresh_addrs = set(current_addrs)
    else:
        refresh_addrs = {a for a in current_addrs if (a not in prev_wallets) or (a in changed_by_activity)}
    sbt_refresh_addrs = set(refresh_addrs)
    if not full_rebuild:
        # SBT depends on username; refresh when username is newly resolved/changed
        # even if activity did not change.
        sbt_refresh_addrs |= username_changed_addrs

    checkpoint_next: dict[str, dict[str, Any]] = {}
    prev_day_keys_map: dict[str, set[str]] = {}
    for addr in current_addrs:
        prev_row = prev_checkpoints.get(addr) or {}
        raw_day_keys = prev_row.get("activity_day_keys")
        if isinstance(raw_day_keys, list):
            prev_day_keys_map[addr] = {str(x or "").strip() for x in raw_day_keys if str(x or "").strip()}
        else:
            prev_day_keys_map[addr] = set()
        cp = str(prev_row.get("last_seen_activity_id") or "").strip()
        usdt_tx_hash = str(prev_row.get("last_seen_usdt_tx_hash") or "").strip().lower()
        if cp or usdt_tx_hash:
            checkpoint_next[addr] = {
                "last_seen_activity_id": cp,
                "last_seen_usdt_tx_hash": usdt_tx_hash,
                "updated_at": str(prev_row.get("updated_at") or ""),
                "activity_day_keys": sorted(prev_day_keys_map.get(addr) or set()),
            }
        latest_id = str(latest_activity_map.get(addr) or "").strip()
        if latest_id:
            checkpoint_next[addr] = {
                "last_seen_activity_id": latest_id,
                "last_seen_usdt_tx_hash": usdt_tx_hash,
                "updated_at": str(prev_row.get("updated_at") or ""),
                "activity_day_keys": sorted(prev_day_keys_map.get(addr) or set()),
            }
        latest_tx_hash = str(latest_usdt_tx_map.get(addr) or "").strip().lower()
        if latest_tx_hash:
            checkpoint_row = checkpoint_next.get(addr) or {
                "last_seen_activity_id": cp,
                "updated_at": str(prev_row.get("updated_at") or ""),
                "activity_day_keys": sorted(prev_day_keys_map.get(addr) or set()),
            }
            checkpoint_row["last_seen_usdt_tx_hash"] = latest_tx_hash
            checkpoint_next[addr] = checkpoint_row

    refresh_cnt = 0
    pending_checkpoint_updates = 0

    def _flush_progress_checkpoint(force: bool = False) -> None:
        nonlocal pending_checkpoint_updates
        if not force and pending_checkpoint_updates < cfg.checkpoint_flush_every:
            return
        now_iso = datetime.now(tz=cfg.tzinfo).isoformat()
        dump = build_checkpoint_dump(current_addrs, checkpoint_next, now_iso)
        _atomic_write_json(cfg.checkpoint_path, dump)
        print(
            f"[INFO] checkpoint flushed entries={len(dump)} "
            f"reason={'final' if force else f'every_{cfg.checkpoint_flush_every}'}",
            flush=True,
        )
        pending_checkpoint_updates = 0

    for rec in records:
        prev = prev_wallets.get(rec.address)
        if rec.address not in refresh_addrs and prev is not None:
            rec.pack_spent_usdt = prev.pack_spent_usdt
            rec.trade_volume_usdt = prev.trade_volume_usdt
            rec.trade_spent_usdt = prev.trade_spent_usdt
            rec.trade_earned_usdt = prev.trade_earned_usdt
            rec.buyback_earned_usdt = prev.buyback_earned_usdt
            rec.card_withdraw_total_usdt = prev.card_withdraw_total_usdt
            rec.total_spent_usdt = prev.total_spent_usdt
            rec.total_earned_usdt = prev.total_earned_usdt
            rec.cash_net_usdt = prev.cash_net_usdt
            rec.participation_days_count = prev.participation_days_count
            rec.sbt_owned_total = prev.sbt_owned_total
            rec.sbt_owned_badge_count = prev.sbt_owned_badge_count
            rec.monthly_gacha_open_count = 0 if monthly_window_changed else prev.monthly_gacha_open_count

    if refresh_addrs:
        refresh_records = [r for r in records if r.address in refresh_addrs]
        refresh_cnt = len(refresh_records)

        def _apply_metrics_fallback(rec: WalletRecord, prev: WalletRecord | None) -> None:
            if prev is not None:
                rec.pack_spent_usdt = prev.pack_spent_usdt
                rec.trade_volume_usdt = prev.trade_volume_usdt
                rec.trade_spent_usdt = prev.trade_spent_usdt
                rec.trade_earned_usdt = prev.trade_earned_usdt
                rec.buyback_earned_usdt = prev.buyback_earned_usdt
                rec.card_withdraw_total_usdt = prev.card_withdraw_total_usdt
                rec.total_spent_usdt = prev.total_spent_usdt
                rec.total_earned_usdt = prev.total_earned_usdt
                rec.cash_net_usdt = prev.cash_net_usdt
                rec.participation_days_count = prev.participation_days_count
                rec.monthly_gacha_open_count = 0 if monthly_window_changed else prev.monthly_gacha_open_count
            else:
                rec.pack_spent_usdt = Decimal("0")
                rec.trade_volume_usdt = Decimal("0")
                rec.trade_spent_usdt = Decimal("0")
                rec.trade_earned_usdt = Decimal("0")
                rec.buyback_earned_usdt = Decimal("0")
                rec.card_withdraw_total_usdt = Decimal("0")
                rec.total_spent_usdt = Decimal("0")
                rec.total_earned_usdt = Decimal("0")
                rec.cash_net_usdt = Decimal("0")
                rec.participation_days_count = 0
                rec.monthly_gacha_open_count = 0

        def _apply_metrics_result(rec: WalletRecord, prev: WalletRecord | None, metrics: dict[str, Any], delta_mode: bool) -> None:
            latest_id = str(metrics.get("latest_activity_id") or "").strip()
            new_day_keys_raw = metrics.get("activity_day_keys")
            new_day_keys = (
                {str(x or "").strip() for x in new_day_keys_raw if str(x or "").strip()}
                if isinstance(new_day_keys_raw, list)
                else set()
            )
            base_day_keys = set(prev_day_keys_map.get(rec.address) or set())
            merged_day_keys = (base_day_keys | new_day_keys) if (delta_mode and prev is not None) else new_day_keys
            prev_hash = str((checkpoint_next.get(rec.address) or {}).get("last_seen_usdt_tx_hash") or "").strip().lower()
            if latest_id:
                checkpoint_next[rec.address] = {
                    "last_seen_activity_id": latest_id,
                    "last_seen_usdt_tx_hash": prev_hash,
                    "updated_at": str((prev_checkpoints.get(rec.address) or {}).get("updated_at") or ""),
                    "activity_day_keys": sorted(merged_day_keys),
                }
            elif rec.address in checkpoint_next:
                checkpoint_next[rec.address]["activity_day_keys"] = sorted(merged_day_keys)
            latest_tx_hash = str(metrics.get("last_seen_usdt_tx_hash") or "").strip().lower()
            if latest_tx_hash:
                checkpoint_row = checkpoint_next.get(rec.address) or {
                    "last_seen_activity_id": latest_id,
                    "updated_at": str((prev_checkpoints.get(rec.address) or {}).get("updated_at") or ""),
                    "activity_day_keys": sorted(merged_day_keys),
                }
                checkpoint_row["last_seen_usdt_tx_hash"] = latest_tx_hash
                checkpoint_next[rec.address] = checkpoint_row
            prev_day_keys_map[rec.address] = set(merged_day_keys)
            rec.participation_days_count = len(merged_day_keys)
            if delta_mode and prev is not None:
                rec.pack_spent_usdt = prev.pack_spent_usdt + _to_decimal(metrics.get("pack_spent_usdt"))
                rec.trade_volume_usdt = prev.trade_volume_usdt + _to_decimal(metrics.get("trade_volume_usdt"))
                rec.trade_spent_usdt = prev.trade_spent_usdt + _to_decimal(metrics.get("trade_spent_usdt"))
                rec.trade_earned_usdt = prev.trade_earned_usdt + _to_decimal(metrics.get("trade_earned_usdt"))
                rec.buyback_earned_usdt = prev.buyback_earned_usdt + _to_decimal(metrics.get("buyback_earned_usdt"))
                rec.card_withdraw_total_usdt = prev.card_withdraw_total_usdt + _to_decimal(
                    metrics.get("card_withdraw_total_usdt")
                )
                rec.total_spent_usdt = prev.total_spent_usdt + _to_decimal(metrics.get("total_spent_usdt"))
                rec.total_earned_usdt = prev.total_earned_usdt + _to_decimal(metrics.get("total_earned_usdt"))
                rec.cash_net_usdt = prev.cash_net_usdt + _to_decimal(metrics.get("cash_net_usdt"))
            else:
                rec.pack_spent_usdt = _to_decimal(metrics.get("pack_spent_usdt"))
                rec.trade_volume_usdt = _to_decimal(metrics.get("trade_volume_usdt"))
                rec.trade_spent_usdt = _to_decimal(metrics.get("trade_spent_usdt"))
                rec.trade_earned_usdt = _to_decimal(metrics.get("trade_earned_usdt"))
                rec.buyback_earned_usdt = _to_decimal(metrics.get("buyback_earned_usdt"))
                rec.card_withdraw_total_usdt = _to_decimal(metrics.get("card_withdraw_total_usdt"))
                rec.total_spent_usdt = _to_decimal(metrics.get("total_spent_usdt"))
                rec.total_earned_usdt = _to_decimal(metrics.get("total_earned_usdt"))
                rec.cash_net_usdt = _to_decimal(metrics.get("cash_net_usdt"))

        def _metrics_worker(rec: WalletRecord) -> tuple[str, dict[str, Any] | None, bool, Exception | None]:
            prev = prev_wallets.get(rec.address)
            prev_cp = str((prev_checkpoints.get(rec.address) or {}).get("last_seen_activity_id") or "")
            try:
                if use_onchain_metrics:
                    assert onchain_cfg is not None
                    chain_metrics = analyze_wallet_onchain(
                        onchain_cfg,
                        rec.address,
                        legacy_open_pack_hints=None,
                        use_delayed_recipient_heuristic=False,
                        strict_pack_contracts_only=True,
                    )
                    prev_day_keys = sorted(prev_day_keys_map.get(rec.address) or set()) if prev is not None else []
                    activity_day_keys = list(prev_day_keys)
                    latest_activity_id = prev_cp
                    stop_reached = True
                    should_refresh_activity_days = bool(
                        (prev is None) or full_rebuild or (rec.address in changed_by_activity)
                    )
                    if should_refresh_activity_days:
                        if (not full_rebuild) and (prev is not None) and prev_cp:
                            activity_metrics = compute_activity_metrics_for_wallet(
                                rec.address,
                                cfg.activity_page_limit,
                                cfg.activity_max_pages,
                                stop_activity_id=prev_cp,
                            )
                            latest_activity_id = str(activity_metrics.get("latest_activity_id") or prev_cp)
                            stop_reached = bool(activity_metrics.get("stop_reached"))
                            new_day_keys = {
                                str(x or "").strip()
                                for x in (activity_metrics.get("activity_day_keys") or [])
                                if str(x or "").strip()
                            }
                            activity_day_keys = sorted(set(prev_day_keys) | new_day_keys)
                        else:
                            activity_metrics = compute_activity_metrics_for_wallet(
                                rec.address,
                                cfg.activity_page_limit,
                                cfg.activity_max_pages,
                                stop_activity_id=None,
                            )
                            latest_activity_id = str(activity_metrics.get("latest_activity_id") or prev_cp)
                            stop_reached = bool(activity_metrics.get("stop_reached"))
                            activity_day_keys = sorted(
                                {
                                    str(x or "").strip()
                                    for x in (activity_metrics.get("activity_day_keys") or [])
                                    if str(x or "").strip()
                                }
                            )
                    card_withdraw_total = _card_withdraw_total_from_token_ids(
                        chain_metrics.get("withdraw_token_ids")
                    )
                    return (
                        rec.address,
                        {
                            "pack_spent_usdt": chain_metrics.get("pack_spent_usdt", Decimal("0")),
                            "trade_volume_usdt": chain_metrics.get("trade_volume_usdt", Decimal("0")),
                            "trade_spent_usdt": chain_metrics.get("trade_spent_usdt", Decimal("0")),
                            "trade_earned_usdt": chain_metrics.get("trade_earned_usdt", Decimal("0")),
                            "buyback_earned_usdt": chain_metrics.get("buyback_earned_usdt", Decimal("0")),
                            "card_withdraw_total_usdt": card_withdraw_total,
                            "total_spent_usdt": chain_metrics.get("total_spent_usdt", Decimal("0")),
                            "total_earned_usdt": chain_metrics.get("total_earned_usdt", Decimal("0")),
                            "cash_net_usdt": (
                                _to_decimal(chain_metrics.get("cash_net_usdt"))
                                + card_withdraw_total
                            ),
                            "latest_activity_id": latest_activity_id,
                            "stop_reached": stop_reached,
                            "activity_day_keys": activity_day_keys,
                            "last_seen_usdt_tx_hash": str(latest_usdt_tx_map.get(rec.address) or "").strip().lower(),
                        },
                        False,
                        None,
                    )
                if (not full_rebuild) and (prev is not None) and (rec.address in changed_by_activity) and prev_cp:
                    delta = compute_activity_metrics_for_wallet(
                        rec.address,
                        cfg.activity_page_limit,
                        cfg.activity_max_pages,
                        stop_activity_id=prev_cp,
                    )
                    if bool(delta.get("stop_reached")):
                        return rec.address, delta, True, None
                full = compute_activity_metrics_for_wallet(
                    rec.address,
                    cfg.activity_page_limit,
                    cfg.activity_max_pages,
                    stop_activity_id=None,
                )
                return rec.address, full, False, None
            except Exception as e:  # noqa: BLE001
                return rec.address, None, False, e

        failed_metrics_records: dict[str, WalletRecord] = {}
        metric_failed_initial = 0
        metrics_total = len(refresh_records)
        metrics_completed = 0
        _print_progress("activity_metrics", 0, metrics_total)
        with ThreadPoolExecutor(max_workers=max(1, min(workers, refresh_cnt))) as pool:
            future_map = {pool.submit(_metrics_worker, rec): rec for rec in refresh_records}
            for future in as_completed(future_map):
                rec = future_map[future]
                addr, metrics, delta_mode, err = future.result()
                metrics_completed += 1
                pending_checkpoint_updates += 1
                _maybe_print_progress("activity_metrics", metrics_completed, metrics_total, cfg.progress_every)
                prev = prev_wallets.get(addr)
                if err is not None or metrics is None:
                    print(f"[WARN] activity metrics failed for {addr}: {err}")
                    failed_metrics_records[addr] = rec
                    metric_failed_initial += 1
                    _flush_progress_checkpoint()
                    continue

                _apply_metrics_result(rec, prev, metrics, delta_mode)
                _flush_progress_checkpoint()

        metric_failed_resolved = 0
        unresolved_metrics = dict(failed_metrics_records)
        for retry_idx in range(cfg.failed_retry_rounds):
            if not unresolved_metrics:
                break
            wait_sec = cfg.failed_retry_sleep_sec * (2 ** retry_idx)
            time.sleep(wait_sec)
            retry_records = list(unresolved_metrics.values())
            retry_total = len(retry_records)
            retry_completed = 0
            _print_progress(f"activity_metrics retry{retry_idx + 1}", 0, retry_total)
            with ThreadPoolExecutor(max_workers=max(1, min(workers, len(retry_records)))) as pool:
                future_map = {pool.submit(_metrics_worker, rec): rec for rec in retry_records}
                for future in as_completed(future_map):
                    rec = future_map[future]
                    addr, metrics, delta_mode, err = future.result()
                    retry_completed += 1
                    pending_checkpoint_updates += 1
                    _maybe_print_progress(
                        f"activity_metrics retry{retry_idx + 1}",
                        retry_completed,
                        retry_total,
                        cfg.progress_every,
                    )
                    if err is not None or metrics is None:
                        print(f"[WARN] activity metrics retry failed for {addr}: {err}")
                        _flush_progress_checkpoint()
                        continue
                    prev = prev_wallets.get(addr)
                    _apply_metrics_result(rec, prev, metrics, delta_mode)
                    unresolved_metrics.pop(addr, None)
                    metric_failed_resolved += 1
                    _flush_progress_checkpoint()

        metric_failed_unresolved = len(unresolved_metrics)
        for addr, rec in unresolved_metrics.items():
            _apply_metrics_fallback(rec, prev_wallets.get(addr))
        _flush_progress_checkpoint(force=True)

    else:
        metric_failed_initial = 0
        metric_failed_resolved = 0
        metric_failed_unresolved = 0

    sbt_scan_stats: dict[str, Any] = {
        "source": cfg.sbt_source,
        "scan_mode": "not_run",
        "api_calls": 0,
        "rows_scanned": 0,
        "refresh_wallets": 0,
        "fallback_wallets": 0,
        "failed_wallets": 0,
    }

    def _apply_sbt_fallback(rec: WalletRecord) -> None:
        prev = prev_wallets.get(rec.address)
        if prev is not None:
            rec.sbt_owned_total = prev.sbt_owned_total
            rec.sbt_owned_badge_count = prev.sbt_owned_badge_count
        else:
            rec.sbt_owned_total = 0
            rec.sbt_owned_badge_count = 0

    sbt_records = records if cfg.sbt_source == "onchain_contract" else [r for r in records if r.address in sbt_refresh_addrs]
    if sbt_records:
        sbt_phase_total = len(sbt_records)
        sbt_scan_stats["refresh_wallets"] = sbt_phase_total
        if cfg.sbt_source == "onchain_contract":
            try:
                print(
                    f"[INFO] sbt source=onchain_contract contract={cfg.onchain_sbt_contract} "
                    f"wallets={len(records)}",
                    flush=True,
                )
                scan_result = scan_erc1155_contract_balances(
                    _build_onchain_cfg(cfg),
                    cfg.onchain_sbt_contract,
                    target_wallets={rec.address for rec in records},
                    prev_state=prev_sbt_state,
                )
                wallet_sbt_map = scan_result.get("wallets") if isinstance(scan_result.get("wallets"), dict) else {}
                next_sbt_state = scan_result.get("state") if isinstance(scan_result.get("state"), dict) else {}
                raw_stats = scan_result.get("stats") if isinstance(scan_result.get("stats"), dict) else {}
                sbt_scan_stats.update(
                    {
                        "scan_mode": str(raw_stats.get("scan_mode") or "erc1155_contract_token1155tx"),
                        "api_calls": int(_to_decimal(raw_stats.get("api_calls")) or 0),
                        "rows_scanned": int(_to_decimal(raw_stats.get("rows_scanned")) or 0),
                        "reset_applied": bool(raw_stats.get("reset_applied")),
                        "contract": cfg.onchain_sbt_contract,
                    }
                )
                if next_sbt_state:
                    _atomic_write_json(cfg.sbt_state_path, next_sbt_state)
                for rec in sbt_records:
                    row = wallet_sbt_map.get(rec.address) if isinstance(wallet_sbt_map, dict) else None
                    if isinstance(row, dict):
                        rec.sbt_owned_total = max(0, int(_to_decimal(row.get("owned_total")) or 0))
                        rec.sbt_owned_badge_count = max(0, int(_to_decimal(row.get("owned_badge_count")) or 0))
                    else:
                        rec.sbt_owned_total = 0
                        rec.sbt_owned_badge_count = 0
                print(
                    "[INFO] sbt onchain_contract done "
                    f"api_calls={sbt_scan_stats['api_calls']} rows={sbt_scan_stats['rows_scanned']} "
                    f"holders={len(wallet_sbt_map)}",
                    flush=True,
                )
            except Exception as e:  # noqa: BLE001
                print(f"[WARN] sbt onchain_contract failed, fallback to previous snapshot: {e}", flush=True)
                sbt_scan_stats["scan_mode"] = "fallback_previous_snapshot"
                sbt_scan_stats["fallback_wallets"] = sbt_phase_total
                sbt_scan_stats["failed_wallets"] = sbt_phase_total
                for rec in sbt_records:
                    _apply_sbt_fallback(rec)
        else:
            sbt_completed = 0
            _print_progress("sbt", 0, sbt_phase_total)
            with ThreadPoolExecutor(max_workers=max(1, min(workers, len(sbt_records)))) as pool:
                future_map = {pool.submit(compute_sbt_for_wallet_official, rec.username): rec for rec in sbt_records}
                for future in as_completed(future_map):
                    rec = future_map[future]
                    try:
                        sbt_owned_total, sbt_badge_count = future.result()
                    except Exception as e:  # noqa: BLE001
                        print(f"[WARN] sbt failed for {rec.address} ({rec.username}): {e}")
                        sbt_scan_stats["fallback_wallets"] = int(sbt_scan_stats.get("fallback_wallets") or 0) + 1
                        sbt_scan_stats["failed_wallets"] = int(sbt_scan_stats.get("failed_wallets") or 0) + 1
                        _apply_sbt_fallback(rec)
                        sbt_completed += 1
                        _maybe_print_progress("sbt", sbt_completed, sbt_phase_total, cfg.progress_every)
                        continue
                    sbt_completed += 1
                    _maybe_print_progress("sbt", sbt_completed, sbt_phase_total, cfg.progress_every)
                    rec.sbt_owned_total = sbt_owned_total
                    rec.sbt_owned_badge_count = sbt_badge_count
            sbt_scan_stats["scan_mode"] = "official_username"

    finished_at = datetime.now(tz=cfg.tzinfo)
    for rec in records:
        day_keys = set(prev_day_keys_map.get(rec.address) or set())
        if day_keys:
            rec.participation_days_count = _participation_days_since_join(day_keys, finished_at)
        else:
            prev = prev_wallets.get(rec.address)
            rec.participation_days_count = prev.participation_days_count if prev is not None else 0
        rec.total_pnl_usdt = rec.cash_net_usdt + rec.holdings_value_usdt
        if monthly_pack_scan_ok:
            rec.monthly_gacha_open_count = int(monthly_pack_counts_map.get(rec.address, 0))

    checkpoint_time = finished_at.isoformat()
    checkpoint_dump = build_checkpoint_dump(current_addrs, checkpoint_next, checkpoint_time)
    canonical_records, migration_stats, source_to_canonical = _merge_wallet_records_by_migration(
        records,
        old_to_new,
        prev_day_keys_map=prev_day_keys_map,
        monthly_pack_counts_map=monthly_pack_counts_map,
        monthly_pack_scan_ok=monthly_pack_scan_ok,
        finished_at=finished_at,
    )
    canonical_username_map_applied = _apply_wallet_username_map(canonical_records.values(), wallet_username_map)
    source_state_payload = build_source_wallet_state(
        records,
        source_to_canonical=source_to_canonical,
        cfg=cfg,
        finished_at=finished_at,
        migration_stats=migration_stats,
    )

    payload = build_payload(
        canonical_records,
        started_at,
        finished_at,
        collectible_pages=collectible_pages,
        collection_fallback_reason=collection_fallback_reason,
        full_rebuild=full_rebuild,
        full_rebuild_reason=full_reason,
        refreshed_wallets=refresh_cnt,
        changed_wallets=len(changed_addrs),
        removed_wallets=removed_wallets,
        wallet_source=cfg.wallet_source,
        holders_file=cfg.holders_file,
        monthly_pack_window_start=monthly_pack_window_start,
        monthly_pack_window_end=monthly_pack_window_end,
        monthly_pack_scan_stats=monthly_pack_scan_stats,
        sbt_scan_stats=sbt_scan_stats,
    )
    if isinstance(payload.get("meta"), dict):
        payload["meta"]["trigger"] = cfg.trigger
        payload["meta"]["metrics_source"] = cfg.metrics_source
        payload["meta"]["monthly_gacha_source"] = cfg.monthly_pack_source
        payload["meta"]["sbt_source"] = cfg.sbt_source
        payload["meta"]["full_rebuild_active_only"] = bool(cfg.full_rebuild_active_only)
        payload["meta"]["collection_snapshot_fallback"] = bool(collection_fallback_reason)
        payload["meta"]["collection_snapshot_fallback_reason"] = collection_fallback_reason
        payload["meta"]["wallet_migration_map_path"] = migration_map_path_text
        payload["meta"]["wallet_migration_pairs"] = int(
            _to_decimal(migration_stats.get("wallet_migration_pairs")) or 0
        )
        payload["meta"]["source_wallet_count"] = int(_to_decimal(migration_stats.get("source_wallet_count")) or 0)
        payload["meta"]["canonical_wallet_count"] = int(
            _to_decimal(migration_stats.get("canonical_wallet_count")) or len(canonical_records)
        )
        payload["meta"]["migrated_source_wallets"] = int(
            _to_decimal(migration_stats.get("migrated_source_wallets")) or 0
        )
        payload["meta"]["canonical_wallets_from_multiple_sources"] = int(
            _to_decimal(migration_stats.get("canonical_wallets_from_multiple_sources")) or 0
        )
        payload["meta"]["source_wallet_state_path"] = str(cfg.source_wallet_state_path)
        payload["meta"]["source_wallet_state_loaded"] = bool(prev_source_state_loaded)
        payload["meta"]["wallet_username_map_path"] = username_map_path_text
        payload["meta"]["wallet_username_map_wallets"] = len(wallet_username_map)
        payload["meta"]["wallet_username_map_applied_source_wallets"] = username_map_applied
        payload["meta"]["wallet_username_map_applied_canonical_wallets"] = canonical_username_map_applied

    _atomic_write_json(cfg.latest_path, payload)
    _atomic_write_json(cfg.history_path(finished_at), payload)
    _atomic_write_json(cfg.checkpoint_path, checkpoint_dump)
    _atomic_write_json(cfg.source_wallet_state_path, source_state_payload)

    prev_last_full = str(prev_state.get("last_full_rebuild_at") or "")
    next_last_full = finished_at.isoformat() if full_rebuild else (prev_last_full or "")
    state_payload = {
        "updated_at": finished_at.isoformat(),
        "trigger": cfg.trigger,
        "metrics_source": cfg.metrics_source,
        "wallet_count": len(canonical_records),
        "source_wallet_count": int(_to_decimal(migration_stats.get("source_wallet_count")) or len(records)),
        "canonical_wallet_count": int(
            _to_decimal(migration_stats.get("canonical_wallet_count")) or len(canonical_records)
        ),
        "wallet_migration_map_path": migration_map_path_text,
        "wallet_migration_pairs": int(_to_decimal(migration_stats.get("wallet_migration_pairs")) or 0),
        "migrated_source_wallets": int(_to_decimal(migration_stats.get("migrated_source_wallets")) or 0),
        "canonical_wallets_from_multiple_sources": int(
            _to_decimal(migration_stats.get("canonical_wallets_from_multiple_sources")) or 0
        ),
        "source_wallet_state_path": str(cfg.source_wallet_state_path),
        "source_wallet_state_loaded": bool(prev_source_state_loaded),
        "wallet_username_map_path": username_map_path_text,
        "wallet_username_map_wallets": len(wallet_username_map),
        "wallet_username_map_applied_source_wallets": username_map_applied,
        "wallet_username_map_applied_canonical_wallets": canonical_username_map_applied,
        "collectible_pages": collectible_pages,
        "collection_snapshot_fallback": bool(collection_fallback_reason),
        "collection_snapshot_fallback_reason": collection_fallback_reason,
        "full_rebuild": full_rebuild,
        "full_rebuild_reason": full_reason,
        "full_rebuild_active_only": bool(cfg.full_rebuild_active_only),
        "last_full_rebuild_at": next_last_full,
        "full_rebuild_interval_days": cfg.full_rebuild_days,
        "changed_wallets": len(changed_addrs),
        "changed_by_collection": len(changed_by_collection),
        "changed_by_activity": len(changed_by_activity),
        "activity_probe_failed": activity_probe_failed,
        "metric_failed_initial": metric_failed_initial,
        "metric_failed_resolved": metric_failed_resolved,
        "metric_failed_unresolved": metric_failed_unresolved,
        "checkpoint_count": len(checkpoint_dump),
        "refreshed_wallets": refresh_cnt,
        "removed_wallets": removed_wallets,
        "duration_sec": (finished_at - started_at).total_seconds(),
        "sbt_source": cfg.sbt_source,
        "sbt_scan_mode": str(sbt_scan_stats.get("scan_mode") or ""),
        "sbt_scan_api_calls": int(_to_decimal(sbt_scan_stats.get("api_calls")) or 0),
        "sbt_scan_rows_scanned": int(_to_decimal(sbt_scan_stats.get("rows_scanned")) or 0),
        "sbt_scan_reset": bool(sbt_scan_stats.get("reset_applied")),
        "sbt_refresh_wallets": int(_to_decimal(sbt_scan_stats.get("refresh_wallets")) or 0),
        "sbt_fallback_wallets": int(_to_decimal(sbt_scan_stats.get("fallback_wallets")) or 0),
        "sbt_failed_wallets": int(_to_decimal(sbt_scan_stats.get("failed_wallets")) or 0),
        "monthly_pack_source": cfg.monthly_pack_source,
        "monthly_pack_scan_mode": str((monthly_pack_scan_stats or {}).get("scan_mode") or ""),
        "monthly_pack_scan_api_calls": int(_to_decimal((monthly_pack_scan_stats or {}).get("api_calls")) or 0),
        "monthly_pack_scan_rows_scanned": int(_to_decimal((monthly_pack_scan_stats or {}).get("rows_scanned")) or 0),
        "monthly_pack_scan_reset": bool((monthly_pack_scan_stats or {}).get("reset_applied")),
        "pack_rank_state_path": str(cfg.pack_rank_state_path),
        "pack_rank_latest_path": str(cfg.pack_rank_latest_path),
        "monthly_pack_scan_state_path": str(cfg.monthly_pack_scan_state_path),
        "sbt_state_path": str(cfg.sbt_state_path),
    }
    _atomic_write_json(cfg.state_path, state_payload)

    return {
        "started_at": started_at,
        "finished_at": finished_at,
        "metrics_source": cfg.metrics_source,
        "wallet_count": len(canonical_records),
        "source_wallet_count": int(_to_decimal(migration_stats.get("source_wallet_count")) or len(records)),
        "canonical_wallet_count": int(
            _to_decimal(migration_stats.get("canonical_wallet_count")) or len(canonical_records)
        ),
        "wallet_migration_pairs": int(_to_decimal(migration_stats.get("wallet_migration_pairs")) or 0),
        "migrated_source_wallets": int(_to_decimal(migration_stats.get("migrated_source_wallets")) or 0),
        "canonical_wallets_from_multiple_sources": int(
            _to_decimal(migration_stats.get("canonical_wallets_from_multiple_sources")) or 0
        ),
        "collectible_pages": collectible_pages,
        "collection_snapshot_fallback": bool(collection_fallback_reason),
        "collection_snapshot_fallback_reason": collection_fallback_reason,
        "changed_wallets": len(changed_addrs),
        "changed_by_collection": len(changed_by_collection),
        "changed_by_activity": len(changed_by_activity),
        "activity_probe_failed": activity_probe_failed,
        "metric_failed_initial": metric_failed_initial,
        "metric_failed_resolved": metric_failed_resolved,
        "metric_failed_unresolved": metric_failed_unresolved,
        "checkpoint_count": len(checkpoint_dump),
        "refreshed_wallets": refresh_cnt,
        "removed_wallets": removed_wallets,
        "full_rebuild": full_rebuild,
        "full_rebuild_reason": full_reason,
        "full_rebuild_active_only": bool(cfg.full_rebuild_active_only),
        "duration_sec": (finished_at - started_at).total_seconds(),
        "push_required": (len(changed_addrs) > 0 or removed_wallets > 0 or full_rebuild),
        "sbt_source": cfg.sbt_source,
        "sbt_scan_mode": str(sbt_scan_stats.get("scan_mode") or ""),
        "sbt_scan_api_calls": int(_to_decimal(sbt_scan_stats.get("api_calls")) or 0),
        "sbt_scan_rows_scanned": int(_to_decimal(sbt_scan_stats.get("rows_scanned")) or 0),
        "sbt_scan_reset": bool(sbt_scan_stats.get("reset_applied")),
        "sbt_refresh_wallets": int(_to_decimal(sbt_scan_stats.get("refresh_wallets")) or 0),
        "sbt_fallback_wallets": int(_to_decimal(sbt_scan_stats.get("fallback_wallets")) or 0),
        "sbt_failed_wallets": int(_to_decimal(sbt_scan_stats.get("failed_wallets")) or 0),
        "monthly_pack_source": cfg.monthly_pack_source,
        "monthly_pack_scan_mode": str((monthly_pack_scan_stats or {}).get("scan_mode") or ""),
        "monthly_pack_scan_api_calls": int(_to_decimal((monthly_pack_scan_stats or {}).get("api_calls")) or 0),
        "monthly_pack_scan_rows_scanned": int(_to_decimal((monthly_pack_scan_stats or {}).get("rows_scanned")) or 0),
        "monthly_pack_scan_reset": bool((monthly_pack_scan_stats or {}).get("reset_applied")),
        "wallet_username_map_path": username_map_path_text,
        "wallet_username_map_wallets": len(wallet_username_map),
        "wallet_username_map_applied_source_wallets": username_map_applied,
        "wallet_username_map_applied_canonical_wallets": canonical_username_map_applied,
    }


def main() -> int:
    args = parse_args()
    cfg = load_config(args)
    validate_config(cfg)

    cfg.data_dir.mkdir(parents=True, exist_ok=True)

    bootstrapped = False
    should_bootstrap_from_git = cfg.bootstrap_from_git and cfg.backup_git_enabled and not args.push_only
    if should_bootstrap_from_git:
        try:
            bootstrapped = bootstrap_from_git(cfg)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] bootstrap_from_git failed: {e}")

    if args.bootstrap_only:
        msg = (
            f"trigger={cfg.trigger} bootstrap_only=1 bootstrapped={bootstrapped} "
            f"latest={cfg.latest_path}"
        )
        print(f"[OK] {msg}")
        write_status(
            cfg,
            success=True,
            message=msg,
            extra={
                "bootstrap_only": True,
                "bootstrapped": bootstrapped,
                "latest_path": str(cfg.latest_path),
            },
        )
        send_webhook(cfg, msg, success=True)
        return 0

    if args.push_only:
        commit_hash = "git-disabled"
        backup_status = "not_attempted"
        if cfg.backup_git_enabled:
            now_dt = datetime.now(tz=cfg.tzinfo)
            if args.market_only:
                commit_message = (
                    f"market push-only {now_dt.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"trigger={cfg.trigger}"
                )
                commit_hash = git_push_market_cache(cfg, commit_message=commit_message)
            else:
                commit_message = (
                    f"ranking push-only {now_dt.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"trigger={cfg.trigger}"
                )
                commit_hash = git_push_rankings(cfg, now_dt=now_dt, commit_message=commit_message)
            backup_status = "pushed" if commit_hash not in ("no-change", "git-disabled", "") else "no-change"
        latest_field = (
            f"market_index={cfg.market_cache_dir / 'market_index.json'}"
            if args.market_only
            else f"latest={cfg.latest_path}"
        )
        msg = (
            f"trigger={cfg.trigger} push_only=1 market_only={1 if args.market_only else 0} "
            f"backup_status={backup_status} commit={commit_hash} "
            f"{latest_field}"
        )
        print(f"[OK] {msg}")
        status_writer = write_market_status if args.market_only else write_status
        status_writer(
            cfg,
            success=True,
            message=msg,
            extra={
                "push_only": True,
                "market_only": bool(args.market_only),
                "backup_status": backup_status,
                "commit": commit_hash,
                "latest_path": str(cfg.latest_path),
                "market_index_path": str(cfg.market_cache_dir / "market_index.json"),
            },
        )
        send_webhook(cfg, msg, success=True)
        return 0

    result = run_sync(cfg)
    now_dt = result["finished_at"]
    commit_hash = "git-disabled"
    backup_status = "not_attempted"
    if cfg.backup_git_enabled:
        if result["push_required"]:
            commit_message = (
                f"ranking sync {now_dt.strftime('%Y-%m-%d %H:%M:%S')} "
                f"trigger={cfg.trigger} changed={result['changed_wallets']} full={1 if result['full_rebuild'] else 0}"
            )
            commit_hash = git_push_rankings(cfg, now_dt=now_dt, commit_message=commit_message)
            backup_status = "pushed"
        else:
            backup_status = "no-change"

    msg = (
        f"trigger={cfg.trigger} metrics_source={result['metrics_source']} "
        f"wallets={result['wallet_count']} collectible_pages={result['collectible_pages']} "
        f"collection_fallback={1 if result['collection_snapshot_fallback'] else 0} "
        f"changed_wallets={result['changed_wallets']} changed_by_activity={result['changed_by_activity']} "
        f"refreshed_wallets={result['refreshed_wallets']} "
        f"removed_wallets={result['removed_wallets']} full_rebuild={result['full_rebuild']} "
        f"checkpoints={result['checkpoint_count']} probe_failed={result['activity_probe_failed']} "
        f"metric_fail={result['metric_failed_initial']} metric_resolved={result['metric_failed_resolved']} "
        f"metric_unresolved={result['metric_failed_unresolved']} "
        f"sbt_source={result['sbt_source']} "
        f"sbt_scan_mode={result['sbt_scan_mode'] or '-'} "
        f"sbt_scan_calls={result['sbt_scan_api_calls']} "
        f"sbt_scan_rows={result['sbt_scan_rows_scanned']} "
        f"sbt_scan_reset={1 if result['sbt_scan_reset'] else 0} "
        f"sbt_fallback={result['sbt_fallback_wallets']} "
        f"pack_source={result['monthly_pack_source']} "
        f"pack_scan_mode={result['monthly_pack_scan_mode'] or '-'} "
        f"pack_scan_calls={result['monthly_pack_scan_api_calls']} "
        f"pack_scan_rows={result['monthly_pack_scan_rows_scanned']} "
        f"duration_sec={result['duration_sec']:.2f} backup_status={backup_status} commit={commit_hash}"
    )
    print(f"[OK] {msg}")
    write_status(
        cfg,
        success=True,
        message=msg,
        extra={
            "metrics_source": result["metrics_source"],
            "wallet_count": result["wallet_count"],
            "collectible_pages": result["collectible_pages"],
            "collection_snapshot_fallback": result["collection_snapshot_fallback"],
            "collection_snapshot_fallback_reason": result["collection_snapshot_fallback_reason"],
            "changed_wallets": result["changed_wallets"],
            "changed_by_collection": result["changed_by_collection"],
            "changed_by_activity": result["changed_by_activity"],
            "activity_probe_failed": result["activity_probe_failed"],
            "metric_failed_initial": result["metric_failed_initial"],
            "metric_failed_resolved": result["metric_failed_resolved"],
            "metric_failed_unresolved": result["metric_failed_unresolved"],
            "checkpoint_count": result["checkpoint_count"],
            "refreshed_wallets": result["refreshed_wallets"],
            "removed_wallets": result["removed_wallets"],
            "full_rebuild": result["full_rebuild"],
            "full_rebuild_reason": result["full_rebuild_reason"],
            "full_rebuild_active_only": result["full_rebuild_active_only"],
            "duration_sec": result["duration_sec"],
            "sbt_source": result["sbt_source"],
            "sbt_scan_mode": result["sbt_scan_mode"],
            "sbt_scan_api_calls": result["sbt_scan_api_calls"],
            "sbt_scan_rows_scanned": result["sbt_scan_rows_scanned"],
            "sbt_scan_reset": result["sbt_scan_reset"],
            "sbt_refresh_wallets": result["sbt_refresh_wallets"],
            "sbt_fallback_wallets": result["sbt_fallback_wallets"],
            "sbt_failed_wallets": result["sbt_failed_wallets"],
            "monthly_pack_source": result["monthly_pack_source"],
            "monthly_pack_scan_mode": result["monthly_pack_scan_mode"],
            "monthly_pack_scan_api_calls": result["monthly_pack_scan_api_calls"],
            "monthly_pack_scan_rows_scanned": result["monthly_pack_scan_rows_scanned"],
            "monthly_pack_scan_reset": result["monthly_pack_scan_reset"],
            "wallet_username_map_path": result["wallet_username_map_path"],
            "wallet_username_map_wallets": result["wallet_username_map_wallets"],
            "wallet_username_map_applied_source_wallets": result["wallet_username_map_applied_source_wallets"],
            "wallet_username_map_applied_canonical_wallets": result["wallet_username_map_applied_canonical_wallets"],
            "backup_status": backup_status,
            "commit": commit_hash,
            "latest_path": str(cfg.latest_path),
            "history_path": str(cfg.history_path(now_dt)),
            "checkpoint_path": str(cfg.checkpoint_path),
            "pack_rank_state_path": str(cfg.pack_rank_state_path),
            "pack_rank_latest_path": str(cfg.pack_rank_latest_path),
            "monthly_pack_scan_state_path": str(cfg.monthly_pack_scan_state_path),
            "sbt_state_path": str(cfg.sbt_state_path),
        },
    )
    send_webhook(cfg, msg, success=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001
        cfg = None
        args = None
        try:
            args = parse_args()
            cfg = load_config(args)
        except Exception:
            cfg = None
        err = f"{type(e).__name__}: {e}"
        print(f"[ERROR] {err}")
        if cfg is not None:
            try:
                if args is not None and bool(getattr(args, "push_only", False)) and bool(getattr(args, "market_only", False)):
                    write_market_status(cfg, success=False, message=err, extra=None)
                else:
                    write_status(cfg, success=False, message=err, extra=None)
            except Exception:
                pass
            send_webhook(cfg, err, success=False)
        raise
