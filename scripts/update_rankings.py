#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

RENAISS_COLLECTIBLE_LIST_URL = "https://www.renaiss.xyz/api/trpc/collectible.list"
RENAISS_ACTIVITY_LIST_URL = "https://www.renaiss.xyz/api/trpc/activity.getSubgraphUserActivities"
RENAISS_SBT_BADGES_URL = "https://www.renaiss.xyz/api/trpc/sbt.getUserBadges"

WEI_DECIMAL = Decimal("1000000000000000000")
SELL_GROSS_DIVISOR = Decimal(os.getenv("PROFILE_MARKET_SELL_GROSS_DIVISOR", "1.02"))
API_MAX_RETRIES = max(1, int(os.getenv("PROFILE_API_MAX_RETRIES", "4")))
API_RETRY_BACKOFF_SEC = max(0.2, float(os.getenv("PROFILE_API_RETRY_BACKOFF_SEC", "0.8")))
HTTP_POOL_MAXSIZE = max(8, int(os.getenv("PROFILE_HTTP_POOL_MAXSIZE", "48")))

_HTTP_SESSION_LOCAL = threading.local()


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
                return []
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


@dataclass
class WalletRecord:
    address: str
    username: str | None
    holdings_value_usdt: Decimal
    collectible_count: int
    trade_volume_usdt: Decimal = Decimal("0")
    trade_spent_usdt: Decimal = Decimal("0")
    trade_earned_usdt: Decimal = Decimal("0")
    sbt_owned_total: int = 0
    sbt_owned_badge_count: int = 0
    volume_rank: int | None = None
    holdings_rank: int | None = None
    sbt_rank: int | None = None


def fetch_all_wallets(max_wallets: int | None = None) -> tuple[dict[str, WalletRecord], int]:
    limit = 100
    offset = 0
    total_pages = 0
    wallets: dict[str, WalletRecord] = {}

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

        collection = result.get("collection") or []
        if not isinstance(collection, list):
            collection = []

        for item in collection:
            if not isinstance(item, dict):
                continue
            address = str(item.get("ownerAddress") or "").strip().lower()
            if not address:
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

        if max_wallets is not None and len(wallets) >= max_wallets:
            break

        pagination = result.get("pagination") or {}
        has_more = bool(pagination.get("hasMore"))
        if not has_more:
            break

        next_limit = int(pagination.get("limit") or limit)
        next_offset = int(pagination.get("offset") or offset) + next_limit
        if next_offset <= offset:
            break
        offset = next_offset

    if max_wallets is not None and len(wallets) > max_wallets:
        ordered_addrs = sorted(wallets.keys())[:max_wallets]
        wallets = {addr: wallets[addr] for addr in ordered_addrs}

    return wallets, total_pages


def compute_trade_volume_for_wallet(address: str, page_limit: int, max_pages: int) -> tuple[Decimal, Decimal, Decimal]:
    cursor: str | None = None
    seen_cursors: set[str] = set()
    spent = Decimal("0")
    earned = Decimal("0")

    for _ in range(max_pages):
        page = _trpc_user_activities(address, cursor=cursor, limit=page_limit)
        rows = page.get("activities") or []
        if not isinstance(rows, list):
            rows = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            row_type = str(row.get("__typename") or "").strip()
            if row_type not in ("BuyActivity", "SellActivity"):
                continue

            amount = _wei_to_usdt(row.get("amount"))
            if amount <= 0:
                continue

            bidder = str(row.get("bidder") or "").strip().lower()
            asker = str(row.get("asker") or "").strip().lower()

            if bidder == address:
                spent += amount
            if asker == address:
                divisor = SELL_GROSS_DIVISOR if SELL_GROSS_DIVISOR > 0 else Decimal("1")
                earned += amount / divisor

        next_cursor = page.get("nextCursor")
        if not next_cursor:
            break
        next_cursor = str(next_cursor)
        if next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    return spent + earned, spent, earned


def compute_sbt_for_wallet(username: str | None) -> tuple[int, int]:
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


def assign_rank(records: list[WalletRecord], value_getter, attr_name: str) -> None:
    sorted_records = sorted(records, key=lambda x: (value_getter(x), x.address), reverse=True)
    prev_val = None
    current_rank = 0
    for idx, rec in enumerate(sorted_records, start=1):
        val = value_getter(rec)
        if prev_val is None or val != prev_val:
            current_rank = idx
            prev_val = val
        setattr(rec, attr_name, current_rank)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp_path, path)


def build_payload(records: list[WalletRecord], started_at: datetime, finished_at: datetime, collectible_pages: int) -> dict[str, Any]:
    assign_rank(records, lambda r: r.trade_volume_usdt, "volume_rank")
    assign_rank(records, lambda r: r.holdings_value_usdt, "holdings_rank")
    assign_rank(records, lambda r: r.sbt_owned_total, "sbt_rank")

    by_volume = sorted(records, key=lambda x: (x.trade_volume_usdt, x.address), reverse=True)
    by_holdings = sorted(records, key=lambda x: (x.holdings_value_usdt, x.address), reverse=True)
    by_sbt = sorted(records, key=lambda x: (x.sbt_owned_total, x.address), reverse=True)

    def to_wallet_dict(rec: WalletRecord) -> dict[str, Any]:
        return {
            "address": rec.address,
            "username": rec.username,
            "collectible_count": rec.collectible_count,
            "trade_volume_usdt": _decimal_to_str(rec.trade_volume_usdt),
            "trade_spent_usdt": _decimal_to_str(rec.trade_spent_usdt),
            "trade_earned_usdt": _decimal_to_str(rec.trade_earned_usdt),
            "holdings_value_usdt": _decimal_to_str(rec.holdings_value_usdt),
            "sbt_owned_total": rec.sbt_owned_total,
            "sbt_owned_badge_count": rec.sbt_owned_badge_count,
            "volume_rank": rec.volume_rank,
            "holdings_rank": rec.holdings_rank,
            "sbt_rank": rec.sbt_rank,
        }

    return {
        "meta": {
            "timezone": "Asia/Taipei",
            "started_at": started_at.isoformat(),
            "updated_at": finished_at.isoformat(),
            "collectible_pages": collectible_pages,
            "wallet_count": len(records),
            "version": 1,
        },
        "top": {
            "volume": [to_wallet_dict(x) for x in by_volume[:100]],
            "holdings": [to_wallet_dict(x) for x in by_holdings[:100]],
            "sbt": [to_wallet_dict(x) for x in by_sbt[:100]],
        },
        "wallets": [to_wallet_dict(x) for x in sorted(records, key=lambda x: x.address)],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update Renaiss rankings and persist to /data")
    parser.add_argument("--data-dir", default=os.getenv("RANKING_DATA_DIR", "/data/rankings"))
    parser.add_argument("--workers", type=int, default=max(2, int(os.getenv("RANKING_WORKERS", "8"))))
    parser.add_argument("--activity-page-limit", type=int, default=50)
    parser.add_argument("--activity-max-pages", type=int, default=max(1, int(os.getenv("PROFILE_ACTIVITY_MAX_PAGES", "120"))))
    parser.add_argument("--max-wallets", type=int, default=None, help="Only for testing; limit how many wallets are processed")
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()

    tz = ZoneInfo("Asia/Taipei")
    started_at = datetime.now(tz=tz)

    wallets, collectible_pages = fetch_all_wallets(max_wallets=args.max_wallets)
    records = list(wallets.values())

    workers = max(1, min(args.workers, len(records))) if records else 1

    if records:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {
                pool.submit(compute_trade_volume_for_wallet, rec.address, args.activity_page_limit, args.activity_max_pages): rec
                for rec in records
            }
            for future in as_completed(future_map):
                rec = future_map[future]
                try:
                    volume, spent, earned = future.result()
                except Exception as e:  # noqa: BLE001
                    print(f"[WARN] volume failed for {rec.address}: {e}")
                    volume, spent, earned = Decimal("0"), Decimal("0"), Decimal("0")
                rec.trade_volume_usdt = volume
                rec.trade_spent_usdt = spent
                rec.trade_earned_usdt = earned

    if records:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {pool.submit(compute_sbt_for_wallet, rec.username): rec for rec in records}
            for future in as_completed(future_map):
                rec = future_map[future]
                try:
                    sbt_total, sbt_badge_count = future.result()
                except Exception as e:  # noqa: BLE001
                    print(f"[WARN] sbt failed for {rec.address} ({rec.username}): {e}")
                    sbt_total, sbt_badge_count = 0, 0
                rec.sbt_owned_total = sbt_total
                rec.sbt_owned_badge_count = sbt_badge_count

    finished_at = datetime.now(tz=tz)
    payload = build_payload(records, started_at, finished_at, collectible_pages)

    data_dir = Path(args.data_dir)
    latest_path = data_dir / "latest.json"
    day_key = finished_at.strftime("%Y-%m-%d")
    history_path = data_dir / "history" / f"{day_key}.json"

    atomic_write_json(latest_path, payload)
    atomic_write_json(history_path, payload)

    print(
        f"[OK] updated rankings: wallets={len(records)} latest={latest_path} history={history_path} "
        f"duration_sec={(finished_at - started_at).total_seconds():.2f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
