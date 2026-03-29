#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import time
from typing import Any

import requests


@dataclass(frozen=True)
class OnchainConfig:
    api_url: str
    chain_id: int
    api_key: str
    usdt_contract: str
    pack_contracts: tuple[str, ...]
    marketplace_contract: str
    page_size: int
    retries: int
    backoff_sec: float


def _to_decimal(v: Any) -> Decimal:
    if v is None:
        return Decimal("0")
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    t = str(v).strip()
    if not t:
        return Decimal("0")
    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _usdt_amount_from_raw(value_raw: Any, decimals_raw: Any) -> Decimal:
    value = _to_decimal(value_raw)
    decimals = int(_to_decimal(decimals_raw) or 18)
    if value <= 0:
        return Decimal("0")
    if decimals <= 0:
        return value
    return value / (Decimal(10) ** decimals)


def _fetch_tokentx_page(cfg: OnchainConfig, wallet: str, *, page: int, sort: str) -> list[dict[str, Any]]:
    params = {
        "chainid": int(cfg.chain_id),
        "module": "account",
        "action": "tokentx",
        "address": str(wallet or "").strip().lower(),
        "contractaddress": cfg.usdt_contract,
        "page": int(page),
        "offset": int(max(1, min(10000, cfg.page_size))),
        "sort": sort,
        "apikey": cfg.api_key,
    }

    last_err: Exception | None = None
    for attempt in range(1, max(1, cfg.retries) + 1):
        try:
            resp = requests.get(cfg.api_url, params=params, timeout=30)
            status_code = int(resp.status_code or 0)
            if status_code >= 500 or status_code == 429:
                raise requests.HTTPError(f"HTTP {status_code}", response=resp)
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError("bscscan tokentx invalid response")

            message = str(data.get("message") or "").strip()
            result = data.get("result")

            if message == "No transactions found":
                return []
            if isinstance(result, list):
                return [x for x in result if isinstance(x, dict)]
            if isinstance(result, str):
                lowered = result.lower()
                if "max rate limit" in lowered or "query timeout" in lowered:
                    raise RuntimeError(result)
                if "no transactions found" in lowered:
                    return []
            raise RuntimeError(f"bscscan tokentx error: {message or result}")
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < max(1, cfg.retries):
                time.sleep(max(0.2, cfg.backoff_sec) * (2 ** (attempt - 1)))
                continue
            break
    raise RuntimeError(f"bscscan tokentx request failed: {last_err}")


def fetch_latest_usdt_tx_hash(cfg: OnchainConfig, wallet: str) -> str:
    rows = _fetch_tokentx_page(cfg, wallet, page=1, sort="desc")
    if not rows:
        return ""
    return str(rows[0].get("hash") or "").strip().lower()


def fetch_all_usdt_transfers(cfg: OnchainConfig, wallet: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    limit = int(max(1, min(10000, cfg.page_size)))
    while True:
        rows = _fetch_tokentx_page(cfg, wallet, page=page, sort="asc")
        if not rows:
            break
        out.extend(rows)
        if len(rows) < limit:
            break
        page += 1
    return out


def _classify_transfer(row: dict[str, Any], wallet: str, cfg: OnchainConfig) -> str:
    frm = str(row.get("from") or "").strip().lower()
    to = str(row.get("to") or "").strip().lower()
    if frm == wallet and to in cfg.pack_contracts:
        return "open_pack"
    if frm in cfg.pack_contracts and to == wallet:
        return "buyback"
    if frm == wallet and to == cfg.marketplace_contract:
        return "mp_buy"
    if frm == cfg.marketplace_contract and to == wallet:
        return "mp_sell"
    return "other"


def analyze_wallet(cfg: OnchainConfig, wallet: str) -> dict[str, Decimal]:
    wallet_norm = str(wallet or "").strip().lower()
    if not wallet_norm:
        return {
            "pack_spent_usdt": Decimal("0"),
            "trade_volume_usdt": Decimal("0"),
            "trade_spent_usdt": Decimal("0"),
            "trade_earned_usdt": Decimal("0"),
            "buyback_earned_usdt": Decimal("0"),
            "total_spent_usdt": Decimal("0"),
            "total_earned_usdt": Decimal("0"),
            "cash_net_usdt": Decimal("0"),
        }

    transfers = fetch_all_usdt_transfers(cfg, wallet_norm)
    pack_spent = Decimal("0")
    buyback_earned = Decimal("0")
    market_buy_spent = Decimal("0")
    market_sell_earned = Decimal("0")
    open_pack_tx_count = 0
    buyback_tx_count = 0
    trade_buy_tx_count = 0
    trade_sell_tx_count = 0

    for row in transfers:
        cls = _classify_transfer(row, wallet_norm, cfg)
        amount = _usdt_amount_from_raw(row.get("value"), row.get("tokenDecimal"))
        if amount <= 0:
            continue
        if cls == "open_pack":
            pack_spent += amount
            open_pack_tx_count += 1
        elif cls == "buyback":
            buyback_earned += amount
            buyback_tx_count += 1
        elif cls == "mp_buy":
            market_buy_spent += amount
            trade_buy_tx_count += 1
        elif cls == "mp_sell":
            market_sell_earned += amount
            trade_sell_tx_count += 1

    total_spent = pack_spent + market_buy_spent
    total_earned = buyback_earned + market_sell_earned
    cash_net = total_earned - total_spent
    trade_volume = market_buy_spent + market_sell_earned

    return {
        "pack_spent_usdt": pack_spent,
        "trade_volume_usdt": trade_volume,
        "trade_spent_usdt": market_buy_spent,
        "trade_earned_usdt": market_sell_earned,
        "buyback_earned_usdt": buyback_earned,
        "total_spent_usdt": total_spent,
        "total_earned_usdt": total_earned,
        "cash_net_usdt": cash_net,
        "open_pack_tx_count": Decimal(open_pack_tx_count),
        "buyback_tx_count": Decimal(buyback_tx_count),
        "trade_buy_tx_count": Decimal(trade_buy_tx_count),
        "trade_sell_tx_count": Decimal(trade_sell_tx_count),
    }
