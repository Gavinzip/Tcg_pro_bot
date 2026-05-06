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


def _fetch_tokentx_page(
    cfg: OnchainConfig,
    wallet: str,
    *,
    page: int,
    sort: str,
    startblock: int | None = None,
    endblock: int | None = None,
) -> list[dict[str, Any]]:
    page_limit = int(max(1, min(1000, cfg.page_size)))
    params = {
        "chainid": int(cfg.chain_id),
        "module": "account",
        "action": "tokentx",
        "address": str(wallet or "").strip().lower(),
        "contractaddress": cfg.usdt_contract,
        "page": int(page),
        "offset": page_limit,
        "sort": sort,
        "apikey": cfg.api_key,
    }
    if startblock is not None and int(startblock) > 0:
        params["startblock"] = int(startblock)
    if endblock is not None and int(endblock) > 0:
        params["endblock"] = int(endblock)

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
            raise RuntimeError(f"bscscan tokentx error: message={message} result={result}")
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < max(1, cfg.retries):
                time.sleep(max(0.2, cfg.backoff_sec) * (2 ** (attempt - 1)))
                continue
            break
    raise RuntimeError(f"bscscan tokentx request failed: {last_err}")


def _fetch_token1155tx_page(
    cfg: OnchainConfig,
    wallet: str,
    contract: str,
    *,
    page: int,
    sort: str,
) -> list[dict[str, Any]]:
    page_limit = int(max(1, min(1000, cfg.page_size)))
    params = {
        "chainid": int(cfg.chain_id),
        "module": "account",
        "action": "token1155tx",
        "address": str(wallet or "").strip().lower(),
        "contractaddress": str(contract or "").strip().lower(),
        "page": int(page),
        "offset": page_limit,
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
                raise RuntimeError("bscscan token1155tx invalid response")

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
            raise RuntimeError(f"bscscan token1155tx error: {message or result}")
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < max(1, cfg.retries):
                time.sleep(max(0.2, cfg.backoff_sec) * (2 ** (attempt - 1)))
                continue
            break
    raise RuntimeError(f"bscscan token1155tx request failed: {last_err}")


def fetch_latest_usdt_tx_hash(cfg: OnchainConfig, wallet: str) -> str:
    rows = _fetch_tokentx_page(cfg, wallet, page=1, sort="desc")
    if not rows:
        return ""
    return str(rows[0].get("hash") or "").strip().lower()


def fetch_all_usdt_transfers(cfg: OnchainConfig, wallet: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    limit = int(max(1, min(1000, cfg.page_size)))
    while True:
        rows = _fetch_tokentx_page(cfg, wallet, page=page, sort="asc")
        if not rows:
            break
        out.extend(rows)
        if len(rows) < limit:
            break
        page += 1
    return out


def fetch_all_erc1155_transfers(cfg: OnchainConfig, wallet: str, contract: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    limit = int(max(1, min(1000, cfg.page_size)))
    while True:
        rows = _fetch_token1155tx_page(cfg, wallet, contract, page=page, sort="asc")
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


def _row_timestamp(row: dict[str, Any]) -> int:
    raw = row.get("timeStamp")
    if raw is None:
        raw = row.get("timestamp")
    ts = int(_to_decimal(raw) or 0)
    return ts if ts > 0 else 0


def _row_block_number(row: dict[str, Any]) -> int:
    raw = row.get("blockNumber")
    n = int(_to_decimal(raw) or 0)
    return n if n > 0 else 0


def _row_tx_index(row: dict[str, Any]) -> int:
    raw = row.get("transactionIndex")
    n = int(_to_decimal(raw) or 0)
    return n if n >= 0 else 0


def _row_hash(row: dict[str, Any]) -> str:
    return str(row.get("hash") or "").strip().lower()


def _fetch_block_number_by_timestamp(cfg: OnchainConfig, ts: int) -> int:
    params = {
        "chainid": int(cfg.chain_id),
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": int(max(0, ts)),
        "closest": "before",
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
                raise RuntimeError("bscscan getblocknobytime invalid response")
            result = data.get("result")
            if isinstance(result, str) and result.isdigit():
                return int(result)
            if isinstance(result, (int, float)):
                return int(result)
            message = str(data.get("message") or "").strip()
            if str(result).lower().find("no transactions found") >= 0:
                return 0
            raise RuntimeError(f"bscscan getblocknobytime error: {message or result}")
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < max(1, cfg.retries):
                time.sleep(max(0.2, cfg.backoff_sec) * (2 ** (attempt - 1)))
                continue
            break
    raise RuntimeError(f"bscscan getblocknobytime request failed: {last_err}")


def scan_pack_open_counts_incremental(
    cfg: OnchainConfig,
    *,
    pack_contracts: tuple[str, ...],
    window_start_ts: int,
    window_end_ts: int,
    prev_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    contract_list: list[str] = []
    seen_contracts: set[str] = set()
    for c in list(pack_contracts or ()):
        contract = str(c or "").strip().lower()
        if not contract.startswith("0x") or len(contract) != 42:
            continue
        if contract in seen_contracts:
            continue
        seen_contracts.add(contract)
        contract_list.append(contract)

    start_ts = int(max(0, window_start_ts or 0))
    end_ts = int(max(start_ts, window_end_ts or 0))
    prev = prev_state if isinstance(prev_state, dict) else {}
    prev_window_start = int(_to_decimal(prev.get("window_start_ts")) or 0)
    reset_applied = prev_window_start != start_ts

    wallet_counts: dict[str, int] = {}
    if not reset_applied:
        prev_counts = prev.get("wallet_counts")
        if isinstance(prev_counts, dict):
            for addr, raw in prev_counts.items():
                key = str(addr or "").strip().lower()
                if not key.startswith("0x") or len(key) != 42:
                    continue
                wallet_counts[key] = max(0, int(_to_decimal(raw)))

    checkpoints: dict[str, dict[str, Any]] = {}
    if not reset_applied:
        prev_checkpoints = prev.get("checkpoints")
        if isinstance(prev_checkpoints, dict):
            for contract, row in prev_checkpoints.items():
                c = str(contract or "").strip().lower()
                if not c.startswith("0x") or len(c) != 42:
                    continue
                if not isinstance(row, dict):
                    continue
                checkpoints[c] = {
                    "last_block": int(_to_decimal(row.get("last_block")) or 0),
                    "last_tx_index": int(_to_decimal(row.get("last_tx_index")) or 0),
                    "last_hash": str(row.get("last_hash") or "").strip().lower(),
                    "last_ts": int(_to_decimal(row.get("last_ts")) or 0),
                }

    api_calls = 0
    rows_scanned = 0
    window_start_block = 0
    if reset_applied and start_ts > 0:
        try:
            window_start_block = _fetch_block_number_by_timestamp(cfg, start_ts)
            api_calls += 1
        except Exception:
            # Fallback to full contract scan when block-by-time endpoint is unavailable.
            window_start_block = 0

    limit = int(max(1, min(1000, cfg.page_size)))
    max_pages_per_window = max(1, 10000 // limit)
    contract_stats: dict[str, dict[str, Any]] = {}
    for contract in contract_list:
        cp = checkpoints.get(contract) or {}
        cp_key = (
            int(_to_decimal(cp.get("last_block")) or 0),
            int(_to_decimal(cp.get("last_tx_index")) or 0),
            str(cp.get("last_hash") or "").strip().lower(),
        )
        has_cp = cp_key[0] > 0 or bool(cp_key[2])
        max_key = cp_key
        max_ts = int(_to_decimal(cp.get("last_ts")) or 0)
        start_block = cp_key[0] if (has_cp and not reset_applied) else window_start_block

        page = 1
        calls_for_contract = 0
        stop_on_window_end = False
        while True:
            if page > max_pages_per_window:
                # Etherscan V2 enforces `page * offset <= 10000`.
                # Continue scan from next block after the largest block already processed.
                next_start_block = int(max(max_key[0], start_block) + 1)
                if next_start_block <= int(start_block):
                    break
                start_block = next_start_block
                page = 1
                continue
            rows = _fetch_tokentx_page(
                cfg,
                contract,
                page=page,
                sort="asc",
                startblock=start_block if start_block > 0 else None,
            )
            api_calls += 1
            calls_for_contract += 1
            if not rows:
                break

            for row in rows:
                rows_scanned += 1
                block = _row_block_number(row)
                tx_index = _row_tx_index(row)
                tx_hash = _row_hash(row)
                if block <= 0 or not tx_hash:
                    continue
                row_key = (block, tx_index, tx_hash)
                if has_cp and row_key <= cp_key:
                    continue

                ts = _row_timestamp(row)
                if ts >= end_ts:
                    stop_on_window_end = True
                    break

                if row_key > max_key:
                    max_key = row_key
                    if ts > 0:
                        max_ts = ts

                if ts < start_ts:
                    continue

                amount = _usdt_amount_from_raw(row.get("value"), row.get("tokenDecimal"))
                if amount <= 0:
                    continue
                to_addr = str(row.get("to") or "").strip().lower()
                from_addr = str(row.get("from") or "").strip().lower()
                if to_addr != contract:
                    continue
                if not from_addr.startswith("0x") or len(from_addr) != 42:
                    continue
                if from_addr == contract:
                    continue
                wallet_counts[from_addr] = wallet_counts.get(from_addr, 0) + 1

            if stop_on_window_end:
                break
            if len(rows) < limit:
                break
            page += 1

        checkpoints[contract] = {
            "last_block": int(max_key[0]),
            "last_tx_index": int(max_key[1]),
            "last_hash": str(max_key[2] or ""),
            "last_ts": int(max_ts or 0),
        }
        contract_stats[contract] = {
            "api_calls": calls_for_contract,
            "start_block": int(start_block or 0),
            "last_block": int(max_key[0]),
            "last_ts": int(max_ts or 0),
            "stop_on_window_end": bool(stop_on_window_end),
        }

    state = {
        "version": 1,
        "window_start_ts": int(start_ts),
        "window_end_ts": int(end_ts),
        "updated_at_ts": int(time.time()),
        "contracts": contract_list,
        "wallet_counts": wallet_counts,
        "checkpoints": checkpoints,
    }
    return {
        "state": state,
        "wallet_counts": wallet_counts,
        "stats": {
            "scan_mode": "contract_center_incremental",
            "api_calls": int(api_calls),
            "rows_scanned": int(rows_scanned),
            "contracts": contract_stats,
            "reset_applied": bool(reset_applied),
            "window_start_ts": int(start_ts),
            "window_end_ts": int(end_ts),
        },
    }


def analyze_sbt_wallet(cfg: OnchainConfig, wallet: str, sbt_contract: str) -> dict[str, int]:
    wallet_norm = str(wallet or "").strip().lower()
    contract_norm = str(sbt_contract or "").strip().lower()
    if not wallet_norm:
        return {}
    if not contract_norm.startswith("0x") or len(contract_norm) != 42:
        return {}

    transfers = fetch_all_erc1155_transfers(cfg, wallet_norm, contract_norm)
    balances: dict[str, int] = {}
    for row in transfers:
        token_id = str(row.get("tokenID") or row.get("tokenId") or "").strip()
        if not token_id:
            continue
        amount = int(_to_decimal(row.get("tokenValue")))
        if amount <= 0:
            continue

        from_addr = str(row.get("from") or "").strip().lower()
        to_addr = str(row.get("to") or "").strip().lower()
        if from_addr == wallet_norm:
            balances[token_id] = balances.get(token_id, 0) - amount
        if to_addr == wallet_norm:
            balances[token_id] = balances.get(token_id, 0) + amount

    return {token_id: amount for token_id, amount in balances.items() if int(amount) > 0}


def analyze_wallet(
    cfg: OnchainConfig,
    wallet: str,
) -> dict[str, Decimal]:
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
