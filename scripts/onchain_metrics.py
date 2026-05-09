#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
import os
import time
from typing import Any

import requests


ONCHAIN_DELAYED_MINT_WINDOW_SEC = max(0, int(os.getenv("ONCHAIN_DELAYED_MINT_WINDOW_SEC", "3600")))


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
    withdraw_addresses: tuple[str, ...] = ()


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


def _fetch_nfttx_page(
    cfg: OnchainConfig,
    wallet: str,
    action: str,
    *,
    page: int,
    sort: str,
    startblock: int | None = None,
    endblock: int | None = None,
) -> list[dict[str, Any]]:
    action_norm = str(action or "").strip().lower()
    if action_norm not in ("tokennfttx", "token1155tx"):
        raise ValueError(f"unsupported nft action: {action}")

    page_limit = int(max(1, min(1000, cfg.page_size)))
    params = {
        "chainid": int(cfg.chain_id),
        "module": "account",
        "action": action_norm,
        "address": str(wallet or "").strip().lower(),
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
                raise RuntimeError(f"bscscan {action_norm} invalid response")

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
            raise RuntimeError(f"bscscan {action_norm} error: {message or result}")
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < max(1, cfg.retries):
                time.sleep(max(0.2, cfg.backoff_sec) * (2 ** (attempt - 1)))
                continue
            break
    raise RuntimeError(f"bscscan {action_norm} request failed: {last_err}")


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


def fetch_all_nft_transfers(cfg: OnchainConfig, wallet: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    limit = int(max(1, min(1000, cfg.page_size)))
    for action in ("tokennfttx", "token1155tx"):
        page = 1
        try:
            while True:
                rows = _fetch_nfttx_page(cfg, wallet, action, page=page, sort="asc")
                if not rows:
                    break
                out.extend(rows)
                if len(rows) < limit:
                    break
                page += 1
        except Exception:
            if action == "token1155tx":
                continue
            raise
    out.sort(key=lambda r: (_row_timestamp(r), _row_block_number(r), _row_tx_index(r), _row_hash(r)))
    return out


def _wallet_nft_tx_sets(nft_rows: list[dict[str, Any]], wallet: str) -> tuple[set[str], set[str]]:
    wallet_norm = str(wallet or "").strip().lower()
    nft_in_txs: set[str] = set()
    nft_out_txs: set[str] = set()
    for row in nft_rows:
        if not isinstance(row, dict):
            continue
        tx_hash = _row_hash(row)
        if not tx_hash:
            continue
        frm = str(row.get("from") or "").strip().lower()
        to = str(row.get("to") or "").strip().lower()
        if to == wallet_norm:
            nft_in_txs.add(tx_hash)
        if frm == wallet_norm:
            nft_out_txs.add(tx_hash)
    return nft_in_txs, nft_out_txs


def _is_zero_chain_address(value: str | None) -> bool:
    text = str(value or "").strip().lower()
    return text in ("", "0x0000000000000000000000000000000000000000")


def _nft_row_token_id(row: dict[str, Any]) -> str:
    return str(row.get("tokenID") or row.get("tokenId") or "").strip()


def _delayed_open_pack_tx_hashes(
    cfg: OnchainConfig,
    wallet: str,
    usdt_rows: list[dict[str, Any]],
    nft_rows: list[dict[str, Any]],
    nft_in_txs: set[str],
) -> set[str]:
    window_sec = int(ONCHAIN_DELAYED_MINT_WINDOW_SEC or 0)
    if window_sec <= 0:
        return set()

    wallet_norm = str(wallet or "").strip().lower()
    marketplace_contract = str(cfg.marketplace_contract or "").strip().lower()
    payment_candidates: list[dict[str, Any]] = []
    for row in usdt_rows:
        if not isinstance(row, dict):
            continue
        tx_hash = _row_hash(row)
        if not tx_hash or tx_hash in nft_in_txs:
            continue
        if _usdt_amount_from_raw(row.get("value"), row.get("tokenDecimal")) <= 0:
            continue
        frm = str(row.get("from") or "").strip().lower()
        to = str(row.get("to") or "").strip().lower()
        if frm != wallet_norm:
            continue
        if not to.startswith("0x") or len(to) != 42:
            continue
        if to == marketplace_contract:
            continue
        if _row_timestamp(row) <= 0:
            continue
        payment_candidates.append(row)

    if not payment_candidates:
        return set()

    payment_hashes = {_row_hash(row) for row in payment_candidates}
    mint_groups_by_tx: dict[str, int] = {}
    for row in nft_rows:
        if not isinstance(row, dict):
            continue
        tx_hash = _row_hash(row)
        if not tx_hash or tx_hash in payment_hashes:
            continue
        if not _nft_row_token_id(row):
            continue
        frm = str(row.get("from") or "").strip().lower()
        to = str(row.get("to") or "").strip().lower()
        if to != wallet_norm or not _is_zero_chain_address(frm):
            continue
        ts = _row_timestamp(row)
        if ts <= 0:
            continue
        prev = mint_groups_by_tx.get(tx_hash)
        mint_groups_by_tx[tx_hash] = ts if prev is None else min(prev, ts)

    mint_groups = sorted(mint_groups_by_tx.items(), key=lambda x: (int(x[1]), str(x[0])))
    if not mint_groups:
        return set()

    matches: set[str] = set()
    used_mint_txs: set[str] = set()
    payment_candidates.sort(key=lambda r: (_row_timestamp(r), _row_block_number(r), _row_tx_index(r), _row_hash(r)))
    for payment in payment_candidates:
        pay_hash = _row_hash(payment)
        pay_ts = _row_timestamp(payment)
        for mint_hash, mint_ts in mint_groups:
            if mint_hash in used_mint_txs:
                continue
            if mint_ts < pay_ts:
                continue
            if mint_ts - pay_ts > window_sec:
                break
            used_mint_txs.add(mint_hash)
            matches.add(pay_hash)
            break
    return matches


def _match_legacy_pull_hints_to_payment_rows(
    cfg: OnchainConfig,
    wallet: str,
    usdt_rows: list[dict[str, Any]],
    nft_in_txs: set[str],
    nft_out_txs: set[str],
    legacy_open_pack_hints: list[dict[str, Any]] | None,
) -> set[str]:
    hints = [x for x in (legacy_open_pack_hints or []) if isinstance(x, dict)]
    window_sec = int(ONCHAIN_DELAYED_MINT_WINDOW_SEC or 0)
    if not hints or window_sec <= 0:
        return set()

    wallet_norm = str(wallet or "").strip().lower()
    rows = sorted(
        [x for x in usdt_rows if isinstance(x, dict)],
        key=lambda r: (_row_timestamp(r), _row_block_number(r), _row_tx_index(r), _row_hash(r)),
    )
    hints_sorted = sorted(
        hints,
        key=lambda h: (
            int(_to_decimal((h or {}).get("timestamp")) or 0),
            str((h or {}).get("pack_key") or ""),
        ),
    )
    matched: set[str] = set()
    used_hint_indexes: set[int] = set()
    for row in rows:
        tx_hash = _row_hash(row)
        if not tx_hash or tx_hash in matched:
            continue
        amount = _usdt_amount_from_raw(row.get("value"), row.get("tokenDecimal"))
        if amount <= 0:
            continue
        frm = str(row.get("from") or "").strip().lower()
        to = str(row.get("to") or "").strip().lower()
        if frm != wallet_norm:
            continue
        if to == str(cfg.marketplace_contract or "").strip().lower():
            continue
        cls = _classify_transfer(
            row,
            wallet_norm,
            cfg,
            tx_has_nft_in=bool(tx_hash and tx_hash in nft_in_txs),
            tx_has_nft_out=bool(tx_hash and tx_hash in nft_out_txs),
        )
        if cls != "other":
            continue
        row_ts = _row_timestamp(row)
        if row_ts <= 0:
            continue

        best_idx: int | None = None
        best_delta: int | None = None
        for idx, hint in enumerate(hints_sorted):
            if idx in used_hint_indexes:
                continue
            hint_ts = int(_to_decimal(hint.get("timestamp")) or 0)
            if hint_ts <= 0:
                continue
            delta = abs(hint_ts - row_ts)
            if delta > window_sec:
                continue
            price = _to_decimal(hint.get("price"))
            if price <= 0 or abs(price - amount) > Decimal("0.000001"):
                continue
            if best_delta is None or delta < best_delta:
                best_idx = idx
                best_delta = delta
        if best_idx is not None:
            used_hint_indexes.add(best_idx)
            matched.add(tx_hash)
    return matched


def _open_pack_tx_hashes(
    cfg: OnchainConfig,
    wallet: str,
    usdt_rows: list[dict[str, Any]],
    nft_rows: list[dict[str, Any]],
    *,
    legacy_open_pack_hints: list[dict[str, Any]] | None = None,
    use_delayed_recipient_heuristic: bool = True,
) -> set[str]:
    wallet_norm = str(wallet or "").strip().lower()
    nft_in_txs, nft_out_txs = _wallet_nft_tx_sets(nft_rows, wallet_norm)
    seen_txs: set[str] = set()
    marketplace_contract = str(cfg.marketplace_contract or "").strip().lower()
    pack_contracts = {str(x or "").strip().lower() for x in (cfg.pack_contracts or ())}
    delayed_txs = _delayed_open_pack_tx_hashes(cfg, wallet_norm, usdt_rows, nft_rows, nft_in_txs)
    legacy_hint_txs = _match_legacy_pull_hints_to_payment_rows(
        cfg,
        wallet_norm,
        usdt_rows,
        nft_in_txs,
        nft_out_txs,
        legacy_open_pack_hints,
    )
    delayed_pack_recipients: set[str] = set()
    if use_delayed_recipient_heuristic:
        delayed_pack_recipients = {
            str((row or {}).get("to") or "").strip().lower()
            for row in usdt_rows
            if isinstance(row, dict) and _row_hash(row) in delayed_txs
        }
        delayed_pack_recipients = {x for x in delayed_pack_recipients if x.startswith("0x") and len(x) == 42}

    for row in usdt_rows:
        if not isinstance(row, dict):
            continue
        tx_hash = _row_hash(row)
        if not tx_hash or tx_hash in seen_txs:
            continue
        amount = _usdt_amount_from_raw(row.get("value"), row.get("tokenDecimal"))
        if amount <= 0:
            continue
        frm = str(row.get("from") or "").strip().lower()
        to = str(row.get("to") or "").strip().lower()
        if frm != wallet_norm:
            continue
        if to == marketplace_contract:
            continue
        if (
            tx_hash in nft_in_txs
            or tx_hash in delayed_txs
            or tx_hash in legacy_hint_txs
            or to in delayed_pack_recipients
            or to in pack_contracts
        ):
            seen_txs.add(tx_hash)
    return seen_txs


def _classify_transfer(
    row: dict[str, Any],
    wallet: str,
    cfg: OnchainConfig,
    *,
    tx_has_nft_in: bool = False,
    tx_has_nft_out: bool = False,
) -> str:
    frm = str(row.get("from") or "").strip().lower()
    to = str(row.get("to") or "").strip().lower()
    marketplace_contract = str(cfg.marketplace_contract or "").strip().lower()
    pack_contracts = {str(x or "").strip().lower() for x in (cfg.pack_contracts or ())}

    if frm == wallet and to == marketplace_contract:
        return "mp_buy"
    if frm == marketplace_contract and to == wallet:
        return "mp_sell"
    if frm == wallet and tx_has_nft_in:
        return "open_pack"
    if to == wallet and tx_has_nft_out:
        return "buyback"
    # Backward-compatible fallback for old configs with explicit pack contracts.
    if frm == wallet and to in pack_contracts:
        return "open_pack"
    if frm in pack_contracts and to == wallet:
        return "buyback"
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


def _fetch_usdt_transfers_in_window(
    cfg: OnchainConfig,
    wallet: str,
    *,
    window_start_ts: int,
    window_end_ts: int,
    startblock: int,
) -> tuple[list[dict[str, Any]], int, int]:
    out: list[dict[str, Any]] = []
    api_calls = 0
    rows_scanned = 0
    limit = int(max(1, min(1000, cfg.page_size)))
    max_pages_per_window = max(1, 10000 // limit)
    page = 1
    current_start_block = int(max(0, startblock or 0))
    max_block_seen = current_start_block
    stop_on_window_end = False

    while True:
        if page > max_pages_per_window:
            next_start_block = int(max(max_block_seen, current_start_block) + 1)
            if next_start_block <= current_start_block:
                break
            current_start_block = next_start_block
            page = 1
            continue

        rows = _fetch_tokentx_page(
            cfg,
            wallet,
            page=page,
            sort="asc",
            startblock=current_start_block if current_start_block > 0 else None,
        )
        api_calls += 1
        if not rows:
            break
        for row in rows:
            rows_scanned += 1
            block = _row_block_number(row)
            if block > max_block_seen:
                max_block_seen = block
            ts = _row_timestamp(row)
            if ts >= int(window_end_ts):
                stop_on_window_end = True
                break
            if ts < int(window_start_ts):
                continue
            out.append(row)
        if stop_on_window_end:
            break
        if len(rows) < limit:
            break
        page += 1

    return out, api_calls, rows_scanned


def _fetch_nft_transfers_in_window(
    cfg: OnchainConfig,
    wallet: str,
    *,
    window_start_ts: int,
    window_end_ts: int,
    startblock: int,
) -> tuple[list[dict[str, Any]], int, int]:
    out: list[dict[str, Any]] = []
    api_calls = 0
    rows_scanned = 0
    limit = int(max(1, min(1000, cfg.page_size)))
    max_pages_per_window = max(1, 10000 // limit)

    for action in ("tokennfttx", "token1155tx"):
        page = 1
        current_start_block = int(max(0, startblock or 0))
        max_block_seen = current_start_block
        stop_on_window_end = False
        try:
            while True:
                if page > max_pages_per_window:
                    next_start_block = int(max(max_block_seen, current_start_block) + 1)
                    if next_start_block <= current_start_block:
                        break
                    current_start_block = next_start_block
                    page = 1
                    continue

                rows = _fetch_nfttx_page(
                    cfg,
                    wallet,
                    action,
                    page=page,
                    sort="asc",
                    startblock=current_start_block if current_start_block > 0 else None,
                )
                api_calls += 1
                if not rows:
                    break
                for row in rows:
                    rows_scanned += 1
                    block = _row_block_number(row)
                    if block > max_block_seen:
                        max_block_seen = block
                    ts = _row_timestamp(row)
                    if ts >= int(window_end_ts):
                        stop_on_window_end = True
                        break
                    if ts < int(window_start_ts):
                        continue
                    out.append(row)
                if stop_on_window_end:
                    break
                if len(rows) < limit:
                    break
                page += 1
        except Exception:
            if action == "token1155tx":
                continue
            raise

    out.sort(key=lambda r: (_row_timestamp(r), _row_block_number(r), _row_tx_index(r), _row_hash(r)))
    return out, api_calls, rows_scanned


def _count_open_pack_txs(
    cfg: OnchainConfig,
    wallet: str,
    usdt_rows: list[dict[str, Any]],
    nft_rows: list[dict[str, Any]],
) -> int:
    return len(_open_pack_tx_hashes(cfg, wallet, usdt_rows, nft_rows))


def scan_wallet_open_counts_by_time_incremental(
    cfg: OnchainConfig,
    *,
    wallets: list[str] | tuple[str, ...] | set[str],
    window_start_ts: int,
    window_end_ts: int,
    prev_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wallet_list: list[str] = []
    seen_wallets: set[str] = set()
    for raw in list(wallets or ()):
        addr = str(raw or "").strip().lower()
        if not addr.startswith("0x") or len(addr) != 42:
            continue
        if addr in seen_wallets:
            continue
        seen_wallets.add(addr)
        wallet_list.append(addr)

    start_ts = int(max(0, window_start_ts or 0))
    end_ts = int(max(start_ts, window_end_ts or 0))
    prev = prev_state if isinstance(prev_state, dict) else {}
    prev_window_start = int(_to_decimal(prev.get("window_start_ts")) or 0)
    reset_applied = prev_window_start != start_ts

    prev_counts: dict[str, int] = {}
    prev_markers: dict[str, str] = {}
    if not reset_applied:
        raw_counts = prev.get("wallet_counts")
        if isinstance(raw_counts, dict):
            for addr, raw in raw_counts.items():
                key = str(addr or "").strip().lower()
                if key.startswith("0x") and len(key) == 42:
                    prev_counts[key] = max(0, int(_to_decimal(raw)))
        raw_markers = prev.get("wallet_latest_usdt_tx")
        if isinstance(raw_markers, dict):
            for addr, raw in raw_markers.items():
                key = str(addr or "").strip().lower()
                if key.startswith("0x") and len(key) == 42:
                    prev_markers[key] = str(raw or "").strip().lower()

    api_calls = 0
    rows_scanned = 0
    wallet_counts: dict[str, int] = {}
    next_markers: dict[str, str] = {}
    to_rescan: list[tuple[str, str]] = []
    reused_wallets = 0
    failed_wallets: dict[str, str] = {}

    for wallet in wallet_list:
        latest_marker = ""
        latest_ok = True
        try:
            latest_marker = fetch_latest_usdt_tx_hash(cfg, wallet)
            api_calls += 1
        except Exception as e:  # noqa: BLE001
            latest_ok = False
            latest_marker = str(prev_markers.get(wallet) or "")
            failed_wallets[wallet] = f"latest_usdt: {type(e).__name__}: {e}"
        if (
            latest_ok
            and not reset_applied
            and wallet in prev_counts
            and str(prev_markers.get(wallet) or "") == str(latest_marker or "")
        ):
            wallet_counts[wallet] = int(prev_counts.get(wallet, 0))
            next_markers[wallet] = latest_marker
            reused_wallets += 1
            continue
        to_rescan.append((wallet, latest_marker))

    window_start_block = 0
    if to_rescan and start_ts > 0:
        try:
            window_start_block = _fetch_block_number_by_timestamp(cfg, start_ts)
            api_calls += 1
        except Exception:
            window_start_block = 0

    rescanned_wallets = 0
    for wallet, latest_marker in to_rescan:
        try:
            usdt_rows, calls, scanned = _fetch_usdt_transfers_in_window(
                cfg,
                wallet,
                window_start_ts=start_ts,
                window_end_ts=end_ts,
                startblock=window_start_block,
            )
            api_calls += calls
            rows_scanned += scanned
            nft_rows, calls, scanned = _fetch_nft_transfers_in_window(
                cfg,
                wallet,
                window_start_ts=start_ts,
                window_end_ts=end_ts,
                startblock=window_start_block,
            )
            api_calls += calls
            rows_scanned += scanned
            wallet_counts[wallet] = _count_open_pack_txs(cfg, wallet, usdt_rows, nft_rows)
            if not latest_marker:
                latest_marker = _row_hash(usdt_rows[-1]) if usdt_rows else ""
            next_markers[wallet] = latest_marker
            rescanned_wallets += 1
        except Exception as e:  # noqa: BLE001
            wallet_counts[wallet] = int(prev_counts.get(wallet, 0))
            next_markers[wallet] = str(prev_markers.get(wallet) or latest_marker or "")
            failed_wallets[wallet] = f"rescan: {type(e).__name__}: {e}"

    state = {
        "version": 2,
        "scan_mode": "wallet_time_incremental",
        "window_start_ts": int(start_ts),
        "window_end_ts": int(end_ts),
        "updated_at_ts": int(time.time()),
        "wallet_counts": wallet_counts,
        "wallet_latest_usdt_tx": next_markers,
    }
    return {
        "state": state,
        "wallet_counts": wallet_counts,
        "stats": {
            "scan_mode": "wallet_time_incremental",
            "api_calls": int(api_calls),
            "rows_scanned": int(rows_scanned),
            "wallets": int(len(wallet_list)),
            "rescanned_wallets": int(rescanned_wallets),
            "reused_wallets": int(reused_wallets),
            "failed_wallets": int(len(failed_wallets)),
            "failed_wallet_errors": failed_wallets,
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
    *,
    legacy_open_pack_hints: list[dict[str, Any]] | None = None,
    use_delayed_recipient_heuristic: bool = True,
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
    nft_rows = fetch_all_nft_transfers(cfg, wallet_norm)
    nft_in_txs, nft_out_txs = _wallet_nft_tx_sets(nft_rows, wallet_norm)
    withdraw_targets = {
        str(x or "").strip().lower()
        for x in (getattr(cfg, "withdraw_addresses", ()) or ())
        if str(x or "").strip().lower().startswith("0x") and len(str(x or "").strip().lower()) == 42
    }
    withdraw_token_ids: set[str] = set()
    if withdraw_targets:
        for row in nft_rows:
            if not isinstance(row, dict):
                continue
            frm = str(row.get("from") or "").strip().lower()
            to = str(row.get("to") or "").strip().lower()
            if frm != wallet_norm or to not in withdraw_targets:
                continue
            token_id = str(row.get("tokenID") or row.get("tokenId") or "").strip()
            if token_id:
                withdraw_token_ids.add(token_id)
    open_pack_hashes = _open_pack_tx_hashes(
        cfg,
        wallet_norm,
        transfers,
        nft_rows,
        legacy_open_pack_hints=legacy_open_pack_hints,
        use_delayed_recipient_heuristic=use_delayed_recipient_heuristic,
    )
    effective_pack_contracts = {str(x or "").strip().lower() for x in (cfg.pack_contracts or ())}
    marketplace_contract = str(cfg.marketplace_contract or "").strip().lower()
    for row in transfers:
        if not isinstance(row, dict):
            continue
        tx_hash = _row_hash(row)
        if not tx_hash or tx_hash not in open_pack_hashes:
            continue
        frm = str(row.get("from") or "").strip().lower()
        to = str(row.get("to") or "").strip().lower()
        if frm != wallet_norm:
            continue
        if not (to.startswith("0x") and len(to) == 42):
            continue
        if to in (wallet_norm, marketplace_contract):
            continue
        effective_pack_contracts.add(to)
    effective_cfg = replace(cfg, pack_contracts=tuple(sorted(effective_pack_contracts)))
    pack_spent = Decimal("0")
    buyback_earned = Decimal("0")
    market_buy_spent = Decimal("0")
    market_sell_earned = Decimal("0")
    open_pack_tx_count = 0
    buyback_tx_count = 0
    trade_buy_tx_count = 0
    trade_sell_tx_count = 0

    for row in transfers:
        tx_hash = _row_hash(row)
        cls = _classify_transfer(
            row,
            wallet_norm,
            effective_cfg,
            tx_has_nft_in=bool(tx_hash and tx_hash in nft_in_txs),
            tx_has_nft_out=bool(tx_hash and tx_hash in nft_out_txs),
        )
        if cls == "other" and tx_hash in open_pack_hashes:
            cls = "open_pack"
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
        "withdraw_token_ids": sorted(withdraw_token_ids),
        "open_pack_tx_count": Decimal(open_pack_tx_count),
        "buyback_tx_count": Decimal(buyback_tx_count),
        "trade_buy_tx_count": Decimal(trade_buy_tx_count),
        "trade_sell_tx_count": Decimal(trade_sell_tx_count),
    }
