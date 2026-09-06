"""Decode VRF V3 checkouts and reconcile them with wallet USDT payments."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable

from runtime.pack_contracts import VRF_V3_CONTRACT, VRF_V3_CHECKOUT_TOPIC


@dataclass(frozen=True)
class VrfCheckout:
    pack_id: str
    checkout_ids: tuple[int, ...]
    amount_raw: int

    @property
    def count(self) -> int:
        return len(self.checkout_ids)

    @property
    def amount(self) -> Decimal:
        return Decimal(self.amount_raw) / Decimal(10**18)


def decode_vrf_checkout(log: dict, wallet: str, usdt_contract: str) -> VrfCheckout | None:
    """CheckoutSuccess(user, packId, builder, token, checkoutIds, totalAmount)."""
    if str(log.get("address") or "").lower() != VRF_V3_CONTRACT or log.get("removed"):
        return None
    topics = [str(value).lower() for value in (log.get("topics") or [])]
    if not topics or topics[0] != VRF_V3_CHECKOUT_TOPIC:
        return None
    if len(topics) != 4 or any(len(topic) != 66 for topic in topics):
        raise RuntimeError("vrf_checkout_invalid_topics")
    if "0x" + topics[1][-40:] != wallet.lower():
        return None
    try:
        data = bytes.fromhex(str(log.get("data") or "").removeprefix("0x"))
        if len(data) < 128 or len(data) % 32:
            raise ValueError("invalid ABI data length")
        word = lambda index: int.from_bytes(data[index * 32:(index + 1) * 32], "big")
        if word(0) >= 2**160:
            raise ValueError("invalid token address")
        if "0x" + data[12:32].hex() != usdt_contract.lower():
            return None
        if word(1) != 96:
            raise ValueError("invalid checkoutIds offset")
        count = word(3)
        if count <= 0 or len(data) != (4 + count) * 32 or word(2) <= 0:
            raise ValueError("invalid checkoutIds or amount")
        ids = tuple(word(index) for index in range(4, 4 + count))
        if len(set(ids)) != count:
            raise ValueError("duplicate checkoutIds")
        int(topics[2], 16)
        return VrfCheckout(topics[2], ids, word(2))
    except (ValueError, TypeError) as exc:
        raise RuntimeError("vrf_checkout_invalid_data") from exc


def reconcile_vrf_checkouts(
    wallet: str,
    payments: list[dict],
    usdt_contract: str,
    fetch_logs: Callable[[int, int], list[dict]],
) -> dict[str, VrfCheckout]:
    """Query only payment block windows. Missing events are not inferred purchases."""
    paid: dict[str, int] = defaultdict(int)
    blocks: set[int] = set()
    for row in payments:
        if (str(row.get("from") or "").lower() != wallet.lower()
                or str(row.get("to") or "").lower() != VRF_V3_CONTRACT):
            continue
        contract = str(row.get("contractAddress") or usdt_contract).lower()
        if contract != usdt_contract.lower():
            continue
        tx_hash = str(row.get("hash") or "").lower()
        amount = int(row.get("value") or 0)
        if not tx_hash or amount <= 0:
            continue
        block_raw = str(row.get("blockNumber") or "0")
        block = int(block_raw, 16 if block_raw.startswith("0x") else 10)
        if block <= 0 or int(row.get("tokenDecimal") or 18) != 18:
            raise RuntimeError("vrf_payment_invalid_block_or_decimals")
        blocks.add(block)
        paid[tx_hash] += amount
    if not paid:
        return {}

    ranges: list[tuple[int, int]] = []
    for block in sorted(blocks):
        if ranges and block - ranges[-1][0] <= 20000:
            ranges[-1] = (ranges[-1][0], block)
        else:
            ranges.append((block, block))
    found: dict[str, VrfCheckout] = {}
    seen: set[tuple[str, str]] = set()
    for start, end in ranges:
        for log in fetch_logs(start, end):
            tx_hash = str(log.get("transactionHash") or log.get("hash") or "").lower()
            if tx_hash not in paid:
                continue
            event = decode_vrf_checkout(log, wallet, usdt_contract)
            if event is None:
                continue
            key = (tx_hash, str(log.get("logIndex")))
            if key in seen:
                continue
            seen.add(key)
            previous = found.get(tx_hash)
            if previous is not None:
                if previous.pack_id != event.pack_id:
                    raise RuntimeError("vrf_checkout_multiple_packs_in_transaction")
                if set(previous.checkout_ids).intersection(event.checkout_ids):
                    raise RuntimeError("vrf_checkout_duplicate_ids")
                event = VrfCheckout(event.pack_id, previous.checkout_ids + event.checkout_ids,
                                    previous.amount_raw + event.amount_raw)
            found[tx_hash] = event
    for tx_hash, event in found.items():
        if event.amount_raw != paid[tx_hash]:
            raise RuntimeError("vrf_checkout_payment_amount_mismatch")
    return found
