"""Reconcile paid USDT with batched checkout events across vending versions."""
from collections import defaultdict
import re

from runtime.pack_contracts import DEFAULT_PACK_CONTRACTS, VRF_V3_CONTRACT, VRF_V3_CHECKOUT_TOPIC
from runtime.vrf_checkouts import decode_vrf_checkout


LEGACY_VENDING_MACHINE = DEFAULT_PACK_CONTRACTS[0]
LEGACY_CHECKOUT_TOPIC = "0xd505514c5f9bb134a66621a7fd46a679442a1a0e45f5ad5dff0724e4b4588fed"
EARLY_CHECKOUT_TOPIC = "0xbce26dc7177b18647918a6cc5cba6dd27ba1a2e1aeb20ccc5820d11b6a705277"
PACK_CHECKOUT_TOPIC = "0xf1ec6bca7c2c281e7c68bbddbba207b6df358f0ef65c5650b1f796d3f1224fe9"


def checkout_topic(contract):
    return {
        VRF_V3_CONTRACT: VRF_V3_CHECKOUT_TOPIC,
        LEGACY_VENDING_MACHINE: LEGACY_CHECKOUT_TOPIC,
    }.get(contract, PACK_CHECKOUT_TOPIC)


def reconcile_checkout_payments(wallet, contract, payments, logs, topic, usdt_contract):
    """Use actual payment amounts for the old event that has no amount field.

    Transaction senders are deliberately irrelevant: trusted relayers may submit
    a checkout that pulls USDT from the buyer via Permit2. A transfer without a
    checkout event (including platform buyback funding) is never a pack purchase.
    """
    confirmed = defaultdict(int)
    seen = set()
    early = topic == EARLY_CHECKOUT_TOPIC
    expected_topics = 4 if contract == VRF_V3_CONTRACT or (contract == LEGACY_VENDING_MACHINE and not early) else 3
    for log in logs:
        tx_hash = log["transactionHash"].lower()
        if tx_hash not in payments or log.get("removed") is True:
            continue
        key = (tx_hash, int(log["logIndex"], 16))
        if key in seen:
            continue
        seen.add(key)
        topics = log.get("topics")
        if (log.get("address", "").lower() != contract or not isinstance(topics, list)
                or len(topics) != expected_topics
                or any(not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value) for value in topics)
                or topics[0].lower() != topic
                or topics[1].lower() != "0x" + wallet[2:].rjust(64, "0")
                or (early and contract != LEGACY_VENDING_MACHINE)):
            raise RuntimeError("unexpected_pack_checkout")
        if early:
            if log.get("data") != "0x":
                raise RuntimeError("invalid_pack_checkout")
            # One transaction's transfers are already aggregated by the payer.
            # Multiple legacy events must not add the same payment again.
            confirmed[tx_hash] = payments[tx_hash][0]
        elif contract == VRF_V3_CONTRACT:
            checkout = decode_vrf_checkout(log, wallet, usdt_contract)
            if checkout is not None:
                confirmed[tx_hash] += checkout.amount_raw
        else:
            data = bytes.fromhex(log.get("data", "").removeprefix("0x"))
            if len(data) != 32:
                raise RuntimeError("invalid_pack_checkout")
            confirmed[tx_hash] += int.from_bytes(data, "big")
    for tx_hash, amount in confirmed.items():
        if amount != payments[tx_hash][0]:
            raise RuntimeError("pack_checkout_payment_mismatch")
    return dict(confirmed)
