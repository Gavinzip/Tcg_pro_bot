"""Wallet-only BBW metrics from confirmed pack payments and Renaiss SBT transfers."""
from __future__ import annotations

from collections import OrderedDict, defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from datetime import datetime, timezone
from hmac import compare_digest
from http import HTTPStatus
import re
import threading
import time

from runtime.pack_contracts import DEFAULT_PACK_CONTRACTS, VRF_V3_CONTRACT, VRF_V3_CHECKOUT_TOPIC
from runtime.vrf_checkouts import decode_vrf_checkout
from scripts.onchain_metrics import (
    EtherscanRequestPacer, OnchainConfig, _fetch_event_logs_page, _fetch_token1155tx_page, _fetch_tokentx_page,
    _fetch_tx_receipt,
)

USDT_CONTRACT = "0x55d398326f99059ff775485246999027b3197955"
SBT_CONTRACT = "0x7d1b7db704d722295fbaa284008f526634673dbf"
LEGACY_VENDING_MACHINE = DEFAULT_PACK_CONTRACTS[0]
LEGACY_OPERATOR = "0x12639a4147af7c777bb9a21fdaefa5ea91e2e3a3"
LEGACY_CUTOVER = int(datetime(2025, 12, 19, tzinfo=timezone.utc).timestamp())
LEGACY_CHECKOUT_TOPIC = "0xd505514c5f9bb134a66621a7fd46a679442a1a0e45f5ad5dff0724e4b4588fed"
PACK_CHECKOUT_TOPIC = "0xf1ec6bca7c2c281e7c68bbddbba207b6df358f0ef65c5650b1f796d3f1224fe9"
USDT_UNIT = 10**18
SBT_AND_SPEND_CACHE_TTL = 45
# The production key allows five calls per second. Leave room for other callers sharing the key.
BBW_ETHERSCAN_REQUESTS_PER_SECOND = 4
MIGRATION_HELPER_CONTRACT = "0x2e737d552b3c601ada4fcd167bfbd8d4e1043b2c"
MIGRATION_HELPER_TOPIC = "0xa8842895c03659cf75d4e5e2202ace2ef1981a77551fc02961bc661af0950830"
MIGRATION_START_BLOCK = 99465073


def _iter_pages(fetch, page_size):
    # Etherscan caps the result window. Treat an unproven end of history as unavailable.
    for page in range(1, 10000 // page_size + 1):
        rows = fetch(page)
        yield from rows
        if len(rows) < page_size:
            return
    raise RuntimeError("metrics_provider_page_limit")


def _sbt_count(cfg, wallet, cutoff):
    balances = defaultdict(int)
    rows = _iter_pages(
        lambda page: _fetch_token1155tx_page(cfg, wallet, SBT_CONTRACT, page=page, sort="asc"),
        cfg.page_size,
    )
    for row in rows:
        if int(row["timeStamp"]) > cutoff:
            break
        if row["contractAddress"].lower() != SBT_CONTRACT:
            raise RuntimeError("unexpected_sbt_contract")
        token_id = int(row["tokenID"])
        value = int(row["tokenValue"])
        if row["from"].lower() == wallet:
            balances[token_id] -= value
        if row["to"].lower() == wallet:
            balances[token_id] += value
    if any(balance < 0 for balance in balances.values()):
        raise RuntimeError("incomplete_sbt_history")
    return sum(balance > 0 for balance in balances.values())


def _spend_source_wallets(cfg, wallet, cutoff):
    """Use TCG Pro's verified migration helper to include the account's former Safe."""
    sources = {wallet}
    rows = _iter_pages(
        lambda page: _fetch_event_logs_page(
            cfg, contract=MIGRATION_HELPER_CONTRACT, topic0=MIGRATION_HELPER_TOPIC,
            from_block=MIGRATION_START_BLOCK, to_block=0, page=page,
            indexed_wallet=wallet, indexed_wallet_topic=3,
        ),
        cfg.page_size,
    )
    for row in rows:
        if row.get("removed") is True:
            continue
        topics = row.get("topics")
        timestamp = row.get("timeStamp")
        if (row.get("address", "").lower() != MIGRATION_HELPER_CONTRACT
                or not isinstance(topics, list) or len(topics) != 4
                or topics[0].lower() != MIGRATION_HELPER_TOPIC
                or not isinstance(timestamp, str) or not re.fullmatch(r"0x[0-9a-fA-F]+", timestamp)):
            raise RuntimeError("invalid_wallet_migration_event")
        if int(timestamp, 16) > cutoff:
            continue
        old_wallet = "0x" + topics[2][-40:].lower()
        new_wallet = "0x" + topics[3][-40:].lower()
        if (not re.fullmatch(r"0x[0-9a-f]{40}", old_wallet)
                or new_wallet != wallet or old_wallet == wallet):
            raise RuntimeError("invalid_wallet_migration_pair")
        sources.add(old_wallet)
        if len(sources) > 6:
            raise RuntimeError("too_many_wallet_migration_sources")
    return tuple(sorted(sources))


def _pack_payments(cfg, wallet, cutoff):
    payments = defaultdict(dict)
    rows = _iter_pages(lambda page: _fetch_tokentx_page(cfg, wallet, page=page, sort="asc"), cfg.page_size)
    for row in rows:
        timestamp = int(row["timeStamp"])
        if timestamp > cutoff:
            break
        recipient = row["to"].lower()
        if row["from"].lower() != wallet or recipient not in DEFAULT_PACK_CONTRACTS:
            continue
        if row["contractAddress"].lower() != USDT_CONTRACT or int(row["tokenDecimal"]) != 18:
            raise RuntimeError("unexpected_payment_currency")
        amount = int(row["value"])
        if amount > 0:
            tx_hash = row["hash"].lower()
            previous = payments[recipient].get(tx_hash)
            payments[recipient][tx_hash] = (amount + (previous[0] if previous else 0), int(row["blockNumber"]), timestamp)
    return payments


def _confirmed_pack_spend(cfg, wallet, contract, payments):
    topic = {
        VRF_V3_CONTRACT: VRF_V3_CHECKOUT_TOPIC,
        LEGACY_VENDING_MACHINE: LEGACY_CHECKOUT_TOPIC,
    }.get(contract, PACK_CHECKOUT_TOPIC)
    blocks = [block for _, block, _ in payments.values()]
    logs = _iter_pages(lambda page: _fetch_event_logs_page(
        cfg, contract=contract, topic0=topic, from_block=min(blocks), to_block=max(blocks),
        page=page, indexed_wallet=wallet,
    ), cfg.page_size)
    confirmed = defaultdict(int)
    seen = set()
    for log in logs:
        tx_hash = log["transactionHash"].lower()
        if tx_hash not in payments or log.get("removed") is True:
            continue
        log_key = (tx_hash, int(log["logIndex"], 16))
        if log_key in seen:
            continue
        seen.add(log_key)
        topics = [value.lower() for value in log["topics"]]
        if (log["address"].lower() != contract or topics[0] != topic
                or "0x" + topics[1][-40:] != wallet):
            raise RuntimeError("unexpected_pack_checkout")
        if contract == VRF_V3_CONTRACT:
            checkout = decode_vrf_checkout(log, wallet, USDT_CONTRACT)
            if checkout is not None:
                confirmed[tx_hash] += checkout.amount_raw
        else:
            data = bytes.fromhex(log["data"].removeprefix("0x"))
            if len(data) != 32:
                raise RuntimeError("invalid_pack_checkout")
            confirmed[tx_hash] += int.from_bytes(data, "big")

    # The oldest AAB5 purchases did not emit CheckoutSuccess. The established
    # ledger identifies direct 60-USDT vending-machine transactions before cutover.
    if contract == LEGACY_VENDING_MACHINE and wallet != LEGACY_OPERATOR:
        for tx_hash, (amount, _, timestamp) in payments.items():
            if tx_hash in confirmed or timestamp >= LEGACY_CUTOVER or amount != 60 * USDT_UNIT:
                continue
            receipt = _fetch_tx_receipt(cfg, tx_hash)
            if (receipt["to"].lower() == contract and receipt["from"].lower() == wallet
                    and receipt["status"] == "0x1"):
                confirmed[tx_hash] = amount
    for tx_hash, amount in confirmed.items():
        if amount != payments[tx_hash][0]:
            raise RuntimeError("pack_checkout_payment_mismatch")
    return sum(confirmed.values())


def _format_usdt(raw_amount):
    whole, remainder = divmod(raw_amount, USDT_UNIT)
    return f"{whole}.{remainder:018d}".rstrip("0") if remainder else str(whole)


class BbwCommunityMetrics:
    def __init__(self, env):
        self.token = env.get("BBW_METRICS_TOKEN", "").strip()
        self.cutoff = None
        source_key = env.get("BSCSCAN_API_KEY", "").strip()
        self.chain = OnchainConfig(
            api_url=env.get("BSCSCAN_API_URL", "").strip() or "https://api.etherscan.io/v2/api",
            chain_id=56, api_key=source_key, usdt_contract=USDT_CONTRACT,
            pack_contracts=DEFAULT_PACK_CONTRACTS, marketplace_contract="",
            page_size=1000, retries=2, backoff_sec=0.2, request_timeout_sec=5,
            request_pacer=EtherscanRequestPacer(requests_per_second=BBW_ETHERSCAN_REQUESTS_PER_SECOND),
        )
        cutoff = env.get("BBW_METRICS_CUTOFF", "").strip()
        if self.token and source_key and cutoff:
            try:
                instant = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
                if instant.utcoffset() is not None:
                    self.cutoff = int(instant.timestamp())
            except (ValueError, OverflowError):
                pass
        self._cache = OrderedDict()
        self._guard = threading.Lock()
        self._slots = threading.BoundedSemaphore(4)
        self._inflight: dict[tuple[str, bool], Future] = {}
        self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="bbw-metrics")

    def authorized(self, authorization):
        return bool(self.token) and compare_digest(authorization.encode(), f"Bearer {self.token}".encode())

    def lookup(self, wallet):
        if self.cutoff is None:
            return HTTPStatus.SERVICE_UNAVAILABLE, {"error": "metrics_not_configured"}

        now = time.time()
        cutoff = min(int(now), self.cutoff)
        key = (wallet, now >= self.cutoff)
        with self._guard:
            cached = self._cache.get(key)
            if cached and cached[0] > now:
                self._cache.move_to_end(key)
                return HTTPStatus.OK, cached[1]
            flight = self._inflight.get(key)
            leader = flight is None
            if flight is None:
                if not self._slots.acquire(blocking=False):
                    return HTTPStatus.TOO_MANY_REQUESTS, {"error": "metrics_busy"}
                flight = self._inflight[key] = Future()

        if not leader:
            try:
                return flight.result(timeout=13)
            except TimeoutError:
                return HTTPStatus.GATEWAY_TIMEOUT, {"error": "metrics_timeout"}

        result = self._compute(wallet, cutoff)
        with self._guard:
            status, payload = result
            if status == HTTPStatus.OK:
                self._cache[key] = (now + SBT_AND_SPEND_CACHE_TTL, payload)
                self._cache.move_to_end(key)
                if len(self._cache) > 512:
                    self._cache.popitem(last=False)
            flight.set_result(result)
            del self._inflight[key]
            self._slots.release()
        return result

    def _compute(self, wallet, cutoff):
        stage = "pack_payments"
        try:
            sbt_job = self._pool.submit(_sbt_count, self.chain, wallet, cutoff)
            source_job = self._pool.submit(_spend_source_wallets, self.chain, wallet, cutoff)
            current_payments = _pack_payments(self.chain, wallet, cutoff)
            stage = "wallet_migration"
            wallets = source_job.result()
            stage = "pack_payments"
            payments_by_wallet = {wallet: current_payments}
            for source_wallet in wallets:
                if source_wallet != wallet:
                    payments_by_wallet[source_wallet] = _pack_payments(self.chain, source_wallet, cutoff)
            spend_jobs = [
                self._pool.submit(_confirmed_pack_spend, self.chain, source_wallet, contract, rows)
                for source_wallet, payments in payments_by_wallet.items()
                for contract, rows in payments.items()
            ]
            stage = "sbt"
            count = sbt_job.result()
            stage = "pack_checkouts"
            spend = _format_usdt(sum(job.result() for job in spend_jobs))
            return HTTPStatus.OK, {"sbtCount": count, "paidPackSpendUsdt": spend}
        except Exception as exc:
            print(f"[bbw-metrics] upstream failed at {stage} ({type(exc).__name__})", flush=True)
            return HTTPStatus.BAD_GATEWAY, {"error": "metrics_source_unavailable"}
