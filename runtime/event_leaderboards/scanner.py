"""Confirmed, contract-wide paid checkout indexing for finite activity windows.

Only audited CheckoutSuccess schemas are accepted. Legacy implementations emit one
success per checkout, including every member of a batch; V3 emits checkoutIds.
Card transfers, transaction prices and asynchronous fulfillment are never inputs.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from runtime.pack_contracts import DEFAULT_PACK_CONTRACTS, VRF_V3_CONTRACT, VRF_V3_CHECKOUT_TOPIC
from .config import AUDITED_CHECKOUT_TOPICS, source_key
from .provider_errors import classify_failure, safe_diagnostic

USDT = "0x55d398326f99059ff775485246999027b3197955"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
LEGACY_PACK = "0xd505514c5f9bb134a66621a7fd46a679442a1a0e45f5ad5dff0724e4b4588fed"
LEGACY_SINGLE = "0xf1ec6bca7c2c281e7c68bbddbba207b6df358f0ef65c5650b1f796d3f1224fe9"
LEGACY_BATCH = "0xede018f74fbd413d7c7627cc667118193cbf02268c4b63013ef3bbc7c8da91a7"
SCHEMAS = {DEFAULT_PACK_CONTRACTS[0]: "legacy_pack", **{a: "legacy_single" for a in DEFAULT_PACK_CONTRACTS[1:4]},
           VRF_V3_CONTRACT: "v3"}
TOPICS = AUDITED_CHECKOUT_TOPICS
_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z")
_HASH = re.compile(r"0x[0-9a-f]{64}\Z")


class SourceIncomplete(RuntimeError):
    """Evidence is unavailable, malformed, inconsistent, or unsupported."""

    def __init__(self, code, *, diagnostic=None):
        super().__init__(code)
        self.diagnostic = diagnostic


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise SourceIncomplete("invalid_integer")
    try:
        result = int(value, 16 if isinstance(value, str) and value.startswith("0x") else 10) if isinstance(value, str) else int(value)
    except (TypeError, ValueError, OverflowError):
        raise SourceIncomplete("invalid_integer") from None
    if result < 0:
        raise SourceIncomplete("negative_integer")
    return result


def address(value):
    result = str(value).lower()
    if not _ADDRESS.fullmatch(result):
        raise SourceIncomplete("invalid_address")
    return result


def hash32(value):
    result = str(value).lower()
    if not _HASH.fullmatch(result):
        raise SourceIncomplete("invalid_hash")
    return result


def words(value):
    try:
        raw = bytes.fromhex(str(value).removeprefix("0x"))
    except ValueError:
        raise SourceIncomplete("invalid_abi_data") from None
    if len(raw) % 32:
        raise SourceIncomplete("invalid_abi_length")
    return [int.from_bytes(raw[i:i + 32], "big") for i in range(0, len(raw), 32)]


def topic_address(value):
    value = hash32(value)
    if value[2:26] != "0" * 24:
        raise SourceIncomplete("invalid_indexed_address")
    return address("0x" + value[-40:])


def identity(log):
    return hash32(log.get("transactionHash")), number(log.get("logIndex"))


class EtherscanProvider:
    """Sanitized, paced Etherscan V2 reader; credentials never enter errors."""
    page_size = 1000
    max_pages = 10

    def __init__(self, env=None):
        env = os.environ if env is None else env
        self.api_key = env.get("BSCSCAN_API_KEY", "").strip()
        if not self.api_key:
            raise SourceIncomplete("missing_chain_api_key")
        self.api_url = env.get("BSCSCAN_API_URL") or "https://api.etherscan.io/v2/api"
        self.chain_id = 56
        self.timeout = float(env.get("EVENT_LEADERBOARD_REQUEST_TIMEOUT", "15"))
        # A dedicated scanner leaves headroom for the existing production Bot.
        self.spacing = max(.5, 1 / max(.1, float(env.get("EVENT_LEADERBOARD_REQUESTS_PER_SECOND", "2"))))
        self._next = 0.
        self._lock = threading.Lock()
        self._blocks = {}

    def request(self, module, action, **params):
        with self._lock:
            delay = self._next - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self._next = time.monotonic() + self.spacing
        query = dict(chainid=self.chain_id, module=module, action=action, apikey=self.api_key, **params)
        started, stage = time.monotonic(), "connect"
        try:
            with urllib.request.urlopen(self.api_url + "?" + urllib.parse.urlencode(query), timeout=self.timeout) as response:
                stage = "read_json"
                data = json.load(response)
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as error:
            raise SourceIncomplete(classify_failure(error, stage=stage), diagnostic=safe_diagnostic(
                module, action, params, (time.monotonic() - started) * 1000, error, stage=stage)) from None
        if not isinstance(data, dict) or data.get("error"):
            raise SourceIncomplete("provider_invalid_response")
        result = data.get("result")
        if module == "logs" and data.get("status") == "0" and str(data.get("message", "")).lower() == "no records found" and result == []:
            return []
        if module != "proxy" and data.get("status") != "1":
            message = str(result).lower()
            if "query timeout" in message or "result window" in message or "more than" in message:
                raise SourceIncomplete("provider_query_limit")
            raise SourceIncomplete("provider_rejected_request")
        if result is None:
            raise SourceIncomplete("provider_missing_result")
        return result

    def head(self):
        self._blocks.clear()
        return number(self.request("proxy", "eth_blockNumber"))

    def block(self, height):
        if height not in self._blocks:
            row = self.request("proxy", "eth_getBlockByNumber", tag=hex(height), boolean="false")
            if not isinstance(row, dict) or number(row.get("number")) != height:
                raise SourceIncomplete("invalid_block")
            self._blocks[height] = {"number": height, "hash": hash32(row.get("hash")), "timestamp": number(row.get("timestamp"))}
        return self._blocks[height]

    def first_block_at(self, timestamp, head):
        """Resolve seconds boundary, including chains with several blocks per second."""
        height = number(self.request("block", "getblocknobytime", timestamp=timestamp, closest="before"))
        if height > head:
            raise SourceIncomplete("boundary_above_head")
        while height > 0 and self.block(height - 1)["timestamp"] >= timestamp:
            height -= 1
        while height <= head and self.block(height)["timestamp"] < timestamp:
            height += 1
        return height

    def logs_page(self, contract, topic, start, end, page):
        rows = self.request("logs", "getLogs", address=contract, topic0=topic, fromBlock=start,
                            toBlock=end, page=page, offset=self.page_size)
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows) or len(rows) > self.page_size:
            raise SourceIncomplete("invalid_logs_page")
        return rows

    def receipt(self, tx_hash):
        result = self.request("proxy", "eth_getTransactionReceipt", txhash=tx_hash)
        if not isinstance(result, dict):
            raise SourceIncomplete("receipt_unavailable")
        return result


def complete_logs(provider, contract, topic, start, end):
    """Split full ranges; exhaust pages within one block without dropping ties.

    No cursor is moved until the entire requested range is proven complete.
    A full provider result window in one block is deliberately an error.
    """
    try:
        first = provider.logs_page(contract, topic, start, end, 1)
    except SourceIncomplete as error:
        if str(error) != "provider_query_limit" or start == end:
            raise
        middle = (start + end) // 2
        return complete_logs(provider, contract, topic, start, middle) + complete_logs(provider, contract, topic, middle + 1, end)
    if len(first) < provider.page_size:
        rows = first
    elif start < end:
        middle = (start + end) // 2
        return complete_logs(provider, contract, topic, start, middle) + complete_logs(provider, contract, topic, middle + 1, end)
    else:
        rows = list(first)
        previous = {identity(row) for row in first}
        for page in range(2, provider.max_pages + 1):
            batch = provider.logs_page(contract, topic, start, end, page)
            keys = {identity(row) for row in batch}
            if keys & previous:
                raise SourceIncomplete("provider_repeated_log_page")
            previous.update(keys)
            rows.extend(batch)
            if len(batch) < provider.page_size:
                break
        else:
            raise SourceIncomplete("provider_single_block_result_limit")
    unique = {}
    for row in rows:
        if not start <= number(row.get("blockNumber")) <= end:
            raise SourceIncomplete("log_outside_requested_range")
        if address(row.get("address")) != contract or not row.get("topics") or str(row["topics"][0]).lower() != topic:
            raise SourceIncomplete("unexpected_checkout_log")
        key = identity(row)
        if key in unique and unique[key] != row:
            raise SourceIncomplete("conflicting_duplicate_log")
        unique[key] = row
    return sorted(unique.values(), key=lambda row: (number(row["blockNumber"]), number(row["logIndex"])))


def decode_checkout(log, schema):
    if log.get("removed"):
        raise SourceIncomplete("removed_checkout_log")
    topics = [hash32(t) for t in log.get("topics", [])]
    expected_len = 3 if schema == "legacy_single" else 4
    if len(topics) != expected_len or topics[0] != TOPICS[schema]:
        raise SourceIncomplete("invalid_checkout_topics")
    wallet = topic_address(topics[1])
    data = words(log.get("data"))
    checkout_ids = [topics[2]]
    if schema == "v3":
        if len(data) < 4 or data[0] >= 2**160 or data[1] != 96 or data[3] <= 0 or len(data) != 4 + data[3]:
            raise SourceIncomplete("invalid_v3_checkout_data")
        token = "0x" + f"{data[0]:040x}"
        if token != USDT:
            raise SourceIncomplete("unsupported_checkout_currency")
        checkout_ids = data[4:]
        if len(set(checkout_ids)) != len(checkout_ids) :
            raise SourceIncomplete("invalid_v3_checkout_ids")
        amount, count, pack_id = data[2], len(checkout_ids), topics[2]
    else:
        if len(data) != 1:
            raise SourceIncomplete("invalid_legacy_checkout_data")
        # Verified legacy source creates exactly one checkout record per event.
        amount, count = data[0], 1
        pack_id = topics[2] if schema == "legacy_pack" else None
        if schema == "legacy_pack":
            checkout_ids = [topics[3]]
    return dict(wallet=wallet, pack_id=pack_id, builder=topics[3] if schema == "v3" else None,
                draw_count=count, paid_amount_raw=amount,
                checkout_ids=checkout_ids, pack_contract=address(log["address"]), tx_hash=identity(log)[0],
                log_index=identity(log)[1], block_number=number(log.get("blockNumber")),
                block_hash=hash32(log.get("blockHash")))


def verified_receipt_checkouts(provider, tx_hash, schemas):
    receipt = provider.receipt(tx_hash)
    if hash32(receipt.get("transactionHash")) != tx_hash or number(receipt.get("status")) != 1:
        raise SourceIncomplete("unsuccessful_or_wrong_receipt")
    block_number, block_hash = number(receipt.get("blockNumber")), hash32(receipt.get("blockHash"))
    canonical = provider.block(block_number)
    if block_hash != canonical["hash"]:
        raise SourceIncomplete("receipt_reorg_mismatch")
    logs = receipt.get("logs")
    if not isinstance(logs, list):
        raise SourceIncomplete("receipt_missing_logs")
    decoded, payments, checkout_keys = {}, defaultdict(int), set()
    pending = defaultdict(list)
    seen_logs = set()
    # Read every checkout for these contracts in this transaction, even other users
    # and packs: reconcile total by actual payer+recipient rather than by tx alone.
    for log in sorted(logs, key=lambda row: number(row.get("logIndex"))):
        if (not isinstance(log, dict) or log.get("removed") or hash32(log.get("transactionHash")) != tx_hash
                or number(log.get("blockNumber")) != block_number or hash32(log.get("blockHash")) != block_hash):
            raise SourceIncomplete("inconsistent_receipt_log")
        log_key = identity(log)
        if log_key in seen_logs:
            raise SourceIncomplete("duplicate_receipt_log")
        seen_logs.add(log_key)
        contract = address(log.get("address"))
        topics = log.get("topics", [])
        if contract == USDT and topics and str(topics[0]).lower() == TRANSFER:
            if len(topics) != 3 or len(words(log.get("data"))) != 1:
                raise SourceIncomplete("invalid_usdt_transfer")
            payments[(topic_address(topics[1]), topic_address(topics[2]))] += words(log["data"])[0]
        elif contract in schemas and topics and str(topics[0]).lower() == TOPICS[schemas[contract]]:
            entry = decode_checkout(log, schemas[contract])
            if (entry["tx_hash"] != tx_hash or entry["block_number"] != block_number or entry["block_hash"] != block_hash):
                raise SourceIncomplete("inconsistent_receipt_log")
            for checkout_id in entry["checkout_ids"]:
                key = (contract, entry["pack_id"], checkout_id)
                if key in checkout_keys:
                    raise SourceIncomplete("duplicate_checkout_id")
                checkout_keys.add(key)
            entry["timestamp"] = canonical["timestamp"]
            key = (entry["tx_hash"], entry["log_index"])
            if key in decoded:
                raise SourceIncomplete("duplicate_receipt_log")
            decoded[key] = entry
            pending[(contract, entry["wallet"])].append(entry)
        elif contract in schemas and schemas[contract] == "legacy_single" and topics and str(topics[0]).lower() == LEGACY_BATCH:
            if len(topics) != 3:
                raise SourceIncomplete("invalid_legacy_batch_topics")
            payer, data = topic_address(topics[1]), words(log.get("data"))
            if len(data) != 2 or data[1] <= 0:
                raise SourceIncomplete("invalid_legacy_batch_data")
            total, count = data
            members = pending[(contract, payer)][-count:]
            if len(members) != count or any(x["paid_amount_raw"] != total // count for x in members):
                raise SourceIncomplete("legacy_batch_members_mismatch")
            # Legacy Solidity divides the payment by numOfTokens for each event.
            # Attribute its exact division remainder to the final member; the
            # aggregate success and same-tx transfer prove the actual total.
            remainder = total - sum(x["paid_amount_raw"] for x in members)
            if total and total // count == 0:
                raise SourceIncomplete("legacy_batch_dust_payment")
            if remainder:
                members[-1]["checkout_event_amount_raw"] = members[-1]["paid_amount_raw"]
                members[-1]["paid_amount_raw"] += remainder
            del pending[(contract, payer)][-count:]
    expected = defaultdict(int)
    for row in decoded.values():
        expected[(row["wallet"], row["pack_contract"])] += row["paid_amount_raw"]
    for pair, amount in expected.items():
        if amount != payments[pair]:
            raise SourceIncomplete("checkout_payment_amount_mismatch")
    return decoded


class EventLeaderboardScanner:
    def __init__(self, store, provider=None, *, confirmations=64, chunk_blocks=2000, max_chunks=100, schemas=None):
        self.store = store
        self.provider = provider or EtherscanProvider()
        self.confirmations = int(confirmations)
        self.chunk_blocks, self.max_chunks = int(chunk_blocks), int(max_chunks)
        self.schemas = dict(SCHEMAS if schemas is None else schemas)
        if self.confirmations < 1 or self.chunk_blocks < 1 or self.max_chunks < 1:
            raise ValueError("invalid scanner limits")

    def sync(self, event_id, *, now=None, usernames=None):
        now = int(time.time()) if now is None else int(now)
        definition = self.store.get_event(event_id)
        if definition.checkout_schema_version < 2:
            definition = self.store.register_event(replace(definition, checkout_schema_version=2))
        schemas = {**self.schemas, **definition.contract_schemas}
        progress = {"event_id": event_id, "revision": definition.revision, "state": "scanning", "sources": {}}
        try:
            snapshot, _, _ = self.store.read_snapshot(event_id)
            if snapshot and snapshot.get("status") == "final":
                return dict(progress, state="finalized", snapshot=snapshot)
            if definition.chain_id != 56 or definition.token_decimals != 18:
                raise SourceIncomplete("unsupported_chain_or_decimals")
            if now < definition.start_ts:
                progress["state"] = "scheduled"
                self.store.set_progress(event_id, progress)
                return progress
            for contract in definition.contracts:
                if contract in self.schemas and self.schemas[contract] != definition.contract_schemas[contract]:
                    raise SourceIncomplete("audited_schema_configuration_mismatch")
                if contract not in schemas or schemas[contract] not in TOPICS:
                    raise SourceIncomplete("unaudited_pack_contract")
            checkpoints = {contract: self.store.get_checkpoint(event_id, source_key(definition, contract))
                           for contract in definition.contracts}
            head = self.provider.head()
            bound = head - self.confirmations
            if bound < 0:
                raise SourceIncomplete("insufficient_confirmations")
            # Each persisted source cursor already records its first unscanned
            # block in this configuration revision. Resolve the start boundary
            # only while at least one source still needs its initial cursor.
            start = (self.provider.first_block_at(definition.start_ts, head)
                     if any(checkpoint is None for checkpoint in checkpoints.values()) else None)
            boundary = self.provider.block(bound)
            finalized = boundary["timestamp"] >= definition.end_ts
            if finalized:
                bound = self.provider.first_block_at(definition.end_ts, head) - 1
                boundary = self.provider.block(bound)
            progress.update(through_block=bound, through_at=definition.end_ts if finalized else boundary["timestamp"], finalized=finalized)
            if start is not None and bound < start:
                progress["state"] = "awaiting_confirmations"
                self.store.set_progress(event_id, progress)
                return progress
            receipts, used = {}, 0
            for contract in definition.contracts:
                schema, topic = schemas[contract], TOPICS[schemas[contract]]
                source = source_key(definition, contract)
                checkpoint = checkpoints[contract]
                next_block = checkpoint["next_block"] if checkpoint else start
                if checkpoint and not checkpoint.get("block_hash"):
                    raise SourceIncomplete("checkpoint_missing_reorg_anchor")
                if checkpoint:
                    if self.provider.block(next_block - 1)["hash"] != checkpoint["block_hash"]:
                        raise SourceIncomplete("checkpoint_reorg_mismatch")
                while next_block <= bound and used < self.max_chunks:
                    end = min(bound, next_block + self.chunk_blocks - 1)
                    logs = complete_logs(self.provider, contract, topic, next_block, end)
                    checkouts = []
                    for log in logs:
                        tx_hash, index = identity(log)
                        if tx_hash not in receipts:
                            receipts[tx_hash] = verified_receipt_checkouts(self.provider, tx_hash, schemas)
                        row = receipts[tx_hash].get((tx_hash, index))
                        candidate = decode_checkout(log, schema)
                        if (row is None or candidate["paid_amount_raw"] != row.get("checkout_event_amount_raw", row["paid_amount_raw"])
                                or any(candidate[k] != row[k] for k in ("wallet", "draw_count", "pack_id", "builder", "block_hash", "block_number", "checkout_ids"))):
                            raise SourceIncomplete("indexed_log_receipt_mismatch")
                        if definition.start_ts <= row["timestamp"] < definition.end_ts and row["paid_amount_raw"] > 0:
                            checkouts.append(dict(row, chain_id=definition.chain_id))
                    self.store.commit_chunk(event_id, source, checkpoint["next_block"] if checkpoint else None,
                                            end + 1, checkouts, block_hash=self.provider.block(end)["hash"], expected_revision=definition.revision)
                    checkpoint = {"next_block": end + 1}
                    next_block, used = end + 1, used + 1
                progress["sources"][source] = {"next_block": next_block, "complete": next_block > bound}
                self.store.set_progress(event_id, progress)
            complete = all(x["complete"] for x in progress["sources"].values())
            progress["state"] = "finalized" if complete and finalized else "active" if complete else "catching_up"
            self.store.set_progress(event_id, progress)
            if complete:
                # Every configured contract has reached the identical confirmed bound.
                progress["snapshot"] = self.store.publish_snapshot(event_id, bound,
                    definition.end_ts if finalized else boundary["timestamp"], finalized=finalized, usernames=usernames, expected_revision=definition.revision)
            return progress
        except Exception as error:
            # Error class/code only: provider URLs and credentials cannot leak to API/logs.
            code = str(error) if isinstance(error, SourceIncomplete) else type(error).__name__
            progress.update(state="stale", error=code)
            if isinstance(error, SourceIncomplete) and error.diagnostic:
                progress["error_details"] = error.diagnostic
            self.store.record_failure(event_id, code, progress=progress)
            return progress
