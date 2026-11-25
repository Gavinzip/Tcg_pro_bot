"""Confirmed, contract-wide paid checkout indexing for finite or ongoing activities.

Only audited CheckoutSuccess schemas are accepted. Legacy implementations emit one
success per checkout, including every member of a batch; V3 emits checkoutIds.
Card transfers, transaction prices and asynchronous fulfillment are never inputs.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import nullcontext
from dataclasses import replace
from itertools import groupby
import json
import http.client
import math
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from runtime.pack_contracts import DEFAULT_PACK_CONTRACTS, VRF_V3_CONTRACT, VRF_V3_CHECKOUT_TOPIC
from .config import AUDITED_CHECKOUT_TOPICS, source_key, builder_code
from .provider_errors import classify_failure, classify_rejected_payload, safe_diagnostic
from .diagnostics import CURRENT, ScanDiagnostics, phase, record_request
from .boundary import first_block_at_timestamp
from .replay import ReplayIncomplete, replay_confirmed_window
from .verification import ReceiptPrefetch
from .transport import PooledJSONHTTP
from .request_safety import RateLimitRecovery

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
        # The campaign has its own quota. Never consume the Bot's shared key.
        self.api_key = env.get("EVENT_LEADERBOARD_API_KEY", "").strip()
        if not self.api_key:
            raise SourceIncomplete("missing_chain_api_key")
        self.api_url = env.get("BSCSCAN_API_URL") or "https://api.etherscan.io/v2/api"
        self.chain_id = 56
        self.timeout = float(env.get("EVENT_LEADERBOARD_REQUEST_TIMEOUT", "15"))
        # One shared gate, including explicit retries, leaves quota headroom.
        # Reject invalid settings rather than silently running at another rate.
        try:
            request_rate = float(env.get("EVENT_LEADERBOARD_REQUESTS_PER_SECOND", "2"))
        except (TypeError, ValueError):
            raise SourceIncomplete('invalid_chain_request_rate') from None
        if not math.isfinite(request_rate) or not .1 <= request_rate <= 4:
            raise SourceIncomplete('invalid_chain_request_rate')
        self.spacing = 1 / request_rate
        self._next = 0.
        self._lock = threading.Lock()
        self._rate_recovery = RateLimitRecovery()
        self._blocks = {}
        self._block_lock = threading.Lock()
        self._block_guards = {}
        self._http = PooledJSONHTTP() if env.get('EVENT_LEADERBOARD_HTTP_POOL') == '1' else None

    def request(self, module, action, **params):
        # All listed operations are idempotent reads. Each attempt uses the same
        # source, query, timeout and shared pacing gate below.
        reads = {("logs", "getLogs"), ("proxy", "eth_blockNumber"),
                 ("proxy", "eth_getBlockByNumber"), ("proxy", "eth_getTransactionReceipt"),
                 ("block", "getblocknobytime")}
        attempts = 3 if (module, action) in reads else 1
        stale_retries = 0
        rate_retries = 0
        fresh_connection = False
        for attempt in range(1, attempts + 1):
            try:
                connection_scope = self._http.fresh_connection() if fresh_connection and self._http is not None else nullcontext()
                with connection_scope:
                    return self._request_once(module, action, **params)
            except SourceIncomplete as error:
                if error.diagnostic is not None:
                    error.diagnostic = dict(error.diagnostic, attempt=attempt, max_attempts=attempts)
                if str(error) == 'provider_rate_limit' and (module, action) in reads:
                    if not self._rate_recovery.on_refusal(can_retry=rate_retries == 0 and attempt < attempts):
                        raise
                    rate_retries += 1
                    # Waiting happens in the shared pacing gate, so every
                    # worker pauses. Its duration is counted as pacing wait.
                    record_request(action, retry=True)
                    continue
                retryable = str(error) == "provider_timeout"
                if str(error) == 'provider_stale_connection' and stale_retries == 0:
                    retryable = True
                    stale_retries += 1
                    fresh_connection = True
                if not retryable or attempt == attempts:
                    raise
                record_request(action, retry=True, retry_wait_ms=(2 ** attempt) * 1000)
                time.sleep(2 ** attempt)

    def _request_once(self, module, action, **params):
        with self._lock:
            while True:
                cooldown = self._rate_recovery.deadline()
                if cooldown is None:
                    # No network attempt is counted for work stopped locally.
                    raise SourceIncomplete('provider_rate_limit')
                delay = max(self._next, cooldown) - time.monotonic()
                if delay <= 0:
                    break
                record_request(action, pacing_wait_ms=round(delay * 1000))
                time.sleep(delay)
            self._next = time.monotonic() + self.spacing
        started, failure = time.monotonic(), None
        try:
            return self._read_response(module, action, params)
        except BaseException as error:
            failure = error
            raise
        finally:
            elapsed = max(0, round((time.monotonic() - started) * 1000))
            record_request(action, elapsed_ms=elapsed, attempted=True, error=failure is not None,
                           timeout=isinstance(failure, SourceIncomplete) and str(failure) == 'provider_timeout',
                           failure_code=str(failure) if isinstance(failure, SourceIncomplete) else
                           'unclassified' if failure is not None else None)

    def _read_response(self, module, action, params):
        query = dict(chainid=self.chain_id, module=module, action=action, apikey=self.api_key, **params)
        started, stage = time.monotonic(), "connect"
        try:
            url = self.api_url + "?" + urllib.parse.urlencode(query)
            if self._http is not None:
                # The session disables internal retries. The provider owns the
                # shared pacing gate and bounded same-source read recovery.
                def reading_response():
                    nonlocal stage
                    stage = "read_json"
                data = self._http.read_json(url, self.timeout, on_response=reading_response)
            else:
                with urllib.request.urlopen(url, timeout=self.timeout) as response:
                    stage = "read_json"
                    data = json.load(response)
        except (urllib.error.URLError, TimeoutError, OSError, ValueError, http.client.HTTPException) as error:
            raise SourceIncomplete(classify_failure(error, stage=stage), diagnostic=safe_diagnostic(
                module, action, params, (time.monotonic() - started) * 1000, error, stage=stage)) from None
        if not isinstance(data, dict) or data.get("error"):
            raise SourceIncomplete("provider_invalid_response")
        result = data.get("result")
        if module == "logs" and data.get("status") == "0" and str(data.get("message", "")).lower() == "no records found" and result == []:
            return []
        # Proxy successes normally use a JSON-RPC envelope with no status field,
        # but gateway failures use status=0/NOTOK even when HTTP is 200.
        if (module != "proxy" or 'status' in data) and data.get("status") != "1":
            raise SourceIncomplete(classify_rejected_payload(result))
        if result is None:
            raise SourceIncomplete("provider_missing_result")
        if module == 'proxy' and action == 'eth_getTransactionReceipt' and not isinstance(result, dict):
            # Validate within the measured request, before it is counted as OK.
            raise SourceIncomplete('receipt_unavailable')
        return result

    def close(self):
        if self._http is not None:
            self._http.close()

    def head(self):
        self._blocks.clear()
        self._block_guards.clear()
        return number(self.request("proxy", "eth_blockNumber"))

    def block(self, height):
        with self._block_lock:
            guard = self._block_guards.setdefault(height, threading.Lock())
        with guard:
            if height not in self._blocks:
                row = self.request("proxy", "eth_getBlockByNumber", tag=hex(height), boolean="false")
                if not isinstance(row, dict) or number(row.get("number")) != height:
                    raise SourceIncomplete("invalid_block")
                self._blocks[height] = {"number": height, "hash": hash32(row.get("hash")), "timestamp": number(row.get("timestamp"))}
            return self._blocks[height]

    def first_block_at(self, timestamp, head):
        """Resolve seconds boundary, including chains with several blocks per second."""
        with phase('boundary', head_block=head) as details:
            height = number(self.request("block", "getblocknobytime", timestamp=timestamp, closest="before"))
            details['hint_block'] = height
            if height > head:
                raise SourceIncomplete("boundary_above_head")
            cached_before = len(self._blocks)
            height = first_block_at_timestamp(self.block, timestamp, head, height,
                                              anchors=tuple(self._blocks.values()))
            details['resolved_block'] = height
            details['block_reads'] = len(self._blocks) - cached_before
            return height

    def logs_page(self, contract, topic, start, end, page, *, builder=None):
        filters = {} if builder is None else {"topic3": builder_code(builder), "topic0_3_opr": "and"}
        rows = self.request("logs", "getLogs", address=contract, topic0=topic, fromBlock=start,
                            toBlock=end, page=page, offset=self.page_size, **filters)
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows) or len(rows) > self.page_size:
            raise SourceIncomplete("invalid_logs_page")
        return rows

    def receipt(self, tx_hash):
        return self.request("proxy", "eth_getTransactionReceipt", txhash=tx_hash)


def complete_logs(provider, contract, topic, start, end, *, builder=None):
    """Split full ranges; exhaust pages within one block without dropping ties.

    No cursor is moved until the entire requested range is proven complete.
    A full provider result window in one block is deliberately an error.
    """
    builder = builder_code(builder) if builder is not None else None
    filters = {} if builder is None else {"builder": builder}
    try:
        first = provider.logs_page(contract, topic, start, end, 1, **filters)
    except SourceIncomplete as error:
        if str(error) != "provider_query_limit" or start == end:
            raise
        middle = (start + end) // 2
        return complete_logs(provider, contract, topic, start, middle, builder=builder) + complete_logs(provider, contract, topic, middle + 1, end, builder=builder)
    if len(first) < provider.page_size:
        rows = first
    elif start < end:
        middle = (start + end) // 2
        return complete_logs(provider, contract, topic, start, middle, builder=builder) + complete_logs(provider, contract, topic, middle + 1, end, builder=builder)
    else:
        rows = list(first)
        previous = {identity(row) for row in first}
        for page in range(2, provider.max_pages + 1):
            batch = provider.logs_page(contract, topic, start, end, page, **filters)
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
        if builder is not None and (len(row["topics"]) != 4 or str(row["topics"][3]).lower() != builder):
            raise SourceIncomplete("unexpected_checkout_builder")
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
    def __init__(self, store, provider=None, *, confirmations=64, chunk_blocks=2000, max_chunks=100, schemas=None,
                 replay_seconds=300, verification_workers=1):
        self.store = store
        self.provider = provider or EtherscanProvider()
        self.confirmations = int(confirmations)
        self.chunk_blocks, self.max_chunks = int(chunk_blocks), int(max_chunks)
        self.schemas = dict(SCHEMAS if schemas is None else schemas)
        self.replay_seconds = int(replay_seconds)
        self.verification_workers = int(verification_workers)
        if self.confirmations < 1 or self.chunk_blocks < 1 or self.max_chunks < 1 or self.replay_seconds < 0:
            raise ValueError("invalid scanner limits")
        if not 1 <= self.verification_workers <= 8:
            raise ValueError("invalid verification worker limit")

    def _prefetch(self, logs, receipts, schemas, known=None):
        if self.verification_workers == 1:
            return nullcontext(None)
        transactions = [identity(log)[0] for log in logs if known is None or identity(log) not in known]
        return ReceiptPrefetch(transactions, receipts,
                               lambda tx: verified_receipt_checkouts(self.provider, tx, schemas),
                               self.verification_workers)

    def _verified_log(self, definition, schemas, schema, log, receipts, known=None):
        tx_hash, index = identity(log)
        # Previously verified identical evidence can be reused. Newly discovered
        # identities still require a complete receipt, payment and canonical block.
        row = known.get((tx_hash, index)) if known is not None else None
        if row is None:
            if tx_hash not in receipts:
                with phase('receipt'):
                    receipts[tx_hash] = verified_receipt_checkouts(self.provider, tx_hash, schemas)
            row = receipts[tx_hash].get((tx_hash, index))
        candidate = decode_checkout(log, schema)
        if (row is None or candidate["paid_amount_raw"] != int(row.get("checkout_event_amount_raw", row["paid_amount_raw"]))
                or str(candidate['pack_id']) != str(row['pack_id'])
                or any(candidate[k] != row[k] for k in ("wallet", "draw_count", "builder", "block_hash", "block_number", "checkout_ids"))):
            raise SourceIncomplete("indexed_log_receipt_mismatch")
        if (row["timestamp"] >= definition.start_ts
                and (definition.end_ts is None or row["timestamp"] < definition.end_ts)
                and int(row["paid_amount_raw"]) > 0):
            return dict(row, chain_id=definition.chain_id)

    def _overlap_rows(self, definition, schemas, contract, start, end, receipts, known):
        schema = schemas[contract]
        logs = complete_logs(self.provider, contract, TOPICS[schema], start, end, builder=definition.builder)
        rows = []
        with self._prefetch(logs, receipts, schemas, known) as pending:
            for log in logs:
                if pending is not None:
                    pending.resolve(identity(log)[0])
                row = self._verified_log(definition, schemas, schema, log, receipts, known)
                if row is not None:
                    rows.append(row)
        return rows

    def sync(self, event_id, *, now=None, usernames=None):
        parent = CURRENT.get()
        with ScanDiagnostics(sink=parent.sink if parent else None,
                             run_id=parent.run_id if parent else None, event_id=event_id) as diagnostic:
            result = self._sync(event_id, now=now, usernames=usernames)
            diagnostic.outcome = 'error' if result['state'] == 'stale' else 'ok'
            return result

    def _sync(self, event_id, *, now=None, usernames=None):
        with phase('event_setup'):
            now = int(time.time()) if now is None else int(now)
            definition = self.store.get_event(event_id)
            if definition.checkout_schema_version < 2:
                definition = self.store.register_event(replace(definition, checkout_schema_version=2))
            schemas = {**self.schemas, **definition.contract_schemas}
            progress = {"event_id": event_id, "revision": definition.revision, "state": "scanning", "sources": {}}
        try:
            with phase('event_setup'):
                snapshot, _, _ = self.store.read_snapshot(event_id)
            if snapshot and snapshot.get("status") == "final":
                if not snapshot.get('checkout_evidence_supported'):
                    # Add the new immutable evidence format from the already
                    # complete, anchored ledger; never rescan a finalized event.
                    with phase('publish'):
                        snapshot = self.store.publish_snapshot(event_id,snapshot['through_block'],
                            snapshot['data_through_at'],finalized=True,usernames=usernames,expected_revision=definition.revision)
                return dict(progress, state="finalized", snapshot=snapshot)
            if definition.chain_id != 56 or definition.token_decimals != 18:
                raise SourceIncomplete("unsupported_chain_or_decimals")
            if now < definition.start_ts:
                progress["state"] = "scheduled"
                with phase('commit'):
                    self.store.set_progress(event_id, progress)
                return progress
            for contract in definition.contracts:
                if contract in self.schemas and self.schemas[contract] != definition.contract_schemas[contract]:
                    raise SourceIncomplete("audited_schema_configuration_mismatch")
                if contract not in schemas or schemas[contract] not in TOPICS:
                    raise SourceIncomplete("unaudited_pack_contract")
            with phase('checkpoint'):
                checkpoints = {contract: self.store.get_checkpoint(event_id, source_key(definition, contract))
                               for contract in definition.contracts}
            with phase('head'):
                head = self.provider.head()
                bound = head - self.confirmations
            if bound < 0:
                raise SourceIncomplete("insufficient_confirmations")
            # Each persisted source cursor already records its first unscanned
            # block in this configuration revision. Resolve the start boundary
            # only while at least one source still needs its initial cursor.
            start = (self.provider.first_block_at(definition.start_ts, head)
                     if any(checkpoint is None for checkpoint in checkpoints.values()) else None)
            with phase('head'):
                boundary = self.provider.block(bound)
            finalized = definition.end_ts is not None and boundary["timestamp"] >= definition.end_ts
            if finalized:
                bound = self.provider.first_block_at(definition.end_ts, head) - 1
                with phase('head'):
                    boundary = self.provider.block(bound)
            progress.update(through_block=bound, through_at=definition.end_ts if finalized else boundary["timestamp"], finalized=finalized)
            if start is not None and bound < start:
                progress["state"] = "awaiting_confirmations"
                with phase('commit'):
                    self.store.set_progress(event_id, progress)
                return progress
            receipts, used = {}, 0
            for source_index, contract in enumerate(definition.contracts):
                schema, topic = schemas[contract], TOPICS[schemas[contract]]
                source = source_key(definition, contract)
                checkpoint = checkpoints[contract]
                next_block = checkpoint["next_block"] if checkpoint else start
                if checkpoint and not checkpoint.get("block_hash"):
                    raise SourceIncomplete("checkpoint_missing_reorg_anchor")
                if checkpoint:
                    with phase('checkpoint', source_index=source_index):
                        if self.provider.block(next_block - 1)["hash"] != checkpoint["block_hash"]:
                            raise SourceIncomplete("checkpoint_reorg_mismatch")
                    if self.replay_seconds:
                        with phase('overlap', source_index=source_index) as overlap_details:
                            report = replay_confirmed_window(store=self.store, provider=self.provider, definition=definition,
                                contract=contract, checkpoint=checkpoint, head=head, bound=bound, seconds=self.replay_seconds,
                                read_verified=lambda start, end, known: self._overlap_rows(
                                    definition, schemas, contract, start, end, receipts, known))
                            progress.setdefault('overlap', {})[source] = report
                            overlap_details.update({k: report[k] for k in ('from_block', 'through_block', 'inserted', 'verified_checkouts') if k in report})
                while next_block <= bound and used < self.max_chunks:
                    end = min(bound, next_block + self.chunk_blocks - 1)
                    with phase('forward_logs', source_index=source_index, from_block=next_block, through_block=end) as log_details:
                        logs = complete_logs(self.provider, contract, topic, next_block, end, builder=definition.builder)
                        log_details['logs'] = len(logs)
                    # getLogs is complete for the whole range. Commit each fully
                    # verified block as an anchored prefix; a hard job deadline
                    # must not discard twenty minutes of completed receipts.
                    # Never checkpoint inside a block or before all its logs pass.
                    with self._prefetch(logs, receipts, schemas) as pending:
                        for block_number, block_logs in groupby(logs, key=lambda log: number(log["blockNumber"])):
                            checkouts = []
                            for log in block_logs:
                                with phase('forward_verify'):
                                    if pending is not None:
                                        pending.resolve(identity(log)[0])
                                    row = self._verified_log(definition, schemas, schema, log, receipts)
                                    if row is not None:
                                        checkouts.append(row)
                            with phase('commit', new_checkouts=len(checkouts)):
                                self.store.commit_chunk(event_id, source, checkpoint["next_block"] if checkpoint else None,
                                                        block_number + 1, checkouts, block_hash=self.provider.block(block_number)["hash"], expected_revision=definition.revision)
                            checkpoint = {"next_block": block_number + 1}
                            progress["sources"][source] = {"next_block": block_number + 1, "complete": block_number >= bound}
                            with phase('commit'):
                                self.store.set_progress(event_id, progress)
                    if checkpoint is None or checkpoint["next_block"] <= end:
                        with phase('commit'):
                            self.store.commit_chunk(event_id, source, checkpoint["next_block"] if checkpoint else None,
                                                    end + 1, [], block_hash=self.provider.block(end)["hash"], expected_revision=definition.revision)
                        checkpoint = {"next_block": end + 1}
                    next_block, used = end + 1, used + 1
                    if definition.end_ts is None and len(definition.contracts) == 1 and end < bound:
                        # A fully verified single-source prefix is complete as
                        # of its own watermark, even while catching up to head.
                        with phase('publish'):
                            progress['snapshot'] = self.store.publish_snapshot(event_id,end,
                                self.provider.block(end)['timestamp'],usernames=usernames,expected_revision=definition.revision)
                progress["sources"][source] = {"next_block": next_block, "complete": next_block > bound}
                with phase('commit'):
                    self.store.set_progress(event_id, progress)
            complete = all(x["complete"] for x in progress["sources"].values())
            progress["state"] = "finalized" if complete and finalized else "active" if complete else "catching_up"
            with phase('commit'):
                self.store.set_progress(event_id, progress)
            if complete:
                # Every configured contract has reached the identical confirmed bound.
                with phase('publish'):
                    progress["snapshot"] = self.store.publish_snapshot(event_id, bound,
                        definition.end_ts if finalized else boundary["timestamp"], finalized=finalized, usernames=usernames, expected_revision=definition.revision)
                with phase('commit'):
                    self.store.set_progress(event_id, progress)
            return progress
        except Exception as error:
            # Error class/code only: provider URLs and credentials cannot leak to API/logs.
            code = str(error) if isinstance(error, (SourceIncomplete, ReplayIncomplete)) else type(error).__name__
            progress.update(state="stale", error=code)
            if isinstance(error, SourceIncomplete) and error.diagnostic:
                progress["error_details"] = error.diagnostic
            with phase('commit'):
                self.store.record_failure(event_id, code, progress=progress)
            return progress
