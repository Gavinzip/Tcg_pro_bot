"""Regression cases for batched BBW checkout verification."""
from decimal import Decimal
import unittest
from unittest.mock import patch

from runtime import bbw_community_metrics as metrics
from runtime.bbw_checkout_events import PACK_CHECKOUT_TOPIC
from runtime.pack_contracts import VRF_V3_CONTRACT, VRF_V3_CHECKOUT_TOPIC


EARLY_CHECKOUT_TOPIC = "0xbce26dc7177b18647918a6cc5cba6dd27ba1a2e1aeb20ccc5820d11b6a705277"


class BbwCheckoutEventTests(unittest.TestCase):
    def setUp(self):
        self.wallet = "0x13bee8596881767e4d4509a0837027445c349daa"
        self.contract = metrics.LEGACY_VENDING_MACHINE
        self.cfg = metrics.OnchainConfig(
            api_url="https://example.invalid", chain_id=56, api_key="test-key",
            usdt_contract=metrics.USDT_CONTRACT, pack_contracts=metrics.DEFAULT_PACK_CONTRACTS,
            marketplace_contract="", page_size=1000, retries=1, backoff_sec=.2,
        )

    def event(self, tx_hash, *, early=False, amount=60, log_index=2):
        topics = [EARLY_CHECKOUT_TOPIC if early else metrics.LEGACY_CHECKOUT_TOPIC,
                  "0x" + self.wallet[2:].rjust(64, "0"), "0x" + "1" * 64]
        if not early:
            topics.append("0x" + "2" * 64)
        return {"address": self.contract, "topics": topics, "transactionHash": tx_hash,
                "logIndex": hex(log_index), "data": "0x" if early else f"0x{int(amount * metrics.USDT_UNIT):064x}"}

    def lookup(self, payments, events):
        def logs(cfg, **kwargs):
            self.assertIs(cfg, self.cfg)
            self.assertEqual(kwargs["contract"], self.contract)
            self.assertEqual(kwargs["indexed_wallet"], self.wallet)
            self.assertGreater(kwargs["to_block"], 0)
            return events.get(kwargs["topic0"], [])

        with (patch.object(metrics, "_fetch_event_logs_page", side_effect=logs) as query,
              patch("scripts.onchain_metrics.requests.get", side_effect=AssertionError("No per-transaction requests"))):
            result = metrics._confirmed_pack_spend(self.cfg, self.wallet, self.contract, payments)
        return result, query.call_count

    def test_real_relayer_paid_legacy_samples_are_counted_without_receipts(self):
        hashes = [
            "0x1adec519e7fc94cd66b2fd14a95607155ff6417f4b91dff0560d82dcfe433668",
            "0x15149c5c76f1ec4f69fdca25bb3683219d243597133420e83db85dd1f163a2c8",
            "0xd8e470e317601facb3ef0c159506f1776a3ad4908b14c9e96870b8e899a9e680",
        ]
        payments = {h: (60 * metrics.USDT_UNIT, 68_000_000 + i, 1_760_000_000) for i, h in enumerate(hashes)}
        result, calls = self.lookup(payments, {EARLY_CHECKOUT_TOPIC: [self.event(h, early=True) for h in hashes]})
        self.assertEqual(result, 180 * metrics.USDT_UNIT)
        self.assertEqual(calls, 2)

    def test_full_missing_175_purchases_are_resolved_in_two_batch_queries(self):
        payments = {f"0x{i:064x}": (60 * metrics.USDT_UNIT, 68_000_000 + i, 1_760_000_000) for i in range(175)}
        events = [self.event(h, early=True) for h in payments]
        result, calls = self.lookup(payments, {EARLY_CHECKOUT_TOPIC: events})
        self.assertEqual(result, 10_500 * metrics.USDT_UNIT)
        self.assertEqual(calls, 2)

    def test_early_events_use_actual_payment_and_do_not_assume_price_or_cutover(self):
        tx_hash = "0x" + "3" * 64
        amount = int(Decimal("72.5") * metrics.USDT_UNIT)
        result, _ = self.lookup({tx_hash: (amount, 100_000_000, 1_800_000_000)},
                                {EARLY_CHECKOUT_TOPIC: [self.event(tx_hash, early=True)]})
        self.assertEqual(result, amount)

    def test_mixed_versions_duplicate_logs_and_removed_events_do_not_double_count(self):
        old_hash, new_hash, removed_hash = (f"0x{i:064x}" for i in (1, 2, 3))
        payments = {h: (60 * metrics.USDT_UNIT, 68_000_000 + i, 1_760_000_000)
                    for i, h in enumerate((old_hash, new_hash, removed_hash))}
        old = self.event(old_hash, early=True)
        new = self.event(new_hash)
        removed = {**self.event(removed_hash, early=True), "removed": True}
        result, _ = self.lookup(payments, {EARLY_CHECKOUT_TOPIC: [old, old, self.event(new_hash, early=True), removed],
                                          metrics.LEGACY_CHECKOUT_TOPIC: [new, new]})
        self.assertEqual(result, 120 * metrics.USDT_UNIT)

    def test_buyback_funding_without_checkout_is_excluded(self):
        # Actual buyback tx: incoming platform funds, then vending machine pays the seller.
        tx_hash = "0x566dabcbb5f90512ea4e7132da9e9ef48cd7cb1b51599fc256a522ea7e1aa38a"
        self.wallet = "0x48fbb6eaa5f8ba3068562b7518b970b32ca7fa8e"
        amount = int(Decimal("40.5") * metrics.USDT_UNIT)
        result, _ = self.lookup({tx_hash: (amount, 67_814_189, 1_760_000_000)}, {})
        self.assertEqual(result, 0)

    def test_modern_amount_mismatch_is_still_rejected(self):
        tx_hash = "0x" + "4" * 64
        with self.assertRaisesRegex(RuntimeError, "pack_checkout_payment_mismatch"):
            self.lookup({tx_hash: (60 * metrics.USDT_UNIT, 68_000_000, 1_760_000_000)},
                        {metrics.LEGACY_CHECKOUT_TOPIC: [self.event(tx_hash, amount=59)]})

    def test_provider_failure_never_becomes_a_partial_spend(self):
        tx_hash = "0x" + "5" * 64
        with (patch.object(metrics, "_fetch_event_logs_page", side_effect=RuntimeError("provider unavailable")),
              self.assertRaisesRegex(RuntimeError, "provider unavailable")):
            metrics._confirmed_pack_spend(self.cfg, self.wallet, self.contract,
                                         {tx_hash: (60 * metrics.USDT_UNIT, 68_000_000, 1_760_000_000)})

    def test_other_vending_machine_uses_its_real_three_topic_event(self):
        self.contract = metrics.DEFAULT_PACK_CONTRACTS[1]
        tx_hash = "0x41efa812d4ad457d0ca83fbd654c8ba28e4b20da6ff46b7ebfd17c068db8f785"
        event = {"address": self.contract, "topics": [PACK_CHECKOUT_TOPIC,
                 "0x" + self.wallet[2:].rjust(64, "0"), "0x" + "1" * 64],
                 "transactionHash": tx_hash, "logIndex": "0x178", "data": f"0x{88 * metrics.USDT_UNIT:064x}"}
        amount, calls = self.lookup({tx_hash: (88 * metrics.USDT_UNIT, 86_000_000, 1_760_000_000)},
                                    {PACK_CHECKOUT_TOPIC: [event]})
        self.assertEqual(amount, 88 * metrics.USDT_UNIT)
        self.assertEqual(calls, 1)

    def test_vrf_v3_retains_currency_and_dynamic_checkout_validation(self):
        self.contract = VRF_V3_CONTRACT
        hashes = [f"0x{i:064x}" for i in (1, 2)]
        payments = {h: (60 * metrics.USDT_UNIT, 100_000_000, 1_760_000_000) for h in hashes}
        logs = []
        for tx_hash, token in zip(hashes, (metrics.USDT_CONTRACT, "0x" + "f" * 40)):
            words = (int(token, 16), 96, 60 * metrics.USDT_UNIT, 1, 102)
            logs.append({"address": self.contract, "topics": [VRF_V3_CHECKOUT_TOPIC,
                         "0x" + self.wallet[2:].rjust(64, "0"), "0x" + "1" * 64, "0x" + "0" * 64],
                         "transactionHash": tx_hash, "logIndex": "0x1",
                         "data": "0x" + "".join(f"{word:064x}" for word in words)})
        amount, calls = self.lookup(payments, {VRF_V3_CHECKOUT_TOPIC: logs})
        self.assertEqual(amount, 60 * metrics.USDT_UNIT)
        self.assertEqual(calls, 1)
