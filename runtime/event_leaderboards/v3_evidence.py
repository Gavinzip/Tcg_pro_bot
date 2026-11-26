"""Version-pinned V3 payment semantics, never a fallback for missing receipts.

The audited implementation transfers via Permit2 before its sole CheckoutSuccess
emit. This strategy trusts that implementation instead of independently summing
USDT Transfer logs. An unknown implementation or any in-range upgrade stops it.
"""
import hashlib
import re

from runtime.pack_contracts import VRF_V3_CONTRACT
from .errors import SourceIncomplete
from .event_blocks import EventBlockEvidence

AUDITED_IMPLEMENTATION = '0x3272bd8b81ebb53254b4ed1d8b67fe832e6fb2c7'
AUDITED_SINCE = 115246746
# SHA-256 of the same 17,626 runtime bytes whose audited Ethereum Keccak-256 is
# 0646d66925eb42b5284bce70741df7921ca2942caca101467d3ebd97b1861ca3.
# SHA-256 avoids adding a cryptography dependency; it is NOT SHA3/Keccak.
AUDITED_CODE_SHA256 = '42cbf5853b9746ce648ba0bfe21923b351d021e55c2c9d4c777232433d6f880c'
IMPLEMENTATION_SLOT = '0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc'
UPGRADED = '0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b'


class V3EventEvidence:
    def __init__(self, provider, through_block, upgrade_logs):
        self.provider, self.through_block, self.upgrade_logs = provider, through_block, upgrade_logs
        self.from_block = None
        self.checkout_ids = {}
        self.transaction_blocks = {}
        self.blocks = EventBlockEvidence(provider)

    def cover(self, start, end):
        if self.from_block is not None and self.from_block <= start <= end <= self.through_block:
            self.blocks.cover(start, end)
            return
        if start <= AUDITED_SINCE or not start <= end <= self.through_block:
            raise SourceIncomplete('v3_unaudited_block_range')
        # The endpoint state plus a complete absence of Upgraded events below
        # fixes the audited implementation for this range. Do not ask a pruned
        # provider for the old starting state or fall back to another strategy.
        value = self.provider.request('proxy', 'eth_getStorageAt', address=VRF_V3_CONTRACT,
                                      position=IMPLEMENTATION_SLOT, tag=hex(self.through_block))
        if not isinstance(value, str) or not re.fullmatch(r'0x[0-9a-fA-F]{64}', value):
            raise SourceIncomplete('v3_invalid_implementation_slot')
        if value.lower() != '0x' + '0' * 24 + AUDITED_IMPLEMENTATION[2:]:
            raise SourceIncomplete('v3_implementation_mismatch')
        code = self.provider.request('proxy', 'eth_getCode', address=AUDITED_IMPLEMENTATION,
                                     tag=hex(self.through_block))
        if not isinstance(code, str) or not re.fullmatch(r'0x(?:[0-9a-fA-F]{2})+', code):
            raise SourceIncomplete('v3_invalid_implementation_code')
        if hashlib.sha256(bytes.fromhex(code[2:])).hexdigest() != AUDITED_CODE_SHA256:
            raise SourceIncomplete('v3_implementation_code_mismatch')
        if self.upgrade_logs(start, self.through_block):
            # Reject even an upgrade back to the pinned version: an endpoint
            # storage read alone cannot prove the implementation used inside.
            raise SourceIncomplete('v3_upgrade_in_scan_range')
        self.from_block = start
        self.blocks.cover(start, end)

    def validate(self, row, block):
        # Interior metadata comes from the provider log; only the enclosing
        # range's endpoints have independently fetched canonical headers.
        if self.from_block is None or not self.from_block <= row['block_number'] <= self.through_block:
            raise SourceIncomplete('v3_unguarded_checkout')
        if row['block_hash'] != block['hash']:
            raise SourceIncomplete('checkout_block_mismatch')
        self.validate_identity(row)
        return dict(row, timestamp=block['timestamp'])

    def validate_identity(self, row):
        location = row['block_number'], row['block_hash']
        tx = row['tx_hash']
        if tx in self.transaction_blocks and self.transaction_blocks[tx] != location:
            raise SourceIncomplete('transaction_block_mismatch')
        self.transaction_blocks[tx] = location
        ids = row['checkout_ids']
        if (row['wallet'] == '0x' + '0' * 40 or ids[0] < 1
                or row['paid_amount_raw'] % len(ids)
                or any(b != a + 1 for a, b in zip(ids, ids[1:]))):
            raise SourceIncomplete('invalid_audited_v3_checkout')
        identity = row['tx_hash'], row['log_index']
        for checkout_id in ids:
            key = row['pack_id'], checkout_id
            if key in self.checkout_ids and self.checkout_ids[key] != identity:
                raise SourceIncomplete('duplicate_checkout_id')
            self.checkout_ids[key] = identity

    def summary(self):
        return dict(mode='audited_v3_event', implementation=AUDITED_IMPLEMENTATION,
                    runtime_sha256=AUDITED_CODE_SHA256, from_block=self.from_block,
                    through_block=self.through_block, block_evidence=self.blocks.summary())
