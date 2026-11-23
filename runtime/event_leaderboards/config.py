"""Validated finite or ongoing activity definitions. Every revision is retained."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
import re

from runtime.pack_contracts import DEFAULT_PACK_CONTRACTS, VRF_V3_CONTRACT, VRF_V3_CHECKOUT_TOPIC

WALLET_RE = re.compile(r"0x[0-9a-f]{40}\Z")
DEFAULT_DB_PATH = "/data/events/leaderboards.sqlite3"
DEFAULT_CONFIG_PATH = "/data/events/events.json"
DEFAULT_CONTRACT_SCHEMAS = {DEFAULT_PACK_CONTRACTS[0]: "legacy_pack",
                            **{item: "legacy_single" for item in DEFAULT_PACK_CONTRACTS[1:4]},
                            VRF_V3_CONTRACT: "v3"}
AUDITED_CHECKOUT_TOPICS = {
    "legacy_pack": "0xd505514c5f9bb134a66621a7fd46a679442a1a0e45f5ad5dff0724e4b4588fed",
    "legacy_single": "0xf1ec6bca7c2c281e7c68bbddbba207b6df358f0ef65c5650b1f796d3f1224fe9",
    "v3": VRF_V3_CHECKOUT_TOPIC,
}


def source_key(definition, contract: str) -> str:
    """Audited checkout source identity shared by the scanner and publication guard."""
    contract = wallet(contract)
    if contract not in definition.contracts:
        raise ValueError("contract outside event definition")
    schema = definition.contract_schemas[contract]
    key = f"checkout-v{definition.checkout_schema_version}:{definition.chain_id}:{contract}:{schema}:{AUDITED_CHECKOUT_TOPICS[schema]}"
    return key if definition.builder is None else key + ":builder:" + definition.builder


def timestamp(value: str | int) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return int(dt.timestamp())


def iso_time(value: int) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def wallet(value: str) -> str:
    normalized = str(value).lower()
    if not WALLET_RE.fullmatch(normalized):
        raise ValueError("invalid wallet/contract address")
    return normalized


def builder_code(value: str) -> str:
    normalized = str(value).lower()
    if not re.fullmatch(r"0x[0-9a-f]{64}", normalized):
        raise ValueError("builder must be 0x followed by 64 hexadecimal characters")
    return normalized


@dataclass(frozen=True)
class EventDefinition:
    event_id: str
    start_ts: int
    end_ts: int | None
    contracts: tuple[str, ...] = DEFAULT_PACK_CONTRACTS
    chain_id: int = 56
    token_decimals: int = 18
    revision: int = 1
    config_hash: str = ""
    contract_schemas: dict[str, str] = field(default_factory=dict)
    checkout_schema_version: int = 2
    builder: str | None = None

    def __post_init__(self):
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", self.event_id):
            raise ValueError("invalid event_id")
        if (type(self.start_ts) is not int or self.start_ts < 0
                or (self.end_ts is not None and (type(self.end_ts) is not int or self.end_ts <= self.start_ts))
                or self.chain_id <= 0 or not 0 <= self.token_decimals <= 36):
            raise ValueError("invalid event definition")
        if type(self.checkout_schema_version) is not int or self.checkout_schema_version not in {1, 2}:
            raise ValueError("unsupported checkout schema version")
        contracts = tuple(sorted(set(wallet(item) for item in self.contracts)))
        if not contracts:
            raise ValueError("contracts cannot be empty")
        object.__setattr__(self, "contracts", contracts)
        schemas = {wallet(key): value for key, value in self.contract_schemas.items()}
        if any(key not in contracts or value not in {"legacy_pack", "legacy_single", "v3"} for key, value in schemas.items()):
            raise ValueError("invalid audited contract schema mapping")
        effective = {key: schemas.get(key, DEFAULT_CONTRACT_SCHEMAS.get(key)) for key in contracts}
        if any(value is None for value in effective.values()):
            raise ValueError("every contract requires an audited schema")
        object.__setattr__(self, "contract_schemas", effective)
        if self.builder is not None:
            object.__setattr__(self, "builder", builder_code(self.builder))
            if any(value != "v3" for value in effective.values()):
                raise ValueError("builder-scoped indexing requires audited V3 contracts")
        content = self.payload()
        object.__setattr__(self, "config_hash", hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest())

    def payload(self) -> dict:
        content = {key: value for key, value in asdict(self).items() if key not in {"revision", "config_hash"}}
        if self.builder is None:
            content.pop("builder")  # Preserve every existing global definition hash.
        if self.checkout_schema_version == 1:
            content.pop("checkout_schema_version")
        return content

    @classmethod
    def from_dict(cls, row: dict, *, stored=False) -> "EventDefinition":
        if "end_at" not in row and "end_ts" not in row:
            raise ValueError("explicit end_at is required; use null for an ongoing activity")
        version = row.get("checkout_schema_version", 1 if stored else 2)
        if not stored and version != 2:
            raise ValueError("new activity configuration requires checkout schema version 2")
        return cls(event_id=row["event_id"], start_ts=timestamp(row.get("start_at", row.get("start_ts"))),
                   end_ts=(None if row.get("end_at", row.get("end_ts")) is None
                           else timestamp(row.get("end_at", row.get("end_ts")))),
                   contracts=tuple(row.get("contracts", DEFAULT_PACK_CONTRACTS)),
                   chain_id=int(row.get("chain_id", 56)), token_decimals=int(row.get("token_decimals", 18)),
                   revision=int(row.get("revision", 1)), contract_schemas=row.get("contract_schemas", {}),
                   checkout_schema_version=version, builder=row.get("builder"))


def load_event_definitions(path: str | None = None) -> list[EventDefinition]:
    """Explicit missing config is an error; absent optional default selects the built-in event."""
    configured = path or os.getenv("EVENT_LEADERBOARD_CONFIG_PATH")
    resolved = configured or DEFAULT_CONFIG_PATH
    if not configured and not os.path.exists(resolved):
        return [EventDefinition.from_dict({"event_id": "2026-11-20", "start_at": "2026-11-20T00:00:00+08:00",
                                           "end_at": "2026-11-21T00:00:00+08:00"})]
    with open(resolved, encoding="utf-8") as stream:
        payload = json.load(stream)
    rows = payload.get("events") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise ValueError("event configuration must contain an events array")
    definitions = [EventDefinition.from_dict(row) for row in rows]
    if len({row.event_id for row in definitions}) != len(definitions):
        raise ValueError("duplicate event_id")
    return definitions
