"""Shared Renaiss pack contracts for Profile and ranking accounting."""

VRF_V3_CONTRACT = "0xd4d18607d6111c5fa2f93a4a5b2c0e28f1563f9f"
VRF_V3_CHECKOUT_TOPIC = "0xd316cbf877f67321c8b602d7d52e44a8db95db232743952de19656b67ccbb419"
DEFAULT_PACK_CONTRACTS = (
    "0xaab5f5fa75437a6e9e7004c12c9c56cda4b4885a",
    "0x94e7732b0b2e7c51ffd0d56580067d9c2e2b7910",
    "0xb2891022648c5fad3721c42c05d8d283d4d53080",
    "0xfda4a907d23d9f24271bc47483c5b983831e325e",
    VRF_V3_CONTRACT,
)

# Official catalogue on https://www.renaiss.xyz/gacha/pandora-248 (2026-09-06).
# Identity is keyed by pack ID; price alone cannot identify a shared-contract pack.
VRF_V3_PACK_NAMES = {
    "0x347ba3e1d2875a0e9e09a378369f6105a6a5f3d2a5c0a51b6837da4beb52dd7d": "PANDORA 248",
    "0xcf11308cc7c642554a781b039a63a542c2f20b36f904b5b6981d48a2f76a5f90": "PANDORA 28",
    "0x4de1e3c158c8630faa2db4e6c5250933188c4990ba30640a44a41eb6732d257d": "PANDORA 48",
    "0xfe35d4de033fa6ffd14fb4e6a74ffef5ea2fc3ae6666a78d33c4bfb860e056fe": "PANDORA 88",
    "0xe95098663e26ee400b1e3e41b735f38bb211c0a7ccfa3c7990bf990affd3db67": "Genesis Pack",
    "0x9f8035be36d451cff9e74ed15e8942ab5792ca23f74099d0af1d754bfee91186": "Niu Lai Pack",
}
