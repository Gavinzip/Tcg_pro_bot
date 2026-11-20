# Paid activity chain evidence

This records read-only validation on BNB Smart Chain (chain ID 56) on 2026-11-20.
No transactions were broadcast. Diagnostic requests were limited to two starts
per second. Secrets were read into the process from an ignored local file and
are absent from this document.

## Audited contract registry

The scanner covers these explicit proxies. It does not claim to discover future
machines. Add a new address and its audited `contract_schemas` entry to the event
configuration; this changes `config_hash` and starts a new ledger revision.

| Proxy | Current verified implementation | Schema |
| --- | --- | --- |
| `0xaab5f5fa75437a6e9e7004c12c9c56cda4b4885a` | `0x9215503e1e14ce0a16dad63d144687ba79485bd7` | `legacy_pack` |
| `0x94e7732b0b2e7c51ffd0d56580067d9c2e2b7910` | `0xed99cf25a39785e54f195365ca9afb2f14724f40` | `legacy_single` |
| `0xb2891022648c5fad3721c42c05d8d283d4d53080` | `0x808ada8da0dc2154251726377cfabdd774ff491c` | `legacy_single` |
| `0xfda4a907d23d9f24271bc47483c5b983831e325e` | `0x0ec5323aa474ea65de0e64b1f951eba32031ce50` | `legacy_single` |
| `0xd4d18607d6111c5fa2f93a4a5b2c0e28f1563f9f` | `0x3272bd8b81ebb53254b4ed1d8b67fe832e6fb2c7` | `v3` |

Sources: each proxy and implementation's verified source/ABI, retrieved with
Etherscan V2 `getsourcecode`. Review implementations at their BscScan code pages,
for example [V3 implementation](https://bscscan.com/address/0x3272bd8b81ebb53254b4ed1d8b67fe832e6fb2c7#code),
[legacy V2 implementation](https://bscscan.com/address/0xed99cf25a39785e54f195365ca9afb2f14724f40#code),
and [oldest implementation](https://bscscan.com/address/0x9215503e1e14ce0a16dad63d144687ba79485bd7#code).
Proxy upgrades require a fresh audit; the registry is explicit, not an assertion
that every possible future schema is supported.

### Quantity and payment semantics

* `legacy_pack`: `CheckoutSuccess(address caller,bytes32 packId,bytes32
  checkoutMessageHash,uint256 amount)`. Source transfers the authorized funds,
  calls `_checkout(packId,checkoutId,buyer)`, then emits one success for that
  checkout. Quantity one is the verified event's semantics, not a missing-field
  default. Pack ID is retained.
* `legacy_single`: `CheckoutSuccess(address user,bytes32 checkoutId,uint256
  amount)`. Single and batch implementations store a checkout record and emit
  one success per individual checkout. Batch functions iterate `numOfTokens`,
  deriving and emitting each individual checkout ID, then emit one
  `BatchCheckoutSuccess(address,bytes32,uint256 amount,uint256 numOfTokens)`.
  The aggregate event is not counted again. Solidity integer division can leave
  a remainder between aggregate payment and individual event amounts; the
  scanner validates aggregate membership/quantity/amount and assigns that exact
  remainder to the final member for accurate raw payment accounting. Legacy
  per-machine events do not expose a shared pack ID, so that field is null.
* `v3`: `CheckoutSuccess(address user,bytes32 packId,bytes32 builder,address
  token,uint256[] checkoutIds,uint256 totalAmount)`. The explicit nonempty,
  unique checkout ID array determines quantity. Current verified source starts
  IDs at one (`startId = next == 0 ? 1 : next`); the general schema decoder does
  not require positivity, allowing an explicitly audited compatible schema
  whose IDs begin at zero.
* A successful receipt, canonical confirmed block, exact receipt log identity
  and same-transaction USDT `Transfer` from the event user to the machine prove
  payment. Totals reconcile per payer and recipient across every checkout in
  the transaction, supporting several users, packs and logs. USDT amounts use
  integer base units with exactly 18 decimals. Positive paid events count; zero
  price/free events do not. Unsupported currency, missing payment, unknown
  schema, malformed quantity or inconsistent receipt evidence fails the chunk.
* Mint/card transfer logs are never used; asynchronous delivery does not shift
  checkout activity into a later event period.

## Real checkout checked against today's window

Requested window: `2026-11-20T00:00:00+08:00` inclusive through
`2026-11-21T00:00:00+08:00` exclusive (UTC seconds 1795104000–1795190400).
The first block at the start boundary was **134531505**. At the first diagnostic
head **134536991** (UTC seconds 1795106470), the first log page through head−64
contained 81 V3 checkouts and zero checkout events for each of the four legacy
machines. This page check alone is not a final activity ranking.

The following actual checkout was decoded and reconciled to its receipt:

| Field | Chain evidence |
| --- | --- |
| Transaction | [0xb6e53912a924e84bd610d439b7a6a5240aa4103a7d7a9d9f8758e47cecc577cf](https://bscscan.com/tx/0xb6e53912a924e84bd610d439b7a6a5240aa4103a7d7a9d9f8758e47cecc577cf) |
| Block / log | 134531577 / 137 |
| Block hash | `0x7f78eacf3b8edcc995bc834af9348bf33a02d929c6f7aa845580c88065650696` |
| Checkout time | `2026-11-20T00:00:32+08:00` (1795104032) |
| User and actual payer | `0x0b5787e81f7e00ae85c3e68345f2f69ee43ba641` |
| Pack ID | `0xcf11308cc7c642554a781b039a63a542c2f20b36f904b5b6981d48a2f76a5f90` |
| Draw quantity | **5**, decoded from `checkoutIds` |
| Paid USDT raw | **140000000000000000000** (140 USDT) |

The successful receipt and USDT transfer were read directly from the chain
provider and compared with this checkout. Unit fixtures do not establish live
coverage of every participant or machine; the bounded live sync below does that
only through its explicit completed source bound.

## Completeness and finalization

Queries scan each configured contract's checkout topic over finite block ranges.
A full result page causes range splitting; a single full block uses page
continuation. A full provider result window in one block is an explicit failure,
not an assumed end. Provider transport failures stop immediately. Etherscan's
[logs endpoint](https://docs.etherscan.io/api-reference/endpoint/getlogs-address-topics)
documents address/topic/block range queries and pagination.

Successful chunks atomically store evidence and their next block plus the
previous block hash. Subsequent runs verify that hash before proceeding.
Conflicting identities, moved blocks or configuration revisions cannot quietly
reuse checkpoints. Publication independently requires anchored checkpoints for
all configured sources through the identical bound. An error retains the last
snapshot and marks stale progress; a timeout never becomes zero activity.

The default confirmation delay is 64 blocks. Only block timestamps satisfying
`start <= timestamp < end` count. After a confirmed block at or beyond the end
exists, the scanner closes the exclusive end boundary, publishes a final
snapshot and stops further chain requests for that event. This is a confirmation
policy, not an absolute promise against deeper chain reorganizations.

Known limitations: explicit contract registry; upstream log/receipt availability;
legacy events cannot identify a shared pack beyond their per-machine address;
migration aliases depend on the protected application's helper-derived map.
Unverified flat maps or heuristic rows are rejected. Verified rows retain helper
transaction/block evidence and a SHA-256 hash of the exact map file. Username
availability does not control wallet inclusion.

## Completed bounded live synchronization

A complete cold synchronization published snapshot
`697b468a13bc42708ffd404c1ed74d95` after **all five** configured contract sources
reached block **134537384**, timestamp `2026-11-20T00:44:06+08:00`.
The retained validation database is `.scratch/event-live-validation.sqlite3`
(ignored local evidence, not a production database or committed asset).

* 88 verified positive paid checkout logs, **248 draws**, **43 distinct on-chain
  payer wallets** before applying production migration aliases.
* Exact total payment **8844000000000000000000** USDT base units = **8844 USDT**.
* The example wallet above had **11 draws / 308 USDT** across its checkouts in
  this completed bound; its first transaction contributed 5 draws / 140 USDT.
* Every source checkpoint was complete at the same bound; health reported
  `stale=false`. The event was still active, so these are interim results.
* This local cold scan did not import Railway's protected migration map or
  populate display names. The production CLI imports the provenance-bearing
  helper map and the existing official username resource before publishing.
  These local 43 wallets must not be presented as a verified canonical-account
  count after migration merging.

Current-code fixture coverage includes boundaries, free exclusions, several
users/packs in one transaction, exact raw payment, legacy batch rounding,
malformed quantity, zero-valued checkout IDs with explicit quantity, full-page
splitting and same-block continuation, result-cap failure, source failures,
checkpoint rollback, all-source publication gating, reorg anchors, immutable
revision scopes, audited schema conflicts, provenance rejection and recovery of
existing event definitions. These tests complement the live evidence; they do
not replace it.
