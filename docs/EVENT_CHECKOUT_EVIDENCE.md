# Checkout evidence and restart progress

Local implementation on `codex/campaign-rewards-scan`, based on production `fb10bf9`. Not uploaded or deployed yet.

`GET /v1/leaderboards/{event_id}/checkouts` reuses website-specific read authentication. It returns only the paid checkout ledger frozen atomically with a complete snapshot. Parameters: `snapshot_id`, `builder`, `cursor`, `limit` (1–1000, default 1000). No arbitrary wallet or source override, chain request, social mutation or secret is exposed.

The response includes the normal snapshot metadata, `checkout_evidence_supported=true`, `evidence_type=confirmed_paid_checkout`, `total_checkouts`, `next_cursor`, and per-checkout rows. Each row retains pack ID, builder, draw quantity, exact string payment, block time, transaction/log identity and original buyer; `wallet`, `aliases` and username use the snapshot's verified migration mapping. First pages also include global frozen identities, so campaign bonuses for migrated wallets can be merged before ranking, even outside the selected builder's paid participants.

Clients must retain snapshot and builder across every page, validate the expected event dates and evidence marker, reject stale/incomplete/duplicate rows and verify total counts. Aggregated draw_count/paid_amount cannot substitute for this contract. No `/search` result or `eligible_pack_ids` assumption is needed to prove an eligible first paid draw.

Initial incomplete scans still return 202/503 without rows. Historical snapshots without evidence return 409 `checkout_evidence_unavailable`; they are never retroactively edited. When an already final snapshot predates this format, the scanner publishes a new immutable snapshot from the already complete anchored ledger without querying the chain again.

The scanner still fetches complete logs for the configured chunk with the same provider, pacing, retry limits and job deadline. It now atomically commits fully verified **whole blocks** inside that range. A timeout on a later block preserves all completed prefix blocks; an incomplete block never advances the checkpoint. All configured sources must still reach the common confirmed bound before any snapshot is published. This fixes repeated loss of verified work at a hard process deadline without weakening payment or reorg checks.

Validation: 75 focused regressions passed, including whole-block deadline recovery, incomplete-block rejection, final-format upgrade with zero chain calls, authenticated normal HTTP checkout reads, frozen aliases, stable pagination and old-snapshot rejection. Existing v1 leaderboard/search clients retain their draw-count semantics.
