# BNB campaign live-query retention

Only event `bnb-renaiss-2026` is selected by default. The separate finalized
single-day event `2026-11-20` is not selected. Newly published non-final query
versions are kept for at most 3600 seconds and 120 versions. The latest published
version remains readable during outages. Finalized results are never expired.

Runtime settings: `EVENT_LEADERBOARD_QUERY_RETENTION_EVENTS` (comma-separated;
empty explicitly disables), `EVENT_LEADERBOARD_QUERY_RETENTION_SECONDS` (minimum
60), and `EVENT_LEADERBOARD_QUERY_MAX_VERSIONS` (minimum 2).

The publisher removes a managed expired snapshot and its rank/builder/checkout
query rows plus its query watermark in one transaction. It never removes the
purchase ledger, insertion-order evidence, verified wallet aliases, event
definitions, scan checkpoints, prefix seeds or final results. Other events are
unchanged. Expired or unavailable pins on selected events return HTTP 409 with
`snapshot_unavailable`; clients must fetch latest without a cursor.

Legacy query versions are not silently physically deleted at deployment. Their
old pins expire under the policy, but removing the existing backlog needs an
exact reviewed allowlist after a successful independent offsite restore test.
SQLite may reuse freed pages; logical cleanup is not proof the billed file size
shrunk. This local change has not deleted production data. Once released, it
expires newly managed query versions only; no VACUUM, table rebuild or restore
is part of publication.
