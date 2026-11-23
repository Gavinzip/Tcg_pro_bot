"""Time-based overlap planning and atomic reconciliation, separate from forward cursors."""


class ReplayIncomplete(RuntimeError):
    """Stable public error code; never includes provider payloads or credentials."""


def replay_confirmed_window(*, store, provider, definition, contract, checkpoint,
                            head, bound, seconds, read_verified):
    end = checkpoint['next_block'] - 1
    if end > bound:
        raise ReplayIncomplete("confirmed_head_below_checkpoint")
    # Use the persisted cursor's chain timestamp, not wall-clock time. After an
    # outage this still rechecks the last completed five minutes before resuming.
    through = provider.block(end)
    since = max(definition.start_ts, through['timestamp'] - seconds)
    start = provider.first_block_at(since, head)
    if start > end:
        return {'from_block': start, 'through_block': end, 'inserted': 0}
    known = store.checkouts_in_range(definition.event_id, contract, start, end,
                                    expected_revision=definition.revision)
    rows = read_verified(start, end, {(r['tx_hash'], r['log_index']): r for r in known})
    key = lambda row: (row['tx_hash'], row['log_index'])
    if not {key(row) for row in known}.issubset({key(row) for row in rows}):
        raise ReplayIncomplete("overlap_missing_known_checkout")
    inserted = store.reconcile_confirmed_range(definition.event_id, contract, checkpoint, start, end, rows)
    return {'from_block': start, 'through_block': end, 'from_at': since,
            'through_at': through['timestamp'], 'inserted': inserted, 'verified_checkouts': len(rows)}
