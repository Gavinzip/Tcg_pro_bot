"""Exact timestamp lower bound on canonical blocks with nondecreasing seconds."""


def first_block_at_timestamp(block_at, timestamp, head, hint):
    """Bracket an inaccurate hint exponentially, then bisect the exact boundary.

    Return head + 1 when the requested timestamp is beyond the current head.
    Every comparison uses the same provider's canonical block timestamps.
    """
    if block_at(hint)['timestamp'] < timestamp:
        low, step = hint, 1
        while True:
            if low == head:
                return head + 1
            high = min(head, hint + step)
            if block_at(high)['timestamp'] >= timestamp:
                break
            low, step = high, step * 2
    else:
        high, step = hint, 1
        while True:
            if high == 0:
                return 0
            low = max(0, hint - step)
            if block_at(low)['timestamp'] < timestamp:
                break
            high, step = low, step * 2
    # Invariant: low.timestamp < timestamp <= high.timestamp.
    while high - low > 1:
        middle = (low + high) // 2
        if block_at(middle)['timestamp'] < timestamp:
            low = middle
        else:
            high = middle
    return high
