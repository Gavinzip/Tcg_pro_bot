"""Exact timestamp lower bound on canonical blocks with nondecreasing seconds."""


def first_block_at_timestamp(block_at, timestamp, head, hint, *, anchors=()):
    """Use current-scan canonical anchors, then find the exact timestamp boundary.

    Return head + 1 when the requested timestamp is beyond the current head.
    Every comparison uses the same provider's canonical block timestamps.
    Two interpolation probes can reduce I/O on regular timestamps; binary
    convergence bounds the cost even for plateaus or highly irregular intervals.
    """
    points = {row['number']: row['timestamp'] for row in anchors if 0 <= row['number'] <= head}
    points[hint] = block_at(hint)['timestamp']
    below = [height for height, stamp in points.items() if stamp < timestamp]
    above = [height for height, stamp in points.items() if stamp >= timestamp]
    low, high = max(below) if below else None, min(above) if above else None
    if high is None:
        origin, step = low, 1
        while True:
            if low == head:
                return head + 1
            high = min(head, origin + step)
            points[high] = block_at(high)['timestamp']
            if points[high] >= timestamp:
                break
            low, step = high, step * 2
    elif low is None:
        origin, step = high, 1
        while True:
            if high == 0:
                return 0
            low = max(0, origin - step)
            points[low] = block_at(low)['timestamp']
            if points[low] < timestamp:
                break
            high, step = low, step * 2
    # Invariant: low.timestamp < timestamp <= high.timestamp.
    if low >= high:
        raise ValueError('nonmonotonic canonical timestamps')
    predictions = 0
    while high - low > 1:
        predict = predictions < 2
        if predict:
            middle = low + (timestamp - points[low]) * (high - low) // (points[high] - points[low])
            middle = max(low + 1, min(high - 1, middle))
            predictions += 1
        else:
            middle = (low + high) // 2
        points[middle] = block_at(middle)['timestamp']
        if points[middle] < timestamp:
            low = middle
        else:
            high = middle
            if predict and high - low > 1:
                # Confirm the predecessor rather than assuming an interpolated
                # block is the first one in a same-second group.
                points[high - 1] = block_at(high - 1)['timestamp']
                if points[high - 1] < timestamp:
                    return high
                high -= 1
    return high
