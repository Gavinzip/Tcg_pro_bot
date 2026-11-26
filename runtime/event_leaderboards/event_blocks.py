"""Provider log block metadata checked against canonical range endpoints.

Interior hashes/times are trusted provider data, not independently fetched headers.
Missing metadata never triggers a receipt/header/provider fallback.
"""
from bisect import bisect_left
import re

from .errors import SourceIncomplete


def _uint(value, code):
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and re.fullmatch(r'(?:0x[0-9a-fA-F]+|[0-9]+)', value):
        return int(value, 16 if value.startswith('0x') else 10)
    raise SourceIncomplete(code)


class EventBlockEvidence:
    def __init__(self, provider):
        self.provider = provider
        self.ranges = []
        self.anchors, self.rows, self.heights = set(), {}, []

    def _observe(self, row):
        height = row['number']
        previous = self.rows.get(height)
        if previous is not None:
            if previous != row:
                code = 'event_block_anchor_mismatch' if height in self.anchors else 'inconsistent_event_block'
                raise SourceIncomplete(code)
            return previous
        position = bisect_left(self.heights, height)
        if ((position and self.rows[self.heights[position - 1]]['timestamp'] > row['timestamp'])
                or (position < len(self.heights) and self.rows[self.heights[position]]['timestamp'] < row['timestamp'])):
            raise SourceIncomplete('nonmonotonic_event_timestamp')
        self.heights.insert(position, height)
        self.rows[height] = row
        return row

    def cover(self, start, end):
        for height in dict.fromkeys((start, end)):
            row = self.provider.block(height)
            self.anchors.add(height)
            self._observe(row)
        self.ranges.append((start, end, self.rows[start]['timestamp'], self.rows[end]['timestamp']))

    def from_log(self, log):
        height = _uint(log.get('blockNumber'), 'invalid_event_block_number')
        if 'timeStamp' not in log:
            raise SourceIncomplete('missing_event_timestamp')
        stamp = _uint(log['timeStamp'], 'invalid_event_timestamp')
        block_hash = log.get('blockHash')
        if not isinstance(block_hash, str) or not re.fullmatch(r'0x[0-9a-fA-F]{64}', block_hash):
            raise SourceIncomplete('invalid_event_block_hash')
        ranges = [row for row in self.ranges if row[0] <= height <= row[1]]
        if not ranges:
            raise SourceIncomplete('event_outside_anchored_range')
        if any(not low <= stamp <= high for _, _, low, high in ranges):
            raise SourceIncomplete('event_timestamp_outside_anchors')
        return self._observe(dict(number=height, hash=block_hash.lower(), timestamp=stamp))

    def block(self, height):
        if height not in self.rows:
            raise SourceIncomplete('missing_verified_event_block')
        return self.rows[height]

    def summary(self):
        return dict(mode='provider_logs_with_range_anchors',
                    ranges=[dict(from_block=start, through_block=end) for start, end, _, _ in self.ranges],
                    canonical_anchor_blocks=sorted(self.anchors))
