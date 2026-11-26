"""Share a complete bounded getLogs result between replay and forward consumers."""

from .integers import parse_integer


class LogWindow:
    def __init__(self, read, *, forward_start, forward_end, combine=False, guard=None):
        self.read, self.forward_start, self.forward_end = read, forward_start, forward_end
        self.combine, self.guard = combine, guard
        self.cached = None

    @staticmethod
    def _height(row):
        return parse_integer(row['blockNumber'])

    def __call__(self, start, end):
        if self.cached is not None:
            low, high, rows = self.cached
            if low <= start <= end <= high:
                return [r for r in rows if start <= self._height(r) <= end]
        high = end
        if self.combine and end == self.forward_start - 1 and self.forward_end >= self.forward_start:
            high = max(end, self.forward_end)
        if self.guard is not None:
            self.guard.cover(start, high)
        # read() must exhaust/split full pages before any consumer advances.
        rows = self.read(start, high)
        self.cached = start, high, rows
        return [r for r in rows if start <= self._height(r) <= end]
