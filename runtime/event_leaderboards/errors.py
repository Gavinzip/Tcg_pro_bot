"""Sanitized scanner failures shared by evidence strategies and providers."""


class SourceIncomplete(RuntimeError):
    """Evidence is unavailable, malformed, inconsistent, or unsupported."""

    def __init__(self, code, *, diagnostic=None):
        super().__init__(code)
        self.diagnostic = diagnostic
