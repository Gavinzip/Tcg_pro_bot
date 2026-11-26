"""Exact integer decoding shared by chain validation and safe observations."""
import re

_DECIMAL = re.compile(r'[+-]?[0-9]+\Z')
_HEX = re.compile(r'0[xX][0-9a-fA-F]+\Z')


def parse_integer(value):
    """Accept integers and explicit ASCII decimal/hex strings, never floats.

    Outer whitespace and hex prefix case are harmless compatibility variants.
    Python-only underscores and Unicode digits are not wire number syntax.
    Sign/range policy belongs to the caller so negatives retain their reason.
    """
    if type(value) is int:
        return value
    if not isinstance(value, str):
        raise ValueError('invalid_integer')
    text = value.strip()
    if _HEX.fullmatch(text):
        return int(text, 16)
    if _DECIMAL.fullmatch(text):
        return int(text, 10)
    raise ValueError('invalid_integer')
