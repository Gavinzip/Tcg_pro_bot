"""Allowlisted provider diagnostics without URLs, keys, bodies or exception text."""
import re
import socket
import ssl
import urllib.error
from .transport import ReusedConnectionClosed


def classify_rejected_payload(result):
    """Classify only an explicit API rejection; never return upstream text."""
    message = result[:4096].lower() if isinstance(result, str) else ''
    if 'daily' in message and any(word in message for word in ('limit', 'credit', 'quota')):
        return 'provider_quota_exhausted'
    if 'rate limit' in message:
        return 'provider_rate_limit'
    if 'query timeout' in message or 'result window' in message or 'more than' in message:
        return 'provider_query_limit'
    return 'provider_rejected_request'


def classify_failure(error, *, stage):
    if isinstance(error, ReusedConnectionClosed):
        return 'provider_stale_connection'
    if isinstance(error, urllib.error.HTTPError):
        status = error.code
        return f"provider_http_{status}" if type(status) is int and 100 <= status <= 599 else "provider_http_error"
    reason = error.reason if isinstance(error, urllib.error.URLError) else error
    if isinstance(reason, ssl.SSLCertVerificationError):
        return "provider_tls_verification_failure"
    if isinstance(reason, ssl.SSLError):
        return "provider_tls_failure"
    if isinstance(reason, socket.gaierror):
        return "provider_dns_failure"
    if isinstance(reason, TimeoutError):
        return "provider_timeout"
    if isinstance(error, ValueError):
        return "provider_invalid_json" if stage == "read_json" else "provider_invalid_request"
    return "provider_connection_failure"


def safe_diagnostic(module, action, params, elapsed_ms, error, *, stage):
    result = {
        "module": module if module in {"proxy", "block", "logs"} else "unknown",
        "action": action if action in {"eth_blockNumber", "eth_getBlockByNumber", "eth_getTransactionReceipt",
                                       "eth_getStorageAt", "eth_getCode", "getblocknobytime", "getLogs"} else "unknown",
        "stage": stage,
        "elapsed_ms": max(0, int(elapsed_ms)),
    }
    if isinstance(error, urllib.error.HTTPError) and type(error.code) is int and 100 <= error.code <= 599:
        result["http_status"] = error.code
    for source, target in (("fromBlock", "from_block"), ("toBlock", "to_block"), ("page", "page"), ("tag", "block")):
        value = params.get(source)
        if type(value) is int and value >= 0:
            result[target] = value
        elif isinstance(value, str) and re.fullmatch(r"(?:0x[0-9a-fA-F]{1,64}|[0-9]{1,78})", value):
            result[target] = int(value, 16 if value.startswith("0x") else 10)
    return result
