"""Closed response evidence: structure, safe numbers and bounded body fingerprints.

Unknown upstream text, URLs, headers and JSON keys are never serialized. Bodies
exist only while reading/parsing; at most 64 KiB contributes to a fingerprint.
"""
import hashlib
import re
from urllib.parse import urlsplit
from .integers import parse_integer

MISSING = object()
MAX_BODY_SAMPLE = 65536
MAX_NUMBER = 10**15
KINDS = {'missing', 'null', 'bool', 'int', 'float', 'string', 'array', 'object', 'other'}
MODULES = {'block', 'proxy', 'logs', 'unknown'}
ACTIONS = {'getblocknobytime', 'eth_blockNumber', 'eth_getBlockByNumber',
           'eth_getTransactionReceipt', 'eth_getStorageAt', 'eth_getCode', 'getLogs', 'unknown'}
MESSAGES = {'missing', 'ok', 'notok', 'no_records_found', 'other'}
RESULT_MESSAGES = {'missing', 'rate_limit', 'daily_quota', 'invalid_api_key', 'query_limit',
                   'no_closest_block', 'invalid_timestamp', 'historical_state_unavailable', 'other'}
INTEGER_ISSUES = {'not_applicable', 'not_inspected', 'missing', 'null', 'wrong_type', 'empty',
                  'invalid_integer', 'negative_integer', 'valid'}


def kind(value):
    if value is MISSING:
        return 'missing'
    if value is None:
        return 'null'
    return {bool: 'bool', int: 'int', float: 'float', str: 'string',
            list: 'array', dict: 'object'}.get(type(value), 'other')


def result_message(value):
    if value is MISSING:
        return 'missing'
    if not isinstance(value, str):
        return 'other'
    text = value[:4096].lower()
    if 'historical' in text and 'state' in text and any(s in text for s in ('unavailable', 'not available')):
        return 'historical_state_unavailable'
    if 'daily' in text and any(s in text for s in ('limit', 'credit', 'quota')):
        return 'daily_quota'
    if 'rate limit' in text:
        return 'rate_limit'
    if 'invalid api key' in text or 'missing api key' in text:
        return 'invalid_api_key'
    if any(s in text for s in ('query timeout', 'result window', 'more than')):
        return 'query_limit'
    if 'no closest block' in text:
        return 'no_closest_block'
    if 'invalid timestamp' in text:
        return 'invalid_timestamp'
    return 'other'


def response_metadata(status=None, content_type=None, payload=None, *, complete=True):
    media = content_type.split(';')[0].strip().lower() if isinstance(content_type, str) else ''
    result = {'http_status': status if type(status) is int and 100 <= status <= 599 else 0,
              'content_type': 'json' if media == 'application/json' or media.endswith('+json') else
                              'html' if media == 'text/html' else 'text' if media.startswith('text/') else
                              'other' if media else 'missing',
              'body_complete': payload is not None and complete,
              'body_bytes': 0, 'body_sample_bytes': 0, 'digest_scope': 'none'}
    if isinstance(payload, str):
        payload = payload.encode('utf-8', errors='replace')
    if isinstance(payload, bytes):
        sample = memoryview(payload)[:MAX_BODY_SAMPLE]
        result.update(body_bytes=len(payload), body_sample_bytes=len(sample),
                      body_digest=hashlib.sha256(sample).hexdigest(),
                      digest_scope='full' if complete and len(payload) <= MAX_BODY_SAMPLE else 'prefix')
    return result


def safe_request(params):
    result = {}
    for source, target in [('timestamp', 'timestamp'), ('fromBlock', 'from_block'),
                           ('toBlock', 'to_block'), ('page', 'page'), ('tag', 'block')]:
        value = params.get(source)
        if type(value) is int and 0 <= value <= MAX_NUMBER:
            result[target] = value
        elif isinstance(value, str) and re.fullmatch(r'(?:0x[0-9a-fA-F]{1,12}|[0-9]{1,16})', value):
            number = int(value, 16 if value.startswith('0x') else 10)
            if number <= MAX_NUMBER:
                result[target] = number
    if 'closest' in params:
        result['closest'] = params['closest'] if params['closest'] in ('before', 'after') else 'other'
    return result


def observation(module, action, params, data, metadata, *, stage, attempt, max_attempts, endpoint,
                request_ref, elapsed_ms, json_state='decoded'):
    value = data.get('result', MISSING) if isinstance(data, dict) else MISSING
    status = data.get('status', MISSING) if isinstance(data, dict) else MISSING
    message = data.get('message', MISSING) if isinstance(data, dict) else MISSING
    normalized = message.strip().lower() if isinstance(message, str) and len(message) <= 64 else None
    try:
        parsed = urlsplit(endpoint)
        source = (parsed.scheme + '://' + (parsed.hostname or '') + parsed.path).encode()
        source_kind = 'etherscan_v2' if parsed.hostname == 'api.etherscan.io' and parsed.path == '/v2/api' else 'custom'
    except ValueError:
        source, source_kind = b'invalid-endpoint', 'unknown'
    result = dict(metadata, module=module if module in MODULES else 'unknown',
        action=action if action in ACTIONS else 'unknown', stage=stage,
        attempt=attempt, max_attempts=max_attempts, request_ref=request_ref,
        duration_ms=max(0, min(MAX_NUMBER, int(elapsed_ms))), endpoint=source_kind,
        endpoint_ref=hashlib.sha256(source).hexdigest(), request=safe_request(params),
        json_state=json_state, envelope_type=kind(data), api_status_type=kind(status),
        api_status='success' if type(status) is str and status == '1' else
                   'rejected' if type(status) is str and status == '0' else 'missing' if status is MISSING else 'other',
        message='missing' if message is MISSING else {'ok': 'ok', 'notok': 'notok', 'no records found': 'no_records_found'}.get(normalized, 'other'),
        result_type=kind(value), result_message=result_message(value), integer_issue='not_applicable')
    if isinstance(value, (str, list, dict)):
        result['result_length'] = min(len(value), MAX_NUMBER)
    if action == 'getblocknobytime':
        if value is MISSING:
            issue = 'missing'
        elif value is None:
            issue = 'null'
        elif type(value) not in (int, str):
            issue = 'wrong_type'
        elif isinstance(value, str) and not value.strip():
            issue = 'empty'
        elif isinstance(value, str) and len(value) > 256:
            issue = 'not_inspected'
        else:
            try:
                number = parse_integer(value)
                issue = 'negative_integer' if number < 0 else 'valid'
                if abs(number) <= MAX_NUMBER:
                    result['hint_number'] = number
            except (ValueError, TypeError, OverflowError):
                issue = 'invalid_integer'
        result['integer_issue'] = issue
        if isinstance(value, str):
            result['result_whitespace'] = value != value.strip()
    error = data.get('error') if isinstance(data, dict) else None
    if isinstance(error, dict):
        if type(error.get('code')) is int and abs(error['code']) <= MAX_NUMBER:
            result['rpc_error_code'] = error['code']
        result['rpc_error_message'] = result_message(error.get('message', MISSING))
    return result


def valid_observation(value):
    required = {'module', 'action', 'stage', 'attempt', 'max_attempts', 'request_ref', 'duration_ms', 'endpoint', 'endpoint_ref', 'request',
                'json_state', 'http_status', 'content_type', 'body_complete', 'body_bytes', 'body_sample_bytes',
                'digest_scope', 'envelope_type', 'api_status_type', 'api_status', 'message',
                'result_type', 'result_message', 'integer_issue'}
    optional = {'body_digest', 'result_length', 'hint_number', 'result_whitespace', 'rpc_error_code', 'rpc_error_message'}
    if not isinstance(value, dict) or not required <= value.keys() or value.keys() - required - optional:
        return False
    enums = {'module': MODULES, 'action': ACTIONS, 'stage': {'connect', 'read_json', 'response_validate', 'complete'},
        'endpoint': {'etherscan_v2', 'custom', 'unknown'}, 'json_state': {'decoded', 'invalid', 'unparsed', 'unavailable'},
        'content_type': {'json', 'html', 'text', 'other', 'missing'}, 'digest_scope': {'none', 'full', 'prefix'},
        'envelope_type': KINDS, 'api_status_type': KINDS, 'api_status': {'success', 'rejected', 'missing', 'other'},
        'message': MESSAGES, 'result_type': KINDS, 'result_message': RESULT_MESSAGES,
        'integer_issue': INTEGER_ISSUES, 'rpc_error_message': RESULT_MESSAGES}
    if any(key in value and (not isinstance(value[key], str) or value[key] not in options) for key, options in enums.items()):
        return False
    for key in ('endpoint_ref', 'body_digest'):
        if key in value and (not isinstance(value[key], str) or not re.fullmatch(r'[a-f0-9]{64}', value[key])):
            return False
    if not isinstance(value['request_ref'], str) or not re.fullmatch(r'[a-f0-9]{32}', value['request_ref']):
        return False
    for key in ('attempt', 'max_attempts', 'duration_ms', 'http_status', 'body_bytes', 'body_sample_bytes', 'result_length'):
        if key in value and (type(value[key]) is not int or not 0 <= value[key] <= MAX_NUMBER):
            return False
    if not 1 <= value['attempt'] <= value['max_attempts'] <= 3 or value['body_sample_bytes'] > MAX_BODY_SAMPLE:
        return False
    if value['http_status'] != 0 and not 100 <= value['http_status'] <= 599:
        return False
    for key in ('hint_number', 'rpc_error_code'):
        if key in value and (type(value[key]) is not int or abs(value[key]) > MAX_NUMBER):
            return False
    for key in ('body_complete', 'result_whitespace'):
        if key in value and type(value[key]) is not bool:
            return False
    request = value['request']
    if not isinstance(request, dict) or request.keys() - {'timestamp', 'closest', 'from_block', 'to_block', 'page', 'block'}:
        return False
    return all((isinstance(v, str) and v in {'before', 'after', 'other'}) if k == 'closest' else
               (type(v) is int and 0 <= v <= MAX_NUMBER) for k, v in request.items())
