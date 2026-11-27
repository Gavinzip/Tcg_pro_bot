"""Bounded omission evidence; no payloads, addresses, transaction hashes or credentials."""
import hashlib
import json
import re

SAMPLE_LIMIT = 8
MAX_NUMBER = 10**15
NUMBERS = {'from_block', 'through_block', 'known_count', 'window_count',
           'verified_count', 'missing_count', 'missing_in_window_count'}
QUERY_NUMBERS = {'query_from_block', 'query_through_block', 'query_count', 'missing_in_query_count'}
DIGESTS = {'known_digest', 'window_digest', 'verified_digest', 'missing_digest'}


def identity_digest(keys):
    canonical = json.dumps(sorted(keys), separators=(',', ':'), ensure_ascii=True).encode()
    return hashlib.sha256(canonical).hexdigest()


def overlap_comparison(start, end, known, window, verified, query=None):
    """Inputs are already-validated identities; fingerprints preserve exact set semantics."""
    missing = set(known) - set(verified)
    if not missing:
        return None
    query_keys = query[2] if query is not None else None
    result = {
        'from_block': start, 'through_block': end,
        'known_count': len(known), 'window_count': len(window), 'verified_count': len(verified),
        'missing_count': len(missing), 'missing_in_window_count': len(missing - set(window)),
        'known_digest': identity_digest(known), 'window_digest': identity_digest(window),
        'verified_digest': identity_digest(verified), 'missing_digest': identity_digest(missing),
        'query_available': query is not None,
        'samples': [dict(identity_ref=identity_digest([key]), block_number=known[key]['block_number'],
                         log_index=key[1], in_window=key in window,
                         **({'in_query': key in query_keys} if query is not None else {}))
                    for key in sorted(missing)[:SAMPLE_LIMIT]],
    }
    if query is not None:
        result.update(query_from_block=query[0], query_through_block=query[1],
                      query_count=len(query_keys), query_digest=identity_digest(query_keys),
                      missing_in_query_count=len(missing - set(query_keys)))
    return result


def valid_overlap_comparison(value):
    if not isinstance(value, dict) or type(value.get('query_available')) is not bool:
        return False
    query = value['query_available']
    numbers = NUMBERS | (QUERY_NUMBERS if query else set())
    digests = DIGESTS | ({'query_digest'} if query else set())
    if set(value) != numbers | digests | {'query_available', 'samples'}:
        return False
    if any(type(value[k]) is not int or not 0 <= value[k] <= MAX_NUMBER for k in numbers):
        return False
    if any(not isinstance(value[k], str) or not re.fullmatch(r'[a-f0-9]{64}', value[k]) for k in digests):
        return False
    if (value['from_block'] > value['through_block']
            or not 0 < value['missing_count'] <= value['known_count']
            or value['missing_in_window_count'] > value['missing_count']):
        return False
    if query and (not value['query_from_block'] <= value['from_block'] <= value['through_block'] <= value['query_through_block']
                  or value['missing_in_query_count'] > value['missing_count']):
        return False
    samples = value['samples']
    if not isinstance(samples, list) or len(samples) != min(SAMPLE_LIMIT, value['missing_count']):
        return False
    for sample in samples:
        if not isinstance(sample, dict) or set(sample) != {'identity_ref', 'block_number', 'log_index', 'in_window'} | ({'in_query'} if query else set()):
            return False
        if not isinstance(sample['identity_ref'], str) or not re.fullmatch(r'[a-f0-9]{64}', sample['identity_ref']):
            return False
        if any(type(sample[k]) is not int or not 0 <= sample[k] <= MAX_NUMBER for k in ('block_number', 'log_index')):
            return False
        if not value['from_block'] <= sample['block_number'] <= value['through_block']:
            return False
        if type(sample['in_window']) is not bool or (query and type(sample['in_query']) is not bool):
            return False
    return len({s['identity_ref'] for s in samples}) == len(samples)
