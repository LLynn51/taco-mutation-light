"""Copied explicit conversions from motherdata/v112/migration.py; no SPEC extraction."""
from copy import deepcopy
import json
from . import ContractError, canonical_bytes

def lines(value,terminal):
    if type(terminal) is not bool or type(value) is not list or any(type(x) is not str or '\n' in x or '\r' in x for x in value):
        raise ContractError('invalid_reviewed_line_conversion')
    return '\n'.join(value)+('\n' if terminal else '')

def transform(raw,rule, *, terminal=None):
    if rule=='as_stored':return deepcopy(raw)
    if rule=='json_value':
        if not isinstance(raw,str):raise ContractError('json_value_requires_text')
        return json.loads(raw)
    if rule=='json_lines_to_args':
        if not isinstance(raw,str):raise ContractError('json_lines_requires_text')
        return [json.loads(line) for line in raw.splitlines() if line.strip()]
    if rule=='single_wrapped':
        if type(raw) is not list or len(raw)!=1:raise ContractError('answer_not_single_wrapped')
        return deepcopy(raw[0])
    if rule=='lines_to_text':return lines(raw,terminal)
    if rule=='text_to_number':
        if type(raw) is not str:raise ContractError('number_conversion_requires_text')
        value=json.loads(raw)
        if type(value) not in (int,float):raise ContractError('not_explicit_number')
        canonical_bytes(value);return value
    if rule=='number_to_text':
        if type(raw) not in (int,float):raise ContractError('text_conversion_requires_number')
        return canonical_bytes(raw).decode()
    raise ContractError('unknown_representation_rule')

