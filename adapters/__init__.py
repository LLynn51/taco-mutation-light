"""Small shared helpers for adapters copied from motherdata/v112."""
import json

class ContractError(ValueError):
    pass

def canonical_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()

def exact_fields(value, required, optional=()):
    if not isinstance(value, dict) or set(required) - value.keys() or value.keys() - set(required) - set(optional):
        raise ContractError("invalid adapter fields")
