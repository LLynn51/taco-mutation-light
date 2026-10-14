"""Rules for interpreter metadata and sequence sampling, without evaluating code."""
import ast
import types

RUNTIME_NAMES = frozenset({"__builtins__", "__loader__", "__spec__", "__cached__",
                           "__file__", "__name__", "__package__", "__doc__"})


def runtime_omissions(source):
    tree = ast.parse(source)
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    for node in ast.walk(tree):
        if isinstance(node, ast.arg):
            names.add(node.arg)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.alias):
            names.add(node.asname or node.name.split(".")[0])
        elif isinstance(node, (ast.ExceptHandler, ast.MatchAs, ast.MatchStar)) and node.name:
            names.add(node.name)
        elif isinstance(node, ast.MatchMapping) and node.rest:
            names.add(node.rest)
    # Dynamic namespace access makes the provenance ambiguous. Retain it.
    if names & {"globals", "locals", "vars", "eval", "exec"}:
        return frozenset()
    return RUNTIME_NAMES - names


def symbol(value):
    cls = type(value)
    if cls in (types.FunctionType, types.BuiltinFunctionType, types.MethodType):
        return {"type": "symbol", "kind": "callable", "name": object.__getattribute__(value, "__name__")}
    if cls is types.ModuleType:
        name = object.__getattribute__(value, "__dict__").get("__name__")
        return {"type": "symbol", "kind": "module", "name": name[:256] if type(name) is str else None}
    if cls is type:
        return {"type": "symbol", "kind": "class", "name": type.__getattribute__(value, "__qualname__")}
    return None


def is_large(value, limits):
    return type(value) in (list, tuple) and len(value) > limits.get("large_sequence_items", limits["max_items"])
