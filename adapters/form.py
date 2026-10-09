# Extracted unchanged from src/form.py; SHA256 556b90efa26bc2406170f93679d73017b5fff7f9637285bf6e58d373649dcf33. No legacy imports.
"""Conservative single-rule mutation verification; never executes candidate code."""
import ast
import copy
import hashlib
import io
import tokenize

RULES = {
    "ROR_EQ_NE": (ast.Eq, ast.NotEq), "ROR_LT_LE": (ast.Lt, ast.LtE),
    "ROR_GT_GE": (ast.Gt, ast.GtE), "LCR_AND_OR": (ast.And, ast.Or),
    "AOR_ADD_SUB": (ast.Add, ast.Sub), "AOR_MUL_FDIV": (ast.Mult, ast.FloorDiv),
    "AOR_FDIV_MOD": (ast.FloorDiv, ast.Mod),
}
FAMILIES = {rule: rule.split("_", 1)[0] for rule in (*RULES, "CLR_INT_UP", "CLR_INT_DOWN",
            "CLR_BOOL_FLIP", "LCR_NOT_INSERT", "LCR_NOT_DELETE", "IBR_INDEX_UP", "IBR_INDEX_DOWN",
            "IBR_STOP_UP", "IBR_STOP_DOWN", "SDL_SIMPLE")}
SYMBOLS = {ast.Eq: "==", ast.NotEq: "!=", ast.Lt: "<", ast.LtE: "<=",
           ast.Gt: ">", ast.GtE: ">=", ast.And: "and", ast.Or: "or",
           ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.FloorDiv: "//", ast.Mod: "%"}


def _dump(tree):
    if isinstance(tree, list):
        return repr([_dump(x) for x in tree])
    return ast.dump(tree, include_attributes=False)


def _integer(node):
    if isinstance(node, ast.Constant) and type(node.value) is int:
        return node.value
    if (isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd))
            and isinstance(node.operand, ast.Constant) and type(node.operand.value) is int):
        return -node.operand.value if isinstance(node.op, ast.USub) else node.operand.value
    return None


def _compatible(old, new, rule):
    if rule in RULES:
        a, b = RULES[rule]
        if rule.startswith("ROR"):
            return (isinstance(old, ast.Compare) and isinstance(new, ast.Compare)
                    and len(old.ops) == len(new.ops) and
                    sum(type(x) is not type(y) for x, y in zip(old.ops, new.ops)) == 1 and
                    all(type(x) is type(y) or {type(x), type(y)} == {a, b}
                        for x, y in zip(old.ops, new.ops)) and
                    _dump(old.left) == _dump(new.left) and
                    _dump(old.comparators) == _dump(new.comparators))
        if rule == "LCR_AND_OR":
            return (isinstance(old, ast.BoolOp) and isinstance(new, ast.BoolOp)
                    and len(old.values) == len(new.values) >= 2
                    and {type(old.op), type(new.op)} == {a, b}
                    and _dump(old.values) == _dump(new.values))
        return (isinstance(old, (ast.BinOp, ast.AugAssign)) and type(old) is type(new)
                and {type(old.op), type(new.op)} == {a, b}
                and all(_dump(getattr(old, field)) == _dump(getattr(new, field))
                        for field in (("left", "right") if isinstance(old, ast.BinOp) else ("target", "value"))))
    if rule in ("CLR_INT_UP", "CLR_INT_DOWN", "CLR_BOOL_FLIP"):
        if rule == "CLR_BOOL_FLIP":
            return (isinstance(old, ast.Constant) and isinstance(new, ast.Constant)
                    and type(old.value) is bool and type(new.value) is bool and old.value != new.value)
        d = 1 if rule.endswith("UP") else -1
        return _integer(old) is not None and _integer(new) == _integer(old) + d
    if rule in ("LCR_NOT_INSERT", "LCR_NOT_DELETE"):
        if rule.endswith("INSERT"):
            return (isinstance(old, ast.expr) and isinstance(new, ast.UnaryOp) and isinstance(new.op, ast.Not)
                and _dump(new.operand) == _dump(old))
        return (isinstance(old, ast.UnaryOp) and isinstance(old.op, ast.Not)
                and _dump(old.operand) == _dump(new))
    if rule.startswith("IBR_"):
        d = 1 if rule.endswith("UP") else -1
        return (isinstance(old, ast.expr) and _integer(old) is None and isinstance(new, ast.BinOp)
                and isinstance(new.op, ast.Add if d == 1 else ast.Sub)
                and _dump(new.left) == _dump(old)
                and isinstance(new.right, ast.Constant) and type(new.right.value) is int
                and new.right.value == 1)
    return False


def _compare(a, b, rule, parent=None, field=None):
    """Return count of allowed changes, or None on any other change."""
    if isinstance(a, ast.AST) and isinstance(b, ast.AST):
        if _compatible(a, b, rule):
            if rule.startswith("IBR_INDEX") and not (isinstance(parent, ast.Subscript) and field == "slice"):
                return None
            if rule.startswith("IBR_STOP") and not (
                    isinstance(parent, ast.Slice) and field == "upper" or
                    isinstance(parent, ast.Call) and field == "args" and
                    isinstance(parent.func, ast.Name) and parent.func.id == "range" and
                    1 <= len(parent.args) <= 3 and a is parent.args[0 if len(parent.args) == 1 else 1]):
                return None
            return 1
        if rule.startswith("CLR_INT") and (_integer(a) is not None or _integer(b) is not None):
            # A signed literal is one site: do not reinterpret -1 -> -2 as +1 on its magnitude.
            return 0 if _dump(a) == _dump(b) else None
        if type(a) is not type(b):
            return None
        total = 0
        for key, av in ast.iter_fields(a):
            n = _compare(av, getattr(b, key), rule, a, key)
            if n is None:
                return None
            total += n
        return total
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return None
        total = 0
        for x, y in zip(a, b):
            n = _compare(x, y, rule, parent, field)
            if n is None:
                return None
            total += n
        return total
    return 0 if type(a) is type(b) and a == b else None


def verify(original, mutant, mutation):
    """Return (valid, reason, provenance). Unknown syntax/context is rejected conservatively."""
    rule = mutation.get("rule_id")
    old, new = mutation.get("old_fragment"), mutation.get("new_fragment")
    if (not all(isinstance(x, str) for x in (old, new, rule)) or not old or original == mutant
            or rule not in FAMILIES or mutation.get("family") != FAMILIES[rule]):
        return False, "form_invalid", None
    declared_span = mutation.get("byte_span")
    if declared_span is not None:
        raw = original.encode("utf-8")
        if (not isinstance(declared_span, list) or len(declared_span) != 2
                or any(type(x) is not int for x in declared_span)
                or not 0 <= declared_span[0] < declared_span[1] <= len(raw)
                or raw[declared_span[0]:declared_span[1]] != old.encode("utf-8")
                or raw[:declared_span[0]] + new.encode("utf-8") + raw[declared_span[1]:] != mutant.encode("utf-8")):
            return False, "form_invalid", None
    elif original.count(old) != 1 or original.replace(old, new, 1) != mutant:
        return False, "form_invalid", None
    try:
        a, b = ast.parse(original, feature_version=(3, 11)), ast.parse(mutant, feature_version=(3, 11))
        # Compilation is a syntax check only; never execute the code here.
        compile(a, "original.py", "exec")
        compile(b, "mutant.py", "exec")
        comments = lambda source: [t.string for t in tokenize.generate_tokens(io.StringIO(source).readline)
                                   if t.type == tokenize.COMMENT]
        if comments(original) != comments(mutant):
            return False, "form_invalid", None
    except SyntaxError:
        return False, "form_invalid", None
    signatures = lambda tree: [(type(n).__name__, n.name, _dump(n.args)) for n in ast.walk(tree)
                               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    if signatures(a) != signatures(b):
        return False, "interface_changed", None
    if rule == "SDL_SIMPLE":
        allowed = (ast.Assign, ast.AnnAssign, ast.AugAssign, ast.Break, ast.Continue,
                   ast.Expr, ast.Return, ast.Raise, ast.Assert, ast.Delete)
        match = False
        for node in ast.walk(a):
            for field, items in ast.iter_fields(node):
                if not isinstance(items, list):
                    continue
                for index, stmt in enumerate(items):
                    if not isinstance(stmt, allowed):
                        continue
                    if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
                        continue
                    trial = copy.deepcopy(a)
                    parent = next((n for n in ast.walk(trial) if type(n) is type(node) and
                                   getattr(n, "lineno", None) == getattr(node, "lineno", None) and
                                   getattr(n, "col_offset", None) == getattr(node, "col_offset", None)), None)
                    if parent is None:
                        continue
                    target = getattr(parent, field)
                    if len(target) == 1:
                        target[index] = ast.Pass()
                    else:
                        del target[index]
                    match |= _dump(trial) == _dump(b)
        count = 1 if match else None
    else:
        count = _compare(a, b, rule)
    if count != 1:
        return False, "form_invalid", None
    offset = declared_span[0] if declared_span is not None else original.encode().find(old.encode())
    prefix = 0
    while prefix < min(len(original), len(mutant)) and original[prefix] == mutant[prefix]:
        prefix += 1
    suffix = 0
    while (suffix < min(len(original), len(mutant)) - prefix
           and original[len(original)-suffix-1] == mutant[len(mutant)-suffix-1]):
        suffix += 1
    edit_span = [len(original[:prefix].encode()), len(original[:len(original)-suffix].encode())]
    return True, None, {"rule_id": rule, "byte_span": [offset, offset + len(old.encode())],
                       "edit_byte_span": edit_span,
                        "original_sha256": hashlib.sha256(original.encode()).hexdigest(),
                        "mutant_sha256": hashlib.sha256(mutant.encode()).hexdigest()}
