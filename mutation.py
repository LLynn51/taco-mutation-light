"""Generation allocation and patch reconstruction; form checks copied from v112."""

import ast
import random
from adapters.form import verify
from state import digest


def applicable(source):
    tree = ast.parse(source)
    nodes = list(ast.walk(tree))
    result = set()
    if any(
        isinstance(n, ast.Compare)
        and any(
            isinstance(o, (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE))
            for o in n.ops
        )
        for n in nodes
    ):
        result.add("ROR")
    if any(
        isinstance(
            n, (ast.BoolOp, ast.UnaryOp, ast.If, ast.While, ast.IfExp, ast.Compare)
        )
        for n in nodes
    ):
        result.add("LCR")
    if any(
        isinstance(n, (ast.BinOp, ast.AugAssign))
        and isinstance(n.op, (ast.Add, ast.Sub, ast.Mult, ast.FloorDiv, ast.Mod))
        for n in nodes
    ):
        result.add("AOR")
    if any(isinstance(n, ast.Constant) and type(n.value) in (int, bool) for n in nodes):
        result.add("CLR")
    if any(
        isinstance(n, (ast.Subscript, ast.Slice))
        or isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "range"
        for n in nodes
    ):
        result.add("IBR")
    if any(
        isinstance(
            n,
            (
                ast.Assign,
                ast.AugAssign,
                ast.Return,
                ast.Break,
                ast.Continue,
                ast.Raise,
                ast.Assert,
                ast.Delete,
                ast.Expr,
            ),
        )
        for n in nodes
    ):
        result.add("SDL")
    return sorted(result)


def allocated(task, config):
    families = [
        f
        for f in config["generation"]["families"]
        if f in applicable(task["original_code"])
    ]
    if config["generation"]["strategy"] == "per_task_random":
        rng = random.Random(str(config["data"]["seed"]) + task["task_id"])
        rng.shuffle(families)
        families = families[: config["generation"]["families_per_task"]]
    return families


def rebuild(original, family, reply):
    if reply.get("skip") is True:
        return None
    old, new, occurrence = (
        reply.get("old_fragment"),
        reply.get("new_fragment"),
        reply.get("occurrence", 0),
    )
    if (
        not isinstance(old, str)
        or not old
        or not isinstance(new, str)
        or type(occurrence) is not int
        or occurrence < 0
    ):
        raise ValueError("invalid_patch_fields")
    offsets = []
    pos = 0
    while True:
        pos = original.find(old, pos)
        if pos < 0:
            break
        offsets.append(pos)
        pos += len(old)
    if occurrence >= len(offsets):
        raise ValueError("patch_fragment_not_found")
    pos = offsets[occurrence]
    mutant = original[:pos] + new + original[pos + len(old) :]
    span = [len(original[:pos].encode()), len(original[: pos + len(old)].encode())]
    mutation = {
        "family": family,
        "rule_id": reply.get("rule_id"),
        "old_fragment": old,
        "new_fragment": new,
        "byte_span": span,
    }
    valid, reason, provenance = verify(original, mutant, mutation)
    if not valid:
        raise ValueError(reason or "form_invalid")
    before = ast.parse(original)
    after = ast.parse(mutant)

    def changes(a, b, path="root"):
        if isinstance(a, ast.AST) and isinstance(b, ast.AST) and type(a) is type(b):
            return [
                c
                for k, v in ast.iter_fields(a)
                for c in changes(v, getattr(b, k), path + "." + k)
            ]
        if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
            return [
                c
                for i, (x, y) in enumerate(zip(a, b))
                for c in changes(x, y, path + f"[{i}]")
            ]

        def show(x):
            return (
                ast.dump(x, include_attributes=False)
                if isinstance(x, ast.AST)
                else repr(x)
            )

        return (
            []
            if show(a) == show(b)
            else [{"path": path, "before": show(a), "after": show(b)}]
        )

    form = {
        **mutation,
        "line": original[:pos].count("\n") + 1,
        "ast_changes": changes(before, after),
    }
    return {
        "code": mutant,
        "form": form,
        "code_key": digest(ast.dump(after, include_attributes=False)),
    }
