"""Freeze values synchronously without repr, deepcopy or user serializers.

Object IDs are snapshot-local, not lifetime identities. Unsupported values and
limits are explicit. Only exact builtins and plain instance dictionaries are read.
"""
import json
import math
import types

try:
    from .adapters.policy import symbol, is_large
    from .adapters.sequences import positions
except ImportError:  # explicit trusted modules loaded by the isolated driver
    from trace_policy import symbol, is_large
    from trace_sequences import positions


class Encoder:
    def __init__(self, limits, *, omitted_names=(), local_context=None, full=False):
        self.limits = limits
        self.seen = {}
        self.nodes = 0
        self.missing = 0
        self.omitted_names = set(omitted_names)
        self.local_context = local_context or {}
        self.full = full
        self.symbols = 0

    def absent(self, reason):
        self.missing += 1
        return {"status": "not_collected", "reason": reason}

    def value(self, value, depth=0):
        self.nodes += 1
        if self.nodes > self.limits["max_nodes"]:
            return self.absent("snapshot_node_limit")
        if depth > self.limits["max_depth"]:
            return self.absent("depth_limit")
        cls = type(value)
        description = symbol(value)
        if description is not None:
            self.symbols += 1
            return description
        if value is None:
            return {"type": "NoneType", "value": None}
        if cls in (bool, int):
            if cls is int and value.bit_length() > self.limits["max_string_bytes"] * 2:
                return self.absent("integer_size_limit")
            return {"type": cls.__name__, "value": value}
        if cls is float:
            return {"type": "float", "value": value if math.isfinite(value) else
                    "nan" if math.isnan(value) else "+inf" if value > 0 else "-inf"}
        if cls in (str, bytes):
            if len(value) > self.limits["max_string_bytes"]:
                return self.absent("string_size_limit")
            if cls is str and len(value.encode("utf-8", "surrogatepass")) > self.limits["max_string_bytes"]:
                return self.absent("string_size_limit")
            return {"type": cls.__name__, "value": value if cls is str else value.hex()}
        if cls not in (list, tuple, dict, set, frozenset):
            if cls in (type, types.FunctionType, types.BuiltinFunctionType, types.MethodType,
                       types.ModuleType, types.CodeType, types.FrameType, types.GeneratorType):
                return self.absent("opaque_runtime_object")
            # Reading __dict__ is allowed only through the builtin descriptor;
            # properties, slots and custom metaclasses are never invoked.
            if type(cls) is not type:
                return self.absent("custom_metaclass")
            descriptor = None
            for base in type.__getattribute__(cls, "__mro__"):
                namespace = type.__getattribute__(base, "__dict__")
                if "__dict__" in namespace:
                    descriptor = namespace["__dict__"]
                    break
            if type(descriptor) is not types.GetSetDescriptorType:
                return self.absent("unsupported_type")
            try:
                fields = object.__getattribute__(value, "__dict__")
            except (AttributeError, TypeError):
                return self.absent("unsupported_instance")
            if type(fields) is not dict:
                return self.absent("unsupported_instance")
            kind = "instance"
        else:
            fields, kind = value, cls.__name__
        identity = id(value)
        if identity in self.seen:
            return {"ref": self.seen[identity]}
        oid = "o" + str(len(self.seen) + 1)
        self.seen[identity] = oid
        result = {"type": kind, "object_id": oid}
        if kind == "instance":
            result["class"] = type.__getattribute__(cls, "__name__")
        count = len(fields)
        result["length"] = count
        if not self.full and is_large(value, self.limits):
            indices, windows = positions(count, self.local_context, self.limits)
            result.update(coverage="partial", selection="head_tail_local_integer_windows",
                          windows=windows, omitted_count=count - len(indices),
                          sampled_items=[{"index": i, "value": self.value(value[i], depth + 1)} for i in indices])
            if len(indices) < count:
                self.missing += 1
            return result
        limit = self.limits["max_items"]
        items = []
        iterator = fields.items() if type(fields) is dict else iter(fields)
        for index, item in enumerate(iterator):
            if index >= limit:
                result["tail"] = self.absent("container_item_limit")
                break
            if type(fields) is dict:
                key, val = item
                items.append([self.value(key, depth + 1), self.value(val, depth + 1)])
            else:
                items.append(self.value(item, depth + 1))
        result["items"] = items
        if cls in (set, frozenset):
            result["comparison"] = "iteration_order_not_semantic"
        return result

    def snapshot(self, local_values, detail=None, include_detail=False):
        self.local_context = local_values
        names = sorted(k for k in local_values if type(k) is str and k not in self.omitted_names)
        result = {"locals": {}, "ignored_runtime_names": sorted(self.omitted_names & local_values.keys())}
        used = 0
        for name in names:
            if used >= self.limits["max_snapshot_bytes"] or self.nodes >= self.limits["max_nodes"]:
                result["remaining_locals"] = self.absent("snapshot_size_limit")
                break
            item = self.value(local_values[name])
            size = len(json.dumps([name, item], ensure_ascii=True).encode())
            if used + size > self.limits["max_snapshot_bytes"]:
                result["locals"][name] = self.absent("snapshot_size_limit")
                result["remaining_locals"] = self.absent("snapshot_size_limit")
                break
            result["locals"][name] = item
            used += size
        if include_detail:
            result["detail"] = self.value(detail)
        result["value_gaps"] = self.missing
        result["symbol_summaries"] = self.symbols
        result["object_id_scope"] = "this_snapshot_only"
        return result
