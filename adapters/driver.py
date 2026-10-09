"""Trusted adapter. Executed ONLY inside a verified isolated container."""
import contextlib
import importlib.util
import io
import json
import os
import sys
import math
from collections import deque

class RepresentationError(Exception):
    pass

def json_value(value,seen=None):
    seen=set() if seen is None else seen
    if value is None or type(value) in (str,bool,int):return True
    if type(value) is float:return math.isfinite(value)
    if type(value) not in (list,dict):return False
    if id(value) in seen:return False
    seen.add(id(value))
    valid=all(json_value(v,seen) for v in value) if type(value) is list else all(type(k) is str and json_value(v,seen) for k,v in value.items())
    seen.remove(id(value));return valid

def node(cls,fields,value):
    result=cls.__new__(cls)
    setattr(result,fields['value'],value)
    for field in ('left','right','next'):
        if field in fields:setattr(result,fields[field],None)
    return result

def build_structure(module,value,s):
    name=s.get('class_name','TreeNode' if s['kind']=='binary_tree' else 'ListNode')
    cls=getattr(module,name,None)
    if cls is None:cls=type(name,(),{})
    if not isinstance(cls,type):raise RepresentationError()
    fields=s['fields']
    if value is None or value==[]:return None
    if type(value) is not list:raise RepresentationError()
    if s['kind']=='linked_list':
        head=tail=None
        for item in value:
            current=node(cls,fields,item)
            if tail is None:head=current
            else:setattr(tail,fields['next'],current)
            tail=current
        return head
    if value[0] is None:raise RepresentationError()
    root=node(cls,fields,value[0]);queue=deque([root]);idx=1
    while queue and idx<len(value):
        current=queue.popleft()
        for field in ('left','right'):
            if idx>=len(value):break
            item=value[idx];idx+=1
            child=None if item is None else node(cls,fields,item)
            setattr(current,fields[field],child)
            if child is not None:queue.append(child)
    if idx!=len(value):raise RepresentationError()
    return root

def serialize_structure(root,s):
    if root is None:return []
    fields=s['fields'];values=[];seen=set();queue=deque([root])
    while queue:
        current=queue.popleft()
        if current is None:
            values.append(None);continue
        if id(current) in seen or len(seen)>=100000:raise RepresentationError()
        seen.add(id(current));values.append(getattr(current,fields['value']))
        if s['kind']=='binary_tree':queue.extend((getattr(current,fields['left']),getattr(current,fields['right'])))
        else:
            nxt=getattr(current,fields['next'])
            if nxt is not None:queue.append(nxt)
    if s['kind']=='binary_tree':
        while values and values[-1] is None:values.pop()
    return values


class OutputLimit(Exception):
    pass


class OutputBudget:
    """One UTF-8 byte allowance shared by the program's two output channels."""
    def __init__(self):
        self.size = 0

    def consume(self, value):
        self.size += len(value.encode("utf-8"))
        if self.size > 1048576:
            raise OutputLimit()


class Capture(io.StringIO):
    def __init__(self, budget):
        super().__init__()
        self.budget = budget

    def write(self, value):
        self.budget.consume(value)
        return super().write(value)


def main():
    payload = json.load(sys.stdin)
    sys.stderr.write("LIGHT_READY\n")
    sys.stderr.flush()
    budget = OutputBudget()
    stdout, stderr = Capture(budget), Capture(budget)
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            if payload["entry"]["mode"] == "function":
                spec = importlib.util.spec_from_file_location("isolated_solution", "/unit/source.py")
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                entry=payload['entry']
                # Entry is already concrete. No failure-based fallback.
                owner=module.Solution() if entry.get('target','function')=='solution_method' else module
                fn = getattr(owner, entry["fn_name"])
                raw = payload["input"]
                for idx,s in entry.get('structures',{}).items():raw[int(idx)]=build_structure(module,raw[int(idx)],s)
                args=[raw[i] for i in entry['parameter_order']]
                value=fn(*args)
                channel=entry.get('output_channel','return')
                if channel=='stdout':value=stdout.getvalue()
                elif channel=='mutated_argument':value=args[entry['output_argument']]
                out=entry.get('output_structure')
                if channel=='mutated_argument' and out is None:
                    out=entry.get('structures',{}).get(str(entry['parameter_order'][entry['output_argument']]))
                if out:
                    try:value=serialize_structure(value,out)
                    except (AttributeError,TypeError,ValueError,RecursionError):raise RepresentationError() from None
            else:
                sys.stdin = io.TextIOWrapper(io.BytesIO(payload["input"].encode('utf-8')),encoding='utf-8')
                # This execution is confined to this container, never called on host.
                source = open("/unit/source.py", encoding="utf-8").read()
                try:
                    exec(compile(source, "/unit/source.py", "exec"), {"__name__": "__main__"})
                except SystemExit as exc:
                    if exc.code not in (None, 0): raise
                value = stdout.getvalue()
        result = {"status": "ok", "output": value, "program_stdout": stdout.getvalue(),
                  "program_stderr": stderr.getvalue(), "exception_class": None}
        try:
            if not json_value(value):raise RepresentationError()
            text = json.dumps(result, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError, RecursionError, RepresentationError):
            # The program returned, but its representation is unsupported.
            result = {"status": "wrapper_error", "output": None, "program_stdout": stdout.getvalue(),
                      "program_stderr": stderr.getvalue(), "exception_class": None}
            text = json.dumps(result)
        if len(text.encode("utf-8")) > 1048576:
            raise OutputLimit()
    except OutputLimit:
        result = {"status": "output_limit", "output": None, "program_stdout": "", "program_stderr": "", "exception_class": None}
    except RepresentationError:
        result = {"status": "wrapper_error", "output": None, "program_stdout": stdout.getvalue(), "program_stderr": stderr.getvalue(), "exception_class": None}
    except BaseException as exc:
        result = {"status": "program_exception", "output": None, "program_stdout": stdout.getvalue(),
                  "program_stderr": stderr.getvalue(),
                  "exception_class": type(exc).__name__}
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
