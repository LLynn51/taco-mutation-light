"""Fixed data-only interface validation and static entry freezing. No target execution."""
import ast
from copy import deepcopy
from . import ContractError, exact_fields, canonical_bytes

def structure(s):
    exact_fields(s,('kind','encoding','fields'),('class_name',))
    needed={'binary_tree':('level_order',{'value','left','right'}),'linked_list':('values',{'value','next'})}
    if s.get('kind') not in needed:raise ContractError('unsupported_structure')
    encoding,fields=needed[s['kind']]
    if s['encoding']!=encoding or set(s['fields'])!=fields or any(type(v) is not str or not v.isidentifier() for v in s['fields'].values()):
        raise ContractError('invalid_structure_fields_or_encoding')
    if len(set(s['fields'].values()))!=len(fields):raise ContractError('aliased_node_fields')
    if 'class_name' in s and (type(s['class_name']) is not str or not s['class_name'].isidentifier()):raise ContractError('invalid_node_class')
    return deepcopy(s)

def validate_entry(e):
    exact_fields(e,('mode','fn_name','parameter_order'),('target','output_channel','output_argument','structures','output_structure'))
    if e.get('target','function') not in ('function','solution_method'):raise ContractError('freeze_concrete_target_required')
    if type(e['fn_name']) is not str or not e['fn_name'].isidentifier():raise ContractError('invalid_function_name')
    order=e['parameter_order']
    if type(order) is not list or any(type(x) is not int for x in order) or sorted(order)!=list(range(len(order))):raise ContractError('invalid_parameter_order')
    channel=e.get('output_channel','return')
    if channel not in ('return','stdout','mutated_argument'):raise ContractError('invalid_output_channel')
    if channel=='mutated_argument':
        if type(e.get('output_argument')) is not int or not 0<=e['output_argument']<len(order):raise ContractError('invalid_output_argument')
    elif 'output_argument' in e:raise ContractError('inactive_output_argument')
    if type(e.get('structures',{})) is not dict:raise ContractError('invalid_structures')
    for key,s in e.get('structures',{}).items():
        if type(key) is not str or not key.isdecimal() or str(int(key))!=key or not 0<=int(key)<len(order):raise ContractError('invalid_structure_argument')
        structure(s)
    if e.get('output_structure') is not None:structure(e['output_structure'])

def signature(source,e):
    tree=ast.parse(source,feature_version=(3,11));name=e.get('fn_name')
    top=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name]
    methods=[m for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='Solution' for m in n.body if isinstance(m,ast.FunctionDef) and m.name==name]
    target=e.get('target','auto')
    choices=([(n,'function') for n in top] if target!='solution_method' else [])+([(n,'solution_method') for n in methods] if target!='function' else [])
    if len(choices)!=1:raise ContractError('entry_missing_or_ambiguous_no_runtime_fallback')
    n,target=choices[0];args=n.args.posonlyargs+n.args.args
    if n.args.vararg or n.args.kwarg or n.args.kwonlyargs or n.decorator_list:raise ContractError('unsupported_signature_or_decorator')
    if target=='solution_method':
        if not args or args[0].arg!='self':raise ContractError('instance_method_requires_self')
        args=args[1:]
        cls=next(c for c in tree.body if isinstance(c,ast.ClassDef) and c.name=='Solution')
        init=next((x for x in cls.body if isinstance(x,ast.FunctionDef) and x.name=='__init__'),None)
        if cls.bases or (init and (len(init.args.args)-len(init.args.defaults)>1 or init.args.posonlyargs or init.args.kwonlyargs)):
            raise ContractError('Solution_constructor_not_fixed_noarg')
    return target,len(args)

def freeze_entry(raw,source):
    if raw.get('mode')=='stdin_stdout':return {'mode':'stdin_stdout'}
    if raw.get('mode')!='function' or not raw.get('fn_name'):raise ContractError('entry_metadata_pending')
    e={k:deepcopy(raw[k]) for k in ('mode','fn_name','target','parameter_order','output_channel','output_argument','structures','output_structure') if k in raw}
    target,arity=signature(source,e)
    e['target']=target;e.setdefault('parameter_order',list(range(arity)))
    validate_entry(e)
    if len(e['parameter_order'])!=arity:raise ContractError('signature_arity_mismatch')
    return e

def public_contract(c):
    """Positive projection: internal origin/evidence locators never reach reviewers."""
    entry=c['entry'];pub={'mode':entry['mode']}
    if entry['mode']=='function':
        for k in ('fn_name','target','parameter_order','output_channel','output_argument'):
            if k in entry:pub[k]=deepcopy(entry[k])
        for k in ('structures','output_structure'):
            if k in entry:pub[k]=deepcopy(entry[k])
    return {'schema_version':c['schema_version'],'entry':pub,'representation_kinds':c['representation_kinds'],
        'terminal_newline':c['terminal_newline'],'comparison':deepcopy(c['comparison']),'exception_policy':c['exception_policy']}

def input_structure(value,s):
    if value is None:return
    if type(value) is not list:raise ContractError('structure_input_requires_list_or_null')
    canonical_bytes(value)
    if s['kind']=='binary_tree' and value:
        if value[0] is None:raise ContractError('nonempty_tree_requires_root')
        parents=1;idx=1
        while parents and idx<len(value):
            parents-=1
            for _ in range(2):
                if idx>=len(value):break
                parents+=value[idx] is not None;idx+=1
        if idx!=len(value):raise ContractError('orphan_tree_nodes')
