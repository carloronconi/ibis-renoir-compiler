import subprocess
from ibis.common.graph import Node
import ibis.expr.operations as ops
from ibis.expr.operations import DatabaseTable
from ibis.expr.types.relations import Table
from ibis.expr.visualize import to_graph
from typing import List, Tuple, Callable, Optional, cast
import codegen.operators as sop
import codegen.utils as utl
from codegen.struct import Struct


def compile_ibis_to_noir(files_tables: list[tuple[str, Table]],
                         query: Table,
                         run_after_gen=True,
                         render_query_graph=True):

    for file, table in files_tables:
        utl.TAB_FILES[table._arg.name] = file

    if render_query_graph:
        to_graph(query).render(utl.ROOT_DIR + "/out/query")
        subprocess.run(f"open {utl.ROOT_DIR}/out/query.pdf", shell=True)

    (operators, structs) = post_order_dfs(query.op(), operator_recognizer)

    gen_noir_code(operators, structs, files_tables)

    subprocess.run(f"cd {utl.ROOT_DIR}/noir-template && cargo-fmt && cargo build", shell=True)
    if run_after_gen:
        subprocess.run(f"cd {utl.ROOT_DIR}/noir-template && cargo run", shell=True)


def post_order_dfs(root: Node, recognizer: Callable[[Node, list[sop.Operator], list[Struct]], None]):
    stack: list[tuple[Node, bool]] = [(root, False)]
    visited: set[Node] = set()
    operators: list[sop.Operator] = []
    structs: list[Struct] = []

    while stack:
        (node, visit) = stack.pop()
        if visit:
            recognizer(node, operators, structs)
        elif node not in visited:
            visited.add(node)
            stack.append((node, True))
            for child in node.__children__:
                stack.append((child, False))

    return operators, structs


def operator_recognizer(node: Node, operators: list[sop.Operator], structs: list[Struct]):
    match node:
        case ops.DatabaseTable():
            operators.append(sop.DatabaseOperator(node, operators, structs))
        case ops.relations.Join():
            operators.append(sop.JoinOperator(node, operators, structs))
        case ops.relations.Aggregation() if any(isinstance(x, ops.core.Alias) for x in node.__children__):
            if any(isinstance(x, ops.TableColumn) for x in node.__children__):
                operators.append(sop.GroupReduceOperator(node, operators, structs))  # group_by().reduce()
            else:
                operators.append(sop.LoneReduceOperator(node, operators, structs))
        case ops.logical.Comparison() if any(isinstance(c, ops.Literal) for c in node.__children__):
            operators.append(sop.FilterOperator(node, operators, structs))
        case ops.core.Alias() if any(isinstance(c, ops.numeric.NumericBinary) for c in node.__children__):
            operators.append(sop.MapOperator(node, operators, structs))
        case ops.relations.Selection() if (any(isinstance(c, ops.TableColumn) for c in node.__children__) and
                                           not any(isinstance(c, ops.Join) for c in node.__children__)):
            operators.append(sop.SelectOperator(node, operators, structs))


def gen_noir_code(operators: List[sop.Operator], structs, tables: List[Tuple[str, Table]]):
    print("generating noir code...")

    mid = ""
    for op in operators:
        mid = op.generate(mid)  # operators can also modify structs while generating, so generate mid before top

    with open(utl.ROOT_DIR + "/noir-template/main_top.rs") as f:
        top = f.read()
    top = gen_noir_code_top(top, structs, tables)

    bot = f"; let out = {structs[-1].name_short}.collect_vec();"
    with open(utl.ROOT_DIR + "/noir-template/main_bot.rs") as f:
        bot += f.read()

    with open(utl.ROOT_DIR + '/noir-template/src/main.rs', 'w') as f:
        f.write(top)
        f.write(mid)
        f.write(bot)

    Struct.name_counter = 0  # resetting otherwise tests fail when running sequentially
    print("done generating code")


def gen_noir_code_top(top: str, structs, tables: List[Tuple[str, Table]]):
    body = top

    for st in structs:
        body = st.generate(body)

    body += "\nfn logic(ctx: StreamContext) {\n"

    return body
