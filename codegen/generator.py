import subprocess
from ibis.common.graph import Node
import ibis.expr.operations as ops
from ibis.expr.types.relations import Table
from ibis.expr.visualize import to_graph
from typing import List, Tuple, Callable, Optional
import codegen.operators as sop
import codegen.utils as utl


def compile_ibis_to_noir(files_tables: list[tuple[str, Table]],
                         query: Table,
                         run_after_gen=True,
                         render_query_graph=True):

    if render_query_graph:
        to_graph(query).render(utl.ROOT_DIR + "/out/query")
        subprocess.run(f"open {utl.ROOT_DIR}/out/query.pdf", shell=True)

    operators = post_order_dfs(query.op(), operator_recognizer)

    gen_noir_code(operators, files_tables)

    subprocess.run(f"cd {utl.ROOT_DIR}/noir-template && cargo-fmt && cargo build", shell=True)
    if run_after_gen:
        subprocess.run(f"cd {utl.ROOT_DIR}/noir-template && cargo run", shell=True)


def post_order_dfs(root: Node, recognizer: Callable[[Node, list[sop.Operator]], Optional[sop.Operator]]):
    stack: list[tuple[Node, bool]] = [(root, False)]
    visited: set[Node] = set()
    out: list[sop.Operator] = []

    while stack:
        (node, visit) = stack.pop()
        if visit:
            recognizer(node, out)
        elif node not in visited:
            visited.add(node)
            stack.append((node, True))
            for child in node.__children__:
                stack.append((child, False))

    return out


def operator_recognizer(node: Node, operators: list[sop.Operator]):
    match node:
        case ops.DatabaseTable():
            pass # TODO: would avoid passing list of tables to compile_ibis_to_noir, only passing query!
        case ops.relations.Join():
            operators.append(sop.JoinOperator(node))
        case ops.relations.Aggregation() if any(isinstance(x, ops.core.Alias) for x in node.__children__):
            if any(isinstance(x, ops.TableColumn) for x in node.__children__):
                operators.append(sop.GroupOperator(node))  # group_by().reduce()
            operators.append(sop.ReduceOperator(node))
        case ops.logical.Comparison() if any(isinstance(c, ops.Literal) for c in node.__children__):
            operators.append(sop.FilterOperator(node))
        case ops.core.Alias() if any(isinstance(c, ops.numeric.NumericBinary) for c in node.__children__):
            operators.append(sop.MapOperator(node, operators))
        case ops.relations.Selection() if any(isinstance(c, ops.TableColumn) for c in node.__children__):
            operators.append(sop.SelectOperator(node, operators))


def gen_noir_code(operators: List[sop.Operator], tables: List[Tuple[str, Table]]):
    print("generating noir code...")

    with open(utl.ROOT_DIR + "/noir-template/main_top.rs") as f:
        top = f.read()
    top = gen_noir_code_top(top, tables)

    with open(utl.ROOT_DIR + "/noir-template/main_bot.rs") as f:
        bot = f.read()

    mid = ""
    for op in operators:
        mid = op.generate(mid)

    with open(utl.ROOT_DIR + '/noir-template/src/main.rs', 'w') as f:
        f.write(top)
        f.write(mid)
        f.write(bot)

    print("done generating code")


def gen_noir_code_top(top: str, tables: List[Tuple[str, Table]]):
    body = top + ""
    names = []
    i = 0
    for file, table in tables:
        # define struct for table's columns
        name_long = table.get_name()
        name = f"table{i}"
        utl.TAB_NAMES[name_long] = name
        i += 1
        columns = table.columns
        body += f"#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]\nstruct Cols_{name} {{"
        for i, col in enumerate(columns):
            body += f"{col}: {to_noir_type(table, i)},"
        body += "}\n"

    body += "\nfn logic(ctx: &StreamContext) {\n"

    for file, table in tables:
        # define table
        name = utl.TAB_NAMES[table.get_name()]
        names.append(name)
        body += f"let {name} = ctx.stream_csv::<Cols_{name}>(\"{file}\");\n"

    body += names.pop(0)
    return body


def to_noir_type(table: Table, index: int) -> str:
    ibis_to_noir_type = {"Int64": "i64", "String": "String"}  # TODO: add nullability with optionals
    ibis_type = table.schema().types[index]
    return ibis_to_noir_type[ibis_type.name]
