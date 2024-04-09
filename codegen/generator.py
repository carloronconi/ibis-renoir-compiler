import subprocess

from ibis.common.graph import Node
from ibis.expr.types.relations import Table
from ibis.expr.visualize import to_graph

import codegen.utils as utl
from codegen.operators import Operator
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

    post_order_dfs(query.op())
    gen_noir_code()

    if subprocess.run(f"cd {utl.ROOT_DIR}/noir-template && cargo-fmt && cargo build", shell=True).returncode != 0:
        raise Exception("Failed to compile generated noir code!")
    if run_after_gen:
        if subprocess.run(f"cd {utl.ROOT_DIR}/noir-template && cargo run", shell=True).returncode != 0:
            raise Exception("Noir code panicked!")


def post_order_dfs(root: Node):
    stack: list[tuple[Node, bool]] = [(root, False)]
    visited: set[Node] = set()

    while stack:
        (node, visit) = stack.pop()
        if visit:
            Operator.from_node(node)
        elif node not in visited:
            visited.add(node)
            stack.append((node, True))
            for child in node.__children__:
                stack.append((child, False))


def gen_noir_code():
    print("generating noir code...")

    mid = ""
    for op in Operator.operators:
        mid += op.generate()  # operators can also modify structs while generating, so generate mid before top

    with open(utl.ROOT_DIR + "/noir-template/main_top.rs") as f:
        top = f.read()
    top += gen_noir_code_top()

    is_keyed = sop.is_keyed_stream(operators[-1], operators)  # check if last operator is keyed stream
    if is_keyed:  # if keyed need to drop key to be able to print to file with serde
        bot = f"; let out = {Struct.structs[-1].name_short}.drop_key().collect_vec();"
    else:
        bot = f"; let out = {Struct.structs[-1].name_short}.collect_vec();"
    with open(utl.ROOT_DIR + "/noir-template/main_bot.rs") as f:
        bot += f.read()

    with open(utl.ROOT_DIR + '/noir-template/src/main.rs', 'w') as f:
        f.write(top)
        f.write(mid)
        f.write(bot)

    Struct.name_counter = 0  # resetting otherwise tests fail when running sequentially
    print("done generating code")


def gen_noir_code_top():
    body = ""

    for st in Struct.structs:
        body += st.generate()

    body += "\nfn logic(ctx: StreamContext) {\n"

    return body
