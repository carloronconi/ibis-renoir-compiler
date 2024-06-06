import os
import subprocess
import time

from ibis.common.graph import Node
from ibis.expr.operations import PhysicalTable
from ibis.expr.visualize import to_graph
from codegen.benchmark import Benchmark

import codegen.utils as utl
from codegen.operators import Operator


def compile_ibis_to_noir(files_tables: list[tuple[str, PhysicalTable]],
                         query: PhysicalTable,
                         run_after_gen=True,
                         render_query_graph=True,
                         benchmark: Benchmark = None):

    if benchmark:
        start_time = time.perf_counter()

    for file, table in files_tables:
        utl.TAB_FILES[str(table._arg.name)] = file

    if render_query_graph:
        to_graph(query).render(utl.ROOT_DIR + "/out/query")
        subprocess.run(f"open {utl.ROOT_DIR}/out/query.pdf", shell=True)

    post_order_dfs(query.op())
    gen_noir_code()

    if subprocess.run(f"cd {utl.ROOT_DIR}/noir_template && cargo-fmt && cargo build --release", shell=True).returncode != 0:
        raise Exception("Failed to compile generated noir code!")

    if benchmark:
        end_time = time.perf_counter()
        benchmark.set_renoir_compile(end_time - start_time)

    if run_after_gen:
        if benchmark:
            start_time = time.perf_counter()
        if subprocess.run(f"cd {utl.ROOT_DIR}/noir_template && cargo run --release", shell=True).returncode != 0:
            raise Exception("Noir code panicked!")
        if benchmark:
            end_time = time.perf_counter()
            benchmark.set_renoir_execute(end_time - start_time)


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
        # operators can also modify structs while generating, so generate mid before top
        mid += op.generate()

    # bottom can also generate new struct, so generate bot before top
    bot = Operator.new_bot().generate()
    top = Operator.new_top().generate()

    directory = utl.ROOT_DIR + '/noir_template/src'
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + '/main.rs', 'w+') as f:
        f.write(top)
        f.write(mid)
        f.write(bot)

    print("done generating code")
