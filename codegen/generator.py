import os
import subprocess
import time

from ibis.common.graph import Node
import ibis.expr
import ibis.expr.datatypes
from ibis.expr.operations import PhysicalTable
import ibis.expr.types
from ibis.expr.visualize import to_graph
from codegen.benchmark import Benchmark

import codegen.utils as utl
from codegen.operators import Operator, DatabaseOperator
import ibis
from codegen.struct import Struct


def compile_ibis_to_noir(files_tables: list[tuple[str, PhysicalTable]],
                         query: PhysicalTable,
                         run_after_gen=True,
                         print_output_to_file=True,
                         render_query_graph=True,
                         benchmark: Benchmark = None,
                         renoir_cached = False):

    if benchmark:
        start_time = time.perf_counter()

    for file, table in files_tables:
        utl.TAB_FILES[str(table._arg.name)] = file

    if render_query_graph:
        to_graph(query).render(utl.ROOT_DIR + "/out/query")
        subprocess.run(f"open {utl.ROOT_DIR}/out/query.pdf", shell=True)

    post_order_dfs(query.op())
    Operator.print_output_to_file = print_output_to_file
    gen_noir_code()

    if subprocess.run(f"cd {utl.ROOT_DIR}/noir_template && cargo-fmt > /dev/null 2>&1 && cargo build --release > /dev/null 2>&1", shell=True).returncode != 0:
        raise Exception("Failed to compile generated noir code!")

    if benchmark:
        end_time = time.perf_counter()
        benchmark.renoir_compile_time_s = end_time - start_time

    if run_after_gen:
        if benchmark:
            start_time = time.perf_counter()
        # add options to print renoir output: capture_output = True, text = True
        if subprocess.run(f"cd {utl.ROOT_DIR}/noir_template && cargo run --release > /dev/null 2>&1", shell=True).returncode != 0:
            raise Exception("Noir code panicked!")
        if benchmark:
            end_time = time.perf_counter()
            benchmark.renoir_execute_time_s = end_time - start_time


def compile_preloaded_tables_evcxr(files_tables: list[tuple[str, PhysicalTable]]):

    mid = "\nfn cache() -> "
    func = "{\nlet ctx = StreamContext::new_local();\n"
    for file, table in files_tables:
        struct = Struct.from_table(table)
        right_path = file.split(utl.ROOT_DIR)[1]
        
        struct.name_short = right_path[1:].replace("/", "_").split(".")[0]
        struct.name_struct = "Struct_" + struct.name_short
        name_temp = struct.name_short + "_temp"

        func += f"let ({struct.name_short}, {name_temp}) = ctx.stream_csv::<{struct.name_struct}>(\"{file}\").batch_mode(BatchMode::fixed(16000)).cache();\n{name_temp}.for_each(|x| {{std::hint::black_box(x);}});\n"
    func += "ctx.execute_blocking();\n"

    if len(Struct.structs) == 0:
        st = Struct.structs[0]
        mid += f"StreamCache<{st.name_struct}>"
        func += f"return {st.name_short};\n}}"
        mid += func
        mid += f"let {st.name_short} = cache();\n"
    else:
        mid += "("
        func += "return ("
        for st in Struct.structs:
            mid += f"StreamCache<{st.name_struct}>,"
            func += f"{st.name_short}, "
        mid += ")"
        func += ");\n}"
        mid += func
        mid += f"let ("
        for st in Struct.structs:
            mid += f"{st.name_short}, "
        mid += ") = cache();\n"

    with open(utl.ROOT_DIR + "/noir_template/main_top_evcxr.rs") as f:
        top = f.read()
    for st in Struct.structs:
        top += st.generate()

    with open(utl.ROOT_DIR + '/noir_template/preload_evcxr.rs', 'w+') as f:
        f.write(top)
        f.write(mid)

    return


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


def gen_noir_code(override_file: str = None):
    mid = ""
    for op in Operator.operators:
        # operators can also modify structs while generating, so generate mid before top
        mid += op.generate()

    # bottom can also generate new struct, so generate bot before top
    bot = Operator.new_bot().generate()
    top = Operator.new_top().generate()

    if not override_file:
        directory = utl.ROOT_DIR + '/noir_template/src'
        if not os.path.exists(directory):
            os.makedirs(directory)
        file = directory + '/main.rs'
    else:
        file = override_file
    with open(file, 'w+') as f:
        f.write(top)
        f.write(mid)
        f.write(bot)
