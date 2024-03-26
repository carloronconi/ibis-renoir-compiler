import subprocess
import inspect
import ibis
from ibis.common.graph import Graph, Node
import ibis.expr.operations as ops
from ibis.expr.types.relations import Table
from ibis.expr.visualize import to_graph
from typing import List, Sequence, Tuple, Callable
import codegen.operators as sop
import codegen.utils as utl


def compile_ibis_to_noir(table_files: list[str],
                         query_gen: Callable[[list[Table]], Table],
                         run_after_gen=True,
                         render_query_graph=True):
    tables = []
    tups = []
    for table_file in table_files:
        tab = ibis.read_csv(table_file)
        tables.append(tab)
        tups.append((table_file, tab))

    query = query_gen(tables)
    if render_query_graph:
        to_graph(query).render(utl.ROOT_DIR + "/out/query")
        subprocess.run(f"open {utl.ROOT_DIR}/out/query.pdf", shell=True)

    operators = create_operators(query, tables[0])

    gen_noir_code(operators, tups)

    subprocess.run(f"cd {utl.ROOT_DIR}/noir-template && cargo-fmt && cargo build", shell=True)
    if run_after_gen:
        subprocess.run(f"cd {utl.ROOT_DIR}/noir-template && cargo run", shell=True)


def create_operators(query: Table, table: Table) -> List[sop.Operator]:
    print("parsing query...")

    graph = Graph.from_dfs(query.op(), filter=ops.Node)  # filtering ops.Selection doesn't work

    operators: list[sop.Operator] = []
    for operator, operands in graph.items():
        print(str(operator) + " |\t" + type(operator).__name__ + ":\t" + str(operands))

        match operator:
            case ops.relations.Join():
                operators.append(sop.JoinOperator(table, operator))

            case ops.core.Alias() if one_reduction_operand(operands):  # find reducers (aka reduce)
                operand = operands[0]
                operators.append(sop.ReduceOperator(operand))

            case ops.core.Alias() if one_numeric_operand(operands):  # find mappers (aka map)
                operand = operands[0]  # maps have a single operand
                operators.append(sop.MapOperator(table, operand, operators))

            # find last grouper (aka group_by): more than one grouper are unsupported because can't group a KeyedStream
            case ops.relations.Aggregation() if is_last_aggregation(operator, graph.items().mapping.keys()):
                operators.append(sop.GroupOperator(table, operator.by))  # by contains list of all group by columns

            case ops.logical.Comparison():  # find filters (aka row selection)
                operators.append(sop.FilterOperator(table, operator))

            case ops.relations.Selection():  # find maps (aka column projection)
                selected_columns = []
                for operand in filter(lambda o: isinstance(o, ops.TableColumn), operands):
                    selected_columns.append(operand)
                if selected_columns:
                    operators.append(sop.SelectOperator(table, selected_columns, operators))

    print("done parsing")
    # all nodes have a 'name' attribute and a 'dtype' and 'shape' attributes: use those to get info!

    return operators


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


def one_numeric_operand(operands: Sequence[Node]) -> bool:
    if len(operands) != 1:
        return False
    if getattr(operands[0], '__module__', None) != ibis.expr.operations.numeric.__name__:
        return False
    return True


def one_reduction_operand(operands: Sequence[Node]) -> bool:
    if len(operands) != 1:
        return False
    return isinstance(operands[0], ibis.expr.operations.Reduction)


def is_last_aggregation(operator: Node, operators) -> bool:
    return (filter(lambda op: isinstance(op, ops.relations.Aggregation), reversed(operators))
            .__next__()
            .equals(operator))
