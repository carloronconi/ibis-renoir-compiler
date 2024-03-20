import subprocess

import inspect

import ibis

from ibis.common.graph import Graph, Node
import ibis.expr.operations as ops
import ibis.expr.types as typ
from ibis.expr.visualize import to_graph
from typing import List, Sequence

bin_ops = {"Equals": "==", "Greater": ">", "GreaterEqual": ">=", "Less": "<", "LessEqual": "<="}
math_ops = {"Multiply": "*", "Add": "+", "Subtract": "-"}
aggr_ops = {"Max": "max", "Min": "min", "Sum": "+"}
ibis_op_type = {"filter": "filter", "group_by": "group", "aggregate": "reduce", "mutate": "map", "select": "select"}


def q_filter_group_mutate_reduce(table: typ.relations.Table) -> typ.relations.Table:
    return (table
            .filter(table.string1 == "unduetre")
            .group_by("string1").aggregate()
            .mutate(int1=table.int1 * 20)
            .aggregate(by=["string1"], max=table.int1.max()))


def q_filter_group_mutate(table: typ.relations.Table) -> typ.relations.Table:
    return (table
            .filter(table.string1 == "unduetre")
            .group_by("string1").aggregate()
            .mutate(int1=table.int1 * 20))  # mutate always results in alias preceded by Multiply (or other bin op)


def q_filter_select(table: typ.relations.Table) -> typ.relations.Table:
    return (table
            .filter(table.string1 == "unduetre")
            .select("int1"))


def q_filter_filter_select_select(table: typ.relations.Table) -> typ.relations.Table:
    return (table
            .filter(table.int1 == 123)
            .filter(table.string1 == "unduetre")
            .select("int1", "string1")
            .select("string1"))


def q_filter_group_select(table: typ.relations.Table) -> typ.relations.Table:
    return (table
            .filter(table.string1 == "unduetre")
            .group_by("string1").aggregate()
            .select("string1"))


def run_noir_query_on_table(table: typ.relations.Table, query_gen):
    query = query_gen(table)

    operators = create_operators(query)
    reorder_operators(operators, query_gen)
    gen_noir_code(operators, table)

    # cd noir-template
    # cargo-fmt
    # cargo run
    subprocess.run("cd noir-template && cargo-fmt && cargo run", shell=True)


def create_operators(query: ibis.expr.types.relations.Table) -> List[tuple]:
    print("parsing query...")

    to_graph(query).render("query3")
    subprocess.run("open query3.pdf", shell=True)

    graph = Graph.from_bfs(query.op(), filter=ops.Node)  # filtering ops.Selection doesn't work

    operators = []
    for tup in graph.items():
        operator = tup[0]
        operands = tup[1]
        print(str(operator) + " |\t" + type(operator).__name__ + ":\t" + str(operands))

        match operator:
            case ops.core.Alias() if one_reduction_operand(operands):  # find reducers (aka reduce)
                operand = operands[0]
                operators.append(("reduce", type(operand).__name__, operand.args[0]))

            case ops.core.Alias() if one_numeric_operand(operands):  # find mappers (aka map)
                operand = operands[0]  # maps have a single operand
                operators.append(("map", type(operand).__name__, operand.left, operand.right))

            case ops.relations.Aggregation():  # find groupers (aka group by)
                operators.append(("group", operator.by))  # by contains list of all group by columns

            case ops.logical.Comparison():  # find filters (aka row selection)
                operators.append(("filter", type(operator).__name__, operator.left, operator.right))

            case ops.relations.Selection():  # find maps (aka column projection)
                selected_columns = []
                for operand in filter(lambda o: isinstance(o, ops.TableColumn), operands):
                    selected_columns.append(operand)
                if selected_columns:
                    operators.append(("select", selected_columns))

    print("done parsing")
    # all nodes have a 'name' attribute and a 'dtype' and 'shape' attributes: use those to get info!

    return operators


def reorder_operators(operators, query_gen):
    operators.reverse()

    source = inspect.getsource(query_gen).splitlines()
    source = source[2:]

    for i, line in enumerate(source):
        line_op = line.strip().split(".")[1].split("(")[0]
        line_op = ibis_op_type[line_op]
        if operators[i][0] != line_op:
            operators[i], operators[i + 1] = operators[i + 1], operators[i]

    while len(operators) > len(source):
        operators.pop()


def gen_noir_code(operators: List[tuple], table):
    print("generating noir code...")

    with open("noir-template/main_top.rs") as f:
        top = f.read()

    with open("noir-template/main_bot.rs") as f:
        bot = f.read()

    mid = ""
    for idx, op in enumerate(operators):
        match op:
            case ("filter", op, left, right):
                op = bin_ops[op]
                left = operator_arg_stringify(left, table)
                right = operator_arg_stringify(right, table)
                mid += ".filter(|x| x." + left + " " + op + " " + right + ")"
            case ("group", by_list):
                for by in by_list:
                    # test if multiple consecutive group_by's have same effect (noir only supports one arg)
                    by = operator_arg_stringify(by, table)
                    mid += ".group_by(|x| x." + by + ".clone())"
            case ("reduce", op, arg):
                op = aggr_ops[op]
                # arg = operator_arg_stringify(arg, table)  # unused: noir doesn't require to specify column,
                # aggregation depends on previous step
                mid += ".reduce(|a, b| *a = (*a)." + op + "(b))"
            case ("map", op, left, right):
                op = math_ops[op]
                left = operator_arg_stringify(left, table)
                right = operator_arg_stringify(right, table)
                mid += ".map(|x| x."
                if operators[idx - 1][0] == "group":
                    # if map preceded by group_by trivially adding because of noir
                    # implementation ("grouped" (old_tuple))
                    mid += "1."
                mid += left + " " + op + " " + right + ")"
            case ("select", col_list):
                mid += ".map(|x| "
                if len(col_list) == 1:
                    index = table.columns.index(col_list[0].name)
                    mid += "x." + str(index) + ")"
                else:
                    mid += "("
                    for col in col_list:
                        index = table.columns.index(col.name)
                        mid += "x." + str(index) + ", "
                    mid += "))"

    with open('noir-template/src/main.rs', 'w') as f:
        f.write(top)
        f.write(mid)
        f.write(bot)

    print("Done generating code")


# if operand is literal, return its value
# if operand is table column, return its index in the original table
def operator_arg_stringify(operand, table) -> str:
    if isinstance(operand, ibis.expr.operations.generic.TableColumn):
        index = table.columns.index(operand.name)
        return str(index)
    elif isinstance(operand, ibis.expr.operations.generic.Literal):
        if operand.dtype.name == "String":
            return "\"" + ''.join(filter(str.isalnum, operand.name)) + "\""
        return operand.name
    raise Exception("Unsupported operand type")


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


if __name__ == '__main__':
    table = ibis.read_csv("int-1-string-1.csv")

    # query_gen = q_filter_select
    # query_gen = q_filter_filter_select_select
    # query_gen = q_filter_group_select
    # query_gen = q_filter_group_mutate
    query_gen = q_filter_group_mutate_reduce

    run_noir_query_on_table(table, query_gen)
