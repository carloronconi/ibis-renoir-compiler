import subprocess

import ibis

from ibis.common.graph import Graph
import ibis.expr.operations as ops
from ibis.expr.visualize import to_graph
from typing import List

bin_ops = {"Equals": "==", "Greater": ">", "GreaterEqual": ">=", "Less": "<", "LessEqual": "<="}
math_ops = {"Multiply": "*", "Add": "+", "Subtract": "-"}


def run_q_map_group_filter():
    table = ibis.read_csv("int-1-string-1.csv")

    query = (table
             .filter(table.string1 == "unduetre")
             .group_by("string1").aggregate()   # careful: group_by "loses" other cols if you don't pass aggregation
                                                # funct into aggregate (e.g. table.int1.max()) - or maybe just duckdb?
                                                # because query graph actually looks correct
             .mutate(int1=table.int1 * 20))  # mutate always results in alias preceded by Multiply (or other bin op)

    run_noir_query_on_table(table, query)


def run_q_select_project():
    table = ibis.read_csv("int-1-string-1.csv")

    query = (table
             .filter(table.string1 == "unduetre")
             .select("int1"))

    run_noir_query_on_table(table, query)


def run_q_multi_select_multi_project():
    table = ibis.read_csv("int-1-string-1.csv")

    query = (table
             .filter(table.int1 == 123).filter(table.string1 == "unduetre")
             .select("int1", "string1").select("string1"))

    run_noir_query_on_table(table, query)


def run_q_select_group_filter():
    table = ibis.read_csv("int-1-string-1.csv")

    query = (table
             .filter(table.string1 == "unduetre")
             .group_by("string1").aggregate()
             .select("string1"))

    run_noir_query_on_table(table, query)


def run_noir_query_on_table(table: ibis.expr.types.relations.Table, query: ibis.expr.types.relations.Table):
    operators = create_operators(query)
    gen_noir_code(operators, table)

    # cd noir-template
    # cargo-fmt
    # cargo run
    subprocess.run("cd noir-template && cargo-fmt && cargo run", shell=True)


def create_operators(query: ibis.expr.types.relations.Table) -> List[tuple]:
    print("parsing query...")

    to_graph(query).render("query3")
    graph = Graph.from_bfs(query.op(), filter=ops.Node)  # filtering ops.Selection doesn't work

    operators = []
    # find maps (aka column projection)
    # need to filter out redundant selections in tree inserted just above filters by ibis
    selectors = filter(is_selection_and_no_logical_operand, graph.items())
    for selector, operands in selectors:
        print(str(selector) + " |\t" + type(selector).__name__ + ":\t" + str(operands))
        selected_columns = []
        for operand in filter(lambda o: isinstance(o, ibis.expr.operations.TableColumn), operands):
            selected_columns.append(operand)
        if selected_columns:
            operators.append(("select", selected_columns))

    # find mappers (aka map)
    mappers = filter(is_alias_and_one_numeric_operand, graph.items())
    for mapper, operands in mappers:
        operand = operands[0]  # maps have a single operand
        operators.append(("map", type(operand).__name__, operand.left, operand.right))

    # find groupers (aka group by)
    groupers = filter(lambda tup: isinstance(tup[0], ibis.expr.operations.relations.Aggregation), graph.items())
    for grouper, operands in groupers:
        operators.append(("group", grouper.by))  # by contains list of all group by columns

    # find filters (aka row selection)
    filters = filter(is_logical_operand, graph.items())
    for fil, operands in filters:
        operators.append(("filter", type(fil).__name__, fil.left, fil.right))

    print("done parsing")
    # all nodes have a 'name' attribute and a 'dtype' and 'shape' attributes: use those to get info!
    return operators


def gen_noir_code(operators: List[tuple], table):
    print("generating noir code...")

    with open("noir-template/main_top.rs") as f:
        top = f.read()

    with open("noir-template/main_bot.rs") as f:
        bot = f.read()

    mid = ""
    for op in reversed(operators):
        match op:
            case ("filter", op, left, right):
                op = bin_ops[op]
                left = operator_arg_stringify(left, table)
                right = operator_arg_stringify(right, table)
                mid += ".filter(|x| x." + left + " " + op + " " + right + ")"
            case ("group", by_list):
                for by in by_list:  # test if multiple consecutive group_by's have same effect (noir only supports one arg)
                    by = operator_arg_stringify(by, table)
                    mid += ".group_by(|x| x." + by + ".clone())"
            case ("map", op, left, right):
                op = math_ops[op]
                left = operator_arg_stringify(left, table)
                right = operator_arg_stringify(right, table)
                mid += ".map(|x| x."
                if "group" in map(lambda tup: tup[0], operators):  # brutally adding because of noir implementation
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


def is_selection_and_no_logical_operand(tup) -> bool:
    if not isinstance(tup[0], ibis.expr.operations.relations.Selection):
        return False
    for operand in tup[1]:
        if getattr(operand, '__module__', None) == ibis.expr.operations.logical.__name__:
            return False
    return True


def is_logical_operand(tup) -> bool:
    if getattr(tup[0], '__module__', None) == ibis.expr.operations.logical.__name__:
        return True
    return False


def is_alias_and_one_numeric_operand(tup) -> bool:
    if not isinstance(tup[0], ibis.expr.operations.core.Alias):
        return False
    if len(tup[1]) != 1:
        return False
    if getattr(tup[1][0], '__module__', None) != ibis.expr.operations.numeric.__name__:
        return False
    return True


if __name__ == '__main__':
    # run_q_select_project()
    # run_q_multi_select_multi_project()
    # run_q_select_group_filter()
    run_q_map_group_filter()
