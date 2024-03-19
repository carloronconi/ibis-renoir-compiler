import subprocess

import ibis

from ibis.common.graph import Graph
import ibis.expr.operations as ops
from ibis.expr.visualize import to_graph

bin_ops = {"Equals": "==", "Greater": ">", "GreaterEqual": ">=", "Less": "<", "LessEqual": "<="}


def run():
    print("Generating...")
    table = ibis.read_csv("int-1-string-1.csv")

    query = (table
             .filter(table.string1 == "unduetre")
             .group_by("string1").aggregate()
             # .filter(table.int1 == 2).filter(table.string1 == "unduetre")
             # .filter(table.string1 == "unduetre")
             # .filter(table.int1 <= 125)
             # .filter(125 >= table.int1)
             # .select("string1", "int1"))
             # .select("int1", "string1").select("string1"))
             .select("string1"))

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
            operators.append(("map", selected_columns))

    # find groupers (aka group by)
    groupers = filter(lambda tup: isinstance(tup[0], ibis.expr.operations.relations.Aggregation), graph.items())
    for grouper, operands in groupers:
        operators.append(("group", grouper.by))  # by contains list of all group by columns

    # find filters (aka row selection)
    filters = filter(is_logical_operand, graph.items())
    for fil, operands in filters:
        operators.append(("filter", type(fil).__name__, fil.left, fil.right))

    print("done")
    # all nodes have a 'name' attribute and a 'dtype' and 'shape' attributes: use those to get info!

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
            case ("map", col_list):
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

    # cd noir-template
    # cargo-fmt
    # cargo run
    subprocess.run("cd noir-template && cargo-fmt && cargo run", shell=True)


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


if __name__ == '__main__':
    run()
