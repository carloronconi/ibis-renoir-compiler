import subprocess

import ibis

from ibis.common.graph import Graph
import ibis.expr.operations as ops
from ibis.expr.visualize import to_graph


def run():
    print("Generating...")
    table = ibis.read_csv("int-1-string-1.csv")

    query = (table
             .filter(table.int1 == 123)
             #.select("string1", "int1"))
             .select("string1"))

    to_graph(query).render("query3")
    graph = Graph.from_bfs(query.op(), filter=ops.Node)  # filtering ops.Selection doesn't work

    operators = []
    # ibis calls 'Selection' both selections and projections: disambiguate by considering "filter" (selections)
    # operations with logical operand, because "map" (projections) only have TableColumn (and other Selection)
    # operands
    selectors = filter(lambda tup: isinstance(tup[0], ibis.expr.operations.relations.Selection), graph.items())
    for selector, operands in selectors:
        print(str(selector) + " |\t" + type(selector).__name__ + ":\t" + str(operands))
        for operand in operands:
            if getattr(operand, '__module__', None) == ibis.expr.operations.logical.__name__:
                # "filter", left.name="int1", left.dtype="Int64", right.name="123", right.dtype"Int8"
                # how do I use col name in rust instead of ordering
                operators.append(("filter", operand.left, operand.right))
                continue
        selected_columns = []
        for operand in filter(lambda o: isinstance(o, ibis.expr.operations.TableColumn), operands):
            selected_columns.append(operand)
        if selected_columns:
            operators.append(("map", selected_columns))

    print("done")
    # all nodes have a 'name' attribute and a 'dtype' and 'shape' attributes: use those to get info!

    with open("noir-template/main_top.rs") as f:
        top = f.read()

    with open("noir-template/main_bot.rs") as f:
        bot = f.read()

    mid = ""
    for op in reversed(operators):
        match op:
            case ("filter", left, right):
                index = table.columns.index(left.name)
                mid += ".filter(|x| x." + str(index) + " == " + right.name + ")"  # TODO: handle other binary ops and check Literals vs (treat as right.name) vs TableColumns (treat as index)
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


if __name__ == '__main__':
    run()
