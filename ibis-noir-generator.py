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
                 .select("string1"))

    to_graph(query).render("query3")
    graph = Graph.from_bfs(query.op(), filter=ops.Node)
    for k, v in graph.items():
        print(str(k) + " |\t" + type(k).__name__ + ":\t" + str(v))

    # TableColumn ops have a 'name' attribute and a 'dtype' and 'shape' attributes: use those to get info!

    operators = list()

    op = query.op()
    while isinstance(op, ibis.expr.operations.relations.Selection):
        if op.args[1]:
            name = op.selections[0].name
            operators.append(("select", name))

        elif op.args[2]:
            name = op.args[2][0].right.name
            operators.append(("filter", name))

        op = op.table

    print(operators)

    #print(query.head().to_pandas())

    with open("noir-template/main_top.rs") as f:
        top = f.read()

    with open("noir-template/main_bot.rs") as f:
        bot = f.read()

    mid = ""
    for op in reversed(operators):
        match op:
            case ("select", name):
                print("NOT IMPL")  # TODO: use a .map(|(x, y, _)| (x, y) to exclude 3rd column and select 1st and 2nd
            case ("filter", name):
                mid += ".filter(|x| x.0 == " + name + ")"  # TODO: handle col numbers (here x.0 is fixed) also here

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
