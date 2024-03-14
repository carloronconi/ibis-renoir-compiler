import subprocess

import ibis


def run():
    print("Generating...")
    table = ibis.read_csv("int-1-string-1.csv")

    transform = (table
                 .filter(table.int1 == 123)
                 .select("string1"))

    operators = list()

    op = transform.op()
    while isinstance(op, ibis.expr.operations.relations.Selection):
        if op.args[1]:
            name = op.selections[0].name
            operators.append(("select", name))

        elif op.args[2]:
            name = op.args[2][0].right.name
            operators.append(("filter", name))

        op = op.table

    print(operators)

    #print(transform.head().to_pandas())

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
