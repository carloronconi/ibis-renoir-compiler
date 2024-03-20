import subprocess

import inspect

import ibis

from ibis.common.graph import Graph, Node
import ibis.expr.operations as ops
import ibis.expr.types as typ
from ibis.expr.visualize import to_graph
from typing import List, Sequence
import operators as sop


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

    operators = create_operators(query, table)
    reorder_operators(operators, query_gen)
    gen_noir_code(operators)

    # cd noir-template
    # cargo-fmt
    # cargo run
    subprocess.run("cd noir-template && cargo-fmt && cargo run", shell=True)


def create_operators(query: ibis.expr.types.relations.Table, table: typ.relations.Table) -> List[sop.Operator]:
    print("parsing query...")

    to_graph(query).render("../out/query3")
    subprocess.run("open out/query3.pdf", shell=True)

    graph = Graph.from_bfs(query.op(), filter=ops.Node)  # filtering ops.Selection doesn't work

    operators: list[sop.Operator] = []
    for tup in graph.items():
        operator = tup[0]
        operands = tup[1]
        print(str(operator) + " |\t" + type(operator).__name__ + ":\t" + str(operands))

        match operator:
            case ops.core.Alias() if one_reduction_operand(operands):  # find reducers (aka reduce)
                operand = operands[0]
                operators.append(sop.ReduceOperator(operand))

            case ops.core.Alias() if one_numeric_operand(operands):  # find mappers (aka map)
                operand = operands[0]  # maps have a single operand
                operators.append(sop.MapOperator(table, operand, operators))

            case ops.relations.Aggregation():  # find groupers (aka group by)
                operators.append(sop.GroupOperator(table, operator.by))  # by contains list of all group by columns

            case ops.logical.Comparison():  # find filters (aka row selection)
                operators.append(sop.FilterOperator(table, operator))

            case ops.relations.Selection():  # find maps (aka column projection)
                selected_columns = []
                for operand in filter(lambda o: isinstance(o, ops.TableColumn), operands):
                    selected_columns.append(operand)
                if selected_columns:
                    operators.append(sop.SelectOperator(table, selected_columns))

    print("done parsing")
    # all nodes have a 'name' attribute and a 'dtype' and 'shape' attributes: use those to get info!

    return operators


def reorder_operators(operators: List[sop.Operator], query_gen):
    operators.reverse()

    source = inspect.getsource(query_gen).splitlines()
    source = source[2:]

    for i, line in enumerate(source):
        line_op = line.strip().split(".")[1].split("(")[0]
        if not isinstance(operators[i], sop.Operator.op_class_from_ibis(line_op)):
            operators[i], operators[i + 1] = operators[i + 1], operators[i]

    while len(operators) > len(source):
        operators.pop()


def gen_noir_code(operators: List[sop.Operator]):
    print("generating noir code...")

    with open("noir-template/main_top.rs") as f:
        top = f.read()

    with open("noir-template/main_bot.rs") as f:
        bot = f.read()

    mid = ""
    for op in operators:
        mid = op.generate(mid)

    with open('noir-template/src/main.rs', 'w') as f:
        f.write(top)
        f.write(mid)
        f.write(bot)

    print("Done generating code")


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
    table = ibis.read_csv("codegen/int-1-string-1.csv")

    # query_gen = q_filter_select
    # query_gen = q_filter_filter_select_select
    # query_gen = q_filter_group_select
    # query_gen = q_filter_group_mutate
    query_gen = q_filter_group_mutate_reduce

    run_noir_query_on_table(table, query_gen)
