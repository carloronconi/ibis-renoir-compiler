import ibis
from ibis.common.graph import Node
import ibis.expr.types as typ
import ibis.expr.operations as ops


class Operator:
    ibis_api_name = None

    def generate(self, to_text: str) -> str:
        pass

    @staticmethod
    def op_class_from_ibis(ibis_api: str):
        for sub in Operator.__subclasses__():
            if sub.ibis_api_name == ibis_api:
                return sub
        raise Exception(f"No subclass implementing the requested operator: {ibis_api}")


class SelectOperator(Operator):
    ibis_api_name = "select"
    columns = []
    table: typ.relations.Table
    other_operators: list[Operator]

    def __init__(self, table: typ.relations.Table,  columns: list[Node], other_operators: list[Operator]):
        self.columns = columns
        self.table = table
        self.other_operators = other_operators

    def generate(self, to_text: str) -> str:
        mid = to_text + ".map(|x| "
        if len(self.columns) == 1:
            mid += "x."
            mid = fix_prev_group_by(mid, self, self.other_operators)
            mid += f"{self.columns[0].name})"
        else:
            mid += "("
            for col in self.columns:
                mid += "x."
                mid = fix_prev_group_by(mid, self, self.other_operators)
                mid += f"{col.name}, "
            mid += "))"
        return mid


class FilterOperator(Operator):
    ibis_api_name = "filter"
    bin_ops = {"Equals": "==", "Greater": ">", "GreaterEqual": ">=", "Less": "<", "LessEqual": "<="}
    comparator: ops.logical.Comparison
    table: typ.relations.Table

    def __init__(self, table: typ.relations.Table, comparator: ops.logical.Comparison):
        self.comparator = comparator
        self.table = table

    def generate(self, to_text: str) -> str:
        op = self.bin_ops[type(self.comparator).__name__]
        left = operator_arg_stringify(self.comparator.left)
        right = operator_arg_stringify(self.comparator.right)
        return to_text + ".filter(|x| x." + left + " " + op + " " + right + ")"


class GroupOperator(Operator):
    ibis_api_name = "group_by"
    bys: list[Node]
    table: typ.relations.Table

    def __init__(self, table: typ.relations.Table, bys: list[Node]):
        self.bys = bys
        self.table = table

    def generate(self, to_text: str) -> str:
        mid = to_text + ""
        for by in self.bys:
            # test if multiple consecutive group_by's have same effect (noir only supports one arg)
            by = operator_arg_stringify(by)
            mid += ".group_by(|x| x." + by + ".clone())"
        return mid


class MapOperator(Operator):
    ibis_api_name = "mutate"
    math_ops = {"Multiply": "*", "Add": "+", "Subtract": "-"}
    table: typ.relations.Table
    mapper: Node
    other_operators: list[Operator]

    def __init__(self, table: typ.relations.Table, mapper: Node, other_operators: list[Operator]):
        self.mapper = mapper
        self.table = table
        self.other_operators = other_operators

    def generate(self, to_text: str) -> str:
        op = self.math_ops[type(self.mapper).__name__]
        left = operator_arg_stringify(self.mapper.left)
        right = operator_arg_stringify(self.mapper.right)
        mid = to_text + ".map(|x| x."

        mid = fix_prev_group_by(mid, self, self.other_operators)

        mid += left + " " + op + " " + right + ")"
        return mid


def fix_prev_group_by(text: str, self: Operator, others: list[Operator]) -> str:
    # alternative: instead of this, could decide to follow every .group_by with a .map(|x| x.1) to make noir behave
    # in the same way as ibis would

    idx = others.index(self)
    if isinstance(others[idx - 1], GroupOperator):
        # if map preceded by group_by trivially adding because of noir
        # implementation ("grouped" (old_tuple))
        return text + "1."
    return text


class ReduceOperator(Operator):
    ibis_api_name = "aggregate"
    aggr_ops = {"Max": "max", "Min": "min", "Sum": "+"}
    reducer: Node

    def __init__(self, reducer: Node):
        self.reducer = reducer

    def generate(self, to_text: str) -> str:
        op = self.aggr_ops[type(self.reducer).__name__]
        # arg = operator_arg_stringify(arg, table)  # unused: noir doesn't require to specify column,
        # aggregation depends on previous step
        return to_text + ".reduce(|a, b| *a = (*a)." + op + "(b))"


class JoinOperator(Operator):
    ibis_api_name = "join"
    join_types = {"InnerJoin": "join", "OuterJoin": "outer_join", "LeftJoin": "left_join"}
    join: ops.relations.Join
    table: typ.relations.Table

    def __init__(self, table: typ.relations.Table, join: ops.relations.Join):
        self.join = join
        self.table = table

    def generate(self, to_text: str) -> str:
        tab = self.join.left.name
        other_tab = self.join.right.name
        equals = self.join.predicates[0]
        col = operator_arg_stringify(equals.left)
        other_col = operator_arg_stringify(equals.right)
        return to_text + f".join({other_tab}, |x| x.{col}, |y| y.{other_col})"



# if operand is literal, return its value
# if operand is table column, return its index in the original table
def operator_arg_stringify(operand) -> str:
    if isinstance(operand, ibis.expr.operations.generic.TableColumn):
        return operand.name
    elif isinstance(operand, ibis.expr.operations.generic.Literal):
        if operand.dtype.name == "String":
            return "\"" + ''.join(filter(str.isalnum, operand.name)) + "\""
        return operand.name
    raise Exception("Unsupported operand type")