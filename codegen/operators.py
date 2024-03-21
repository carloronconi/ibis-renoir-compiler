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

    def __init__(self, table: typ.relations.Table,  columns: list[Node]):
        self.columns = columns
        self.table = table

    def generate(self, to_text: str) -> str:
        mid = to_text + ".map(|x| "
        if len(self.columns) == 1:
            index = self.table.columns.index(self.columns[0].name)
            mid += "x." + str(index) + ")"
        else:
            mid += "("
            for col in self.columns:
                index = self.table.columns.index(col.name)
                mid += "x." + str(index) + ", "
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
        left = operator_arg_stringify(self.comparator.left, self.table)
        right = operator_arg_stringify(self.comparator.right, self.table)
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
            by = operator_arg_stringify(by, self.table)
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
        left = operator_arg_stringify(self.mapper.left, self.table)
        right = operator_arg_stringify(self.mapper.right, self.table)
        mid = to_text + ".map(|x| x."

        idx = self.other_operators.index(self)
        if isinstance(self.other_operators[idx - 1], GroupOperator):
            # if map preceded by group_by trivially adding because of noir
            # implementation ("grouped" (old_tuple))
            mid += "1."
        mid += left + " " + op + " " + right + ")"
        return mid


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
        mid = to_text + ".join("
        pass


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