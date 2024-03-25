import ibis
from ibis.common.graph import Node
import ibis.expr.types as typ
import ibis.expr.operations as ops
import codegen.utils


class Operator:
    def generate(self, to_text: str) -> str:
        raise NotImplementedError

    def ibis_api_name(self) -> str:
        raise NotImplementedError


class SelectOperator(Operator):
    columns = []
    table: typ.relations.Table

    def __init__(self, table: typ.relations.Table, columns: list[Node]):
        self.columns = columns
        self.table = table

    def generate(self, to_text: str) -> str:
        mid = to_text + ".map(|x| "
        if len(self.columns) == 1:
            mid += f"x.{self.columns[0].name})"
        else:
            mid += "("
            for col in self.columns:
                mid += f"x.{col.name}, "
            mid += "))"
        return mid

    def ibis_api_name(self) -> str:
        return "select"


class FilterOperator(Operator):
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

    def ibis_api_name(self) -> str:
        return "filter"


class GroupOperator(Operator):
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
            # following group_by with drop_key because noir has different behaviour from ibis when performing
            # group_by: instead of just grouping, it turns Stream into KeyedStream: table_cols into tuple
            # (group_by_col,(table_cols)) previously kept the new column and modified mutate/select behaviour
            # following group_by but the approach was unstable
            mid += ".group_by(|x| x." + by + ".clone()).drop_key()"
        return mid

    def ibis_api_name(self) -> str:
        return "group_by"


class MapOperator(Operator):
    math_ops = {"Multiply": "*", "Add": "+", "Subtract": "-"}
    table: typ.relations.Table
    mapper: Node

    def __init__(self, table: typ.relations.Table, mapper: Node):
        self.mapper = mapper
        self.table = table

    def generate(self, to_text: str) -> str:
        op = self.math_ops[type(self.mapper).__name__]
        left = operator_arg_stringify(self.mapper.left)
        right = operator_arg_stringify(self.mapper.right)

        return to_text + f".map(|x| x.{left} {op} {right})"

    def ibis_api_name(self) -> str:
        return "mutate"


class ReduceOperator(Operator):
    aggr_ops = {"Max": "max", "Min": "min", "Sum": "+"}
    reducer: Node

    def __init__(self, reducer: Node):
        self.reducer = reducer

    def generate(self, to_text: str) -> str:
        op = self.aggr_ops[type(self.reducer).__name__]
        # arg = operator_arg_stringify(arg, table)  # unused: noir doesn't require to specify column,
        # aggregation depends on previous step
        return to_text + ".reduce(|a, b| *a = (*a)." + op + "(b))"

    def ibis_api_name(self) -> str:
        return "aggregate"


class JoinOperator(Operator):
    noir_types = {"InnerJoin": "join", "OuterJoin": "outer_join", "LeftJoin": "left_join"}
    ibis_types = {"InnerJoin": "join", "OuterJoin": "outer_join", "LeftJoin": "left_join"}
    join: ops.relations.Join
    table: typ.relations.Table

    def __init__(self, table: typ.relations.Table, join: ops.relations.Join):
        self.join = join
        self.table = table

    def generate(self, to_text: str) -> str:
        other_tab = codegen.utils.TAB_NAMES[self.join.right.name]
        equals = self.join.predicates[0]
        col = operator_arg_stringify(equals.left)
        other_col = operator_arg_stringify(equals.right)
        join_t = self.noir_types[type(self.join).__name__]
        return to_text + f".{join_t}({other_tab}, |x| x.{col}, |y| y.{other_col})"

    def ibis_api_name(self) -> str:
        return self.ibis_types[type(self.join).__name__]


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
