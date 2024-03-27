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
    operators: list[Operator]

    def __init__(self, node: Node, operators: list[Operator]):
        for operand in filter(lambda o: isinstance(o, ops.TableColumn), node.__children__):
            self.columns.append(operand)

        self.operators = operators

    def generate(self, to_text: str) -> str:
        mid = to_text
        if is_preceded_by_grouper(self, self.operators):
            mid += ".map(|(_, x)|"
        else:
            mid += ".map(|x| "
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

    def __init__(self, comparator: ops.logical.Comparison):
        self.comparator = comparator

    def generate(self, to_text: str) -> str:
        op = self.bin_ops[type(self.comparator).__name__]
        left = operator_arg_stringify(self.comparator.left)
        right = operator_arg_stringify(self.comparator.right)
        return to_text + ".filter(|x| x." + left + " " + op + " " + right + ")"

    def ibis_api_name(self) -> str:
        return "filter"


class GroupOperator(Operator):
    bys = []
    table: typ.relations.Table

    def __init__(self, operator: ops.relations.Aggregation):
        self.bys = operator.by

    def generate(self, to_text: str) -> str:
        mid = to_text + ""
        for by in self.bys:
            by = operator_arg_stringify(by)
            mid += ".group_by(|x| x." + by + ".clone())"
        return mid

    def ibis_api_name(self) -> str:
        return "group_by"


class MapOperator(Operator):
    math_ops = {"Multiply": "*", "Add": "+", "Subtract": "-"}
    table: typ.relations.Table
    mapper: Node
    operators: list[Operator]

    def __init__(self, operator: Node, operators: list[Operator]):
        self.mapper = operator.__children__[0]
        self.operators = operators

    def generate(self, to_text: str) -> str:
        op = self.math_ops[type(self.mapper).__name__]
        left = operator_arg_stringify(self.mapper.left)
        right = operator_arg_stringify(self.mapper.right)

        if is_preceded_by_grouper(self, self.operators):
            return to_text + f".map(|(_, x)| x.{left} {op} {right})"
        return to_text + f".map(|x| x.{left} {op} {right})"

    def ibis_api_name(self) -> str:
        return "mutate"


class ReduceOperator(Operator):
    aggr_ops = {"Max": "*a = (*a).max(b)", "Min": "*a = (*a).min(b)", "Sum": "*a = *a + b", "First": "*a = *a"}
    reducer: Node

    def __init__(self, node: Node):
        alias = next(filter(lambda c: isinstance(c, ops.Alias), node.__children__))
        self.reducer = alias.__children__[0]

    def generate(self, to_text: str) -> str:
        op = self.aggr_ops[type(self.reducer).__name__]
        # arg = operator_arg_stringify(arg, table)  # unused: noir doesn't require to specify column,
        # aggregation depends on previous step
        return to_text + f".reduce(|a, b| {op})"

    def ibis_api_name(self) -> str:
        return "aggregate"


class JoinOperator(Operator):
    noir_types = {"InnerJoin": "join", "OuterJoin": "outer_join", "LeftJoin": "left_join"}
    ibis_types = {"InnerJoin": "join", "OuterJoin": "outer_join", "LeftJoin": "left_join"}
    join: ops.relations.Join

    def __init__(self,  join: ops.relations.Join):
        self.join = join

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


def is_preceded_by_grouper(op: Operator, operators: list[Operator]) -> bool:
    map_idx = operators.index(op)
    for i, o in zip(range(0, map_idx), operators):
        if isinstance(o, GroupOperator):
            return True
    return False
