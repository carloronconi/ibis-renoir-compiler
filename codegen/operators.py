import ibis
from ibis.common.graph import Node
import ibis.expr.types as typ
import ibis.expr.operations as ops
import codegen.utils as utl
from codegen.struct import Struct


class Operator:
    def generate(self, to_text: str) -> str:
        raise NotImplementedError

    def ibis_api_name(self) -> str:
        raise NotImplementedError


class SelectOperator(Operator):

    def __init__(self, node: ops.Node, operators: list[Operator], structs: list[Struct]):
        for operand in filter(lambda o: isinstance(o, ops.TableColumn), node.__children__):
            self.columns.append(operand)

        self.operators = operators

    def generate(self, to_text: str) -> str:
        self.structs.append(Struct())
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

    def __init__(self, node: ops.logical.Comparison, operators: list[Operator], structs: list[Struct]):
        self.comparator = node

    def generate(self, to_text: str) -> str:
        op = self.bin_ops[type(self.comparator).__name__]
        left = operator_arg_stringify(self.comparator.left)
        right = operator_arg_stringify(self.comparator.right)
        return to_text + ".filter(|x| x." + left + " " + op + " " + right + ")"

    def ibis_api_name(self) -> str:
        return "filter"


class GroupOperator(Operator):

    def __init__(self, node: ops.relations.Aggregation, operators: list[Operator], structs: list[Struct]):
        self.bys = node.by

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

    def __init__(self, node: ops.Node, operators: list[Operator], structs: list[Struct]):
        self.mapper = node.__children__[0]
        self.operators = operators
        self.structs = structs

    def generate(self, to_text: str) -> str:
        self.structs.append(Struct.from_alias())

        op = self.math_ops[type(self.mapper).__name__]
        left = operator_arg_stringify(self.mapper.left)
        right = operator_arg_stringify(self.mapper.right)

        if is_preceded_by_grouper(self, self.operators):
            return to_text + f".map(|(_, x)| x.{left} {op} {right})"
        return to_text + f".map(|x| x.{left} {op} {right})"

    def ibis_api_name(self) -> str:
        return "mutate"


class ReduceOperator(Operator):
    aggr_ops = {"Max": "a.{0} = max(a.{0}, b.{0})", "Min": "a.{0} = min(a.{0}, b.{0})", "Sum": "a.{0} = a.{0} + b.{0}",
                "First": "a.{0} = a.{0}"}

    def __init__(self, node: ops.Node, operators: list[Operator], structs: list[Struct]):
        alias = next(filter(lambda c: isinstance(c, ops.Alias), node.__children__))
        self.reducer = alias.__children__[0]

    def generate(self, to_text: str) -> str:
        col = operator_arg_stringify(self.reducer.__children__[0])
        op = self.aggr_ops[type(self.reducer).__name__].format(col)
        return to_text + f".reduce(|a, b| {op})"

    def ibis_api_name(self) -> str:
        return "aggregate"


class JoinOperator(Operator):
    noir_types = {"InnerJoin": "join", "OuterJoin": "outer_join", "LeftJoin": "left_join"}
    ibis_types = {"InnerJoin": "join", "OuterJoin": "outer_join", "LeftJoin": "left_join"}

    def __init__(self, node: ops.relations.Join, operators: list[Operator], structs: list[Struct]):
        self.join = node

    def generate(self, to_text: str) -> str:
        other_tab = utl.TAB_NAMES[self.join.right.name]
        equals = self.join.predicates[0]
        col = operator_arg_stringify(equals.left)
        other_col = operator_arg_stringify(equals.right)
        join_t = self.noir_types[type(self.join).__name__]
        return to_text + f".{join_t}({other_tab}, |x| x.{col}, |y| y.{other_col})"

    def ibis_api_name(self) -> str:
        return self.ibis_types[type(self.join).__name__]


class DatabaseOperator(Operator):
    def __init__(self, node: ops.DatabaseTable, operators: list[Operator], structs: list[Struct]):
        self.table = node
        self.structs = structs

    def generate(self, to_text: str) -> str:
        struct = Struct.from_table(self.table)
        self.structs.append(struct)
        return (to_text +
                f"let {struct.name_short} = ctx.stream_csv::<Cols_{struct.name_short}>(\"{utl.TAB_FILES[struct.name_long]}\");\n" +
                self.structs[0].name_short)


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
