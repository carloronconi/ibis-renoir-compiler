from typing import Union, Type

import ibis
from ibis.common.graph import Node
import ibis.expr.types as typ
import ibis.expr.operations as ops
from ibis.expr.operations import DatabaseTable

import codegen.utils as utl
from codegen.struct import Struct


class Operator:
    def generate(self, to_text: str) -> str:
        raise NotImplementedError

    def does_add_struct(self) -> bool:
        return False


class SelectOperator(Operator):

    def __init__(self, node: ops.Selection, operators: list[Operator], structs: list[Struct]):
        self.node = node
        self.structs = structs
        self.columns = []
        for operand in filter(lambda o: isinstance(o, ops.TableColumn), node.__children__):
            self.columns.append(operand)
        self.operators = operators

    def generate(self, to_text: str) -> str:
        new_struct = Struct.from_aggregation(self.node)
        self.structs.append(new_struct)

        mid = to_text
        if is_keyed_stream(self, self.operators):
            mid += ".map(|(_, x)|"
        else:
            mid += ".map(|x| "

        mid += f"{new_struct.name_struct}{{"
        for new_col, col in zip(new_struct.columns, self.columns):
            mid += f"{new_col}: x.{col.name}, "
        mid += "})"

        return mid

    def does_add_struct(self) -> bool:
        return True


class FilterOperator(Operator):
    bin_ops = {"Equals": "==", "Greater": ">", "GreaterEqual": ">=", "Less": "<", "LessEqual": "<="}

    def __init__(self, node: ops.logical.Comparison, operators: list[Operator], structs: list[Struct]):
        self.comparator = node

    def generate(self, to_text: str) -> str:
        op = self.bin_ops[type(self.comparator).__name__]
        left = operator_arg_stringify(self.comparator.left)
        right = operator_arg_stringify(self.comparator.right)
        return to_text + ".filter(|x| x." + left + " " + op + " " + right + ")"


class MapOperator(Operator):
    math_ops = {"Multiply": "*", "Add": "+", "Subtract": "-"}

    def __init__(self, node: ops.core.Alias, operators: list[Operator], structs: list[Struct]):
        self.mapper = node.__children__[0]
        self.operators = operators
        self.structs = structs
        self.node = node

    def generate(self, to_text: str) -> str:
        prev_struct = self.structs[-1]
        cols = prev_struct.columns.copy()
        cols.append(self.node.name)
        typs = prev_struct.types.copy()
        typs.append(self.node.dtype)

        new_struct = Struct.from_args(str(id(self.node)), cols, typs)
        self.structs.append(new_struct)

        op = self.math_ops[type(self.mapper).__name__]
        left = operator_arg_stringify(self.mapper.left)
        right = operator_arg_stringify(self.mapper.right)

        mid = to_text
        if is_keyed_stream(self, self.operators):
            mid += ".map(|(_, x)|"
        else:
            mid += ".map(|x| "

        mid += f"{new_struct.name_struct}{{"
        for new_col, prev_col in zip(new_struct.columns, prev_struct.columns):
            mid += f"{new_col}: x.{prev_col}, "
        mid += f"{self.node.name}: x.{left} {op} {right},"
        mid += "})"

        return mid

    def does_add_struct(self) -> bool:
        return True


class LoneReduceOperator(Operator):
    aggr_ops = {"Sum": "{0}: a.{0} + b.{0}"}

    def __init__(self, node: ops.Aggregation, operators: list[Operator], structs: list[Struct]):
        alias = next(filter(lambda c: isinstance(c, ops.Alias), node.__children__))
        self.reducer = alias.__children__[0]
        self.structs = structs
        self.node = node

    def generate(self, to_text: str) -> str:
        col = operator_arg_stringify(self.reducer.__children__[0])
        op = self.aggr_ops[type(self.reducer).__name__].format(col)

        mid = to_text + f".reduce(|a, b| {self.structs[-1].name_struct}{{{op}, ..a }} )"

        # map after the reduce to conform to ibis renaming reduced column!
        new_struct = Struct.from_aggregation(self.node)
        self.structs.append(new_struct)

        mid += f".map(|x| {new_struct.name_struct}{{{new_struct.columns[0]}: x.{col}}})"

        return mid

    def does_add_struct(self) -> bool:
        return True


class GroupReduceOperator(Operator):
    aggr_ops = {"Max": "a.{0} = max(a.{0}, b.{0})", "Min": "a.{0} = min(a.{0}, b.{0})", "Sum": "a.{0} = a.{0} + b.{0}",
                "First": "a.{0} = a.{0}"}

    def __init__(self, node: ops.Aggregation, operators: list[Operator], structs: list[Struct]):
        self.alias = next(filter(lambda c: isinstance(c, ops.Alias), node.__children__))
        self.reducer = self.alias.__children__[0]
        self.structs = structs
        self.bys = node.by
        self.node = node

    def generate(self, to_text: str) -> str:
        mid = to_text
        for by in self.bys:
            by = operator_arg_stringify(by)
            mid += ".group_by(|x| x." + by + ".clone())"

        col = operator_arg_stringify(self.reducer.__children__[0])
        op = self.aggr_ops[type(self.reducer).__name__].format(col)

        mid += f".reduce(|a, b| {op})"

        last_col_name = self.node.schema.names[-1]
        last_col_type = self.node.schema.types[-1]

        new_struct = Struct.from_args(str(id(self.alias)), [last_col_name], [last_col_type])
        self.structs.append(new_struct)

        mid += f".map(|(_, x)| {new_struct.name_struct}{{{new_struct.columns[0]}: x.{col}}})"

        return mid

    def does_add_struct(self) -> bool:
        return True


class JoinOperator(Operator):
    noir_types = {"InnerJoin": "join", "OuterJoin": "outer_join", "LeftJoin": "left_join"}
    ibis_types = {"InnerJoin": "join", "OuterJoin": "outer_join", "LeftJoin": "left_join"}

    def __init__(self, node: ops.relations.Join, operators: list[Operator], structs: list[Struct]):
        self.join = node
        self.structs = structs
        self.operators = operators

    def generate(self, to_text: str) -> str:
        right_struct = Struct.last_complete_transform
        left_struct = self.structs[-1]

        join_struct: Struct = Struct.from_join(left_struct, right_struct)
        self.structs.append(join_struct)

        equals = self.join.predicates[0]
        col = operator_arg_stringify(equals.left)
        other_col = operator_arg_stringify(equals.right)
        join_t = self.noir_types[type(self.join).__name__]

        result = to_text
        left_keyed_stream = is_keyed_stream(self, self.operators)

        # to discover if right var is a KeyedStream, check if between latest db operator and previous db operator there
        # were any operators that turn Stream into KeyedStream
        i, db = find_operators_database(self, self.operators)
        right_keyed_stream = is_keyed_stream(db, self.operators)

        if left_keyed_stream and not right_keyed_stream:
            result += f".{join_t}({right_struct.name_short}.group_by(|x| x.{col}.clone()))"
        elif left_keyed_stream and right_keyed_stream:
            result += f".{join_t}({right_struct.name_short})"
        else:
            result += f".{join_t}({right_struct.name_short}, |x| x.{col}, |y| y.{other_col})"

        if join_t == "left_join":
            result += ".map(|(_, (x, y))| (x, y.unwrap_or_default()))"
        elif join_t == "outer_join":
            result += ".map(|(_, (x, y))| (x.unwrap_or_default(), y.unwrap_or_default()))"

        result += f".map(|(_, x)| {join_struct.name_struct} {{"
        left_cols = 0
        for col in left_struct.columns:
            result += f"{col}: x.0.{col}, "
            left_cols += 1
        for col, col_r in zip(join_struct.columns[left_cols:], right_struct.columns):
            result += f"{col}: x.1.{col_r}, "
        return result + "})"

    def does_add_struct(self) -> bool:
        return True


class DatabaseOperator(Operator):
    def __init__(self, node: ops.DatabaseTable, operators: list[Operator], structs: list[Struct]):
        self.table = node
        self.structs = structs
        self.operators = operators

    def generate(self, to_text: str) -> str:
        # database operator means that previous table's transforms are over
        # will use this to perform joins
        if len(self.structs) > 0:
            Struct.last_complete_transform = self.structs[-1]

        struct = Struct.from_table(self.table)
        self.structs.append(struct)

        # need to have count_id of last struct produced by this table's transformations:
        # increment this struct's id counter by the number of operations in this table that produce structs
        this_idx = self.operators.index(self)
        end_idx = this_idx + 1
        while end_idx < len(self.operators):
            op = self.operators[end_idx]
            if isinstance(op, DatabaseOperator):
                break
            end_idx += 1
        count_structs = len(list(filter(lambda o: o.does_add_struct(), self.operators[this_idx + 1:end_idx])))

        return (to_text +
                f";\nlet {struct.name_short} = ctx.stream_csv::<{struct.name_struct}>(\"{utl.TAB_FILES[struct.name_long]}\");\n" +
                f"let var_{struct.id_counter + count_structs} = {struct.name_short}")

    def does_add_struct(self) -> bool:
        return True


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


def is_keyed_stream(op: Operator, operators: list[Operator]) -> bool:
    """
    Operator receives KeyedStream if between its DatabaseOperator and itself (itself excluded), there is an operation
    turning the Stream into a KeyedStream: either a GroupReduceOperator or a JoinOperator
    """
    curr_op_idx = operators.index(op)

    db_idx, db = find_operators_database(op, operators)

    for o in operators[db_idx:curr_op_idx]:
        if isinstance(o, GroupReduceOperator) or isinstance(o, JoinOperator):
            return True
    return False


def find_operators_database(op: Operator, operators: list[Operator]) -> tuple[int, DatabaseOperator]:
    curr_op_idx = operators.index(op)

    # find last DatabaseOperator instance before the current operator
    db_idx = 0
    db = None
    for i, o in enumerate(operators[:curr_op_idx]):
        if isinstance(o, DatabaseOperator):
            db_idx = i
            db = o
    return db_idx, db


def find_node_database(node: Node) -> DatabaseTable:
    curr = node
    while curr and not isinstance(curr, DatabaseTable):
        curr = curr.table
    return curr
