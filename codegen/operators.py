import ibis
import ibis.expr.operations as ops
from ibis.common.graph import Node

import codegen.utils as utl
from codegen.struct import Struct


class Operator:
    operators = []

    def __init__(self):
        Operator.operators.append(self)

    @classmethod
    def from_node(cls, node: Node):
        match node:
            case ops.PhysicalTable():
                return DatabaseOperator(node)
            case ops.relations.Join():
                return JoinOperator(node)
            case ops.relations.Aggregation() if any(isinstance(x, ops.core.Alias) for x in node.__children__):
                if any(isinstance(x, ops.TableColumn) for x in node.__children__):
                    return GroupReduceOperator(node)  # group_by().reduce()
                else:
                    return LoneReduceOperator(node)
            case ops.logical.Comparison() if any(isinstance(c, ops.Literal) for c in node.__children__):
                return FilterOperator(node)
            case ops.core.Alias() if any(isinstance(c, ops.numeric.NumericBinary)
                                         # and not any(isinstance(cc, ops.WindowFunction) for cc in c.__children__)
                                         for c in node.__children__):
                return MapOperator(node)
            case ops.relations.Selection() if (any(isinstance(c, ops.TableColumn) for c in node.__children__)
                                               and not any(isinstance(c, ops.Join) for c in node.__children__)):
                return SelectOperator(node)
            case ops.core.Alias() if any(isinstance(c, ops.WindowFunction) for c in node.__children__):
                return WindowOperator(node)

    @classmethod
    def new_top(cls):
        return TopOperator()

    @classmethod
    def new_bot(cls):
        return BotOperator()

    @classmethod
    def cleanup(cls):
        cls.operators = []

    def generate(self) -> str:
        raise NotImplementedError

    def does_add_struct(self) -> bool:
        return False


class SelectOperator(Operator):

    def __init__(self, node: ops.Selection):
        self.node = node
        self.columns = []
        for operand in filter(lambda o: isinstance(o, ops.TableColumn), node.__children__):
            self.columns.append(operand)
        super().__init__()

    def generate(self) -> str:
        new_struct = Struct.from_relation(self.node)

        mid = ""
        if new_struct.is_keyed_stream:
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
    bin_ops = {"Equals": "==", "Greater": ">",
               "GreaterEqual": ">=", "Less": "<", "LessEqual": "<="}

    def __init__(self, node: ops.logical.Comparison):
        self.comparator = node
        super().__init__()

    def generate(self) -> str:
        op = self.bin_ops[type(self.comparator).__name__]
        left = operator_arg_stringify(self.comparator.left)
        right = operator_arg_stringify(self.comparator.right)
        if Struct.last().is_col_nullable(left):
            return f".filter(|x| x.{left}.clone().is_some_and(|v| v {op} {right}))"
        return f".filter(|x| x.{left} {op} {right})"


class MapOperator(Operator):

    def __init__(self, node: ops.core.Alias):
        self.mapper = node.__children__[0]
        self.node = node
        super().__init__()

    def generate(self) -> str:
        prev_struct = Struct.last()
        cols = prev_struct.columns.copy()
        cols.append(self.node.name)
        typs = prev_struct.types.copy()
        typs.append(self.node.dtype)

        new_struct = Struct.from_args(str(id(self.node)), cols, typs)

        mid = ""
        if new_struct.is_keyed_stream:
            mid += ".map(|(_, x)|"
        else:
            mid += ".map(|x| "

        mid += f"{new_struct.name_struct}{{"
        for new_col, prev_col in zip(new_struct.columns, prev_struct.columns):
            mid += f"{new_col}: x.{prev_col}, "

        num_ops = operator_arg_stringify(self.mapper, True, "x")
        mid += f"{self.node.name}: {num_ops},}})"

        return mid

    def does_add_struct(self) -> bool:
        return True


class LoneReduceOperator(Operator):
    aggr_ops = {"Sum": "+"}

    def __init__(self, node: ops.Aggregation):
        alias = next(filter(lambda c: isinstance(
            c, ops.Alias), node.__children__))
        self.reducer = alias.__children__[0]
        self.node = node
        super().__init__()

    def generate(self) -> str:
        col = operator_arg_stringify(self.reducer.__children__[0])
        op = self.aggr_ops[type(self.reducer).__name__]

        is_reduced_col_nullable = Struct.last().is_col_nullable(col)
        if is_reduced_col_nullable:
            mid = (f".reduce(|a, b| {Struct.last().name_struct}{{"
                   f"{col}: a.{col}.zip(b.{col}).map(|(x, y)| x {op} y), ..a }} )")
        else:
            mid = f".reduce(|a, b| {Struct.last().name_struct}{{{col}: a.{col} {op} b.{col}, ..a }} )"

        # map after the reduce to conform to ibis renaming reduced column!
        new_struct = Struct.from_relation(self.node)

        if is_reduced_col_nullable:
            mid += f".map(|x| {new_struct.name_struct}{{{new_struct.columns[0]}: x.{col}}})"
        else:
            mid += f".map(|x| {new_struct.name_struct}{{{new_struct.columns[0]}: Some(x.{col})}})"

        return mid

    def does_add_struct(self) -> bool:
        return True


class GroupReduceOperator(Operator):
    aggr_ops = {"Max": "max(x, y)", "Min": "min(x, y)", "Sum": "x + y",
                "First": "x"}
    aggr_ops_form = {"Max": "a.{0} = max(a.{0}, b.{0})", "Min": "a.{0} = min(a.{0}, b.{0})",
                     "Sum": "a.{0} = a.{0} + b.{0}",
                     "First": "a.{0} = a.{0}"}

    def __init__(self, node: ops.Aggregation):
        self.alias = next(
            filter(lambda c: isinstance(c, ops.Alias), node.__children__))
        self.reducer = self.alias.__children__[0]
        self.bys = node.by
        self.node = node
        super().__init__()

    def generate(self) -> str:
        mid = ""
        for by in self.bys:
            by = operator_arg_stringify(by)
            mid += ".group_by(|x| x." + by + ".clone())"

        col = operator_arg_stringify(self.reducer.__children__[0])

        is_reduced_col_nullable = Struct.last().is_col_nullable(col)
        if is_reduced_col_nullable:
            op = self.aggr_ops[type(self.reducer).__name__]
            mid += f".reduce(|a, b| {{a.{col} = a.{col}.zip(b.{col}).map(|(x, y)| {op});}})"
        else:
            op = self.aggr_ops_form[type(self.reducer).__name__].format(col)
            mid += f".reduce(|a, b| {op})"

        last_col_name = self.node.schema.names[-1]
        last_col_type = self.node.schema.types[-1]

        Struct.with_keyed_stream = (self.bys[0].name, self.bys[0].dtype)
        new_struct = Struct.from_args(
            str(id(self.alias)), [last_col_name], [last_col_type])

        # when reducing, ibis turns results of non-nullable types to nullable! So new_struct will always have nullable
        # field while reduced col could have been either nullable or non-nullable
        if is_reduced_col_nullable:
            mid += f".map(|(_, x)| {new_struct.name_struct}{{{new_struct.columns[0]}: x.{col}}})"
        else:
            mid += f".map(|(_, x)| {new_struct.name_struct}{{{new_struct.columns[0]}: Some(x.{col})}})"

        return mid

    def does_add_struct(self) -> bool:
        return True


class JoinOperator(Operator):
    noir_types = {"InnerJoin": "join",
                  "OuterJoin": "outer_join", "LeftJoin": "left_join"}
    ibis_types = {"InnerJoin": "join",
                  "OuterJoin": "outer_join", "LeftJoin": "left_join"}

    def __init__(self, node: ops.relations.Join):
        self.join = node
        super().__init__()

    def generate(self) -> str:
        right_struct = Struct.last_complete_transform
        left_struct = Struct.last()

        equals = self.join.predicates[0]
        left_col = operator_arg_stringify(equals.left)
        right_col = operator_arg_stringify(equals.right)
        join_t = self.noir_types[type(self.join).__name__]

        Struct.with_keyed_stream = (equals.left.name, equals.left.dtype)
        join_struct, cols_turned_nullable = Struct.from_join(
            left_struct, right_struct)

        if left_struct.is_keyed_stream and not right_struct.is_keyed_stream:  # make right struct KS
            result = f".{join_t}({right_struct.name_short}.group_by(|x| x.{right_col}.clone()))"
        elif not left_struct.is_keyed_stream and right_struct.is_keyed_stream:  # make left struct KS
            result = f".group_by(|x| x.{left_col}.clone()).{join_t}({right_struct.name_short})"
        elif left_struct.is_keyed_stream and right_struct.is_keyed_stream:
            result = f".{join_t}({right_struct.name_short})"
        else:  # neither is keyed stream
            result = f".{join_t}({right_struct.name_short}, |x| x.{left_col}.clone(), |y| y.{right_col}.clone())"

        if join_t == "left_join":
            result += f".map(|(_, x)| {{\nlet mut v = {join_struct.name_struct} {{"
            result += self.fill_join_struct_fields_with_join_struct(left_struct.columns, left_struct.columns,
                                                                    cols_turned_nullable)
            result += self.fill_join_struct_fields_with_none(
                join_struct.columns[len(left_struct.columns):])
            result += "};\nif let Some(i) = x.1 {\n"
            result += self.fill_join_struct_fields_with_join_struct(join_struct.columns[len(left_struct.columns):],
                                                                    right_struct.columns, cols_turned_nullable,
                                                                    is_left=False,
                                                                    is_if_let=True)
            result += "};\nv})"

        elif join_t == "outer_join":
            result += f".map(|(_, x)| {{\nlet mut v = {join_struct.name_struct} {{"
            result += self.fill_join_struct_fields_with_none(
                join_struct.columns)
            result += "};\nif let Some(i) = x.0 {\n"
            result += self.fill_join_struct_fields_with_join_struct(join_struct.columns, left_struct.columns,
                                                                    cols_turned_nullable, is_if_let=True)
            result += "};\nif let Some(i) = x.1 {\n"
            result += self.fill_join_struct_fields_with_join_struct(join_struct.columns[len(left_struct.columns):],
                                                                    right_struct.columns, cols_turned_nullable,
                                                                    is_left=False,
                                                                    is_if_let=True)
            result += "};\nv})"

        else:  # inner join
            result += f".map(|(_, x)| {join_struct.name_struct} {{"
            result += self.fill_join_struct_fields_with_join_struct(left_struct.columns, left_struct.columns,
                                                                    cols_turned_nullable)
            result += self.fill_join_struct_fields_with_join_struct(join_struct.columns[len(left_struct.columns):],
                                                                    right_struct.columns, cols_turned_nullable,
                                                                    is_left=False)
            result += "})"
        return result

    @staticmethod
    def fill_join_struct_fields_with_join_struct(struct_cols, joined_cols, cols_turned_nullable, is_left=True, is_if_let=False) -> str:
        result = ""
        s_col_pref = "v." if is_if_let else ""
        j_col_pref = "i." if is_if_let else "x.0." if is_left else "x.1."
        assign_symbol = "=" if is_if_let else ":"
        sep_symbol = ";" if is_if_let else ","
        for s_col, j_col in zip(struct_cols, joined_cols):
            if s_col in cols_turned_nullable:
                result += f"{s_col_pref}{s_col} {assign_symbol} Some({j_col_pref}{j_col}){sep_symbol} "
            else:
                result += f"{s_col_pref}{s_col} {assign_symbol} {j_col_pref}{j_col}{sep_symbol} "
        return result

    @staticmethod
    def fill_join_struct_fields_with_none(struct_cols) -> str:
        result = ""
        for s_col in struct_cols:
            result += f"{s_col}: None, "
        return result

    def does_add_struct(self) -> bool:
        return True


class WindowOperator(Operator):
    fold_func_type_init = {ibis.dtype(
        "int64"): "Some(0)", ibis.dtype("float64"): "Some(0.0)"}

    class FoldFunc:
        def __init__(self, struct_fields={}, fold_actions={}, map_actions={}):
            self.struct_fields = struct_fields
            self.fold_actions = fold_actions
            self.map_actions = map_actions

    def __init__(self, node: ops.WindowFunction):
        self.alias = node
        super().__init__()

    def generate(self) -> str:
        window = self.alias.arg
        frame = window.frame
        if frame.start and frame.start.following:
            raise Exception(
                "Following window frames are not supported in noir!")

        text = ""

        # generate .group_by if needed
        if (group_by := frame.group_by):
            by = group_by[0]
            col = operator_arg_stringify(by)
            text += f".group_by(|x| x.{col}.clone())"
            # if we have group_by, .fold will generate a KeyedStream so
            # we set Struct.with_keyed_stream with key's name/type so following
            # map knows how to handle it
            Struct.with_keyed_stream = (by.name, by.dtype)
        # otherwise, we still need to group_by to have fold keep row distinct
        # else:
        #     text += ".group_by(|_| Some(\"key\".to_string()))"
        #     Struct.with_keyed_stream = ("string", ibis.dtype("string"))

        # if no start, it means we need to aggregate over all values within each group,
        # so no window is required
        # generate .window with size depending on how many preceding rows are included
        # and fixed step of 1 and exact framing
        if frame.start:
            size = frame.start.value.value + 1
            if frame.group_by:
                text += f".window(CountWindow::new({size}, 1, true))"
            else:
                text += f".window_all(CountWindow::new({size}, 1, true))"

        prev_struct = Struct.last()

        match type(window.func).__name__:
            case "Sum":
                op = WindowOperator.FoldFunc(struct_fields={self.alias.name: self.alias.dtype},
                                             fold_actions={self.alias.name: "acc.{0} = acc.{0}.zip(x.{1}).map(|(a, b)| a + b);"})
            case "Mean":
                op = WindowOperator.FoldFunc(struct_fields={"grp_sum": ibis.dtype("int64"),
                                                            "grp_count": ibis.dtype("int64"),
                                                            self.alias.name: self.alias.dtype},
                                             fold_actions={"grp_sum": "acc.grp_sum = acc.grp_sum.zip(x.{1}).map(|(a, b)| a + b);",
                                                           "grp_count": "acc.grp_count = acc.grp_count.map(|v| v + 1);"},
                                             map_actions={self.alias.name: "{0}: x.grp_sum.zip(x.grp_count).map(|(a, b)| a as f64 / b as f64),"})

        # create the new struct by adding struct_fields to previous struct's columns
        new_cols_types = dict(prev_struct.cols_types)
        for n, t in op.struct_fields.items():
            new_cols_types[n] = t
        new_struct = Struct.from_args_dict(str(id(window)), new_cols_types)

        # generate .fold to apply the reduction function while maintaining other row fields
        text += f".fold({new_struct.name_struct}{{"
        # fold accumulator initialization
        for col in prev_struct.columns:
            text += f"{col}: None, "
        for col, typ in op.struct_fields.items():
            text += f"{col}: {self.fold_func_type_init[typ]}, "
        # fold update step
        text += "}, |acc, x| {"
        for col in prev_struct.columns:
            text += f"acc.{col} = x.{col}; "
        arg = window.func.args[0].name
        for col, action in op.fold_actions.items():
            text += action.format(col, arg)
        text += "},)"

        # generate .map required by some window functions
        if op.map_actions:
            if new_struct.is_keyed_stream:
                text += ".map(|(_, x)| "
            else:
                text += ".map(|x| "
            text += f"{new_struct.name_struct}{{"
            for col, action in op.map_actions.items():
                text += action.format(col)
            text += "..x })"

        # folding a WindowedStream without group_by still produces a KeyedStream, with unit tuple () as key
        # so discard it in that case
        if frame.start and not frame.group_by:
            text += ".drop_key()"

        return text

    def does_add_struct(self) -> bool:
        return True


class DatabaseOperator(Operator):
    def __init__(self, node: ops.DatabaseTable):
        self.table = node
        super().__init__()

    def generate(self) -> str:
        # database operator means that previous table's transforms are over
        # will use this to perform joins
        Struct.transform_completed()
        Struct.with_keyed_stream = None
        struct = Struct.from_relation(self.table)

        # need to have count_id of last struct produced by this table's transformations:
        # increment this struct's id counter by the number of operations in this table that produce structs
        this_idx = self.operators.index(self)
        end_idx = this_idx + 1
        while end_idx < len(self.operators):
            op = self.operators[end_idx]
            if isinstance(op, DatabaseOperator):
                break
            end_idx += 1
        count_structs = len(list(
            filter(lambda o: o.does_add_struct(), self.operators[this_idx + 1:end_idx])))

        return (f";\nlet {struct.name_short} = ctx.stream_csv::<{struct.name_struct}>(\"{utl.TAB_FILES[struct.name_long]}\");\n" +
                f"let var_{struct.id_counter + count_structs} = {struct.name_short}")

    def does_add_struct(self) -> bool:
        return True


class TopOperator(Operator):
    def __init__(self):
        super().__init__()

    def generate(self) -> str:
        # cleanup operators: TopOperator should be the last to generate
        Operator.cleanup()

        with open(utl.ROOT_DIR + "/noir-template/main_top.rs") as f:
            top = f.read()
        for st in Struct.structs:
            top += st.generate()

        # cleanup structs: same reason
        Struct.cleanup()

        top += "\nfn logic(ctx: StreamContext) {\n"
        return top


class BotOperator(Operator):
    def __init__(self):
        super().__init__()

    def generate(self) -> str:
        last_struct = Struct.last()
        bot = f"; let out = {last_struct.name_short}.collect_vec();"
        bot += "\ntracing::info!(\"starting execution\");\nctx.execute_blocking();\nlet out = out.get().unwrap();\n"

        if last_struct.is_keyed_stream:
            col_name, col_type = Struct.last().with_keyed_stream
            new_struct = Struct.from_args(str(id(self)), [col_name], [
                                          col_type], with_name_short="collect")
            bot += (f"let out = out.iter().map(|(k, v)| ({new_struct.name_struct}{{{col_name}: k.clone()}}, "
                    f"v)).collect::<Vec<_>>();")

        with open(utl.ROOT_DIR + "/noir-template/main_bot.rs") as f:
            bot += f.read()
        return bot


# if operand is literal, return its value
# if operand is table column, return its index in the original table
# if resolve_optionals_to_some we're recursively resolving a binary operation and also need struct_name
def operator_arg_stringify(operand: Node, recursively=False, struct_name=None) -> str:
    math_ops = {"Multiply": "*", "Add": "+", "Subtract": "-", "Divide": "/"}
    if isinstance(operand, ibis.expr.operations.generic.TableColumn):
        if recursively:
            return f"{struct_name}.{operand.name}"
        return operand.name
    elif isinstance(operand, ibis.expr.operations.generic.Literal):
        if operand.dtype.name == "String":
            return "\"" + ''.join(filter(str.isalnum, operand.name)) + "\""
        return operand.name
    elif isinstance(operand, ops.numeric.NumericBinary):
        # resolve recursively

        # for ibis, dividing two int64 results in a float64, but for noir it's still int64
        # so we need to cast the operands
        cast = ""
        if type(operand).__name__ == "Divide":
            cast = "as f64"
        # for ibis, any operation including a float64 (e.g. a int64 with a float64) produces a float64
        # so any time the result is a float64, we cast all its operands (casting f64 to f64 has no effect)
        if operand.dtype.name == "Float64":
            cast = "as f64"

        # careful: ibis considers literals as optionals, while in noir a numeric literal is not an Option<T>
        is_left_nullable = operand.left.dtype.nullable and not isinstance(
            operand.left, ops.Literal)
        is_right_nullable = operand.right.dtype.nullable and not isinstance(
            operand.right, ops.Literal)
        if is_left_nullable and is_right_nullable:
            result = f"{operator_arg_stringify(operand.left, recursively, struct_name)}\
                    .zip({operator_arg_stringify(operand.right, recursively, struct_name)})\
                    .map(|(a, b)| a {cast} {math_ops[type(operand).__name__]} b {cast})"
        elif is_left_nullable and not is_right_nullable:
            result = f"{operator_arg_stringify(operand.left, recursively, struct_name)}\
                    .map(|v| v {cast} {math_ops[type(operand).__name__]} {operator_arg_stringify(operand.right, recursively, struct_name)} {cast})"
        elif not is_left_nullable and is_right_nullable:
            result = f"{operator_arg_stringify(operand.right, recursively, struct_name)}\
                    .map(|v| {operator_arg_stringify(operand.left, recursively, struct_name)} {cast} {math_ops[type(operand).__name__]} v {cast})"
        else:
            result = f"{operator_arg_stringify(operand.left, recursively, struct_name)} {cast} {math_ops[type(operand).__name__]} {operator_arg_stringify(operand.right, recursively, struct_name)} {cast}"

        return result
    elif isinstance(operand, ops.WindowFunction):
        # alias is above WindowFunction and not dependency, but because .map follows .fold
        # in this case, we can get the column added by the DatabaseOperator in the struct
        # it just created, which is second to last (last is new col added by MapOperator)
        # since python 3.7, dict maintains insertion order
        return f"{struct_name}.{Struct.last().columns[-2]}"
    raise Exception(f"Unsupported operand type: {operand}")
