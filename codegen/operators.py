import ibis
import ibis.expr.operations as ops
from ibis.common.graph import Node

import codegen.utils as utl
from codegen.struct import Struct
from ibis.expr.datatypes.core import DataType


class Operator:
    operators = []
    # when two operators use the same root node for recognition, increase this
    # value to prioritize one over the other and change ordeding of operators in noir code
    priority = 0

    def __init__(self):
        Operator.operators.append(self)

    @classmethod
    def from_node(cls, node: Node):
        # recursively find subclasses to include only leaves
        operator_classes = []
        stack = [cls]
        while stack:
            curr = stack.pop()
            subclasses = sorted(curr.__subclasses__(),
                                key=lambda x: x.priority)
            if subclasses:
                stack.extend(subclasses)
            else:
                operator_classes.append(curr)

        for Op in operator_classes:
            Op.recognize(node)

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

    @classmethod
    def recognize(cls, node: Node):
        return


class SelectOperator(Operator):

    def __init__(self, node: ops.Selection):
        self.node = node
        self.columns = [c for c in node.__children__ if isinstance(c, ops.TableColumn)]
        super().__init__()

    def generate(self) -> str:
        new_struct = Struct.from_relation(self.node)

        mid = ""
        if new_struct.is_keyed_stream:
            mid += ".map(|(_, x)|"
        else:
            mid += ".map(|x| "

        mid += f"{new_struct.name_struct}{{"
        for col in new_struct.columns:
            mid += f"{col}: x.{col}, "
        mid += "})"

        return mid

    def does_add_struct(self) -> bool:
        return True

    @classmethod
    def recognize(cls, node: Node):
        if (isinstance(node, ops.relations.Selection) and
                (any(isinstance(c, ops.TableColumn) for c in node.__children__) and
                 not any(isinstance(c, ops.Join) for c in node.__children__))):
            return cls(node)


class FilterOperator(Operator):
    priority = 1

    def __init__(self, node: ops.logical.Comparison):
        self.comparator = node
        super().__init__()

    def generate(self) -> str:
        filter_expr = operator_arg_stringify(self.comparator, struct_name="x")
        if Struct.last().is_keyed_stream:
            return f".filter(|(_, x)| {filter_expr})"
        return f".filter(|x| {filter_expr})"

    @classmethod
    def recognize(cls, node: Node):

        def is_equals_col_lit_or_col_col(node: Node) -> bool:
            if not (isinstance(node, ops.logical.Comparison) and len(node.__children__) == 2):
                return False
            left, right = node.__children__[0], node.__children__[1]
            if isinstance(left, ops.TableColumn) and isinstance(right, ops.Literal):
                return True
            if isinstance(left, ops.Literal) and isinstance(right, ops.TableColumn):
                return True
            if isinstance(left, ops.TableColumn) and isinstance(right, ops.TableColumn):
                return True
            return False

        if not (isinstance(node, ops.Selection) or isinstance(node, ops.Aggregation)):
            return
        equalses = list(filter(is_equals_col_lit_or_col_col, node.__children__))
        log_bins = list(filter((lambda c: isinstance(c, ops.logical.LogicalBinary) and any(
            is_equals_col_lit_or_col_col(cc) for cc in c.__children__)), node.__children__))

        for eq in equalses:
            cls(eq)

        for lb in log_bins:
            cls(lb)


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

        cols_to_copy = prev_struct.columns.copy()
        # when WindowFunction is below same alias as map's, it produces struct with col with the same name
        # so we need to use that as the argument for the map's calculation and avoid copying it
        if len(prev_struct.columns) == len(new_struct.columns):
            cols_to_copy.pop()

        for col in cols_to_copy:
            mid += f"{col}: x.{col}, "

        # override WindowFunction node resolution, so in case WindowFunction is below mapper, it will be resolved
        # to prev struct's last col name for reason above
        num_ops = operator_arg_stringify(
            self.mapper, "x", window_resolve=prev_struct.columns[-1])
        mid += f"{self.node.name}: {num_ops},}})"

        return mid

    def does_add_struct(self) -> bool:
        return True

    @classmethod
    def recognize(cls, node: Node):
        if (isinstance(node, ops.core.Alias) and
                any(isinstance(c, ops.numeric.NumericBinary)
                    # and not any(isinstance(cc, ops.WindowFunction) for cc in c.__children__)
                    for c in node.__children__)):
            return cls(node)


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

    @classmethod
    def recognize(cls, node: Node):
        if (isinstance(node, ops.relations.Aggregation) and
                any(isinstance(x, ops.core.Alias) for x in node.__children__) and
                not any(isinstance(x, ops.TableColumn) for x in node.__children__)):
            return cls(node)


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
        aggr_name = type(self.reducer).__name__
        bys = [operator_arg_stringify(by) for by in self.bys]
        col = operator_arg_stringify(self.reducer.__children__[0])
        is_reduced_col_nullable = Struct.last().is_col_nullable(col)

        # this group_by follows another, so drop_key is required
        if Struct.last().is_keyed_stream:
            mid += ".drop_key()"

        # for Mean we use specific `.group_by_avg` method, to avoid needing to keep sum and count and
        # then divide them in the end and map to new struct
        if aggr_name == "Mean":
            mid += ".group_by_avg(|x| ("
            for by in bys:
                mid += f"x.{by}.clone(), "
            # remove last comma and space
            mid = mid[:-2]
            mid += "), |x| "
            if is_reduced_col_nullable:
                mid += f"x.{col}.unwrap_or(0) as f64)"
            else:
                mid += f"x.{col} as f64)"

        # simple cases with only one accumulator required
        else:
            mid += ".group_by(|x| ("
            for by in bys:
                mid += f"x.{by}.clone(), "
            # remove last comma and space
            mid = mid[:-2]
            mid += "))"

            if is_reduced_col_nullable:
                op = self.aggr_ops[aggr_name]
                mid += f".reduce(|a, b| {{a.{col} = a.{col}.zip(b.{col}).map(|(x, y)| {op});}})"
            else:
                op = self.aggr_ops_form[aggr_name].format(col)
                mid += f".reduce(|a, b| {op})"

        bys_n_t = {b.name: b.dtype for b in self.bys}
        aggr_col_name = self.node.schema.names[-1]
        aggr_col_type = self.node.schema.types[-1]

        Struct.with_keyed_stream = bys_n_t
        # new struct will contain the aggregated field plus the "by" fields also preserved by
        # the key of the keyed stream, kept in both to be able to drop_key if other group_by follows
        new_struct = Struct.from_args(
            str(id(self.alias)), list(bys_n_t.keys()) + [aggr_col_name], list(bys_n_t.values()) + [aggr_col_type])

        mid += f".map(|(k, x)| {new_struct.name_struct}{{"
        if len(bys_n_t) == 1:
            mid += f"{new_struct.columns[0]}: k.clone(),"
        else: 
            for i, column in enumerate(new_struct.columns):
                # skip last
                if i == len(new_struct.columns) - 1:
                    break
                mid += f"{column}: k.{i}, "
        # when reducing, ibis turns results of non-nullable types to nullable! So new_struct will always have nullable
        # field while reduced col could have been either nullable or non-nullable
        if aggr_name == "Mean":
            # in this case aggregation produces single result, not struct
            # and x is always an f64 (not nullable)
            mid += f"{new_struct.columns[-1]}: Some(x)}})"
        elif is_reduced_col_nullable:
            mid += f"{new_struct.columns[-1]}: x.{col}}})"
        else:
            mid += f"{new_struct.columns[-1]}: Some(x.{col})}})"

        return mid

    def does_add_struct(self) -> bool:
        return True

    @classmethod
    def recognize(cls, node: Node):
        if (isinstance(node, ops.relations.Aggregation) and
                any(isinstance(x, ops.core.Alias) for x in node.__children__) and
                any(isinstance(x, ops.TableColumn) for x in node.__children__)):
            return cls(node)


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
        # ibis has left and right switched
        left_col = operator_arg_stringify(equals.right)
        right_col = operator_arg_stringify(equals.left)
        join_t = self.noir_types[type(self.join).__name__]

        Struct.with_keyed_stream = {equals.left.name: equals.left.dtype}
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

    @classmethod
    def recognize(cls, node: Node):
        if isinstance(node, ops.relations.Join):
            return cls(node)


class WindowOperator(Operator):
    priority = 1

    def generate(self) -> str:
        # abstract class so should not actually be used
        raise NotImplementedError

    def __init__(self, node: ops.WindowFunction):
        self.alias = node
        self.window = self.find_window_func_from_alias(node)
        super().__init__()

    def does_add_struct(self) -> bool:
        return True

    @staticmethod
    def find_window_func_from_alias(alias: ops.core.Alias) -> ops.WindowFunction:
        stack = []
        stack.append(alias)

        while stack:
            curr = stack.pop()
            for child in curr.__children__:
                if isinstance(child, ops.WindowFunction):
                    return child
                if isinstance(child, ops.numeric.NumericBinary):
                    stack.append(child)


class ExplicitWindowOperator(WindowOperator):

    def generate(self) -> str:
        window = self.window
        frame = window.frame
        if frame.start and frame.start.following:
            raise Exception(
                "Following window frames are not supported in noir!")

        text = ""

        # generate .group_by if needed
        if (bys := frame.group_by):

            # this group_by follows another, so drop_key is required
            if Struct.last().is_keyed_stream:
                text += ".drop_key()"

            text += ".group_by(|x| ("
            for by in [operator_arg_stringify(b) for b in bys]:
                text += f"x.{by}.clone(), "
            # remove last comma and space
            text = text[:-2]
            text += "))"
            # if we have group_by, .fold will generate a KeyedStream so
            # we set Struct.with_keyed_stream with key's name/type so following
            # map knows how to handle it
            Struct.with_keyed_stream = {b.name: b.dtype for b in bys}

        # .fold still generates a KeyedStream, but the key in this case
        # is a unit tuple () and we will discard it before the next operation

        # if no start, it means we need to aggregate over all values within each group,
        # so no window is required - ImplicitWindowOperator handles that case
        # generate .window with size depending on how many preceding rows are included
        # and fixed step of 1 and exact framing
        size = frame.start.value.value + 1
        if frame.group_by:
            text += f".window(CountWindow::new({size}, 1, true))"
        else:
            text += f".window_all(CountWindow::new({size}, 1, true))"

        prev_struct = Struct.last()

        name = type(window.func).__name__
        if name == "Sum":
            op = WindowFuncGen(
                [WindowFuncGen.Func(self.alias.name, self.alias.dtype, fold_action="acc.{0} = acc.{0}.zip(x.{1}).map(|(a, b)| a + b);")])
        elif name == "Mean":
            op = WindowFuncGen(
                [WindowFuncGen.Func("grp_sum", ibis.dtype("int64"), fold_action="acc.grp_sum = acc.grp_sum.zip(x.{1}).map(|(a, b)| a + b);"),
                 WindowFuncGen.Func("grp_count", ibis.dtype(
                     "int64"), fold_action="acc.grp_count = acc.grp_count.map(|v| v + 1);"),
                 WindowFuncGen.Func(self.alias.name, self.alias.dtype, map_action="{0}: x.grp_sum.zip(x.grp_count).map(|(a, b)| a as f64 / b as f64),")])
        elif name == "Max":
            op = WindowFuncGen(
                [WindowFuncGen.Func(self.alias.name, self.alias.dtype, fold_action="acc.{0} = acc.{0}.zip(x.{1}).map(|(a, b)| max(a, b));")])
        else:
            raise Exception(f"Window function {type(window.func).__name__} not supported!")

        # create the new struct by adding struct_fields to previous struct's columns
        new_cols_types = dict(prev_struct.cols_types)
        for n, t in op.fields():
            new_cols_types[n] = t
        new_struct = Struct.from_args_dict(str(id(window)), new_cols_types)

        # generate .fold to apply the reduction function while maintaining other row fields
        text += f".fold({new_struct.name_struct}{{"
        # fold accumulator initialization
        for col in prev_struct.columns:
            text += f"{col}: None, "
        for col, typ in op.fields():
            text += f"{col}: {op.type_init(typ)}, "
        # fold update step
        text += "}, |acc, x| {"
        for col in prev_struct.columns:
            text += f"acc.{col} = x.{col}; "
        arg = window.func.args[0].name
        for col, action in op.fold_actions():
            text += action.format(col, arg)
        text += "},)"

        # folding a WindowedStream without group_by still produces a KeyedStream, with unit tuple () as key
        # so discard it in that case
        if frame.start and not frame.group_by:
            text += ".drop_key()"

        # generate .map required by some window functions
        map_actions = op.map_actions()
        if map_actions:
            if new_struct.is_keyed_stream:
                text += ".map(|(_, x)| "
            else:
                text += ".map(|x| "
            text += f"{new_struct.name_struct}{{"
            for col, action in map_actions:
                text += action.format(col)
            text += "..x })"

        return text

    @classmethod
    def recognize(cls, node: Node):
        # node should be Alias, Alias should have WindowFunction successor, either direct or
        # through a chain of NumericBinary operations
        # WindowFunctions have RowsWindowFrame frame attribute, and
        # the window operator is Explicit only if the RowsWindowFrame has a start attribute
        # with value not None
        if not isinstance(node, ops.core.Alias):
            return

        window_func = cls.find_window_func_from_alias(node)
        if not window_func:
            return

        # check if window function has already been used by other WindowOperator to avoid duplicates
        # when WindowFunction has >1 aliases above
        if window_func in [op.window for op in Operator.operators if isinstance(op, WindowOperator)]:
            return

        if (hasattr(window_func, "frame") and
            hasattr(window_func.frame, "start") and
                window_func.frame.start):
            return cls(node)


class ImplicitWindowOperator(WindowOperator):

    def generate(self) -> str:
        window = self.window
        frame = window.frame
        text = ""

        # generate .group_by if needed
        if (group_by := frame.group_by):
            by = group_by[0]
            col = operator_arg_stringify(by)
            text += f".group_by(|x| x.{col}.clone())"
            # if we have group_by, .fold will generate a KeyedStream so
            # we set Struct.with_keyed_stream with key's name/type so following
            # map knows how to handle it
            Struct.with_keyed_stream = {by.name: by.dtype}

        # here no .window, just use .reduce_scan to reduce while actually keeping a row for each row
        text += ".reduce_scan("
        prev_struct = Struct.last()

        name = type(window.func).__name__
        if name == "Sum":
            op = WindowFuncGen(
                [WindowFuncGen.Func("sum", ibis.dtype("!int64"), init_action="x.{1}.unwrap_or(0)", fold_action="a_sum + b_sum"),
                 WindowFuncGen.Func(self.alias.name, self.alias.dtype, map_action="{0}: Some(*sum),")])
        elif name == "Mean":
            op = WindowFuncGen(
                [WindowFuncGen.Func("sum", ibis.dtype("!int64"), init_action="x.{1}.unwrap_or(0)", fold_action="a_sum + b_sum"),
                 WindowFuncGen.Func("count", ibis.dtype(
                     "!int64"), init_action="1", fold_action="a_count + b_count"),
                 WindowFuncGen.Func(self.alias.name, self.alias.dtype, map_action="{0}: Some(*sum as f64 / *count as f64),")])
        else:
            raise Exception(f"Window function {name} not supported!")

        # create the new struct by adding struct_fields to previous struct's columns
        new_cols_types = dict(prev_struct.cols_types)
        for n, t in op.fields():
            new_cols_types[n] = t
        new_struct = Struct.from_args_dict(str(id(window)), new_cols_types)

        # generate initialization (aka first_map) within .reduce_scan
        text += f"|x| ("
        arg = window.func.args[0].name
        for col, init in op.init_actions():
            text += init.format(col, arg) + ", "
        text += "),\n"

        # generate folding within .reduce_scan
        fold_tup_fields = op.fields()
        fold_tup_fields.remove((self.alias.name, self.alias.dtype))
        text += "|("
        for col, _ in fold_tup_fields:
            text += f"a_{col}, "
        text += "), ("
        for col, _ in fold_tup_fields:
            text += f"b_{col}, "
        text += ")| ("
        for _, act in op.fold_actions():
            text += act + ", "
        text += "),\n"

        # generate second_map within .reduce_scan
        text += "|x, ("
        for col, _ in fold_tup_fields:
            text += f"{col}, "
        text += f")| {new_struct.name_struct}{{"
        for col in prev_struct.columns:
            text += f"{col}: x.{col}, "
        for col, _ in fold_tup_fields:
            text += f"{col}: *{col}, "
        for col, act in op.map_actions():
            text += act.format(col)
        text += "})"

        return text

    @classmethod
    def recognize(cls, node: Node):
        # node should be Alias, Alias should have WindowFunction arg attribute
        # WindowFunctions have RowsWindowFrame frame attribute, and
        # the window operator is Implicit only if the RowsWindowFrame has a start attribute
        # with value None
        if not isinstance(node, ops.core.Alias):
            return

        window_func = cls.find_window_func_from_alias(node)
        if not window_func:
            return

        # check if window function has already been used by other WindowOperator to avoid duplicates
        # when WindowFunction has >1 aliases above
        if window_func in [op.window for op in Operator.operators if isinstance(op, WindowOperator)]:
            return

        if (hasattr(window_func, "frame") and
            hasattr(window_func.frame, "start")
                and window_func.frame.start is None):
            return cls(node)


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

        # turning full path to relative path so that rust code contains relative path and expected code can work across machines
        full_path = utl.TAB_FILES[self.table.name]
        rel_path = ".." + full_path.split(utl.ROOT_DIR)[1]
        return (f";\nlet {struct.name_short} = ctx.stream_csv::<{struct.name_struct}>(\"{rel_path}\");\n" +
                f"let var_{struct.id_counter + count_structs} = {struct.name_short}")

    def does_add_struct(self) -> bool:
        return True

    @classmethod
    def recognize(cls, node: Node):
        if isinstance(node, ops.PhysicalTable):
            return cls(node)


class TopOperator(Operator):
    def __init__(self):
        super().__init__()

    def generate(self) -> str:
        # cleanup operators: TopOperator should be the last to generate
        Operator.cleanup()

        with open(utl.ROOT_DIR + "/noir_template/main_top.rs") as f:
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
            names_types = Struct.last().with_keyed_stream
            new_struct = Struct.from_args(str(id(self)), list(names_types.keys()), list(
                names_types.values()), with_name_short="collect")
            bot += f"let out = out.iter().map(|(k, v)| ({new_struct.name_struct}{{"
            if len(names_types) == 1:
                bot += f"{list(names_types.keys())[0]}: k.clone(),"
            else:
                for i, name in enumerate(names_types):
                    bot += f"{name}: k.{i}.clone(),"
            bot += "}, v)).collect::<Vec<_>>();"

        with open(utl.ROOT_DIR + "/noir_template/main_bot.rs") as f:
            bot += f.read()
        return bot


class WindowFuncGen:
    func_type_init = {ibis.dtype("int64"): "Some(0)",
                      ibis.dtype("float64"): "Some(0.0)"}

    class Func:
        def __init__(self, name: str, type: DataType, init_action: str = None, fold_action: str = None, map_action: str = None):
            self.name = name
            self.type = type
            self.init_action = init_action
            self.fold_action = fold_action
            self.map_action = map_action

    def __init__(self, func_descriptor: list["WindowFuncGen.Func"]):
        self.funcs = func_descriptor

    def fields(self) -> list[tuple[str, DataType]]:
        return [(f.name, f.type) for f in self.funcs]

    def init_actions(self) -> list[tuple[str, str]]:
        return [(f.name, f.init_action) for f in self.funcs if f.init_action]

    def fold_actions(self) -> list[tuple[str, str]]:
        return [(f.name, f.fold_action) for f in self.funcs if f.fold_action]

    def map_actions(self) -> list[tuple[str, str]]:
        return [(f.name, f.map_action) for f in self.funcs if f.map_action]

    def type_init(self, type: DataType):
        return self.func_type_init[type]


# if operand is literal, return its value
# if operand is table column, return its index in the original table
# if resolve_optionals_to_some we're recursively resolving a binary operation and also need struct_name
def operator_arg_stringify(operand: Node, struct_name=None, window_resolve=None) -> str:
    math_ops = {"Multiply": "*", "Add": "+", "Subtract": "-", "Divide": "/"}
    comp_ops = {"Equals": "==", "Greater": ">",
                "GreaterEqual": ">=", "Less": "<", "LessEqual": "<="}
    log_ops = {"And": "&", "Or": "|"}
    if isinstance(operand, ibis.expr.operations.generic.TableColumn):
        if struct_name:
            return f"{struct_name}.{operand.name}"
        return operand.name
    elif isinstance(operand, ibis.expr.operations.generic.Literal):
        if operand.dtype.name == "String":
            return f"\"{operand.value}\""
        return operand.name
    elif isinstance(operand, ops.logical.Comparison):
        left = operator_arg_stringify(
            operand.left, struct_name, window_resolve)
        right = operator_arg_stringify(
            operand.right, struct_name, window_resolve)
        # careful: ibis considers literals as optionals, while in noir a numeric literal is not an Option<T>
        is_left_nullable = operand.left.dtype.nullable and not isinstance(
            operand.left, ops.Literal)
        is_right_nullable = operand.right.dtype.nullable and not isinstance(
            operand.right, ops.Literal)
        op = comp_ops[type(operand).__name__]
        if is_left_nullable and not is_right_nullable:
            return f"{left}.clone().is_some_and(|v| v {op} {right})"
        if not is_left_nullable and is_right_nullable:
            return f"{right}.clone().is_some_and(|v| {left} {op} v)"
        if is_left_nullable and is_right_nullable:
            return f"{left}.clone().zip({right}.clone()).map_or(false, |(a, b)| a {op} b)"
        return f"{left} {op} {right}"
    elif isinstance(operand, ops.LogicalBinary):
        left = operator_arg_stringify(
            operand.left, struct_name, window_resolve)
        right = operator_arg_stringify(
            operand.right, struct_name, window_resolve)
        return f"{left} {log_ops[type(operand).__name__]} {right}"
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
            result = f"{operator_arg_stringify(operand.left, struct_name, window_resolve)}\
                    .zip({operator_arg_stringify(operand.right, struct_name, window_resolve)})\
                    .map(|(a, b)| a {cast} {math_ops[type(operand).__name__]} b {cast})"
        elif is_left_nullable and not is_right_nullable:
            result = f"{operator_arg_stringify(operand.left, struct_name, window_resolve)}\
                    .map(|v| v {cast} {math_ops[type(operand).__name__]} {operator_arg_stringify(operand.right, struct_name, window_resolve)} {cast})"
        elif not is_left_nullable and is_right_nullable:
            result = f"{operator_arg_stringify(operand.right, struct_name, window_resolve)}\
                    .map(|v| {operator_arg_stringify(operand.left, struct_name, window_resolve)} {cast} {math_ops[type(operand).__name__]} v {cast})"
        else:
            result = f"{operator_arg_stringify(operand.left, struct_name, window_resolve)} {cast} {math_ops[type(operand).__name__]} {operator_arg_stringify(operand.right, struct_name, window_resolve)} {cast}"

        return result
    elif isinstance(operand, ops.WindowFunction):
        # window resolve case: map is preceded by WindowFunction which used same name as map's result for its result
        if window_resolve:
            return f"{struct_name}.{window_resolve}"
        # alias is above WindowFunction and not dependency, but because .map follows .fold
        # in this case, we can get the column added by the DatabaseOperator in the struct
        # it just created, which is second to last (last is new col added by MapOperator)
        # since python 3.7, dict maintains insertion order
        return f"{struct_name}.{Struct.last().columns[-2]}"
    raise Exception(f"Unsupported operand type: {operand}")
