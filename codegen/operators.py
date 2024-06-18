import ibis
import ibis.expr.operations as ops
from ibis.common.graph import Node

import codegen.utils as utl
from codegen.struct import Struct
from ibis.expr.datatypes.core import DataType
from codegen.argument_parser import ArgumentParser


class Operator:
    operators = []
    # when two operators use the same root node for recognition, increase this
    # value to prioritize one over the other and change ordeding of operators in noir code
    priority = 0
    print_output_to_file = True

    def __init__(self):
        self.arg_parser = ArgumentParser()
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
        self.columns = [
            c for c in node.__children__ if isinstance(c, ops.TableColumn)]
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
        self.arg_parser.struct_name = "x"
        filter_expr = self.arg_parser.parse(self.comparator)
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
        equalses = list(
            filter(is_equals_col_lit_or_col_col, node.__children__))
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

        new_struct = Struct.from_args(cols, typs)

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
        self.arg_parser.window_resolve = prev_struct.columns[-1]
        self.arg_parser.struct_name = "x"
        num_ops = self.arg_parser.parse(
            self.mapper)
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
        col = self.arg_parser.parse(self.reducer.__children__[0])
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
                "First": "x", "Mean": "x + y"}
    aggr_ops_form = {"Max": "a.{0} = max(a.{0}, b.{0})", "Min": "a.{0} = min(a.{0}, b.{0})",
                     "Sum": "a.{0} = a.{0} + b.{0}",
                     "First": "a.{0} = a.{0}", "Mean": "a.{0} = a.{0} + b.{0}"}

    class AliasInfo:
        def __init__(self, alias: ops.Alias):
            self.alias = alias
            self.reducer = alias.__children__[0]
            self.aggr_name = type(self.reducer).__name__
            if self.aggr_name == "CountStar":
                self.col = "reduce_count"
                self.is_reduced_col_nullable = False
            else:
                self.col = ArgumentParser().parse(self.reducer.__children__[0])
                self.is_reduced_col_nullable = Struct.last().is_col_nullable(self.col)

    def __init__(self, node: ops.Aggregation):
        self.aliases = [
            c for c in node.__children__ if isinstance(c, ops.Alias)]
        self.bys = node.by
        self.node = node
        super().__init__()

    def generate(self) -> str:
        mid = ""
        self.aliases = [self.AliasInfo(alias) for alias in self.aliases]
        bys = [self.arg_parser.parse(by) for by in self.bys]

        last_struct = Struct.last()
        # this group_by follows another, so drop_key is required
        if last_struct.is_keyed_stream:
            mid += ".drop_key()"

        # map to a struct with additional count field useful for mean
        count_struct = Struct.from_args(last_struct.columns + ["reduce_count"],
                                        last_struct.types + [ibis.dtype("!int64")])
        mid += f".map(|x| {count_struct.name_struct}{{"
        for col in last_struct.columns:
            mid += f"{col}: x.{col}, "
        mid += "reduce_count: 1})"

        # group by
        mid += ".group_by(|x| ("
        for by in bys:
            mid += f"x.{by}.clone(), "
        # remove last comma and space
        mid = mid[:-2]
        mid += "))"

        # reduce for each reducer
        mid += ".reduce(|a, b| {"
        for alias in self.aliases:
            if alias.aggr_name == "CountStar":
                continue
            elif alias.is_reduced_col_nullable:
                op = self.aggr_ops[alias.aggr_name]
                mid += f"a.{alias.col} = a.{alias.col}.zip(b.{alias.col}).map(|(x, y)| {op});"
            else:
                op = self.aggr_ops_form[alias.aggr_name].format(alias.col)
                mid += f"{op};"
        mid += "a.reduce_count = a.reduce_count + b.reduce_count;})"

        # final map copying the keys of the keyed stream to the struct and the aggregated fields
        bys_n_t = {b.name: b.dtype for b in self.bys}
        aggr_col_names = self.node.schema.names
        aggr_col_types = self.node.schema.types
        Struct.with_keyed_stream = bys_n_t
        # new struct will contain the aggregated field plus the "by" fields also preserved by
        # the key of the keyed stream, kept in both to be able to drop_key if other group_by follows
        new_struct = Struct.from_args(
            list(bys_n_t.keys()) + list(aggr_col_names), list(bys_n_t.values()) + list(aggr_col_types))

        mid += f".map(|(k, x)| {new_struct.name_struct}{{"

        for i, column in enumerate(new_struct.columns):
            if (i < len(bys_n_t)):
                if (len(bys_n_t) == 1):
                    mid += f"{column}: k.clone(), "
                else:
                    mid += f"{column}: k.{i}.clone(), "
            else:
                alias = self.aliases[i - len(bys_n_t)]
                val = f"x.{alias.col}"
                if alias.aggr_name == "Mean":
                    val = f"x.{alias.col}.map(|a| a as f64 / x.reduce_count as f64), "
                elif alias.is_reduced_col_nullable:
                    mid += f"{column}: {val}, "
                else:
                    mid += f"{column}: Some({val}), "
        mid += "})"

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
        left_col = self.arg_parser.parse(equals.right)
        right_col = self.arg_parser.parse(equals.left)
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
            for by in [self.arg_parser.parse(b) for b in bys]:
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
            raise Exception(
                f"Window function {type(window.func).__name__} not supported!")

        # create the new struct by adding struct_fields to previous struct's columns
        new_cols_types = dict(prev_struct.cols_types)
        for n, t in op.fields():
            new_cols_types[n] = t
        new_struct = Struct.from_args_dict(new_cols_types)

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
            col = self.arg_parser.parse(by)
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
        new_struct = Struct.from_args_dict(new_cols_types)

        # generate initialization (aka first_map) within .reduce_scan
        text += f"|x| (" if not Struct.with_keyed_stream else f"|_, x| ("
        arg = window.func.args[0].name
        for col, init in op.init_actions():
            text += init.format(col, arg) + ", "
        text += "),\n"

        # generate folding within .reduce_scan
        fold_tup_fields = op.fields()
        fold_tup_fields.remove((self.alias.name, self.alias.dtype))
        text += "|(" if not Struct.with_keyed_stream else "|_, ("
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
        text += "|x, " if not Struct.with_keyed_stream else "|_, "
        text += "("
        for col, _ in fold_tup_fields:
            text += f"{col}, "
        text += f")"
        if Struct.with_keyed_stream:
            text += ", x"
        text += f" | {new_struct.name_struct}{{"
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

        if not self.print_output_to_file:
            bot = f"; {last_struct.name_short}.for_each(|x| {{std::hint::black_box(x);}});"
            with open(utl.ROOT_DIR + "/noir_template/main_bot_no_print.rs") as f:
                bot += f.read()
            return bot

        if not last_struct.is_keyed_stream:
            bot = f";\n{last_struct.name_short}.write_csv_one(\"../out/noir-result.csv\", true);"
        else:
            names_types = Struct.last().with_keyed_stream
            new_struct = Struct.from_args(list(names_types.keys()), list(
                names_types.values()), with_name_short="collect")
            bot = f";\n{last_struct.name_short}.map(|(k, v)| ({new_struct.name_struct}{{"
            if len(names_types) == 1:
                bot += f"{list(names_types.keys())[0]}: k.clone(),"
            else:
                for i, name in enumerate(names_types):
                    bot += f"{name}: k.{i}.clone(),"
            bot += "}, v)).drop_key().write_csv_one(\"../out/noir-result.csv\", true);"

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
