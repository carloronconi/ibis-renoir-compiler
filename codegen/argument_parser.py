import ibis
import ibis.expr.operations as ops
from ibis.common.graph import Node

import codegen.utils as utl
from codegen.struct import Struct
from ibis.expr.datatypes.core import DataType


class ArgumentParser:
    def __init__(self, struct_name=None, window_resolve=None):
        self.struct_name = struct_name
        self.window_resolve = window_resolve
        self.other_is_date = False

    # if operand is literal, return its value
    # if operand is table column, return its index in the original table
    # if resolve_optionals_to_some we're recursively resolving a binary operation and also need struct_name
    def parse(self, operand: Node) -> str:
        math_ops = {"Multiply": "*", "Add": "+", "Subtract": "-", "Divide": "/", "Modulus": "%"}
        comp_ops = {"Equals": "==", "Greater": ">",
                    "GreaterEqual": ">=", "Less": "<", "LessEqual": "<="}
        log_ops = {"And": "&", "Or": "|"}
        if isinstance(operand, ibis.expr.operations.generic.TableColumn):
            self.last_tab_col_type = operand.dtype
            self.last_tab_col_name = operand.name
            if self.struct_name:
                return f"{self.struct_name}.{operand.name}"
            return operand.name
        elif isinstance(operand, ibis.expr.operations.generic.Literal):
            if operand.dtype.name == "String":
                return f"\"{operand.value}\""
            return operand.name
        elif isinstance(operand, ops.logical.Comparison):
            left = self.parse(operand.left)
            right = self.parse(operand.right)
            # careful: ibis considers literals as optionals, while in noir a numeric literal is not an Option<T>
            is_left_nullable = operand.left.dtype.nullable and not isinstance(
                operand.left, ops.Literal)
            is_right_nullable = operand.right.dtype.nullable and not isinstance(
                operand.right, ops.Literal)
            op = comp_ops[type(operand).__name__]
            if is_left_nullable and not is_right_nullable:
                r = right
                if operand.left.dtype.name == "Date":
                    r = f"NaiveDate::parse_from_str({r}, \"%Y-%m-%d\").unwrap()"
                return f"{left}.clone().is_some_and(|v| v {op} {r})"
            if not is_left_nullable and is_right_nullable:
                l = left
                if operand.right.dtype.name == "Date":
                    l = f"NaiveDate::parse_from_str({l}, \"%Y-%m-%d\").unwrap()"
                return f"{right}.clone().is_some_and(|v| {l} {op} v)"
            if is_left_nullable and is_right_nullable:
                return f"{left}.clone().zip({right}.clone()).map_or(false, |(a, b)| a {op} b)"
            return f"{left} {op} {right}"
        elif isinstance(operand, ops.LogicalBinary):
            left = self.parse(operand.left)
            right = self.parse(operand.right)
            return f"{left} {log_ops[type(operand).__name__]} {right}"
        elif isinstance(operand, ops.NumericBinary):
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
                result = f"{self.parse(operand.left)}\
                        .zip({self.parse(operand.right)})\
                        .map(|(a, b)| a {cast} {math_ops[type(operand).__name__]} b {cast})"
            elif is_left_nullable and not is_right_nullable:
                result = f"{self.parse(operand.left)}\
                        .map(|v| v {cast} {math_ops[type(operand).__name__]} {self.parse(operand.right)} {cast})"
            elif not is_left_nullable and is_right_nullable:
                result = f"{self.parse(operand.right)}\
                        .map(|v| {self.parse(operand.left)} {cast} {math_ops[type(operand).__name__]} v {cast})"
            else:
                result = f"{self.parse(operand.left)} {cast} {math_ops[type(operand).__name__]} {self.parse(operand.right)} {cast}"

            return result
        elif isinstance(operand, ops.WindowFunction):
            # window resolve case: map is preceded by WindowFunction which used same name as map's result for its result
            if self.window_resolve:
                return f"{self.struct_name}.{self.window_resolve}"
            # alias is above WindowFunction and not dependency, but because .map follows .fold
            # in this case, we can get the column added by the DatabaseOperator in the struct
            # it just created, which is second to last (last is new col added by MapOperator)
            # since python 3.7, dict maintains insertion order
            return f"{self.struct_name}.{Struct.last().columns[-2]}"
        elif isinstance(operand, ops.NotNull):
            return f"{self.parse(operand.__children__[0])}.is_some()"
        elif isinstance(operand, ops.StringContains):
            haystack = self.parse(operand.haystack)
            needle = self.parse(operand.needle)
            if not operand.haystack.dtype.nullable: 
                return f"{haystack}.contains({needle})"
            else:
                return f"{haystack}.clone().is_some_and(|x| x.contains({needle}))"
        raise Exception(f"Unsupported operand type: {operand}")
