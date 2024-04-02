from typing import Any

from ibis.expr.operations import DatabaseTable, Alias, Aggregation


class Struct(object):
    name_counter = 0
    ibis_to_noir_type = {"Int64": "i64", "String": "String"}  # TODO: add nullability with optionals
    last_complete_transform: "Struct"

    @classmethod
    def id_counter_to_name_short(cls, id_c: int) -> str:
        return f"var_{id_c}"

    @classmethod
    def name_short_to_name_struct(cls, name_short: str) -> str:
        return f"Struct_{name_short}"

    def __init__(self, name: str, cols_types: list[tuple[str, Any]]):
        self.name_long = name
        self.id_counter = Struct.name_counter
        self.name_short = Struct.id_counter_to_name_short(self.id_counter)
        self.name_struct = Struct.name_short_to_name_struct(self.name_short)
        Struct.name_counter += 1
        self.cols_types = cols_types

    @classmethod
    def from_table(cls, table: DatabaseTable):
        names = list(table.schema.names)
        types = list(table.schema.types)
        return cls(name=table.name, cols_types=list(zip(names, types)))

    @classmethod
    def from_aggregation(cls, agg: Aggregation):
        return cls(name=str(id(agg)), cols_types=list(zip(list(agg.schema.names), list(agg.schema.types))))

    @classmethod
    def from_join(cls, left: "Struct", right: "Struct"):
        n = left.name_long[len(left.name_long)//2:] + right.name_long[len(right.name_long)//2:]
        c_t = list(left.cols_types)
        right_to_append = [(c, t) for c, t in right.cols_types if c not in left.columns]
        c_t.extend(right_to_append)
        # add from other structs, skipping overlapping column names
        return cls(name=n, cols_types=c_t)

    @classmethod
    def from_args(cls, name: str, columns: list, types: list):
        return cls(name, list(zip(columns, types)))

    def generate(self, to_text: str) -> str:
        body = to_text
        body += f"#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]\nstruct {self.name_struct} {{"
        for col, typ in self.cols_types:
            body += f"{col}: {Struct.ibis_to_noir_type[typ.name]},"
        body += "}\n"
        return body

    @property
    def columns(self):
        cols = [c for c, t in self.cols_types]
        return cols

    @property
    def types(self):
        typs = [t for c, t in self.cols_types]
        return typs
