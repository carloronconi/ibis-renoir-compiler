import ibis.formats
from ibis.expr.operations import DatabaseTable, Aggregation


class Struct(object):
    name_counter = 0
    ibis_to_noir_type = {"Int64": "i64", "String": "String"}
    last_complete_transform: "Struct"

    @classmethod
    def id_counter_to_name_short(cls, id_c: int) -> str:
        return f"var_{id_c}"

    @classmethod
    def name_short_to_name_struct(cls, name_short: str) -> str:
        return f"Struct_{name_short}"

    @classmethod
    def type_ibis_to_noir_str(cls, ibis_name: str, ibis_nullable: bool) -> str:
        if ibis_nullable:
            return f"Option<{cls.ibis_to_noir_type[ibis_name]}>"
        return cls.ibis_to_noir_type[ibis_name]

    def __init__(self, name: str, cols_types: dict[str, ibis.expr.datatypes.core.DataType]):
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
        return cls(name=table.name, cols_types=dict(zip(names, types)))

    @classmethod
    def from_aggregation(cls, agg: Aggregation):
        return cls(name=str(id(agg)), cols_types=dict(zip(list(agg.schema.names), list(agg.schema.types))))

    @classmethod
    def from_join(cls, left: "Struct", right: "Struct"):
        n = left.name_long[len(left.name_long)//2:] + right.name_long[len(right.name_long)//2:]
        c_t = dict(left.cols_types)
        right_to_append = {}
        for c, t in right.cols_types.items():
            if c not in left.columns:
                right_to_append[c] = t
            else:
                right_to_append[c + "_right"] = t
        c_t.update(right_to_append)
        # overlapping column names are renamed according to ibis convention
        return cls(name=n, cols_types=c_t)

    @classmethod
    def from_args(cls, name: str, columns: list, types: list):
        return cls(name, dict(zip(columns, types)))

    def generate(self, to_text: str) -> str:
        body = to_text
        # here the fact that the external struct derives Default, combined with the fact that its fields are optional
        # means that a None struct will be automatically turned, in the next struct with optional fields copying
        # the previous struct's fields into None fields
        body += f"#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]\nstruct {self.name_struct} {{"
        for col, typ in self.cols_types.items():
            body += f"{col}: {Struct.type_ibis_to_noir_str(typ.name, typ.nullable)},"
        body += "}\n"
        return body

    @property
    def columns(self):
        return list(self.cols_types.keys())

    @property
    def types(self):
        return list(self.cols_types.values())

    def is_col_nullable(self, name: str) -> bool:
        return self.cols_types[name].nullable
