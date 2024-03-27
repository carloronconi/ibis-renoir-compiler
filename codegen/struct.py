from ibis.expr.operations import DatabaseTable, Alias, Aggregation


class Struct(object):
    name_counter = 0
    ibis_to_noir_type = {"Int64": "i64", "String": "String"}  # TODO: add nullability with optionals

    @classmethod
    def id_counter_to_name_short(cls, id_c: int) -> str:
        return f"var_{id_c}"

    @classmethod
    def name_short_to_name_struct(cls, name_short: str) -> str:
        return f"Struct_{name_short}"

    def __init__(self, name: str, columns: list, types: list):
        self.name_long = name
        self.id_counter = Struct.name_counter
        self.name_short = Struct.id_counter_to_name_short(self.id_counter)
        self.name_struct = Struct.name_short_to_name_struct(self.name_short)
        Struct.name_counter += 1
        self.columns = columns
        self.types = types

    @classmethod
    def from_table(cls, table: DatabaseTable):
        return cls(name=table.name, columns=list(table.schema.names), types=list(table.schema.types))

    @classmethod
    def from_aggregation(cls, agg: Aggregation):
        return cls(name=str(id(agg)), columns=list(agg.schema.names), types=list(agg.schema.types))

    @classmethod
    def from_args(cls, name: str, columns: list, types: list):
        return cls(name=name, columns=columns, types=types)

    def generate(self, to_text: str) -> str:
        body = to_text
        body += f"#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]\nstruct {self.name_struct} {{"
        for typ, col in zip(self.types, self.columns):
            body += f"{col}: {Struct.ibis_to_noir_type[typ.name]},"
        body += "}\n"
        return body
