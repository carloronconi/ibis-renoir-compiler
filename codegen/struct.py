from ibis.expr.operations import DatabaseTable, Alias


class Struct(object):
    name_counter = 0
    ibis_to_noir_type = {"Int64": "i64", "String": "String"}  # TODO: add nullability with optionals

    def __init__(self, name: str, columns: list, types: list):
        self.name_long = name
        self.name_short = f"table{Struct.name_counter}"
        Struct.name_counter += 1
        self.columns = columns
        self.types = types

    @classmethod
    def from_table(cls, table: DatabaseTable):
        return cls(name=table.name, columns=list(table.schema.names), types=table.schema.types)

    @classmethod
    def from_alias(cls, alias: Alias):
        pass

    def generate(self, to_text: str) -> str:
        body = to_text
        body += f"#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]\nstruct Cols_{self.name_short} {{"
        for typ, col in zip(self.types, self.columns):
            body += f"{col}: {Struct.ibis_to_noir_type[typ.name]},"
        body += "}\n"
        return body
