import ibis.formats
from ibis.expr.operations import Relation
from ibis.expr.datatypes.core import DataType


class Struct(object):
    name_counter = 0
    ibis_to_noir_type = {"Int64": "i64", "String": "String", "Float64": "f64"}
    last_complete_transform: "Struct"
    structs: list["Struct"] = []
    # copied when generating new structs: toggle if operator turns to keyed/un-keyed
    with_keyed_stream: dict[str, DataType] = None

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

    def __init__(self, name: str, cols_types: dict[str, ibis.expr.datatypes.core.DataType], with_name_short=None):
        self.name_long = name
        self.id_counter = Struct.name_counter
        if with_name_short:
            self.name_short = with_name_short
        else:
            self.name_short = Struct.id_counter_to_name_short(self.id_counter)
        self.name_struct = Struct.name_short_to_name_struct(self.name_short)
        Struct.name_counter += 1
        self.cols_types = cols_types
        self.is_keyed_stream = Struct.with_keyed_stream
        Struct.structs.append(self)

    @classmethod
    def from_relation(cls, node: Relation):
        names = list(node.schema.names)
        types = list(node.schema.types)
        return cls(name=str(id(node)), cols_types=dict(zip(names, types)))
    
    @classmethod
    def from_table(cls, table):
        names = list(table.schema().names)
        types = list(table.schema().types)
        return cls(name=str(id(table)), cols_types=dict(zip(names, types)))

    @classmethod
    def from_join(cls, left: "Struct", right: "Struct"):
        n = left.name_long[len(left.name_long)//2:] + right.name_long[len(right.name_long)//2:]
        c_t = dict(left.cols_types)
        right_to_append = {}
        for c, t in right.cols_types.items():
            if c not in left.columns:
                right_to_append[c] = t
            else:
                right_to_append[c + "_right"] = t  # overlapping column names are renamed according to ibis convention
        c_t.update(right_to_append)

        # following ibis behaviour: after join, all non-nullable cols are cast to nullable
        cols_turned_nullable = set()
        for c, t in c_t.items():
            if not t.nullable:
                # make t nullable (type is immutable so creating new one defaulting to nullable)
                c_t[c] = ibis.dtype(t.name)
                cols_turned_nullable.add(c)

        return cls(name=n, cols_types=c_t), cols_turned_nullable

    @classmethod
    def from_args(cls, name: str, columns: list, types: list, with_name_short=None):
        return cls(name, dict(zip(columns, types)), with_name_short=with_name_short)
    
    @classmethod
    def from_args_dict(cls, name: str, cols_types: dict, with_name_short=None):
        return cls(name, cols_types, with_name_short=with_name_short)

    @classmethod
    def last(cls) -> "Struct":
        if not cls.structs:
            raise Exception("No struct instances built yet!")
        return cls.structs[-1]

    @classmethod
    def some(cls) -> bool:
        return len(cls.structs) > 0

    @classmethod
    def transform_completed(cls):
        if cls.some():
            cls.last_complete_transform = cls.last()

    @classmethod
    def cleanup(cls):
        cls.name_counter = 0
        cls.structs = []
        cls.last_complete_transform = None
        cls.with_keyed_stream = None

    def generate(self) -> str:
        # here the fact that the external struct derives Default, combined with the fact that its fields are optional
        # means that a None struct will be automatically turned, in the next struct with optional fields copying
        # the previous struct's fields into None fields
        body = f"#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]\nstruct {self.name_struct} {{"
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
        if name not in self.cols_types:
            return False
        return self.cols_types[name].nullable
