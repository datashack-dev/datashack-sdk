from dataclasses import dataclass
from typing import MutableMapping
from .base import ResourceConfig


@dataclass
class Column(ResourceConfig):
    col_type: str
    partition: bool
    required: bool

    def __init__(self, col_type: str, partition: bool = False, required: bool = True):
        assert col_type in SchemaEditor.JSON_SCHEMA_MAPPER.keys(), f"{col_type} is not supported type"
        self.col_type = col_type
        self.partition = partition
        self.required = required

    def __add__(self, x):
        # tbd
        pass

    def __sub__(self, x):
        # tbd
        pass

    def __mul__(self, x):
        # tbd
        pass

    def __truediv__(self, x):
        # tbd
        pass

    def __eq__(self, x):
        return ConditionEq(self, x)

    def __str__(self):
        return str(self.table) + '.' + self.col_name

    def is_assigned_to_table(self) -> bool:
        return bool(self._table)

    def set_table(self, table):
        self._table = table

    @property
    def table(self):
        return self._table


class SchemaEditor(MutableMapping):
    JSON_SCHEMA_MAPPER = {
        "string": {"type": "string"},
        "str": {"type": "string"},
        "int": {"type": "number"},
        "double": {"type": "number"},
        "integer": {"type": "number"},
        "map": {"type": "object"},
        "array": {"type": "array"},
        "date": {"type": "string", "format": "date"},
        "timestamp": {"type": "string", "format": "date-time"},
        "boolean": {"type": "booelan"},
    }

    def __init__(self, schema_type: str = "json", **kwargs):
        """
        :param schema_type: json or avro schema for writing messages
        """
        super().__init__(**kwargs)
        self.schema_type = schema_type.lower()

    def __getitem__(self, key):
        return self._resource_config.columns[key]

    def __setitem__(self, key: str, value: 'Column'):
        self.validate_type(value.col_type)
        self._resource_config.columns[key] = value

    def __delitem__(self, key):
        del self._resource_config.columns[key]

    def __iter__(self):
        return iter(self._resource_config.columns)

    def __len__(self):
        return len(self._resource_config.columns)

    @property
    def columns(self):
        return {c[0]: c[1] for c in self.items()}

    @property
    def schema(self):
        return

    @staticmethod
    def validate_type(type_name):
        assert type_name in ["string", "int", "integer", "bigint", "double", "boolean", "date",
                             "timestamp", "array", "map", "struct"], \
            "column_type is not jsonschema type"
