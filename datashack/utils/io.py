from enum import Enum
from dacite import from_dict, Config
from dataclasses import asdict, dataclass
import yaml
import stat
import shutil
import tempfile
from typing import Dict
import os
from pathlib import Path
from typing import Sequence


import json
from dataclasses import dataclass
from datetime import datetime

# from confluent_kafka import avro
# from dataclasses_avroschema import AvroModel, DateTimeMicro




def exists(path)->bool:
    return os.path.exists(path)

def mkdir(dir):
    Path(dir).mkdir(parents=True, exist_ok=True)

def rmdir(dir):
    if exists(dir):
        shutil.rmtree(dir)

def touch(file):
    Path(file).touch()

def file_read_lines(file) -> Sequence[str]:
    with open(file, 'r') as fp:
        return map(lambda s: s.strip(), fp.readlines())

def file_write_lines(file, lines):
    with open(file, 'w') as fp:
        fp.writelines(map(lambda s: f'{s}\n', lines))

def file_write(file, content):
    with open(file, 'w') as fp:
        fp.write(content)

def get_tmp_file():
    return tempfile.NamedTemporaryFile()

def get_tmp():
    return tempfile.gettempdir()

def chmod_x(file):
    st = os.stat(file)
    os.chmod(file, st.st_mode | stat.S_IEXEC)

@dataclass(frozen=True)
class DictLoader:
    @classmethod
    def from_dict(cls, data):
        return from_dict(data_class=cls, data=data)

    def to_dict(self):
        
        def _dict_factory(data):
            return {
                field: value.value if isinstance(value, Enum) else value
                for field, value in data
            }

        return asdict(self, dict_factory=_dict_factory)

class YamlIO:
    @staticmethod
    def from_file(file: str, cls: DictLoader=None)->DictLoader:
        with open(file, "r") as fp:
            text = fp.read()
            obj = yaml.safe_load(text)
        
        if cls:
            obj = cls.from_dict(obj)
        return obj

    @staticmethod
    def to_file(file: str, data: DictLoader):
        with open(file, "w") as fp:
            yaml.dump(data.to_dict(), fp, default_flow_style=False)




# class AvroConverter:
#     MAPPER = {
#         str: "string",
#         'str': "string",
#         int: "int",
#         datetime:  {"type": "string", "logicalType": "timestamp-millis"}
#     }

#     def __init__(self, namespace, record_name):
#         self._record_schema = {
#             "type": "record",
#             "namespace": namespace,
#             "name": record_name,
#             "fields": []
#         }

#     def add_column(self, col_name, col_type):
#         typed_col = {"name": col_name, "type": self.MAPPER[col_type]}
#         self._record_schema["fields"].append(typed_col)

#     @property
#     def record_schema(self):
#         return self._record_schema

#     @classmethod
#     def get_avro_schema(cls, table):
#         avro_schema = cls(table._table_name, table._table_name)
#         for col_name, col in table.items():
#             avro_schema.add_column(col_name, col.col_type)
#         return avro.loads(json.dumps(avro_schema.record_schema))
