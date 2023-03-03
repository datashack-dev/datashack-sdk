from dataclasses import dataclass
from typing import Dict
from .resource_schemas import Column
from .base import ResourceConfig

from dacite import from_dict
from dataclasses import asdict, dataclass
from enum import Enum


@dataclass
class DatabaseConf(ResourceConfig):
    database_name: str
    s3_bucket: str


@dataclass
class GlueTableConf(ResourceConfig):
    table_name: str
    database_name: str
    table_external_loc: str
    columns: Dict[str, Column]


@dataclass
class StreamingTableConf(ResourceConfig):
    # glue table
    table_name: str
    database_name: str
    columns: Dict[str, Column]
    no_shards: int
