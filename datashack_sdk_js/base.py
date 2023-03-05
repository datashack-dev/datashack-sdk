from abc import ABC
from os import environ as env
import requests
from functools import lru_cache
from dataclasses import dataclass
from typing import Dict

from dacite import from_dict
from dataclasses import asdict, dataclass
from enum import Enum

class DatashackSdk(ABC):
    def __init__(self, resource_id: str, *args, **kwargs):
        super().__init__(**kwargs)
        self._DS_ENV = env['DATASHACK_ENV']  # TODO Moshe maybe we need to move it to get_runtime_context()??
        self._resource_id = resource_id

    @lru_cache
    def get_runtime_context(self):
        raise NotImplementedError()

    @property
    def resource_id(self):
        return self._resource_id

    @property
    def resource(self):
        return self._resource_config

    @property
    def resource_json(self):
        return self.resource.to_dict()

    @property
    def resource_type(self):
        return self.resource.__class__.__name__


@dataclass
class ResourceConfig:
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
