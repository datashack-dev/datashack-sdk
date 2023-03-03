from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Type, Any, Union
from cdktf import TerraformOutput
from constructs import Construct


class DatashackResourceOutputs(TerraformOutput):
    def __init__(self, scope: Construct, r_output_id:str, value: Any):
        super().__init__(scope, r_output_id, value=value)


class DatashackPlugin(ABC):

    @abstractmethod
    def init_outputs_obj(self, scope) -> DatashackResourceOutputs:
        pass


@dataclass(eq=False)
class DatashackResourceConf(ABC):
    _resource_id: Union[str, None]
    _resource_output_id: Union[str, None]

    def __init__(self, resource_id, *args, **kwargs):
        """

        :rtype: object
        """
        self.resource_id = resource_id
        self.resource_output_id = self.output_resource_id_parser(self.resource_id)

    @classmethod
    @abstractmethod
    def get_plugin_class(cls) -> Type[DatashackPlugin]:
        raise NotImplementedError()

    @staticmethod
    def output_resource_id_parser(r_id):
        return f"{r_id}_datashack_output"

    @property
    def resource_id(self):
        return self._resource_id

    @resource_id.setter
    def resource_id(self, value):
        self._resource_id = value

    @property
    def resource_output_id(self):
        return self._resource_output_id

    @resource_output_id.setter
    def resource_output_id(self, value):
        self._resource_output_id = value

    def __hash__(self):
        return hash(self.resource_id)