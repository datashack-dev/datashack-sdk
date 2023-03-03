from dataclasses import dataclass, field
from functools import lru_cache
from importlib import import_module
from inspect import isclass
from pkgutil import iter_modules
from typing import Type, List, Dict, Set, Union, Any, Tuple, Optional
from cdktf import TerraformStack
from constructs import Construct
from cdktf_cdktf_provider_aws.provider import AwsProvider
from pathlib import Path
from .datashack_base import DatashackPlugin, DatashackResourceConf



@dataclass
class DatashackStackConf:
    env: str
    resources: List['ResourceConfigSchema']

    def __init__(self, env: str,
                 resources: List['ResourceConfigSchema']):
        self.env = env
        self.resources = resources


class DatashackStack(TerraformStack):
    def __init__(self, scope: Construct, stack_conf: DatashackStackConf):
        super().__init__(scope, stack_conf.env)
        self.define_datashack_backend()
        plugins_to_apply: Dict[str, List] = {}
        ds_plugins, ds_confs = self.get_datashack_elements()
        for i, resource in enumerate(stack_conf.resources):

            conf_class: Type[DatashackResourceConf] = get_datashack_obj(resource["resource_type"], ds_confs)
            plugin_class: Type[DatashackPlugin] = conf_class.get_plugin_class()
            if plugin_class.__name__ in plugins_to_apply.keys():
                plugins_to_apply[plugin_class.__name__].append(conf_class(**resource["resource_json"]))
            else:
                plugins_to_apply[plugin_class.__name__] = [conf_class(**resource["resource_json"])]

        for plugin_name in plugins_to_apply:
            plugin = get_datashack_obj(plugin_name, ds_plugins)
            plugin(self, plugins_to_apply[plugin_name], stack_conf.env)

    def define_datashack_backend(self,):
        # define backend here
        # https://registry.terraform.io/providers/hashicorp/aws/latest/docs#aws-configuration-reference
        AwsProvider(self, "aws_provider")


    @classmethod
    def get_class_name(cls) -> str:
        return cls.__name__

    # iterates over all modules that are datashack resource
    @classmethod
    @lru_cache(None)
    def get_datashack_elements(cls) -> Tuple[Set[Type[DatashackPlugin]], Set[Type[DatashackResourceConf]]]:
        # TODO this is by resource so the function above may be redundant
        # TODO no need to load all resources, give a lost of only required only

        plugins = []
        configs = []
        package_dir = Path(__file__).resolve().parent
        for (_, module_name, _) in iter_modules([package_dir]):

            # import the module and iterate through its attributes
            module = import_module(f"{__name__}.{module_name}")
            for attribute_name in dir(module):
                attribute = getattr(module, attribute_name)

                if isclass(attribute) and issubclass(attribute, DatashackPlugin):
                    # Add the class to this package's variables
                    plugins.append(attribute)
                elif isclass(attribute) and issubclass(attribute, DatashackResourceConf):
                    # Add the class to this package's variables
                    configs.append(attribute)
        return set(plugins), set(configs)


def get_datashack_obj(resource: str, resources_list: Set[Union[Type[DatashackResourceConf], Type[DatashackPlugin]]]) \
        -> Union[Type[DatashackResourceConf], Type[DatashackPlugin]]:
    return [r for r in resources_list if r.__name__.__eq__(resource)][0]
