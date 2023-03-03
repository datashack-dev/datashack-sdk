from .base import Loader

from datashack.utils.py import dynamic_load_classes
from datashack_sdk import DatashackSdk


class PyLoader(Loader):
    
    def from_folder(self, src: str):
        members = dynamic_load_classes(src)
        return tuple(
            obj
            for obj in members
            if isinstance(obj, DatashackSdk)
        )
