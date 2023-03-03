from abc import ABC, abstractmethod
from datashack.utils.io import exists


class Loader(ABC):

    def __init__(self, src: str):
        self.resource_configs = None

        if exists(src):
            self.resource_configs = self.from_folder(src)
        else:
            raise RuntimeError(f"unable to identify loader source {src}")

        if not self.resource_configs:
            raise RuntimeError("empty state")

    @abstractmethod
    def from_folder(self, src: str):
        raise NotImplementedError()
