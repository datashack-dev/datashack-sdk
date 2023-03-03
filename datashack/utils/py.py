from importlib.machinery import SourceFileLoader
from inspect import isclass
from pkgutil import iter_modules
from pathlib import Path
from typing import Union, Optional, Sequence, Any
from functools import lru_cache
import inspect
import os
import pkg_resources
from modulefinder import ModuleFinder
import imp
import sys
import sys, inspect
from typing import Tuple, Sequence
import importlib
import itertools

def dynamic_load_classes(name: str)->Sequence[object]:
    modules = tuple(_dynamic_import_from_dir(name))
    return itertools.chain.from_iterable(dict(inspect.getmembers(mod)).values() for mod in modules)


def _get_py_files(src):
    cwd = os.getcwd() # Current Working directory
    py_files = [] 
    for root, dirs, files in os.walk(src):
        for file in files:
            if file.endswith(".py"):
                py_files.append(os.path.join(cwd, root, file))
    return py_files

def _dynamic_path_import(module_name, py_path):
    module_spec = importlib.util.spec_from_file_location(module_name, py_path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module

def _dynamic_import_from_dir(src):
    my_py_files = _get_py_files(src)
    for py_file in my_py_files:
        module_name = os.path.split(py_file)[-1].replace(".py", "")
        imported_module = _dynamic_path_import(module_name, py_file)
        yield imported_module
