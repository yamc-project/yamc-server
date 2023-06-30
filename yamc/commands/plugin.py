# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import click

import pkgutil
import importlib
import inspect
from yamc.json2table import Table

from yamc.writers import Writer
from yamc.providers import BaseProvider
from yamc.collectors import BaseCollector
from functools import cmp_to_key


def find_yamc_subclasses():
    """
    Find all subclasses of base writer, provider and collector classes in modules
    whose names start with `yamc`.
    """

    def _add_class(data, class_module, cls, base_class, module, type):
        if issubclass(cls, base_class) and cls != base_class:
            data.append(
                {
                    "class": cls,
                    "classname": cls.__name__,
                    "class_module": class_module.__name__,
                    "type": type,
                    "module": module.__name__,
                    "version": module.__version__,
                }
            )

    def _compare(x, y):
        if x["module"] == "yamc":
            return -1 if y["module"] != "yamc" else 0
        elif y["module"] == "yamc":
            return 1
        return len(x["class_module"]) - len(y["class_module"])

    writers, providers, collectors = [], [], []
    for module in pkgutil.iter_modules():
        if module.name.startswith("yamc"):
            modules = [importlib.import_module(module.name)]
            for submodule in pkgutil.walk_packages(modules[0].__path__, modules[0].__name__ + "."):
                if submodule.name.split(".")[-1] != "__main__":
                    modules.append(importlib.import_module(submodule.name))
            for m in modules:
                for class_name, cls in inspect.getmembers(m, inspect.isclass):
                    _add_class(writers, m, cls, Writer, modules[0], "Writer")
                    _add_class(providers, m, cls, BaseProvider, modules[0], "Provider")
                    _add_class(collectors, m, cls, BaseCollector, modules[0], "Collector")
    _data = []
    _set = set()
    for c in sorted(writers + providers + collectors, key=cmp_to_key(_compare)):
        if c["classname"] not in _set:
            _data.append(c)
            _set.add(c["classname"])
    return _data


### common options


@click.group(help="Plugin commands.")
def plugin():
    pass


@click.command("list", help="List of installed plugins")
def list():
    components = find_yamc_subclasses()
    table_def = [
        {"name": "CLASS", "value": "{class_module}.{classname}", "help": "Class name"},
        {"name": "TYPE", "value": "{type}", "help": "Plugin type"},
        {"name": "MODULE", "value": "{module}", "help": "Module name"},
        {"name": "VERSION", "value": "{version}", "help": "Module version"},
    ]
    Table(table_def, None, False).display(components)


plugin.add_command(list)
