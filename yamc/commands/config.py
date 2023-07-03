# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

from yamc.config import Config, read_raw_config
import logging
import time
import json
import os

import yamc.config as yamc_config
from .click_ext import BaseCommandConfig, BaseCommand
from yamc import __version__ as version

from yamc.component import WorkerComponent

from yamc.utils import deep_find

import click


@click.group(help="Config commands.")
def config():
    pass


@click.command("get", help="Get configuration details.", cls=BaseCommandConfig, log_handlers=["file"])
@click.argument("path", required=False, default=None)
def config_get(config, log, path):
    c = deep_find(config.raw_config, path, {}) if path is not None else config.raw_config
    print(json.dumps(c, indent=4, sort_keys=True, default=str))


@click.command("env", help="Show environment variables.", cls=BaseCommand, log_handlers=["file"])
def config_env():
    for e in yamc_config.env_variables:
        print(f"{e}={os.environ.get(e, '<not-set>')}")


config.add_command(config_get)
config.add_command(config_env)
