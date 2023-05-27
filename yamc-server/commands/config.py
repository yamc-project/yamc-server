# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

from yamc.config import Config, read_raw_config
import logging
import time
import json

import yamc.config as yamc_config

from yamc import __version__ as version

from yamc.component import WorkerComponent

import click


### common options


@click.command("config", help="Config command.")
@click.option(
    "-c",
    "--config",
    "config",
    metavar="<file>",
    is_flag=False,
    required=True,
    help="Configuration file",
)
@click.option(
    "-e",
    "--env",
    "env",
    metavar="<file>",
    is_flag=False,
    required=False,
    help="Environment variable file",
)
def config(config, env):
    config = Config(config, env, False, "DEBUG" if yamc_config.DEBUG else "INFO")
    print(json.dumps(config.raw_config, indent=4, sort_keys=True, default=str))
