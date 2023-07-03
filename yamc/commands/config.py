# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

from yamc.config import Config, read_raw_config
import logging
import time
import json

import yamc.config as yamc_config
from .click_ext import BaseCommandConfig
from yamc import __version__ as version

from yamc.component import WorkerComponent

import click


### common options


@click.command("config", help="Get configuration details.", cls=BaseCommandConfig, log_handlers=["file"])
def config(config, log):
    print(json.dumps(config.raw_config, indent=4, sort_keys=True, default=str))
