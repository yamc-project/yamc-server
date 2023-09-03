# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas.vitvar@oracle.com

import click
import signal
import traceback
import sys

from yamc import __version__
from .click_ext import CoreCommandGroup, FlagOptions, token_normalize_func
from .run import run
from .plugin import plugin
from .config import config
from .collector import command_collector
from .provider import command_provider

import yamc.config as yamc_config


@click.group(cls=CoreCommandGroup, context_settings=dict(token_normalize_func=token_normalize_func))
@click.option("--no-ansi", "no_ansi", is_flag=True, default=not yamc_config.ANSI_COLORS, help="No ANSI colors.")
@click.option(
    "--debug",
    "debug",
    is_flag=True,
    default=(yamc_config.YAMC_DEBUG, yamc_config.YAMC_DEBUG_PARAMS),
    type=FlagOptions(),
    help="Print debug information.",
)
@click.option(
    "--traceback", "traceback", is_flag=True, default=yamc_config.TRACEBACK, help="Print traceback for errors."
)
@click.version_option(version=__version__)
def yamc():
    pass


yamc.add_command(run)
yamc.add_command(plugin)
yamc.add_command(config)
yamc.add_command(command_collector)
yamc.add_command(command_provider)
