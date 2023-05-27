# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas.vitvar@oracle.com

import click
import signal
import traceback
import sys

from yamc import __version__
from .click_ext import CoreCommandGroup
from .run import run
from .plugin import plugin
from .config import config


@click.group(cls=CoreCommandGroup)
@click.option("--no-ansi", "no_ansi", is_flag=True, default=False, help="No ANSI colors.")
@click.option("-d", "--debug", "debug", is_flag=True, default=False, help="Print debug information.")
@click.version_option(version=__version__)
def yamc():
    pass


yamc.add_command(run)
yamc.add_command(plugin)
yamc.add_command(config)
