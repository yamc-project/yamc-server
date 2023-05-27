# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas.vitvar@oracle.com

import json
import os
import re
import sys
import time
import sys
import signal

import click
import traceback

import yamc.config as yamc_config
from yamc.utils import format_str_color, bcolors


class CoreCommandGroup(click.core.Group):
    """
    The `CoreCommand` is the main entry point for the CLI. It initializes the global variables and
    the logger, and handles the global options such as `--no-ansi` and `--debug`.
    """

    def invoke(self, ctx):
        """
        The main method to run the command.
        """
        # retrieve the global options
        yamc_config.ANSI_COLORS = not ctx.params.pop("no_ansi", False)
        yamc_config.DEBUG = ctx.params.pop("debug", False)

        # pylint: disable=broad-except
        try:
            for sig in ("TERM", "INT"):
                signal.signal(
                    getattr(signal, "SIG" + sig),
                    lambda x, y: yamc_config.exit_event.set(),
                )
            click.core.Group.invoke(self, ctx)
        except click.exceptions.Exit as exception:
            sys.exit(int(str(exception)))
        except click.core.ClickException as exception:
            raise exception
        except Exception as exception:
            sys.stderr.write(
                format_str_color(
                    f"ERROR: {str(exception)}\n",
                    bcolors.ERROR,
                    not yamc_config.ANSI_COLORS,
                )
            )
            if yamc_config.DEBUG:
                print("---")
                traceback.print_exc()
                print("---")

            sys.exit(1)
