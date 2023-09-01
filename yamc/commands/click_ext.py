# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas.vitvar@oracle.com

import json
import os
import re
import sys
import time
import sys
import signal
import typing as t

import click
import traceback
import logging

from click.core import Command

import yamc.config as yamc_config

from yamc import __version__ as version
from yamc.config import Config, init_logging
from yamc.utils import format_str_color, bcolors
from yamc.json2table import Table

from typing import Any, Dict, Sequence
from click import Option


def token_normalize_func(param):
    if ":" in param:
        param = param.split(":")[0]
    return param


class FlagOptions(click.ParamType):
    name = "flag_with_params"

    def convert(self, value, param, ctx):
        for opt in param.opts:
            for v in sys.argv[1:]:
                if v == opt:
                    return (True, [])
                if v.startswith(opt + ":"):
                    pattern = rf"^{opt}:(\w+(?:,\w+)*)$"
                    match = re.search(pattern, v)
                    if match:
                        parts = match.group(1).split(",")
                        return (True, parts)
                    self.fail(f"Invalid flag option {v}")
        return (False, [])


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
        yamc_config.DEBUG, yamc_config.DEBUG_PARAMS = ctx.params.pop("debug", (False, []))
        yamc_config.TRACEBACK = ctx.params.pop("traceback", False)

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
            if yamc_config.TRACEBACK:
                print("---")
                traceback.print_exc()
                print("---")

            sys.exit(1)


class BaseCommand(click.core.Command):
    """
    The `BaseCommand` is the base class for all commands. It initializes the logger.
    """

    def __init__(self, *args, **kwargs):
        if kwargs.get("log_handlers"):
            self.log_handlers = kwargs.get("log_handlers")
            kwargs.pop("log_handlers")
        else:
            # default log handlers
            self.log_handlers = ["file", "console"]
        super().__init__(*args, **kwargs)

    def init_logging(self, command_path):
        logs_dir = os.path.join(yamc_config.YAMC_HOME, "logs", "-".join(command_path.split(" ")[1:]))
        os.makedirs(logs_dir, exist_ok=True)
        filename_suffix = "-".join(command_path.split(" ")[1:])
        init_logging(
            logs_dir,
            filename_suffix,
            log_level="DEBUG" if yamc_config.DEBUG and len(yamc_config.DEBUG_PARAMS) == 0 else "INFO",
            handlers=self.log_handlers,
        )

    def command_run(self, ctx):
        self.init_logging(ctx.command_path)
        self.log = yamc_config.get_logger(ctx.command.name)
        self.log.info(f"Yet another metric collector, yamc v{version}")


class BaseCommandConfig(BaseCommand):
    """
    The `BaseCommandConfig` is the base class for all commands that require the configuration file.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params.insert(
            0,
            Option(
                ("-c", "--config"),
                metavar="<file>",
                required=True,
                help="Configuration file",
                default=yamc_config.CONFIG_FILE,
            ),
        )
        self.params.insert(
            0,
            Option(
                ("-e", "--env"),
                metavar="<file>",
                required=False,
                help="Environment variable file",
                default=yamc_config.CONFIG_ENV,
            ),
        )
        self.log = None

    def command_run(self, ctx):
        super().command_run(ctx)
        config_file = ctx.params.pop("config")
        env_file = ctx.params.pop("env")
        config = Config(config_file, env_file)
        self.log.info(f"The configuration loaded from {config.config_file}")

        config.init_config()

        ctx.params["config"] = config
        ctx.params["log"] = self.log

    def invoke(self, ctx):
        self.command_run(ctx)
        return super().invoke(ctx)


class TableCommand(BaseCommandConfig):
    def __init__(self, *args, **kwargs):
        self.table = Table(kwargs.pop("table_def", None), None, False)
        self.watch_opts = kwargs.pop("watch_opts", [])
        super().__init__(*args, **kwargs)
        self.params.insert(
            0,
            Option(
                ("-d", "--describe"),
                help="Describe the table columns",
                is_flag=True,
            ),
        )
        if "option" in self.watch_opts:
            self.params.insert(
                0,
                Option(
                    ("-w", "--watch"),
                    help="Watch the data for changes and update the table.",
                    is_flag=True,
                ),
            )

        self.describe = False
        self.watch = False

    def invoke(self, ctx):
        self.describe = ctx.params.pop("describe")
        self.watch = "always" in self.watch_opts or ctx.params.pop("watch", False)
        if self.describe:
            self.table.describe()
        else:
            data = super().invoke(ctx)
            if isinstance(data, list):
                if not self.watch:
                    self.table.display(data)
                else:
                    self.table.watch(lambda: data)
            elif callable(data):
                if self.watch:
                    self.table.watch(data)
                else:
                    self.table.display(data())
            else:
                raise Exception("The data must be either a list or a callable object!")
