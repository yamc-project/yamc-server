# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import json
import os
import re

import click
import pandas as pd

import yamc.config as yamc_config
from yamc.config import Config
from yamc.json2table import Table
from yamc.providers import BaseProvider, EventProvider, PerformanceProvider, PerformanceAnalyzer
from yamc.utils import Map
from datetime import timedelta

from .click_ext import BaseCommandConfig, TableCommand


def validate_offset(ctx, param, value):
    """
    Validator for the offset option.
    """
    if value and value[-1] in ("h", "m", "d") and value[:-1].isdigit():
        num = int(value[:-1])
        unit = value[-1]
        if unit == "d":
            offset = timedelta(days=num)
        elif unit == "h":
            offset = timedelta(hours=num)
        elif unit == "m":
            offset = timedelta(minutes=num)
        else:
            raise ValueError("Invalid time unit. Use 'd', 'h', or 'm'.")
        return offset.total_seconds()
    else:
        raise click.BadParameter("Invalid format. Use a numeric value followed by h, m, or d.")


def parse_list(ctx, param, value):
    if value is not None:
        return [x.strip() for x in value.split(",")]
    else:
        return value


def format_id(d, v, e):
    if len(v) > 30:
        parts = re.split(r"[.:]", v)
        shortened_parts = [part[0] for part in parts[:-1]]
        shortened_parts.append(parts[-1])
        s = ".".join(shortened_parts)
        if len(s) > 30:
            return s[:30] + "..."
        else:
            return s
    else:
        return v


def format_float(d, v, e):
    return f"{v:.2f}"


def find_provider(config, provider_id, raise_exception=True):
    """
    Find a provider by its ID.
    """
    provider = None
    for component in config.scope.all_components:
        if isinstance(component, BaseProvider) and component.component_id == provider_id:
            provider = component
            break
    if raise_exception and provider is None:
        raise Exception(f"Provider with ID '{provider_id}' not found.")
    return provider


PROVIDER_PERF_TABLE = [
    {"name": "PROVIDER", "value": "{provider}", "help": "Provider Id."},
    {
        "name": "ID",
        "value": "{id}",
        "format": format_id,
        "help": "Data Id used by the collector when calling the provider.",
    },
    {"name": "RUNS", "value": "{runs}", "help": "Number of totoal runs."},
    {
        "name": "WAITS",
        "value": "{waits}",
        "help": "Number of waitings due to an error or a higher respone time.",
    },
    {"name": "ERRS", "value": "{errors}", "help": "Number of error runs."},
    {"name": "S_RATE", "value": "{rate}", "format": format_float, "help": "Number of succesful runs."},
    {"name": "T_AVG [s]", "value": "{duration_mean}", "format": format_float, "help": "Mean time of successful runs."},
    {
        "name": "T_MAX [s]",
        "value": "{duration_max}",
        "format": format_float,
        "help": "Maximum time of successful runs.",
    },
    {"name": "RECORDS", "value": "{records}", "help": "Number of records the provider retrieved."},
    {"name": "LAST_ERROR", "value": "{error}", "help": "The last error message the provider returned."},
]

PROVIDER_LIST_TABLE = [
    {"name": "PROVIDER", "value": "{provider}", "help": "Provider Id."},
    {"name": "CLASS", "value": "{clazz}", "help": "Provider class"},
    {"name": "PERF", "value": "{perf}", "help": "The provider is performance-enabled."},
    {"name": "EVENT", "value": "{event}", "help": "The provider is event-based."},
    {"name": "SOURCE", "value": "{source}", "help": "The source of the provider."},
]


@click.group("provider", help="Provider commands.")
def command_provider():
    """
    Provider commands.
    """
    pass


@click.command(
    "list", help="List all providers.", cls=TableCommand, table_def=PROVIDER_LIST_TABLE, log_handlers=["file"]
)
def provider_list(config, log):
    """
    List all providers.
    """
    data = []
    for component in config.scope.all_components:
        if isinstance(component, BaseProvider):
            data.append(
                Map(
                    provider=component.component_id,
                    clazz=component.__class__.__name__,
                    perf="yes" if isinstance(component, PerformanceProvider) else "no",
                    event="yes" if isinstance(component, EventProvider) else "no",
                    source=component.source,
                )
            )
    return data


@click.command("config", help="Get a provider configuration.", cls=BaseCommandConfig, log_handlers=["file"])
@click.argument(
    "provider_id",
    metavar="<provider_id>",
    required=True,
)
def provider_config(config, log, provider_id):
    """
    Get a provider configuration.
    """
    provider = find_provider(config, provider_id)
    print(json.dumps(provider.config._config, indent=4, sort_keys=True, default=str))


@click.command(
    "perf",
    help="Show providers' performance.",
    cls=TableCommand,
    log_handlers=["file"],
    table_def=PROVIDER_PERF_TABLE,
    watch_opts=["option"],
)
@click.argument(
    "provider_ids",
    metavar="<id1 | pattern1, id2 | pattern2, ...>",
    required=False,
    callback=parse_list,
)
@click.option(
    "--perf-dir",
    "perf_dir",
    metavar="<dir>",
    required=False,
    help="Directory where the performance data is stored.",
)
@click.option(
    "--offset",
    "-o",
    type=click.STRING,
    metavar="<offset>",
    callback=validate_offset,
    default="1h",
    help="The time offset from the max time (e.g., 2h, 30m, 3d)",
)
def provider_perf(config, log, provider_ids, perf_dir, offset):
    """
    Show providers' performance.
    """
    if perf_dir is None:
        perf_dir = yamc_config.YAMC_PERFDIR
    log.info(f"Using {perf_dir} to search performance csv files.")

    providers = config.search(BaseProvider, provider_ids)
    provider_ids = [x.component_id for x in providers]
    log.info(f"Will use the following providers to analyze the performance: {provider_ids}")

    analyzer = PerformanceAnalyzer(perf_dir)

    def get_data():
        min_time, max_time, data = analyzer.get_perf_data(offset, None)
        range_info = f"{min_time.strftime('%Y-%m-%d %H:%M:%S')}-{max_time.strftime('%Y-%m-%d %H:%M:%S')}"
        print(f"Time range: {range_info}")
        return data

    return get_data


command_provider.add_command(provider_list)
command_provider.add_command(provider_config)
command_provider.add_command(provider_perf)
