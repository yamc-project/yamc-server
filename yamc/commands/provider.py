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
from yamc.providers import BaseProvider, EventProvider, PerformanceProvider
from yamc.utils import Map
from datetime import timedelta

from .click_ext import BaseCommandConfig

PERF_CSV_COLUMNS_DEF = {
    "Provider": None,
    "Time": float,
    "Id": str,
    "Records": int,
    "Duration": float,
    "WaitingCycles": int,
    "ReasonToWait": int,
    "Error": None,
    "ErrorMessage": str,
}


def validate_offset(ctx, param, value):
    """
    Validator for the offset option.
    """
    if value and value[-1] in ("h", "m", "d") and value[:-1].isdigit():
        return value
    else:
        raise click.BadParameter("Invalid format. Use a numeric value followed by h, m, or d.")


def offset_seconds(offset_param):
    numeric_part = int(offset_param[:-1])
    time_unit = offset_param[-1]
    if time_unit == "d":
        offset = timedelta(days=numeric_part)
    elif time_unit == "h":
        offset = timedelta(hours=numeric_part)
    elif time_unit == "m":
        offset = timedelta(minutes=numeric_part)
    else:
        raise ValueError("Invalid time unit. Use 'd', 'h', or 'm'.")
    return offset.total_seconds()


def parse_list(ctx, param, value):
    if value is not None:
        return [x.strip() for x in value.split(",")]
    else:
        return value


def str_bool(v):
    return str(v)


def last_error(x):
    error_messages = x[x != "-"]
    return error_messages.iloc[-1] if len(error_messages) > 0 else None


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
    collector = None
    for component in config.scope.all_components:
        if isinstance(component, BaseProvider) and component.component_id == provider_id:
            collector = component
            break
    if raise_exception and collector is None:
        raise Exception(f"Provider with ID '{provider_id}' not found.")
    return collector


def get_perf_data(csv_files, modified_time, offset, provider_ids, log):
    """
    Get performance data from csv files. The data is filtered by the offset and provider IDs.
    """
    # time information
    offset_s = offset_seconds(offset)
    latest_modified_time = pd.Timestamp.fromtimestamp(modified_time)
    reference_time = latest_modified_time - timedelta(seconds=offset_s)
    log.info(f"The latest modified time of the csv file is {latest_modified_time}.")
    log.info(f"Filtering csv files between {reference_time} and {latest_modified_time}.")

    # read csv files
    dfs = []
    for csv_file in csv_files:
        last_modified_time = pd.Timestamp.fromtimestamp(os.path.getmtime(csv_file))
        if last_modified_time >= reference_time:
            df = pd.read_csv(
                csv_file,
                header=None,
                quotechar='"',
                escapechar="\\",
                names=[x for x in PERF_CSV_COLUMNS_DEF.keys()],
                dtype={k: v for k, v in PERF_CSV_COLUMNS_DEF.items() if v is not None},
                converters={0: lambda v: v.split("/")[-1], 7: lambda v: str_bool(v)},
            )
            df["Time"] = pd.to_datetime(df["Time"], unit="s")
            df = df[df["Provider"].isin(provider_ids)]
            df.set_index("Time", inplace=True)
            dfs.append(df)

    log.info(f"Using {len(dfs)} csv files to load data.")
    df = pd.concat(dfs, ignore_index=False)

    # min and max time
    max_time = df.index.max()
    min_time = max_time - pd.Timedelta(seconds=offset_s)
    if min_time < df.index.min():
        min_time = df.index.min()
    range_info = f"{min_time.strftime('%Y-%m-%d %H:%M:%S')}-{max_time.strftime('%Y-%m-%d %H:%M:%S')}"
    df = df[df.index >= min_time]
    log.info(f"The final time range to calculate performance stats is {range_info}.")
    print(f"Time range: {range_info}")

    # aggregate the data
    agg_funcs = {
        "Duration": ["mean", "max"],
        "Error": [
            lambda x: (x == "True").sum(),
            lambda x: (x == "False").sum(),
            lambda x: (x == "None").sum(),
        ],
        "Records": ["sum"],
        "ErrorMessage": [last_error],
    }

    result = df.groupby(["Provider", "Id"]).agg(agg_funcs).reset_index()

    # rename columns and add more calculations
    result.columns = [
        "provider",
        "id",
        "duration_mean",
        "duration_max",
        "errors",
        "success",
        "waits",
        "records",
        "error",
    ]
    result["rate"] = result["success"] / (result["success"] + result["errors"])
    result["runs"] = result["success"] + result["errors"] + result["waits"]

    # convert to dict and display data
    return result.to_dict(orient="records")


@click.group("provider", help="Provider commands.")
def command_provider():
    """
    Provider commands.
    """
    pass


@click.command("list", help="List all providers.", cls=BaseCommandConfig, log_handlers=["file"])
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

    table_def = [
        {"name": "PROVIDER", "value": "{provider}", "help": "Collector ID"},
        {"name": "CLASS", "value": "{clazz}", "help": "Collector class"},
        {"name": "PERF", "value": "{perf}", "help": "Performance-enabled"},
        {"name": "EVENT", "value": "{event}", "help": "Event-based"},
        {"name": "SOURCE", "value": "{source}", "help": "Provider source"},
    ]
    Table(table_def, None, False).display(data)


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


@click.command("perf", help="Show providers' performance.", cls=BaseCommandConfig, log_handlers=["file"])
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
    help="The time offset from the last time (e.g., 2h, 30m, 3d)",
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

    # get of all csv files
    csv_files = [os.path.join(perf_dir, filename) for filename in os.listdir(perf_dir)]
    modified_time = max(os.path.getmtime(csv_file) for csv_file in csv_files)
    log.info(f"There are {len(csv_files)} files in the directory.")

    # retrieve data
    data = get_perf_data(csv_files, modified_time, offset, provider_ids, log)
    log.info(f"The performance data is {data}")

    table_def = [
        {"name": "PROVIDER", "value": "{provider}", "help": "Provider ID"},
        {"name": "ID", "value": "{id}", "format": format_id, "help": "Data ID"},
        {"name": "RUNS", "value": "{runs}", "help": "Number of runs"},
        {"name": "WAITS", "value": "{waits}", "help": "Number of waitings"},
        {"name": "ERRS", "value": "{errors}", "help": "Number of errors"},
        {"name": "S_RATE", "value": "{rate}", "format": format_float, "help": "Number of successes"},
        {"name": "T_AVG [s]", "value": "{duration_mean}", "format": format_float, "help": "Duration mean value"},
        {"name": "T_MAX [s]", "value": "{duration_max}", "format": format_float, "help": "Duration mean value"},
        {"name": "RECORDS", "value": "{records}", "help": "Number of records"},
        {"name": "LAST_ERROR", "value": "{error}", "help": "Last error message"},
    ]
    Table(table_def, None, False).display(data)


command_provider.add_command(provider_list)
command_provider.add_command(provider_config)
command_provider.add_command(provider_perf)