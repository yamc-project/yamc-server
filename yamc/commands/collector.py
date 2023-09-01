# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

from yamc.config import Config

import yamc.config as yamc_config
import re
import sys
import signal

from yamc.collectors import BaseCollector, CronCollector
from yamc.utils import Map
from yamc.json2table import Table
from yamc.providers import OperationalError


from .click_ext import BaseCommandConfig, TableCommand

import click
import json
import time
import threading


def find_collector(config, collector_id, raise_exception=True):
    """
    Finds a collector with the given ID in the configuration.
    """
    collector = None
    for component in config.scope.all_components:
        if isinstance(component, BaseCollector) and component.component_id == collector_id:
            collector = component
            break
    if raise_exception and collector is None:
        raise Exception(f"Collector with ID {collector_id} not found!")
    return collector


def _format_duration(d, v, e):
    if e.status == "RUNNING" and e.end_time is None:
        duration = time.time() - e.start_time
    elif e.status in ["DONE", "ERROR"]:
        duration = e.end_time - e.start_time
    else:
        return "--"
    return f"{duration:.2f}"


def _format_records(d, v, e):
    if e.status == "DONE":
        return len(v)
    else:
        return "--"


def _format_result(d, v, e):
    if v is not None:
        return v[:100]
    else:
        return "--"


def _format_id(d, v, e):
    return v[:25]


COLLECTOR_TEST_TABLE = [
    {"name": "COLLECTOR", "value": "{id}", "format": _format_id, "help": "Collector ID"},
    {"name": "STATUS", "value": "{status}", "help": "Data retrieval status"},
    {"name": "TIME [s]", "value": "{d}", "format": _format_duration, "help": "Running duration"},
    {"name": "RECORDS", "value": "{data}", "format": _format_records, "help": "Number of records retrieved"},
    {"name": "RESULT", "value": "{result}", "format": _format_result, "help": "Result message"},
]

COLLECTOR_LIST_TABLE = [
    {"name": "COLLECTOR", "value": "{collector}", "help": "Collector ID"},
    {"name": "CLASS", "value": "{clazz}", "help": "Collector class"},
    {"name": "STATUS", "value": "{status}", "help": "Collector status"},
    {"name": "SCHEDULE", "value": "{schedule}", "help": "Cron schedule"},
    {"name": "WRITERS", "value": "{writers}", "help": "List of collector's writers"},
]


@click.group("collector", help="Collector commands.")
def command_collector():
    """
    Collector commands.
    """
    pass


@click.command(
    "list", help="List all collectors.", cls=TableCommand, table_def=COLLECTOR_LIST_TABLE, log_handlers=["file"]
)
def collector_list(config, log):
    """
    List all collectors.
    """
    data = []
    for component in config.scope.all_components:
        if isinstance(component, BaseCollector):
            data.append(
                Map(
                    collector=component.component_id,
                    clazz=component.__class__.__name__,
                    status="ENABLED" if component.enabled else "DISABLED",
                    schedule=component.schedule if isinstance(component, CronCollector) else "--",
                    writers=",".join([w["__writer"].component_id for w in component.writers.values()]),
                )
            )
    return data


@click.command("config", help="Get a collector configuration.", cls=BaseCommandConfig, log_handlers=["file"])
@click.argument(
    "collector_id",
    metavar="<collector_id>",
    required=True,
)
def collector_config(config, log, collector_id):
    """
    Get a collector configuration.
    """
    collector = find_collector(config, collector_id)
    print(json.dumps(collector.config._config, indent=4, sort_keys=True, default=str))


@click.command("data", help="Retrieve data using collector's provider.", cls=BaseCommandConfig, log_handlers=["file"])
@click.argument(
    "collector_id",
    metavar="<collector_id>",
    required=True,
)
@click.option(
    "-w",
    "--writer",
    "show_writer",
    is_flag=True,
    required=False,
    help="Show data of writers instead of the provider",
)
@click.option(
    "-l",
    "--limit",
    "limit",
    metavar="<records>",
    is_flag=False,
    default=0,
    type=int,
    required=False,
    help="Limit number of records to show",
)
@click.option(
    "--count",
    "count",
    metavar="<iterations>",
    is_flag=False,
    default=1,
    type=int,
    required=False,
    help="Number of iterations to retrieve data",
)
@click.option(
    "--delay",
    "delay",
    metavar="<seconds>",
    is_flag=False,
    default=0,
    type=int,
    required=False,
    help="Delay between iterations in seconds",
)
@click.option(
    "--force",
    "force",
    is_flag=True,
    required=False,
    help="Collect data regardless of the collector's status",
)
def collector_data(config, log, collector_id, show_writer, limit, count, delay, force):
    """
    Retrieve data using collector's provider. The data is retrieved from the provider and printed to the console.
    Alternativelly, the data can be retrieved in the form of the writer's data.
    """
    yamc_config.TEST_MODE = True
    collector = find_collector(config, collector_id)
    if not collector.enabled and not force:
        raise Exception("The collector is disabled! Use --force option to force the data collection.")
    _iter = 0
    while True:
        if _iter > 0:
            print(f"-- sleeping {delay} seconds...")
            yamc_config.exit_event.wait(delay)
            if yamc_config.exit_event.is_set():
                break

        print(f"-- retrieving data, iteration: {_iter + 1}/{count}")
        x = time.time()
        data = collector.test()
        print(
            f"-- retrieved {len(data) if data is not None else 0} records from the provider in {time.time()-x:.4f} seconds"
        )
        if not show_writer:
            if isinstance(data, list):
                data = data[:limit] if limit > 0 else data
            print(json.dumps(data, indent=4, sort_keys=True, default=str))
        else:
            print(f"-- getting writers data...")
            collector.write(data, ignore_healthcheck=True)
            for writer in collector.writers.values():
                w = writer["__writer"]
                record_num = 0
                while w.queue.qsize() > 0:
                    print(json.dumps(w.queue.get(), indent=4, sort_keys=True, default=str))
                    w.queue.task_done()
                    record_num += 1
                    if limit > 0 and record_num >= limit:
                        break
        _iter += 1
        if _iter >= count:
            break
        else:
            print("++")


@click.command(
    "test",
    help="Test one or more collectors.",
    cls=TableCommand,
    log_handlers=["file"],
    table_def=COLLECTOR_TEST_TABLE,
    watch_opts=["always"],
)
@click.argument(
    "collector_ids",
    metavar="<id1 | pattern1, id2 | pattern2, ...>",
    required=False,
)
@click.option(
    "--force",
    "force",
    is_flag=True,
    required=False,
    help="Collect data regardless of the collector's status",
)
def collector_test(config, log, collector_ids, force):
    """
    Test one or more collectors. The collectors are run in parallel and the results are displayed in a table.
    """

    def _run_collector(item):
        item.start_time = time.time()
        try:
            item.status = "RUNNING"
            item.data = item.collector.test()
            item.status = "DONE"
            item.result = "OK"
        except Exception as e:
            item.status = "ERROR"
            if isinstance(e, OperationalError) and e.original_exception is not None:
                item.result = str(e.original_exception).split("\n")[0]
            else:
                item.result = str(e)
        item.end_time = time.time()

    yamc_config.TEST_MODE = True
    struct = lambda x: Map(
        collector=x,
        id=x.component_id,
        start_time=None,
        end_time=None,
        data=None,
        result=None,
        status="WAITING" if x.enabled and not force else "DISABLED",
        enabled=x.enabled,
    )
    data = []
    if collector_ids is not None:
        id_patterns = [x.strip() for x in collector_ids.split(",")]
        for component in config.scope.all_components:
            if isinstance(component, BaseCollector):
                for pattern in id_patterns:
                    if re.match(pattern, component.component_id):
                        data.append(struct(component))
                        break
    else:
        data = [struct(x) for x in config.scope.all_components if isinstance(x, BaseCollector)]

    threads = []
    for item in data:
        if item.enabled or force:
            threads.append(threading.Thread(target=_run_collector, args=(item,), daemon=True))
            threads[-1].start()

    return lambda: data if not all([not t.is_alive() for t in threads]) else None


command_collector.add_command(collector_list)
command_collector.add_command(collector_config)
command_collector.add_command(collector_data)
command_collector.add_command(collector_test)
