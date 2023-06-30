# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

from yamc.config import Config

import yamc.config as yamc_config

from yamc.collectors import BaseCollector, CronCollector
from yamc.utils import Map
from yamc.json2table import Table

from .click_ext import BaseCommandConfig

import click
import json
import time


@click.group("collector", help="Collector commands.")
def command_collector():
    pass


@click.command("list", help="List all collectors.", cls=BaseCommandConfig, log_handlers=["file"])
def collector_list(config, log):
    data = []
    for component in config.scope.all_components:
        if isinstance(component, BaseCollector):
            data.append(
                Map(
                    collector=component.component_id,
                    schedule=component.schedule if isinstance(component, CronCollector) else "--",
                    writers=",".join([w["__writer"].component_id for w in component.writers.values()]),
                )
            )

    table_def = [
        {"name": "COLLECTOR", "value": "{collector}", "help": "Collector ID"},
        {"name": "SCHEDULE", "value": "{schedule}", "help": "Collector ID"},
        {"name": "WRITERS", "value": "{writers}", "help": "List of collector writers"},
    ]
    Table(table_def, None, False).display(data)


@click.command("test", help="Show collector data.", cls=BaseCommandConfig, log_handlers=["file"])
@click.option(
    "-i",
    "collector_id",
    metavar="<id>",
    is_flag=False,
    required=True,
    help="Collector ID to test",
)
@click.option(
    "--provider",
    "show_provider",
    is_flag=True,
    required=False,
    help="Show data from collectos' provider",
)
@click.option(
    "--writer",
    "show_writer",
    is_flag=True,
    required=False,
    help="Show data from collectos' writers",
)
@click.option(
    "--limit",
    "limit",
    is_flag=False,
    default=0,
    type=int,
    required=False,
    help="Limit number of records to show",
)
@click.option(
    "--count",
    "count",
    is_flag=False,
    default=1,
    type=int,
    required=False,
    help="Number of iterations to retrieve data",
)
@click.option(
    "--delay",
    "delay",
    is_flag=False,
    default=0,
    type=int,
    required=False,
    help="Delay between iterations in seconds",
)
def collector_test(config, log, collector_id, show_provider, show_writer, limit, count, delay):
    if not show_provider and not show_writer:
        raise Exception("One of --provider or --writer must be specified!")

    yamc_config.TEST_MODE = True
    collector = None
    for component in config.scope.all_components:
        if isinstance(component, BaseCollector) and component.component_id == collector_id:
            collector = component
            break

    if collector is None:
        raise Exception(f"Collector with ID {collector_id} not found!")

    _iter = 0
    while True:
        if _iter > 0:
            print(f"-- sleeping {delay} seconds...")
            yamc_config.exit_event.wait(delay)
            if yamc_config.exit_event.is_set():
                break

        print(f"-- retrieving data, iteration: {_iter + 1}/{count}")
        x = time.time()
        data = collector.prepare_data()
        print(f"-- retrieved {len(data)} records from the provider in {time.time()-x} seconds")
        if show_provider:
            if isinstance(data, list):
                data = data[:limit] if limit > 0 else data
            print(json.dumps(data, indent=4, sort_keys=True, default=str))
        elif show_writer:
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


command_collector.add_command(collector_list)
command_collector.add_command(collector_test)