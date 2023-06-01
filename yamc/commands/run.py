# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

from yamc.config import Config, read_raw_config
import logging
import time

import yamc.config as yamc_config

from yamc import __version__ as version

from yamc.component import WorkerComponent

import click


### common options


@click.command("run", help="Run command.")
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
def run(config, env):
    config = Config(config, env, False, "DEBUG" if yamc_config.DEBUG else "INFO")
    log = yamc_config.get_logger("loop")
    log.info(f"Yet another metric collector, yamc v{version}")

    config.init_config()
    log.info(f"The configuration loaded from {config.config_file}")

    log.info("Starting the components.")
    for component in config.scope.all_components:
        if isinstance(component, WorkerComponent):
            component.start(yamc_config.exit_event)
    try:
        log.info("Running the main loop.")
        yamc_config.exit_event.wait()
    finally:
        log.info("Waiting for components' workers to end.")
        for component in config.scope.all_components:
            if isinstance(component, WorkerComponent):
                component.join()

        log.info("Destroying components.")
        for component in config.scope.all_components:
            component.destroy()

        log.info("Done.")
