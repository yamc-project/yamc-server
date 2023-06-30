# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

from yamc.config import Config, read_raw_config
import logging
import time

import yamc.config as yamc_config

from yamc.component import WorkerComponent
from .click_ext import BaseCommandConfig

import click


### common options


@click.command("run", help="Run command.", cls=BaseCommandConfig)
def run(config, log):
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
