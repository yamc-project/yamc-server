# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import threading
import croniter
import sys
import copy

from datetime import datetime
from yamc.component import WorkerComponent
from yamc.providers import Topic
from yamc.utils import Map, deep_eval, merge_dicts, PythonExpression

import yamc.config as yamc_config


class BaseCollector(WorkerComponent):
    def __init__(self, config, component_id):
        if config.scope.writers is None:
            raise Exception("CRITICAL: There are no writers! Have you loaded writers before collectors?")

        super().__init__(config, component_id)
        self.config = config.collector(component_id)
        self.enabled = self.config.value_bool("enabled", default=True)
        self.writers = {}

        # read writer configurations for this this collector
        # the writer objects will be later provided in set_writers method
        for w in self.config.value("writers", default=[]):
            if w["writer_id"] not in config.writers.keys():
                self.log.warn(
                    f"The writer with id {w['writer_id']} does not exist. The collector will not write data using this writer definition!"
                )
            self.writers[w["writer_id"]] = {k: v for k, v in w.items() if k != "writer_id"}
            self.writers[w["writer_id"]]["__writer"] = None

        for w in config.scope.writers.values():
            if w.component_id in self.writers.keys():
                self.writers[w.component_id]["__writer"] = w

        if not self.enabled:
            self.log.debug(f"The collector {component_id} is disabled")

        self.data_def = self.config.value("data", required=False, no_eval=True)
        if self.data_def is None:
            self.data_def = Map(__nod=0)
        if not isinstance(self.data_def, dict) and not callable(getattr(self.data_def, "eval", None)):
            raise Exception("The value of data property must be dict or a Python expression!")
        self.max_history = self.config.value_int("max_history", default=120)
        self.history = []

    def add_time(self, data):
        if data.get("time") is None:
            data["time"] = int(time.time())
        return data

    def prepare_data(self, scope=None):
        _data, data = [], None
        if isinstance(self.data_def, dict):
            data = deep_eval(
                Map(self.data_def),
                scope=self.base_scope(custom_scope=scope),
                log=self.log,
                raise_ex=False,
            )
        elif callable(getattr(self.data_def, "eval", None)):
            data = self.data_def.eval(self.base_scope(custom_scope=scope))
        else:
            # this should not really happen
            raise Exception("CRITICAL: Invalid structure of data definition!")

        if data is None:
            return None
        if isinstance(data, list):
            for d in data:
                _data.append(self.add_time(d))
        elif isinstance(data, dict):
            _data.append(self.add_time(data))
        else:
            raise Exception("The data must be dict or list!")
        if self.max_history > 0:
            self.history += _data
            self.history = self.history[-min(self.max_history, len(self.history)) :]
        return _data

    def write(self, data, scope=None):
        if data is None:
            self.log.debug("There is no data to write.")
            return
        for w in self.writers.values():
            if w["__writer"] is not None:
                writer_def = Map({k: v for k, v in w.items() if k != "__writer"})
                w["__writer"].write(self.component_id, data, writer_def, scope)


class CronCollector(BaseCollector):
    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.schedule = self.config.value_str("schedule", required=True)
        if not croniter.croniter.is_valid(self.schedule):
            raise Exception("The value of schedule property '%s' is not valid!" % self.schedule)
        self.log.info("The cron schedule is %s" % (self.schedule))

    def get_time_to_sleep(self, itr):
        while True:
            next_run = itr.get_next(datetime)
            seconds = (next_run - datetime.now()).total_seconds()
            if seconds > 0:
                break
            else:
                self.log.warning(
                    f"The next run of the job {self.component_id} already passed by {seconds} seconds. Trying the next iteration."
                )
        self.log.info(f"The next job of '{self.component_id}' will run in {seconds} seconds (@{next_run}).")
        return seconds

    def worker(self, exit_event):
        itr = croniter.croniter(self.schedule, datetime.now())
        time2sleep = self.get_time_to_sleep(itr)
        while not exit_event.is_set():
            exit_event.wait(time2sleep)
            if not exit_event.is_set():
                self.log.info("Running job '%s'." % self.component_id)
                try:
                    self.write(self.prepare_data())
                except Exception as e:
                    self.log.error("The job failed due to %s" % (str(e)))
                time2sleep = self.get_time_to_sleep(itr)


class EventCollector(BaseCollector):
    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        if not isinstance(self.data_def, PythonExpression):
            raise Exception(f"The data must be of type {PythonExpression.__class__.__name__}")

        self.source = self.config.eval(self.data_def)
        self.data_def = Map(__nod=0)
        if isinstance(self.source, list):
            for x in self.source:
                if not isinstance(x, Topic):
                    raise Exception(f"The data must be the list of types {Topic.__class__.__name__}")
        elif not isinstance(self.source, Topic):
            raise Exception(f"The data must be of type {Topic.__class__.__name__}")

        self.log.info("The event sources are: %s" % (", ".join([x.topic_id for x in self.source])))

    def worker(self, exit_event):
        def _event(e):
            self.log.info(f"Received event {e.topic_id}")
            self.write(e.as_dict(), scope=Map(data=e.as_dict()))

        for s in self.source:
            self.log.info(f"Subscribing to events from '{s.topic_id}'")
            s.subscribe(_event)
        while not exit_event.is_set():
            exit_event.wait(1)
