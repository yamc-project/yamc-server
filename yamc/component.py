# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import logging
import threading
import time

from threading import Timer
from typing import Any

from .utils import merge_dicts, Map, deep_find

from . import config as yamc_config


class State:
    """
    The state object serves as a container for data state management across various providers in the configuration.
    It encompasses the timer functionality and can be allocated by components like writers, which update data within
    the state object, or providers such as StateProvider, a subclass of EventProvider, which can read data from the state.
    """

    def __init__(self, name):
        self.name = name
        self.data = Map()
        self.data_callbacks = []
        self.log = yamc_config.get_logger("%s" % (name))
        self.timers = {}

    def add_data_callback(self, callback):
        self.data_callbacks.append(callback)

    def update(self, data):
        def _on_timer(name, data):
            timer = self.timers[name]
            del self.timers[name]
            self.log.info(f"Timer elapsed after {data['timer'][name]['value']} seconds.")
            for data_callback in self.data_callbacks:
                data_callback(data)

        for k, v in data.items():
            if k == "timer" and isinstance(v, dict):
                for k1, v1 in v.items():
                    try:
                        name, value = k1, float(v1["value"])
                        timer = self.timers.get(name)
                        if timer is None and value > 0:
                            self.log.info(f"The timer created, name='{name}', timeout={value}.")
                            self.timers[name] = Timer(value, lambda: _on_timer(name, {"timer": v}))
                            self.timers[name].start()
                        elif timer is not None and value == 0:
                            self.log.info(f"The timer cancelled, name='{name}', timeout=0.")
                            self.timers[name].cancel()
                            del self.timers[name]
                        elif timer is not None:
                            self.log.debug(f"The timer '{name}' already exists and it will not be updated.")
                    except (ValueError, KeyError) as e:
                        self.log.error(f"Cannot handle the timer. The timer has an invalid definition. {str(e)}")

        # delete timer data
        if "timer" in data:
            del data["timer"]

        # call data callbacks for the data that has been changed
        for data_callback in self.data_callbacks:
            data_callback(data)
        self.data = merge_dicts(self.data, data)


class GlobalState:
    def __init__(self):
        self.data = Map()

    def get_state(self, name, component):
        state = self.data.setdefault(name, State(name))
        return state


global_state = GlobalState()


class BaseComponent:
    """
    Base class for all components.
    """

    def __init__(self, config, component_id):
        self.base_config = config
        self.component_id = component_id
        self.log = yamc_config.get_logger("%s" % (component_id))
        self.enabled = True
        self.test_mode = yamc_config.TEST_MODE

    def base_scope(self, custom_scope=None):
        """
        Return the base scope for the component by merging the scope from the main
        configuration, custon functions and `custom_scope` provided as a parameter.
        """
        return merge_dicts(self.base_config.scope, self.base_config.custom_functions, custom_scope)

    # TODO: the destroy method should be replaced by a standard __del__ method
    def destroy(self):
        pass


class WorkerComponent(BaseComponent):
    """
    The base class for all worker components, i.e. components that run worker threads.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.thread = None
        self.start_time = None

    def worker(self, exit_event):
        """
        The main method to run work. This should be run in the component's worker thread.
        """
        pass

    def start(self, exit_event):
        """
        Start the worker thread.
        """
        self.log.info(f"Starting the worker thread '{self.component_id}'.")
        self.start_time = time.time()
        self.thread = threading.Thread(target=self.worker, args=(exit_event,), daemon=True)
        self.thread.start()

    def running(self):
        """
        Return `True` is the worker thread is running and is alive.
        """
        return self.thread is not None and self.thread.is_alive()

    def join(self):
        """
        Call `join` on the worker thread if the worker thread is running.
        """
        if self.running():
            self.thread.join()
