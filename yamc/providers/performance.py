# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import re
import sys
import time
import inspect
import hashlib

from yamc.utils import Map
from datetime import datetime

from .provider import BaseProvider, OperationalError
from .event import EventSource

import yamc.config as yamc_config


class PerformanceProvider(BaseProvider, EventSource):
    """
    Performance provider is a base class for all providers that need to
    implement performance pause functionality.
    """

    def __init__(self, config, component_id):
        BaseProvider.__init__(self, config, component_id)
        EventSource.__init__(self)
        self.perf_topic = self.add_topic(f"yamc/performance/providers/{component_id}")

        # performance pause configuration
        self.performance_pause = Map(
            running_time=self.config.value("performance.pause.running_time", default=99999999, required=False),
            duration_cycles=self.config.value("performance.pause.duration_cycles", default=1, required=False),
            exponential_backoff=self.config.value(
                "performance.pause.exponential_backoff", default=False, required=False
            ),
            max_waiting_cycles=self.config.value("performance.pause.max_waiting_cycles", default=10, required=False),
        )
        self.perf_objects = Map()

    def get_perf_info(self, func, id_arg, *args, **kwargs):
        """
        Returns the performance information for the given function and arguments.
        """
        performance_id_value = "n/a"
        if id_arg is not None:
            signature = inspect.signature(func)
            performance_id_value = None
            if id_arg is not None:
                for inx, param in enumerate(signature.parameters.values()):
                    if param.name == id_arg:
                        try:
                            performance_id_value = kwargs.get(id_arg)
                            if performance_id_value is None:
                                performance_id_value = args[inx]
                            break
                        except Exception as e:
                            performance_id_value = None
            if performance_id_value is None:
                self.log.warn(f"The performance id value cannot be found for argument '{id_arg}'!")

        md5 = hashlib.md5()
        md5.update(
            str(self.component_id).encode("utf-8")
            + str(args[1:]).encode("utf-8")
            + str(kwargs).encode("utf-8")
            + performance_id_value.encode("utf-8")
        )
        _id = md5.hexdigest()
        if _id not in self.perf_objects.keys():
            self.perf_objects.setdefault(
                _id,
                Map(
                    hash=_id,
                    started_time=None,
                    id=performance_id_value,
                    last_running_time=0,
                    cycles_to_wait=0,
                    records=0,
                    last_error=None,
                    cycles_to_wait_int=0,
                    reason_to_wait=0,
                ),
            )

        perf_obj = self.perf_objects[_id]
        self.log.info(f"Perf_info object: id_arg={id_arg}, perf_id={perf_obj.id}, hash={_id}")
        return perf_obj

    def update_perf(self, perf_info):
        """
        Updates the performance information for the given performance object.
        """
        self.log.info(f"Updating topic with perf_info object id: {perf_info.id}")
        self.perf_topic.update(
            Map(
                id=str(perf_info.id),
                started_time=perf_info.started_time,
                records=int(perf_info.records),
                running_time=float(perf_info.last_running_time),
                wait_cycles=int(perf_info.cycles_to_wait),
                reason_to_wait=perf_info.reason_to_wait,
                is_error=True if perf_info.last_error is not None else False if perf_info.reason_to_wait == 0 else None,
                error=str(perf_info.last_error) if perf_info.last_error else "-",
            )
        )

    def wrapper(self, func, id_arg, *args, **kwargs):
        """
        This method runs the function `func` from the decorator wrapper, checks the performance of the function and
        pauses the function when the performance does not meet the defined requirements of response time or when an error occurs.
        """
        result = None

        # get the performance object
        perf_info = self.get_perf_info(func, id_arg, *args, **kwargs)
        perf_info.last_error = None
        perf_info.started_time = datetime.now()

        # check if the provider is waiting
        if perf_info.cycles_to_wait > 0:
            if perf_info.reason_to_wait == 1:
                self.log.warn(
                    f"The provider {self.component_id}/{perf_info.id} is waiting {perf_info.cycles_to_wait} more cycles "
                    + "(the last call resulted with the error)."
                )
            elif perf_info.reason_to_wait == 2:
                self.log.warn(
                    f"The provider {self.component_id}/{perf_info.id} is waiting {perf_info.cycles_to_wait} more cycles (the last running time was "
                    + f"{perf_info.last_running_time:.2f} seconds)."
                )
            else:
                self.log.warn(
                    f"The provider {self.component_id}/{perf_info.id} is waiting {perf_info.cycles_to_wait} more cycles."
                )
            perf_info.cycles_to_wait -= 1
            perf_info.last_running_time = 0
            perf_info.records = 0
        else:
            # run the function
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                perf_info.last_running_time = time.time() - start_time
                perf_info.records = len(result) if result is not None else 0
                perf_info.last_error = None
            except OperationalError as e:
                self.log.error(f"Operational error in the provider '{self.component_id}/{perf_info.id}': {e}")
                if yamc_config.TEST_MODE:
                    raise e
                last_error = str(e)
                if e.original_exception is not None:
                    last_error = str(e.original_exception)
                perf_info.last_running_time = 0
                perf_info.records = 0
                perf_info.last_error = last_error

            # eval the result
            if perf_info.last_error is not None or perf_info.last_running_time > self.performance_pause.running_time:
                if perf_info.cycles_to_wait_int > 0:
                    if self.performance_pause.exponential_backoff:
                        perf_info.cycles_to_wait_int *= 2
                    else:
                        perf_info.cycles_to_wait_int += 1
                    if perf_info.cycles_to_wait_int > self.performance_pause.max_waiting_cycles:
                        perf_info.cycles_to_wait_int = self.performance_pause.max_waiting_cycles
                    perf_info.cycles_to_wait = perf_info.cycles_to_wait_int
                else:
                    perf_info.cycles_to_wait = self.performance_pause.duration_cycles
                    perf_info.cycles_to_wait_int = self.performance_pause.duration_cycles
                if perf_info.last_error is not None:
                    self.log.warn(
                        f"The provider {self.component_id}/{perf_info.id} has failed. "
                        + f"Will wait {perf_info.cycles_to_wait} cycles before next update!"
                    )
                    perf_info.reason_to_wait = 1
                else:
                    self.log.warn(
                        f"The provider {self.component_id}/{perf_info.id} took {perf_info.last_running_time:.2f} seconds to update the data! "
                        + f"Will wait {perf_info.cycles_to_wait} cycles before next update!"
                    )
                    perf_info.reason_to_wait = 2
            else:
                if perf_info.cycles_to_wait > 0:
                    self.log.info(f"The provider {self.component_id}/{perf_info.id} is back to normal!")
                perf_info.cycles_to_wait = 0
                perf_info.cycles_to_wait_int = 0
                perf_info.reason_to_wait = 0

        self.update_perf(perf_info)
        return result


def perf_checker(id_arg=None):
    """
    Decorator for checking the performance of a provider and controlling its operation based on the performance.
    The decorator must be used with the PerformanceProvider instances only. It calls the `wrapper` method of the
    provider and checks the performance of the provider. If the performance is not good, the decorator pauses the
    provider for a defined number of cycles.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            provider = args[0]
            if not isinstance(provider, PerformanceProvider):
                raise Exception("The performance checker can only be used with PerformanceProvider instances!")
            return provider.wrapper(func, id_arg, *args, **kwargs)

        return wrapper

    return decorator
