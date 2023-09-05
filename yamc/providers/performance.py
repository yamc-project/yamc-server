# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import re
import sys
import os
import time
import inspect
import hashlib
import logging

from yamc.utils import Map
from datetime import datetime, timedelta

from .provider import BaseProvider, OperationalError
from .event import EventSource, global_event_source

import yamc.config as yamc_config

import pandas as pd


class PerformanceProvider(BaseProvider, EventSource):
    """
    Performance provider is a base class for all providers that need to
    implement performance pause functionality.
    """

    def __init__(self, config, component_id):
        self.perf_topic = global_event_source.add_topic(f"yamc/performance/providers/{component_id}")

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
        self.log.debug(f"Perf_info object: id_arg={id_arg}, perf_id={perf_obj.id}, hash={_id}")
        return perf_obj

    def update_perf(self, perf_info):
        """
        Updates the performance information for the given performance object.
        """
        self.log.debug(f"Updating topic with perf_info object id: {perf_info.id}")
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


class PerformanceAnalyzer:
    """
    Performance analyzer class is used to analyze the performance of the providers using CSV files where
    performance of the providers is stored. Such files can be produced using the events from the performance
    provider.
    """

    def __init__(self, perf_dir):
        self.perf_dir = perf_dir
        self.last_modified = None
        self.data = None
        self.min_time = None
        self.max_time = None
        self.log = yamc_config.get_logger("perf_analyser")

    def csv_files(self):
        """
        Returns the list of CSV files that contain the performance information for the given time range.
        """
        csv_files = [
            os.path.join(self.perf_dir, filename)
            for filename in os.listdir(self.perf_dir)
            if re.match(r"^(.*\.csv)(\.[0-9\-\.]*)?$", filename)
        ]
        return csv_files

    def get_perf_data(self, offset, provider_ids=None):
        """
        Get performance data from csv files. The data is filtered by the offset and provider IDs.
        """

        def __str_bool(v):
            return str(v)

        def __last_error(x):
            error_messages = x[x != "-"]
            return error_messages.iloc[-1] if len(error_messages) > 0 else None

        PERF_CSV_COLUMNS_DEF = {
            "STARTED_TIME": str,
            "TOPIC_ID": lambda v: v.split("/")[-1],
            "ID": str,
            "RUNNING_TIME": float,
            "RECORDS": int,
            "WAIT_CYCLES": int,
            "IS_ERROR": lambda v: __str_bool(v),
            "REASON_TO_WAIT": int,
            "ERROR": str,
        }

        csv_files = self.csv_files()
        last_modified = max(os.path.getmtime(csv_file) for csv_file in csv_files)
        if self.last_modified == last_modified and self.data is not None:
            return self.min_time, self.max_time, self.data

        self.last_modified = last_modified

        # time information
        modified_time = pd.Timestamp.fromtimestamp(self.last_modified)
        reference_time = modified_time - timedelta(seconds=offset)
        self.log.info(f"The latest modified time of the csv file is {modified_time}.")
        self.log.info(f"Filtering csv files between {reference_time} and {modified_time}.")

        # read csv files
        dfs = []
        for csv_file in csv_files:
            last_modified_time = pd.Timestamp.fromtimestamp(os.path.getmtime(csv_file))
            if last_modified_time >= reference_time:
                df = pd.read_csv(
                    csv_file,
                    header=1,
                    quotechar='"',
                    escapechar="\\",
                    names=[x for x in PERF_CSV_COLUMNS_DEF.keys()],
                    dtype={k: v for k, v in PERF_CSV_COLUMNS_DEF.items() if not callable(v)},
                    converters={k: v for k, v in PERF_CSV_COLUMNS_DEF.items() if callable(v)},
                )
                df["STARTED_TIME"] = pd.to_datetime(df["STARTED_TIME"])
                if provider_ids is not None:
                    df = df[df["TOPIC_ID"].isin(provider_ids)]
                df.set_index("STARTED_TIME", inplace=True)
                dfs.append(df)

        self.log.info(f"Using {len(dfs)} csv files to load data.")
        df = pd.concat(dfs, ignore_index=False)

        # min and max time
        max_time = df.index.max()
        min_time = max_time - pd.Timedelta(seconds=offset)
        if min_time < df.index.min():
            min_time = df.index.min()
        range_info = f"{min_time.strftime('%Y-%m-%d %H:%M:%S')}-{max_time.strftime('%Y-%m-%d %H:%M:%S')}"
        df = df[df.index >= min_time]
        self.log.info(f"The final time range to calculate performance stats is {range_info}.")

        # aggregate the data
        agg_funcs = {
            "RUNNING_TIME": ["mean", "max"],
            "IS_ERROR": [
                lambda x: (x == "True").sum(),
                lambda x: (x == "False").sum(),
                lambda x: (x == "None").sum(),
            ],
            "RECORDS": ["sum"],
            "ERROR": [__last_error],
        }

        result = df.groupby(["TOPIC_ID", "ID"]).agg(agg_funcs).reset_index()

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
        self.data = result.to_dict(orient="records")
        self.min_time = min_time
        self.max_time = max_time
        return self.min_time, self.max_time, self.data
