# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import requests
import time
import re
import threading
import unidecode
import logging
import time

from lxml import etree
from yamc.component import BaseComponent, global_state

from enum import Enum
import hashlib

from yamc.utils import Map, deep_find, merge_dicts


class BaseProvider(BaseComponent):
    """
    Base data provider definining the base interface with update and diff methods.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.config = config.provider(component_id)
        self._updated_time = None
        self.data = None
        self.diff_storage = {}

    @property
    def updated_time(self):
        self.update()
        return self._updated_time

    def diff(self, id, value):
        """
        Calculate the difference of the value identified by id
        """
        if not isinstance(value, int) and not isinstance(value, float):
            raise Exception(
                "Can only caclulate diff on values of type int or float! The value type is %s."
                % value.__class__.__name__
            )

        v = self.diff_storage.get(id)
        if v is None:
            v = Map(prev_value=None, last_value=None)
            self.diff_storage[id] = v

        if v.last_value is not None:
            v.prev_value = v.last_value
            v.last_value = value
            return v.last_value - v.prev_value
        else:
            v.last_value = value
            return 0

    def update(self, data=None):
        pass


class HttpProvider(BaseProvider):
    """
    A generic HTTP provider that retrieves data using HTTP.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.url = self.config.value_str("url")
        self.max_age = self.config.value("max_age", default=10)
        self.init_url = self.config.value("init_url", default=None)
        self.init_max_age = self.config.value("init_max_age", default=None)
        self.lock = threading.Lock()
        self.session = requests.session()
        self.init_time = None
        self.init_session()

    def init_session(self):
        try:
            if self.init_url is not None and (
                self.init_time is None or time.time() - self.init_time > self.init_max_age
            ):
                self.init_time = time.time()
                self.log.info("Running the initialization request at %s" % (self.init_url))
                self.session.get(self.init_url)
        except Exception as e:
            self.log.error("The initialization request failed due to %s" % (str(e)))

    def update(self):
        with self.lock:
            if self._updated_time is None or self.data is None or time.time() - self._updated_time > self.max_age:
                self.init_session()
                num_retries = 0
                while num_retries < 3:
                    self._updated_time = time.time()
                    r = self.session.get(self.url)
                    if r.status_code == 404:
                        raise Exception("The resource at %s does not exist!" % (self.url))
                    elif r.status_code >= 400:
                        self.log.error(
                            "The request at %s failed, status-code=%d, num-retries=%d"
                            % (self.url, r.status_code, num_retries)
                        )
                        num_retries += 1
                        if num_retries == 3:
                            raise Exception(
                                "Cannot retrieve the resource at %s after %d attempts!" % (self.url, num_retries)
                            )
                        time.sleep(1)
                    else:
                        break
                # self.log.debug("The url '%s' retrieved the following data: %s"%(self.url,str(r.content.decode(self.encoding))))
                self.log.debug("The url '%s' retrieved the following data: (strip)" % (self.url))
                self.data = r.content
                return True
            else:
                self.log.debug("The url '%s' retrieved data from cache." % (self.url))
                return False


class XmlHttpProvider(HttpProvider):
    """
    XML data provider retrieved over HTTP.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.encoding = self.config.value_str("encoding", default="utf-8")
        self.namespaces = self.config.value("namespaces", default=None)
        self.str_decode_unicode = self.config.value("str_decode_unicode", default=True)
        self.xmlroot = None

    def update(self):
        if super().update() or self.xmlroot is None:
            self.xmlroot = etree.fromstring(self.data)
            return True
        else:
            return False

    def xpath(self, xpath, diff=False):
        def _value(v):
            return v if not diff else self.diff(xpath, v)

        def _int_or_float_or_str(v):
            if isinstance(v, str):
                try:
                    return _value(int(v.strip()))
                except:
                    try:
                        return _value(float(v.strip()))
                    except:
                        return unidecode.unidecode(v) if self.str_decode_unicode else v
            elif isinstance(v, int) or isinstance(v, float):
                return _value(v)
            else:
                raise Exception(
                    "The xpath expression '%s' must provide a value of type int or float! The value was '%s'."
                    % (xpath, str(v))
                )

        self.update()
        d = self.xmlroot.xpath(xpath, namespaces=self.namespaces)
        if isinstance(d, list):
            if len(d) > 0:
                return _int_or_float_or_str(d[0])
            else:
                self.log.error(
                    "The xpath '%s' cannot be evaluated on the following data: %s"
                    % (xpath, str(self.content.decode(self.encoding)))
                )
                raise Exception("The xpath '%s' cannot be evaluated!" % xpath)
        else:
            return _int_or_float_or_str(d)


class CsvHttpProvider(HttpProvider):
    """
    CSV data provider retrieved over HTTP.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.encoding = self.config.value_str("encoding", default="utf-8")
        self.str_decode_unicode = self.config.value("str_decode_unicode", default=True)
        self.delimiter = self.config.value("delimiter", default=";")
        self.header = None
        self.lines = None

    def update(self):
        if super().update():
            # decode the data
            s = self.data.decode(self.encoding)
            if self.str_decode_unicode:
                s = unidecode.unidecode(s)

            # read csv
            self.header = None
            self.lines = []
            for l in s.split("\r\n"):
                if self.header is None:
                    self.header = l.split(self.delimiter)
                else:
                    if l.strip() != "":
                        self.lines.append(l.split(self.delimiter))

            return True
        else:
            return False

    def field(self, row_inx, name):
        def _int_or_float_or_str(v):
            if isinstance(v, str):
                try:
                    return int(v.strip())
                except:
                    try:
                        return float(v.strip())
                    except:
                        return v
            elif isinstance(v, int) or isinstance(v, float):
                return v
            else:
                raise Exception(
                    "The field value '%s' must be of type int or float! The value was '%s'." % (name, str(v))
                )

        self.update()
        col_inx = self.header.index(name)
        if col_inx >= 0:
            if abs(row_inx) >= 0 and abs(row_inx) < len(self.lines):
                return _int_or_float_or_str(self.lines[row_inx][col_inx])


class Topic:
    """
    Topic object provides a link between a specific pub/sub mechanism (such as MQTT)
    and yamc providers and collectors.
    """

    def __init__(self, id, provider):
        self.topic_id = id
        self.time = 0
        self.data = None
        self.callbacks = []
        self.history = []
        self.provider = provider

    def update(self, data):
        self.time = time.time()
        self.data = data
        self.history.append(data)
        self.provider.update(self)
        for callback in self.callbacks:
            callback(self)

    def as_dict(self):
        return Map(merge_dicts(Map(topic_id=self.topic_id, time=self.time), self.data))

    @property
    def last(self):
        return self.history[-1] if len(self.history) > 0 else Map()

    def subscribe(self, callback):
        self.callbacks.append(callback)


class EventSource:
    """
    Event source object provides a link between a specific pub/sub mechanism
    (such as MQTT) and yamc providers.
    """

    def __init__(self):
        self.topics = Map()

    def add_topic(self, topic_id):
        if self.topics.get(topic_id) is not None:
            raise Exception(f"The topic with id {topic_id} already exists!")
        topic = self.create_topic(topic_id)
        self.topics[topic_id] = topic
        return topic

    def create_topic(self, topic_id):
        return Topic(topic_id, self)

    def select(self, *ids, silent=False):
        sources = []
        for id in ids:
            found = False
            topic = self.topics.get(id)
            if topic is not None:
                sources.append(topic)
                found = True
            else:
                for k, v in self.topics.items():
                    if re.match(id, k):
                        found = True
                        if v not in sources:
                            sources.append(v)
            # if not found and not silent:
            #     self.log.warn(f"The topic with pattern '{id}' cannot be found!")
        return sources

    def select_one(self, id):
        topics = self.select(id, silent=True)
        if len(topics) > 0:
            return topics[0]
        else:
            return None


class PerformanceProvider(BaseProvider, EventSource):
    def __init__(self, config, component_id):
        BaseProvider.__init__(self, config, component_id)
        EventSource.__init__(self)
        self.perf_topic = self.add_topic(f"yamc/performance/providers/{component_id}")
        self.performance = Map()
        self.performance.max_running_time = self.config.value("performance.max_running_time", default=0.5)
        self.performance.exponential_backoff = self.config.value_bool("performance.exponential_backoff", default=False)
        self.perf_objects = Map()

    def get_perf(self, *args, **kwargs):
        md5 = hashlib.md5()
        md5.update(str(args).encode("utf-8") + str(kwargs).encode("utf-8"))
        id = md5.hexdigest()
        self.perf_objects.setdefault(id, Map(running_time=0, last_running_time=0, cycles_to_wait=0, __cycles_to_wait=0))
        return self.perf_objects[id]

    def update_perf(self, id, size, running_time):
        self.perf_topic.update(Map(id=id, size=size, running_time=running_time))


def perf_checker(func):
    """
    Decorator for checking the performance of a provider.
    """

    def perf_decorator(*args, **kwargs):
        instance = args[0]
        if not isinstance(instance, PerformanceProvider):
            raise Exception("The performance checker can only be used with PerformanceProvider instances!")
        try:
            # get the performance object
            perf = instance.get_perf(*args[1:], **kwargs)

            # check if the provider is waiting
            if perf.cycles_to_wait > 0:
                instance.log.warn(
                    f"The provider is waiting {perf.cycles_to_wait} more cycles as the last running time was "
                    + f"{perf.last_running_time:.2f} seconds!"
                )
                perf.cycles_to_wait -= 1
                result = None
            else:
                # run the function
                start_time = time.time()
                result = func(*args, **kwargs)
                perf.last_running_time = time.time() - start_time

                # eval the result
                if perf.last_running_time > instance.performance.max_running_time:
                    if perf.__cycles_to_wait > 0:
                        if instance.performance.exponential_backoff:
                            perf.__cycles_to_wait *= 2
                        else:
                            perf.__cycles_to_wait += 1
                        perf.cycles_to_wait = perf.__cycles_to_wait
                    else:
                        perf.cycles_to_wait = 1
                        perf.__cycles_to_wait = 1
                    instance.log.warn(
                        f"The provider {instance.component_id} took {perf.last_running_time:.2f} seconds to update the data! "
                        + f"Will wait {perf.cycles_to_wait} cycles before next update!"
                    )
                else:
                    if perf.cycles_to_wait > 0:
                        instance.log.info("The provider is back to normal!")
                    perf.cycles_to_wait = 0
                    perf.__cycles_to_wait = 0

        except Exception as e:
            raise Exception("Exception in interface: %s" % str(e))
        return result

    return perf_decorator


class EventProvider(BaseProvider, EventSource):
    """
    Event data provider providing the base class for event-based providers.
    """

    def __init__(self, config, component_id):
        BaseProvider.__init__(self, config, component_id)
        EventSource.__init__(self)
        for topic_id in self.config.value("topics"):
            self.add_topic(topic_id)

    def update(self, topic=None):
        self._updated_time = time.time()
        if self.data is None:
            self.data = Map()
        if topic is None:
            for topic in self.topics.values():
                self.data[topic.topic_id] = Map(time=topic.time, data=topic.data)
        else:
            self.data[topic.topic_id] = Map(time=topic.time, data=topic.data)
        return True


class StateProvider(EventProvider):
    """
    Event provider for the state object. It can be used to subscribe to topics
    that correspond to paths in the state object data dict. When the state object data
    change, the state provider gets notified via `on_data` method and updates the
    corresponding topic's data.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.name = self.config.value("name")

        # get the state object and register the data callback on it
        self.state = global_state.get_state(self.name, self)
        self.state.add_data_callback(self.on_data)

    def get(self, path):
        return deep_find(self.state.data, path, default=None, delim="/")

    def on_data(self, data):
        def _walk(d, callback, path=""):
            if path != "":
                callback(path, d)
            if isinstance(d, dict):
                for k, v in d.items():
                    _walk(v, callback, path=f"{path}{k}/")
            elif isinstance(d, list):
                for num, x in enumerate(d, start=0):
                    _walk(x, callback, path=f"{path}[{num}]/")

        def _update_topic(path, data):
            for topic in self.topics.values():
                if topic.topic_id == path[:-1]:
                    topic.update(data)

        _walk(data, _update_topic)
