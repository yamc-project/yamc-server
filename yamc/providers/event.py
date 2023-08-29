# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import re
import time

from threading import Lock

from yamc.utils import Map, merge_dicts

from .provider import BaseProvider

from yamc.utils import deep_find
from yamc.component import global_state


class Topic:
    """
    Topic object provides a link between a specific pub/sub mechanism (such as MQTT)
    and yamc providers and collectors.
    """

    def __init__(self, id, event_source):
        self.topic_id = id
        self.time = 0
        self.data = None
        self.subscribers = []
        self.history = []
        self.event_source = event_source
        self.lock = Lock()

    def update(self, data):
        with self.lock:
            self.time = time.time()
            self.data = data
            self.history.append(data)
            self.event_source.on_topic_update(topic=self)
            for queue in self.subscribers:
                queue.put(self.as_dict())

    def as_dict(self):
        return Map(merge_dicts(Map(topic_id=self.topic_id, time=self.time), self.data))

    @property
    def last(self):
        return self.history[-1] if len(self.history) > 0 else Map()

    def subscribe(self, queue):
        self.subscribers.append(queue)


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

    def on_topic_update(self, topic=None):
        pass


class EventProvider(BaseProvider, EventSource):
    """
    Event data provider providing the base class for event-based providers.
    """

    def __init__(self, config, component_id):
        BaseProvider.__init__(self, config, component_id)
        EventSource.__init__(self)
        for topic_id in self.config.value("topics"):
            self.add_topic(topic_id)

    def on_topic_update(self, topic=None):
        self.update(topic=topic)

    def update(self, *kwargs):
        topic = kwargs.get("topic")
        if topic is None:
            raise Exception("The update method of the EventProvider object must be called with the topic parameter!")
        self._updated_time = time.time()
        if self.data is None:
            self.data = Map()
        if topic is None:
            for topic in self.topics.values():
                self.data[topic.topic_id] = topic.as_dict()  # Map(time=topic.time, data=topic.data)
        else:
            self.data[topic.topic_id] = topic.as_dict()  # Map(time=topic.time, data=topic.data)
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
