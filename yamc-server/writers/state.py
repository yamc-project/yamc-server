# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import re

from .writer import Writer

from yamc.component import global_state

from yamc.utils import deep_merge


class StateWriter(Writer):
    """
    The state machine writer.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.name = self.config.value("name")
        self.state = global_state.get_state(self.name, self)
        if self.config.value("write_interval", None) is None:
            self.write_interval = 0

    def healthcheck(self):
        pass

    def do_write(self, items):
        self.log.debug(f"Writing {len(items)} rows to the global state object '{self.name}'")
        for data in items:
            self.log.debug(f"The data is {data}")
            self.state.update(data.data)
        self.log.debug(f"The state object data is {self.state.data}")
