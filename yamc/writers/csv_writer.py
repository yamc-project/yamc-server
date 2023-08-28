# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import logging
import os

from .writer import Writer, HealthCheckException
from yamc.utils import import_class, randomString

# we need a new manager not to mess up with yamc logging configuration
manager = logging.Manager(logging.RootLogger(logging.INFO))


class CsvWriter(Writer):
    """
    Writer that writes data to a CSV file.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.handler_def = self.config.value("handler")
        clazz = import_class(self.handler_def["class"])
        self.csv_writer = manager.getLogger(f"CsvWriter_{randomString()}")
        self.csv_writer.setLevel(logging.INFO)
        if "filename" in self.handler_def:
            self.handler_def["filename"] = self.config.get_dir_path(self.handler_def["filename"], check=False)
            os.makedirs(os.path.dirname(self.handler_def["filename"]), exist_ok=True)
        self.csv_writer.addHandler(clazz(**{k: v for k, v in self.handler_def.items() if k != "class"}))

    def healthcheck(self):
        super().healthcheck()

    def do_write(self, items):
        """
        Writes the data to the CSV file.
        """

        def _format_value(v):
            if isinstance(v, str):
                return '"' + v.replace('"', '\\"').replace("\n", " ") + '"'
            else:
                return str(v)

        self.log.debug(f"Writing {len(items)} rows to {self.handler_def['filename']}")
        for data in items:
            line = [_format_value(v) for k, v in data.data.items()]
            self.csv_writer.info(",".join(line))
