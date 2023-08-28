# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import requests
import time
import threading
import unidecode
import time

from lxml import etree
from yamc.component import BaseComponent

from yamc.utils import Map


class OperationalError(Exception):
    """
    Exception raised when a provider is in an operational error state.
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


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

    def update(self, **kwargs):
        pass

    @property
    def source(self):
        return "n/a"


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

    @property
    def source(self):
        return self.url

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

    def update(self, **kwargs):
        with self.lock:
            if self._updated_time is None or self.data is None or time.time() - self._updated_time > self.max_age:
                start_time = time.time()
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
                self.log.debug(
                    f"The url '{self.url}' retrieved the following data in {time.time()-start_time} seconds: (strip)"
                )
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

    def update(self, **kwargs):
        if super().update(**kwargs) or self.xmlroot is None:
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

    def update(self, **kwargs):
        if super().update(**kwargs):
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
