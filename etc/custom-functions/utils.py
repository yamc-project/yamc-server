# Common utility functions

import datetime
import socket
import logging
import platform
import hashlib
import re

DEV_SERVER = "brenta.local"

log = logging.getLogger("utils")


def platform():
    return platform.platform().spli("-")[0]


def epoch_time(datetime_str, format):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (datetime.datetime.strptime(datetime_str, format) - epoch).total_seconds()


def check_hostname(hostname):
    return socket.gethostname() in [hostname, DEV_SERVER]


def format(msg, **kwargs):
    return msg.format(**kwargs)


def handle_error(expr, default):
    try:
        return eval(expr)
    except:
        return default


def expand(data, include=None, exclude=None, convert={}):
    """
    Filter dictionary properties based on include and exclude parameters.
    """

    if include:
        elements_to_remove = []
        for k in include:
            s = re.split("\s*!\s*", k)
            if len(s) == 2:
                if s[0] not in convert.keys():
                    if s[1] == "int":
                        convert[s[0]] = lambda x: int(x) if x else None
                    elif s[1] == "float":
                        convert[s[0]] = lambda x: float(x) if x else None
                    else:
                        log.warn(f"Cannot convert {s[0]} to {s[1]} because it is not a valid type.")
                    elements_to_remove.append(k)
                    include.append(s[0])
                else:
                    log.warn(f"Cannot convert {s[0]} to {s[1]} because it is already defined in the convert dict.")
            elif len(s) > 2:
                log.warn(f"Invalid include parameter: {k}")

        for k in elements_to_remove:
            include.remove(k)

    filtered_data = {}

    if include:
        filtered_data = {key: convert.get(key, lambda v: v)(value) for key, value in data.items() if key in include}
    else:
        filtered_data = data.copy()

    if exclude:
        filtered_data = {
            key: convert.get(key, lambda v: v)(value) for key, value in filtered_data.items() if key not in exclude
        }

    return filtered_data


def current_time():
    return datetime.datetime.now()
