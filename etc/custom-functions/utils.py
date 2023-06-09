# Common utility functions

import datetime
import socket
import logging
import platform
import hashlib

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

    Args:
        data (dict): The dictionary to filter.
        include (list, optional): List of properties to include. Defaults to None.
        exclude (list, optional): List of properties to exclude. Defaults to None.

    Returns:
        dict: The filtered dictionary.
    """

    def _convert(k, v):
        if convert:
            return convert(k, v)
        else:
            return v

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
