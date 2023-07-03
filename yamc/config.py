# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import os
import io
import sys
import yaml
import logging
import logging.config
import re
import warnings
import json
import jinja2

from threading import Event

warnings.filterwarnings("ignore", category=DeprecationWarning)

import imp

from .utils import PythonExpression
from .utils import deep_find, import_class, Map, deep_merge, str2bool
from functools import reduce

import yamc.config as yamc_config

from yamc.providers import EventSource

# they must be in a form ${VARIABLE_NAME}
ENVNAME_PATTERN = "[A-Z0-9_]+"
ENVPARAM_PATTERN = "\$\{%s\}" % ENVNAME_PATTERN

# consolidated variables supplied via env file and environment variables
ENV = {}

DEBUG = str2bool(os.getenv("YAMC_DEBUG", "False"))
ANSI_COLORS = not str2bool(os.getenv("YAMC_NO_ANSI", "False"))
CONFIG_FILE = os.getenv("YAMC_CONFIG", None)
CONFIG_ENV = os.getenv("YAMC_ENV", None)
TRACEBACK = os.getenv("YAMC_TRACEBACK", None)
YAMC_HOME = os.getenv("YAMC_HOME", "~/.yamc")

env_variables = ["YAMC_HOME", "YAMC_CONFIG", "YAMC_ENV", "YAMC_DEBUG", "YAMC_TRACEBACK", "YAMC_NO_ANSI"]

TEST_MODE = False

# global exit event
exit_event = Event()


class Jinja2TemplateLoader(jinja2.BaseLoader):
    def get_source(self, environment, template):
        if not os.path.exists(template):
            raise jinja2.TemplateNotFound(template)
        with open(template, "r", encoding="utf-8") as f:
            source = f.read()
        return source, template, lambda: True


class Jinja2Template(io.BytesIO):
    name = None

    def size(self):
        self.seek(0, io.SEEK_END)
        size = self.tell()
        self.seek(0, io.SEEK_SET)
        return size

    def __init__(self, file, scope=None, strip_blank_lines=False):
        super(Jinja2Template, self).__init__(None)
        self.name = file
        env = jinja2.Environment(loader=Jinja2TemplateLoader(), trim_blocks=True, lstrip_blocks=True)
        if scope is not None:
            env.globals.update(scope)
        try:
            content = env.get_template(file).render()
            if strip_blank_lines:
                content = "\n".join([x for x in content.split("\n") if x.strip() != ""])
            self.write(content.encode())
            self.seek(0)
        except Exception as e:
            raise Exception(f"Error when processing template {os.path.basename(file)}: {str(e)}")


def get_dir_path(config_dir, path, base_dir=None, check=False):
    """
    Return the directory for the path specified.
    """
    d = os.path.normpath((((config_dir if base_dir is None else base_dir) + "/") if path[0] != "/" else "") + path)
    if check and not os.path.exists(d):
        raise Exception(f"The directory {d} does not exist!")
    return d


def jinja2_scope():
    """
    Return the scope for the Jinja2 template engine.
    """

    def range1(n):
        return range(1, n + 1)

    def property(name):
        vals = ENV.get(name)
        if not vals:
            raise Exception(f"Property '{name}' does not exist!")
        try:
            return int(vals)
        except:
            return vals

    def non_empty(name):
        vals = ENV.get(name)
        return vals is not None and vals.strip() != ""

    return Map(range1=range1, property=property, non_empty=non_empty)


def read_raw_config(config_file, env_file):
    """
    Read the raw configuration file by processing config `include` instructions and
    populating `defaults` to `providers`, `collectors` and `writers`. This is a wrapper function
    for the function `read_complex_config`.
    """
    if not (os.path.exists(config_file)):
        raise Exception(f"The configuration file {config_file} does not exist!")
    if env_file and not (os.path.exists(env_file)):
        raise Exception(f"The environment file {env_file} does not exist!")

    # init yaml reader
    global ENV
    ENV = init_env(env_file)
    yaml.add_implicit_resolver("!env", re.compile(r".*%s.*" % ENVPARAM_PATTERN))
    yaml.add_constructor("!env", env_constructor)
    yaml.add_constructor("!py", py_constructor)

    # read configuration
    config, config_file = read_complex_config(config_file, True, jinja2_scope())
    config_dir = os.path.dirname(config_file)

    # add defaults
    process_templates(config, "collectors")
    process_templates(config, "providers")
    process_templates(config, "writers")

    return config, config_file, config_dir


def read_complex_config(file, use_template=False, scope=None):
    """
    Read complex configuration file by processing `include` instructions.
    """

    def _read_yaml(config_file):
        stream = (
            open(config_file, "r", encoding="utf-8")
            if not use_template
            else Jinja2Template(config_file, scope, strip_blank_lines=True)
        )
        try:
            return yaml.load(stream, Loader=yaml.FullLoader)
        except Exception as e:
            raise Exception(f"Error when reading the configuration file {file}: {str(e)}")
        finally:
            stream.close()

    def _traverse(config_dir, d):
        if isinstance(d, dict):
            result = {}
            for k, v in d.items():
                if k == "include" and isinstance(v, list):
                    for f in v:
                        result = deep_merge(
                            result,
                            read_complex_config(get_dir_path(config_dir, f), use_template=use_template, scope=scope)[0],
                        )
                elif isinstance(v, dict):
                    result[k] = _traverse(config_dir, v)
                else:
                    result[k] = v
            return result
        else:
            return d

    config_file = os.path.realpath(file)
    config = _read_yaml(config_file)
    return _traverse(os.path.dirname(config_file), config), config_file


def process_templates(config, component_type):
    """
    Process a template for all components of component type
    """
    all_templates = deep_find(config, f"templates.{component_type}", None)
    if all_templates is not None:
        components = deep_find(config, component_type, default={})
        for _, component in components.items():
            template_name = component.get("template")
            if template_name is not None:
                try:
                    template = next(iter([x for x in all_templates if x["name"] == template_name]))
                    for k1, v1 in template.items():
                        if k1 not in component.keys():
                            component[k1] = v1
                except StopIteration:
                    raise Exception(f"The template with name {template_name} does not exist!")


def init_env(env_file, sep="=", comment="#"):
    """
    Read environment varialbes from the `env_file` and combines them with the OS environment variables.
    """
    env = {}
    if env_file:
        with open(env_file, "rt") as f:
            for line in f:
                l = line.strip()
                if l and not l.startswith(comment):
                    key_value = l.split(sep)
                    key = key_value[0].strip()
                    if not re.match(f"^{ENVNAME_PATTERN}$", key):
                        raise Exception(f"Invalid variable name '{key}'.")
                    value = sep.join(key_value[1:]).strip().strip("\"'")
                    env[key] = value
    for k, v in os.environ.items():
        env[k] = v
    return env


def replace_env_variable(value):
    """
    Replace all environment varaibles in a string privided in `value` parameter
    with values of variable in `ENV` global variable.
    """
    params = list(set(re.findall("(%s)" % ENVPARAM_PATTERN, value)))
    if len(params) > 0:
        for k in params:
            env_value = ENV.get(k[2:-1])
            if env_value is None:
                raise Exception(f"The environment variable {k} does not exist!")
            else:
                value = value.replace(k, env_value)
    return value


def env_constructor(loader, node):
    """
    A constructor for environment varaibles provided in the yaml configuration file.
    It populates strings that contain environment variables in a form `${var_name}` with
    their values.
    """
    return replace_env_variable(node.value)


def py_constructor(loader, node):
    """
    A constructor for Python expression in the yaml configuration file. The python expression
    must be prefixed by `!py` directive. The result is the `PythonExpression` object.
    """
    try:
        return PythonExpression(replace_env_variable(node.value))
    except Exception as e:
        raise Exception('Cannot create python expression from string "%s". %s' % (node.value, str(e)))


class Config:
    """
    The main yamc confuguration. It reads the configuration from the yaml file, initializes logging,
    loads custom functions' modules and provides methods to access individual `providers`,
    `collectors` and `writers` configurations.
    """

    def __init__(self, file, env):
        """
        Read and parse the configuration from the yaml file and initializes the logging.
        """
        self.collectors = {}
        self.writers = {}
        self.providers = {}
        self.scope = Map(writers=None, collectors=None, providers=None, all_components=[], topics=None)

        if not (os.path.exists(file)):
            raise Exception(f"The configuration file {file} does not exist!")

        self.raw_config, self.config_file, self.config_dir = read_raw_config(file, env)
        self.log = logging.getLogger("config")

    def init_config(self):
        """
        Create the main configuration object, load the custom functions' modules and
        initialize the yamc scope.
        """

        def __load_components(name):
            components = Map()
            if self.config.value(name) is None:
                raise Exception("There are no components of type %s" % name)
            for component_id, component_config in self.config.value(name).items():
                try:
                    clazz = import_class(component_config["class"])
                    component = clazz(self, component_id)
                    if component.enabled:
                        components[component_id] = component
                except Exception as e:
                    raise Exception("Cannot load component '%s'. %s" % (component_id, str(e)))
            return components

        def __select_topics(*topics):
            sources = []
            for name, provider in self.scope.providers.items():
                if isinstance(provider, EventSource):
                    sources.extend(provider.select(*topics, silent=True))
            return sources

        self.config = ConfigPart(self, None, self.raw_config, self.config_dir)
        self.data_dir = self.get_dir_path(self.config.value("directories.data", default="../data"))
        os.makedirs(self.data_dir, exist_ok=True)

        # load custom functions
        from inspect import getmembers, isfunction

        self.custom_functions = {}
        for name, file in self.config.value("custom-functions", default={}).items():
            filename = self.get_dir_path(file, check=True)
            directory = os.path.dirname(filename)
            modulename = re.sub(r"\.py$", "", os.path.basename(filename))
            self.log.debug(
                "Importing custom module with id %s: module=%s, directory=%s" % (name, modulename, directory)
            )
            fp, path, desc = imp.find_module(modulename, [directory])
            module = imp.load_module(modulename, fp, path, desc)
            self.custom_functions[name] = Map({x[0]: x[1] for x in getmembers(module, isfunction)})

        # initialize scope
        self.log.info("Initializing scope.")
        if self.custom_functions is not None:
            for k, v in self.custom_functions.items():
                self.scope[k] = v

        self.scope.select = __select_topics
        self.scope.writers = __load_components("writers")
        self.scope.providers = __load_components("providers")
        self.scope.collectors = __load_components("collectors")
        self.scope.all_components = (
            list(self.scope.writers.values())
            + list(self.scope.collectors.values())
            + list(self.scope.providers.values())
        )

    def get_dir_path(self, path, base_dir=None, check=False):
        """
        Return the full directory of the path with `config_dir` as the base directory.
        """
        return get_dir_path(self.config_dir, path, base_dir, check)

    def collector(self, collector_id):
        """
        Return a `ConfigPart` object for a collector with `collector_id`
        """
        if collector_id not in self.collectors:
            self.collectors[collector_id] = ConfigPart(
                self,
                "collectors.%s" % collector_id,
                self.config._config,
                self.config_dir,
            )
        return self.collectors[collector_id]

    def writer(self, writer_id):
        """
        Return a `ConfigPart` object for a writer with `writer_id`
        """
        if writer_id not in self.writers:
            self.writers[writer_id] = ConfigPart(self, "writers.%s" % writer_id, self.config._config, self.config_dir)
        return self.writers[writer_id]

    def provider(self, provider_id):
        """
        Return a `ConfigPart` object for a provider with `provider_id`
        """
        if provider_id not in self.providers:
            self.providers[provider_id] = ConfigPart(
                self, "providers.%s" % provider_id, self.config._config, self.config_dir
            )
        return self.providers[provider_id]


class ConfigPart:
    def __init__(self, parent, base_path, config, config_dir):
        self.parent = parent
        self.config_dir = config_dir
        self.base_path = base_path
        if base_path is not None:
            self._config = deep_find(config, base_path)
        else:
            self._config = config

    def get_dir_path(self, path, base_dir=None, check=False):
        return get_dir_path(self.config_dir, path, base_dir, check)

    def path(self, path):
        return "%s.%s" % (self.base_path, path) if self.base_path is not None else path

    def eval(self, val):
        if callable(getattr(val, "eval", None)):
            return val.eval(self.parent.scope)
        return val

    def value(self, path, default=None, type=None, required=True, no_eval=False):
        required = default is not None and required
        r = default
        if self._config is not None:
            val = reduce(
                lambda di, key: di.get(key, default) if isinstance(di, dict) else default,
                path.split("."),
                self._config,
            )
            if val == default:
                r = default
            else:
                if not no_eval:
                    if callable(getattr(val, "eval", None)):
                        try:
                            val = self.eval(val)
                        except Exception as e:
                            raise Exception(
                                "Cannot evaluate Python expression for property '%s'. %s" % (self.path(path), str(e))
                            )
                r = type(val) if type != None else val
        if not r and required:
            raise Exception("The property '%s' does not exist!" % (self.path(path)))
        return r

    def value_str(self, path, default=None, regex=None, required=False):
        v = self.value(path, default=default, type=str, required=required)
        if regex is not None and not re.match(regex, v):
            raise Exception("The property %s value %s does not match %s!" % (self.path(path), v, regex))
        return v

    def value_int(self, path, default=None, min=None, max=None, required=False):
        v = self.value(path, default=default, type=int, required=required)
        if min is not None and v < min:
            raise Exception("The property %s value %s must be greater or equal to %d!" % (self.path(path), v, min))
        if max is not None and v > max:
            raise Exception("The property %s value %s must be less or equal to %d!" % (self.path(path), v, max))
        return v

    def value_bool(self, path, default=None, required=False):
        return self.value(path, default=default, type=bool, required=required)


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format_header = "%(asctime)s [%(name)-10.10s] "
    format_msg = "[%(levelname)-1.1s] %(message)s"

    FORMATS = {
        logging.DEBUG: format_header + grey + format_msg + reset,
        logging.INFO: format_header + grey + format_msg + reset,
        logging.WARNING: format_header + yellow + format_msg + reset,
        logging.ERROR: format_header + red + format_msg + reset,
        logging.CRITICAL: format_header + bold_red + format_msg + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def init_logging(logs_dir, command_name, log_level="INFO", handlers=["file", "console"]):
    """
    Initialize the logging, set the log level and logging directory.
    """
    os.makedirs(logs_dir, exist_ok=True)

    # log handlers
    log_handlers = handlers

    # main logs configuration
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "standard": {"format": CustomFormatter.format_header + CustomFormatter.format_msg},
                "colored": {"()": CustomFormatter},
            },
            "handlers": {
                "console": {
                    "formatter": "colored" if ANSI_COLORS else "standard",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",  # Default is stderr
                },
                "file": {
                    "formatter": "standard",
                    "class": "logging.handlers.TimedRotatingFileHandler",
                    "filename": f"{logs_dir}/yamc-{command_name}.log",
                    "when": "midnight",
                    "interval": 1,
                    "backupCount": 30,
                },
            },
            "loggers": {
                "": {  # all loggers
                    "handlers": log_handlers,
                    "level": f"{log_level}",
                    "propagate": False,
                }
            },
        }
    )


def get_logger(name):
    """
    Return a logger proxy that will forward the log messages to the logger with the provided name.
    """

    class LoggingProxy:
        def __init__(self, name):
            self.log = logging.getLogger(name)

        def info(self, msg, *args, **kwargs):
            self.log.info(msg, *args, **kwargs)

        def warning(self, msg, *args, **kwargs):
            self.log.warning(msg, *args, **kwargs)

        def warn(self, msg, *args, **kwargs):
            self.log.warn(msg, *args, **kwargs)

        def error(self, msg, *args, **kwargs):
            kwargs["exc_info"] = yamc_config.TRACEBACK
            self.log.error(msg, *args, **kwargs)

        def exception(self, msg, *args, exc_info=True, **kwargs):
            self.log.exception(msg, *args, exc_info=exc_info, **kwargs)

        def critical(self, msg, *args, **kwargs):
            self.log.critical(msg, *args, **kwargs)

        def fatal(self, msg, *args, **kwargs):
            self.log.fatal(msg, *args, **kwargs)

        def log(self, level, msg, *args, **kwargs):
            self.log.log(level, msg, *args, **kwargs)

        def debug(self, msg, *args, **kwargs):
            self.log.log(logging.DEBUG, msg, *args, **kwargs)

    return LoggingProxy(name)
