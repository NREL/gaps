"""Logging utilities

Some of the code in this module is borrowed from rex
(https://github.com/NREL/rex).
"""

import sys
import logging
from functools import partial
from pathlib import Path

import psutil

import rex
import gaps

FORMAT = "gaps (%(levelname)s) - [%(filename)s:%(lineno)d] : %(message)s"
"""Default format for gaps logging"""


def log_versions():
    """Log package versions"""
    gaps.logger.info("Running with gaps version %s", gaps.__version__)
    rex.utilities.log_versions(gaps.logger)


def log_mem(log_level="DEBUG"):
    """Log the memory usage to the input logger object

    Parameters
    ----------
    logger : logging.Logger
        Logger object to log memory message to.
    log_level : str
        DEBUG or INFO for different log levels for this log message.

    Returns
    -------
    msg : str
        Memory utilization log message string.
    """
    mem = psutil.virtual_memory()
    msg = (
        f"Memory utilization is {mem.used / (1024.0**3):.3f} GB "
        f"out of {mem.total / (1024.0**3):.3f} GB total "
        f"({mem.used / mem.total:.1%} used)"
    )
    gaps.logger.log(logging.getLevelName(log_level), msg)

    return msg


def init_logger(stream=True, level="INFO", file=None, fmt=FORMAT):
    """Initialize and setup logging instance

    Parameters
    ----------
    stream : bool, optional
        Option to add a StreamHandler along with FileHandler.
        By default `True`.
    level : str, optional
        Level of logging to capture. If multiple handlers/log_files are
        requested in a single call of this function, the specified
        logging level will be applied to all requested handlers.
        By default, "INFO".
    file : str | list, optional
        Path to file that should be used for logging. This can also be
        a list of filepaths to use for multiple log files.
        By default, `None`.
    fmt : str, optional
        Format for loggings. By default `FORMAT`.
    """

    file = file or []
    if isinstance(file, str):
        file = [file]

    handlers = [
        make_handler(partial(make_log_file_handler, f), level=level, fmt=fmt)
        for f in file
    ]

    if stream:
        handlers.append(
            make_handler(make_log_stream_handler, level=level, fmt=fmt)
        )

    add_handlers(handlers)


def make_handler(factory, level="INFO", fmt=FORMAT):
    """Make a handler to add to a Logger instance.

    Parameters
    ----------
    factory : callable
        A callable function to create the default handler.
    level : str, optional
        A string representing the logging level for the handler.
        By default "INFO".
    fmt : str, optional
        A string representing the formatting for logging calls.
        By default `FORMAT`.

    Returns
    -------
    handler : `logging.Handler`
        Handler with the specified log level and format.
    """

    handler = factory()

    handler.setLevel(level.upper())

    log_format = logging.Formatter(fmt)
    handler.setFormatter(log_format)

    return handler


def make_log_file_handler(file_path):
    """Make a file handler to add to a Logger instance.

    If the directory structure for `file_path` does not exist, it is
    created before initializing the FileHandler.

    Parameters
    ----------
    file_path : str, optional
        Path to the output log file.

    Returns
    -------
    handler : `logging.FileHandler`
        File handler with `file_path` as the name.
    """

    log_file = Path(file_path).expanduser().resolve()
    log_file.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(log_file, mode="a")
    handler.set_name(log_file.name)

    return handler


def make_log_stream_handler():
    """Make a file handler to add to a Logger instance.

    Returns
    -------
    handler : `logging.StreamHandler`
        Stream handler with name "stream".
    """

    handler = logging.StreamHandler(sys.stdout)
    handler.set_name("stream")

    return handler


def add_handlers(handlers):
    """Add handlers to logger ensuring they do not already exist.

    Parameters
    ----------
    handlers : list
        Handlers to add to logger instance.
    """
    current_handlers = {h.name: h for h in gaps.logger.handlers}
    for handler in handlers:
        name = handler.name
        if name not in current_handlers:
            gaps.logger.addHandler(handler)
            current_handlers.update({name: handler})
        else:
            existing_handler = current_handlers[name]
            if handler.level < existing_handler.level:
                existing_handler.setLevel(handler.level)


def print_logging_info():
    """Print logger names, levels, and handlers"""

    logger_names = [__name__.split(".", maxsplit=1)[0]]
    for name in logger_names:
        print(f"LOGGER: {name!r}")
        logger_to_debug = logging.getLogger(name)
        while logger_to_debug is not None:
            print(
                f"level: {logger_to_debug.level}, "
                f"name: {logger_to_debug.name}, "
                f"handlers: "
            )
            for handler in logger_to_debug.handlers:
                print(f"\t +++ {handler.name} - {handler}")
            logger_to_debug = logger_to_debug.parent


def print_logging_info_all_libraries():
    """Print logger info from all libraries

    Reference
    ---------
    https://stackoverflow.com/questions/3630774/logging-remove-inspect-modify-handlers-configured-by-fileconfig
    """
    loggers = logging.Logger.manager.loggerDict
    for package, logger_ in loggers.items():
        print(f"+ [{package:<40}] {{{__cls_name(logger_)}}} ")
        if isinstance(logger_, logging.PlaceHolder):
            continue
        for handler in gaps.logger.handlers:
            print(f"     +++ {__cls_name(handler)}")


def __cls_name(obj):
    """Format the class name of the input logger/handler object"""
    return str(obj.__class__)[8:-2]
