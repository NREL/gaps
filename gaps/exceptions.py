# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""Custom Exceptions and Errors for gaps. """
import sys
import logging

logger = logging.getLogger("gaps")


class gapsError(Exception):
    """Generic gaps Error."""

    def __init__(self, *args, **kwargs):
        """Init exception and broadcast message to logger."""
        super().__init__(*args, **kwargs)
        if args:
            l_kwargs = {}
            if sys.version_info[1] >= 8:  # pragma: no cover
                l_kwargs["stacklevel"] = 2
            logger.error(str(args[0]), **l_kwargs)


class gapsConfigError(gapsError):
    """gaps ConfigError."""


class gapsExecutionError(gapsError):
    """gaps ExecutionError."""


class gapsIndexError(gapsError, IndexError):
    """gaps IndexError."""


class gapsIOError(gapsError, IOError):
    """gaps IOError."""


class gapsKeyError(gapsError, KeyError):
    """gaps KeyError."""


class gapsRuntimeError(gapsError, RuntimeError):
    """gaps RuntimeError."""


class gapsTypeError(gapsError, TypeError):
    """gaps TypeError."""


class gapsValueError(gapsError, ValueError):
    """gaps ValueError."""


class gapsHPCError(gapsError):
    """gaps HPCError."""
