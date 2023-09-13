# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""Custom Exceptions and Errors for gaps. """
import logging

logger = logging.getLogger("gaps")


class gapsError(Exception):
    """Generic gaps Error."""

    def __init__(self, *args, **kwargs):
        """Init exception and broadcast message to logger."""
        super().__init__(*args, **kwargs)
        if args:
            logger.error(str(args[0]), stacklevel=2)


class gapsConfigError(gapsError):
    """gaps ConfigError."""


class gapsExecutionError(gapsError):
    """gaps ExecutionError."""

class gapsFileNotFoundError(gapsError, FileNotFoundError):
    """gaps FileNotFoundError."""


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
