"""Custom Exceptions and Errors for gaps"""

import logging

logger = logging.getLogger("gaps")


class gapsError(Exception):  # noqa: N801
    """Generic gaps Error"""

    def __init__(self, *args, **kwargs):
        """Init exception and broadcast message to logger"""
        super().__init__(*args, **kwargs)
        if args:
            logger.error(str(args[0]), stacklevel=2)


class gapsConfigError(gapsError):  # noqa: N801
    """gaps ConfigError"""


class gapsExecutionError(gapsError):  # noqa: N801
    """gaps ExecutionError"""


class gapsFileNotFoundError(gapsError, FileNotFoundError):  # noqa: N801
    """gaps FileNotFoundError"""


class gapsIndexError(gapsError, IndexError):  # noqa: N801
    """gaps IndexError"""


class gapsIOError(gapsError, IOError):  # noqa: N801
    """gaps IOError"""


class gapsKeyError(gapsError, KeyError):  # noqa: N801
    """gaps KeyError"""


class gapsRuntimeError(gapsError, RuntimeError):  # noqa: N801
    """gaps RuntimeError"""


class gapsTypeError(gapsError, TypeError):  # noqa: N801
    """gaps TypeError"""


class gapsValueError(gapsError, ValueError):  # noqa: N801
    """gaps ValueError"""


class gapsHPCError(gapsError):  # noqa: N801
    """gaps HPCError"""
