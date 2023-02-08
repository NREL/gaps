# -*- coding: utf-8 -*-
"""GAPs exception tests. """
from pathlib import Path

import pytest

from gaps.exceptions import (
    gapsError,
    gapsConfigError,
    gapsExecutionError,
    gapsHPCError,
    gapsIndexError,
    gapsIOError,
    gapsKeyError,
    gapsRuntimeError,
    gapsTypeError,
    gapsValueError,
)


BASIC_ERROR_MESSAGE = "An error message"


def test_exceptions_log_error(caplog, assert_message_was_logged):
    """Test that a raised exception logs message, if any."""

    try:
        raise gapsError
    except gapsError:
        pass

    assert not caplog.records

    try:
        raise gapsError(BASIC_ERROR_MESSAGE)
    except gapsError:
        pass

    assert_message_was_logged(BASIC_ERROR_MESSAGE, "ERROR")


def test_exceptions_log_uncaught_error(assert_message_was_logged):
    """Test that a raised exception logs message if uncaught."""

    with pytest.raises(gapsError):
        raise gapsError(BASIC_ERROR_MESSAGE)

    assert_message_was_logged(BASIC_ERROR_MESSAGE, "ERROR")


@pytest.mark.parametrize(
    "raise_type, catch_types",
    [
        (gapsExecutionError, [gapsError, gapsExecutionError]),
        (gapsConfigError, [gapsError, gapsConfigError]),
        (gapsHPCError, [gapsError, gapsHPCError]),
        (gapsIndexError, [gapsError, gapsIndexError, IndexError]),
        (gapsIOError, [gapsError, gapsIOError, IOError]),
        (gapsKeyError, [gapsError, gapsKeyError, KeyError]),
        (gapsRuntimeError, [gapsError, gapsRuntimeError, RuntimeError]),
        (gapsTypeError, [gapsError, gapsTypeError, TypeError]),
        (gapsValueError, [gapsError, gapsValueError, ValueError]),
    ],
)
def test_catching_error_by_type(
    raise_type, catch_types, assert_message_was_logged
):
    """Test that gaps exceptions are caught correctly."""
    for catch_type in catch_types:
        with pytest.raises(catch_type) as exc_info:
            raise raise_type(BASIC_ERROR_MESSAGE)

        assert BASIC_ERROR_MESSAGE in str(exc_info.value)
        assert_message_was_logged(BASIC_ERROR_MESSAGE, "ERROR")


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
