# -*- coding: utf-8 -*-
"""
GAPs CLI command configuration tests.
"""
from pathlib import Path

import click
import pytest

from gaps.cli.command import (
    CLICommandFromFunction,
    GAPS_SUPPLIED_ARGS,
    _WrappedCommand,
)


def test_cli_command_configuration():
    """Test the CLICommandFromFunction class."""

    def _test_func():
        pass

    ccc = CLICommandFromFunction(_test_func, name="run")
    assert not ccc.split_keys
    assert not ccc.config_preprocessor({})
    assert len(ccc.preprocessor_args) == 1
    assert "config" in ccc.preprocessor_args
    assert not ccc.preprocessor_defaults
    assert len(ccc.documentation.signatures) == 2
    assert not ccc.is_split_spatially
    assert all(
        param in ccc.documentation.skip_params
        for param in GAPS_SUPPLIED_ARGS
    )

    def _test_preprocessor(config, name, _a_default=3):
        config["name"] = name
        return config

    ccc = CLICommandFromFunction(
        _test_func,
        name="run",
        split_keys=["project_points"],
        config_preprocessor=_test_preprocessor,
    )
    assert ccc.is_split_spatially
    assert "project_points" not in ccc.split_keys
    assert "project_points_split_range" in ccc.split_keys
    assert len(ccc.preprocessor_args) == 3
    assert "config" in ccc.preprocessor_args
    assert "name" in ccc.preprocessor_args
    assert "_a_default" in ccc.preprocessor_args
    assert ccc.preprocessor_defaults == {"_a_default": 3}
    assert len(ccc.documentation.signatures) == 2

    config_in = {"project_points": [0, 1]}
    expected_out = {
        "project_points": [0, 1],
        "project_points_split_range": [(0, 2)],
        "name": "test",
    }
    assert ccc.config_preprocessor(config_in, "test") == expected_out

    config_in = {"project_points": [0, 1]}
    assert ccc.config_preprocessor(config_in, name="test") == expected_out


def test_wrapped_command():
    """Test the `get_help` method of the wrapped command."""
    command = _WrappedCommand(
        "test",
        help="""::\n }\nParameters [required].\n""",
    )
    ctx = click.Context(command)
    assert ":\n\n" in command.get_help(ctx)
    assert "\n[required]" in command.get_help(ctx)
    assert ".\n\n" in command.get_help(ctx)

    command = _WrappedCommand(
        "test",
        help="""Parameters\n----------.\n""",
    )
    assert ".\n\n" in command.get_help(click.Context(command))


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
