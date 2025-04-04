# -*- coding: utf-8 -*-
# pylint: disable=unused-argument,redefined-outer-name,invalid-name
"""
GAPs template command tests.
"""

from pathlib import Path

import pytest

from gaps.config import ConfigType
from gaps.cli.command import CLICommandFromFunction
from gaps.cli.pipeline import template_pipeline_config
from gaps.cli.templates import template_command
from gaps.warn import gapsWarning


@pytest.mark.parametrize("commands", [[], ["run", "pipeline", "dne"]])
@pytest.mark.parametrize(
    "config_type", [None] + list(ConfigType.members_as_str())
)
def test_status(
    tmp_cwd, cli_runner, commands, config_type, assert_message_was_logged
):
    """Test the status command."""

    def _test_func(a, b=3):
        pass

    run_config = CLICommandFromFunction(_test_func, name="run")
    template_configs = {
        "run": run_config.documentation.template_config,
        "pipeline": template_pipeline_config([run_config]),
    }
    templates = template_command(template_configs)

    assert not list(tmp_cwd.glob("*"))

    extra_args = ["-t", config_type] if config_type else []
    if commands:
        with pytest.warns(gapsWarning):
            cli_runner.invoke(templates, commands + extra_args, obj={})
    else:
        cli_runner.invoke(templates, extra_args, obj={"VERBOSE": True})

    assert len(list(tmp_cwd.glob("*"))) == 2

    assert_message_was_logged(
        "Generating template config file for command 'run': 'config_run.",
        "INFO",
    )
    assert_message_was_logged(
        "Generating template config file for command 'pipeline': "
        "'config_pipeline.",
        "INFO",
    )
    config_type = config_type or "json"
    config = ConfigType(config_type).load(
        tmp_cwd / f"config_run.{config_type}"
    )

    assert config["a"] == run_config.documentation.REQUIRED_TAG
    assert config["b"] == 3
    assert "execution_control" in config

    config = ConfigType(config_type).load(
        tmp_cwd / f"config_pipeline.{config_type}"
    )

    assert config["pipeline"] == [{"run": f"./config_run.{config_type}"}]
    assert "logging" in config


@pytest.mark.parametrize("config_type", list(ConfigType.members_as_str()))
def test_existing_file(
    tmp_cwd, cli_runner, config_type, assert_message_was_logged
):
    """Test that the template configs command does not overwrite existing
    configs."""

    def _test_func(a, b=3):
        pass

    run_config = CLICommandFromFunction(_test_func, name="run")
    template_configs = {
        "run": run_config.documentation.template_config,
        "pipeline": template_pipeline_config([run_config]),
    }
    templates = template_command(template_configs)

    assert not list(tmp_cwd.glob("*"))

    ConfigClass = ConfigType[config_type.upper()]
    existing_file = tmp_cwd / f"config_pipeline.{config_type}"
    dummy_config = {"test": "test"}

    with open(existing_file, "w") as f:
        ConfigClass.dump(dummy_config, f)

    assert len(list(tmp_cwd.glob("*"))) == 1

    cli_runner.invoke(templates, ["-t", config_type], obj={"VERBOSE": True})

    assert_message_was_logged(
        "Template config already exists: 'config_pipeline.", "INFO"
    )

    assert len(list(tmp_cwd.glob("*"))) == 2

    config = ConfigClass.load(existing_file)
    assert config == dummy_config


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
