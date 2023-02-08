# -*- coding: utf-8 -*-
# pylint: disable=unused-argument,redefined-outer-name,invalid-name
"""
GAPs pipeline command tests.
"""
import os
import json
from pathlib import Path

import click
import pytest

from gaps import Pipeline, Status
from gaps.status import StatusField, StatusOption
from gaps.cli.command import CLICommandConfiguration
import gaps.cli.pipeline
from gaps.cli.pipeline import (
    template_pipeline_config,
    pipeline_command,
    pipeline,
    _can_run_background,
)
from gaps.exceptions import gapsExecutionError


TEST_FILE_DIR = Path(__file__).parent.as_posix()
SAMPLE_CONFIG = {
    "logging": {"log_level": "INFO"},
    "pipeline": [
        {"run": "./config_run.json"},
    ],
}


@pytest.fixture
def runnable_pipeline():
    """Add run to pipeline commands for test only."""
    try:
        Pipeline.COMMANDS["run"] = run
        yield
    finally:
        Pipeline.COMMANDS.pop("run")


@click.command()
@click.option("--config", "-c", default=".", help="Path to config file")
def run(config):
    """Test command."""
    config_fp = Path(config)
    config_fp.touch()
    attrs = {StatusField.JOB_STATUS: StatusOption.SUCCESSFUL}
    Status.make_single_job_file(config_fp.parent, "run", "test", attrs)


# pylint: disable=no-value-for-parameter
def test_can_run_background(monkeypatch, test_ctx, tmp_path):
    """Test the `_can_run_background` method"""

    monkeypatch.setattr(os, "setsid", lambda: None, raising=False)
    monkeypatch.setattr(os, "fork", lambda: None, raising=False)

    assert _can_run_background()

    monkeypatch.delattr(os, "setsid", raising=False)
    monkeypatch.delattr(os, "fork", raising=False)

    assert not _can_run_background()

    pipe_config_fp = tmp_path / "config_pipe.json"
    pipe_config_fp.touch()
    with pytest.raises(gapsExecutionError) as exc_info:
        pipeline(tmp_path, cancel=False, monitor=False, background=True)

    assert "Cannot run pipeline in background on system" in str(exc_info)


@pytest.mark.parametrize("extra_args", [[], ["--monitor"]])
def test_pipeline_command(
    extra_args,
    tmp_path,
    cli_runner,
    runnable_pipeline,
    assert_message_was_logged,
):
    """Test the pipeline_command creation."""

    target_config_fp = tmp_path / "config_run.json"
    pipe_config_fp = tmp_path / "config_pipe.json"
    with open(pipe_config_fp, "w") as config_file:
        json.dump(SAMPLE_CONFIG, config_file)

    assert not target_config_fp.exists()
    pipe = pipeline_command({})
    if _can_run_background():
        assert "background" in [opt.name for opt in pipe.params]
    else:
        assert "background" not in [opt.name for opt in pipe.params]
    cli_runner.invoke(pipe, ["-c", pipe_config_fp.as_posix()] + extra_args)

    if not extra_args:
        cli_runner.invoke(pipe, ["-c", pipe_config_fp.as_posix()])

    assert target_config_fp.exists()
    assert_message_was_logged("Pipeline job", "INFO")
    assert_message_was_logged("is complete.", "INFO")


@pytest.mark.parametrize(
    "extra_args", [["--background"], ["--monitor", "--background"]]
)
def test_pipeline_command_with_background(
    extra_args, tmp_path, cli_runner, monkeypatch
):
    """Test the pipeline_command creation with background."""

    kickoff_background_cache = []

    def _new_run(config_file):
        kickoff_background_cache.append(config_file)

    monkeypatch.setattr(
        gaps.cli.pipeline, "_kickoff_background", _new_run, raising=True
    )
    monkeypatch.setattr(os, "setsid", lambda: None, raising=False)
    monkeypatch.setattr(os, "fork", lambda: None, raising=False)

    pipe_config_fp = tmp_path / "config_pipe.json"
    pipe_config_fp.touch()

    pipe = pipeline_command({})
    assert "background" in [opt.name for opt in pipe.params]
    assert not kickoff_background_cache
    cli_runner.invoke(
        pipe, ["-c", pipe_config_fp.as_posix()] + extra_args, obj={}
    )

    assert len(kickoff_background_cache) == 1
    assert pipe_config_fp.as_posix() in kickoff_background_cache[0]


def test_pipeline_command_cancel(tmp_path, cli_runner, monkeypatch):
    """Test the pipeline_command with --cancel."""

    pipe_config_fp = tmp_path / "config_pipe.json"
    pipe_config_fp.touch()

    def _new_cancel(config):
        assert config == pipe_config_fp.as_posix()

    monkeypatch.setattr(Pipeline, "cancel_all", _new_cancel, raising=True)

    pipe = pipeline_command({})
    cli_runner.invoke(pipe, ["-c", pipe_config_fp.as_posix(), "--cancel"])


def test_template_pipeline_config():
    """Test generating the template pipeline config"""

    def _test_func():
        pass

    commands = [
        CLICommandConfiguration("run", _test_func),
        CLICommandConfiguration("collect-run", _test_func),
        CLICommandConfiguration("analysis_and_qa", _test_func),
    ]
    config = template_pipeline_config(commands)
    expected_config = {
        "pipeline": [
            {"run": "./config_run.json"},
            {"collect-run": "./config_collect_run.json"},
            {"analysis_and_qa": "./config_analysis_and_qa.json"},
        ],
        "logging": {"log_file": None, "log_level": "INFO"},
    }

    assert config == expected_config


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
