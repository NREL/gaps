# -*- coding: utf-8 -*-
# pylint: disable=unused-argument,redefined-outer-name,invalid-name
"""
GAPs pipeline command tests.
"""
import os
import json
import shutil
from pathlib import Path

import click
import pytest

from gaps import Pipeline, Status
from gaps.status import StatusField, StatusOption
from gaps.cli.command import CLICommandFromFunction
import gaps.cli.pipeline
from gaps.cli.pipeline import (
    template_pipeline_config,
    pipeline_command,
    pipeline,
    _can_run_background,
)
from gaps.exceptions import gapsExecutionError
from gaps.warnings import gapsWarning


TEST_FILE_DIR = Path(__file__).parent.as_posix()
SAMPLE_CONFIG = {
    "logging": {"log_level": "INFO"},
    "pipeline": [
        {"run": "./config_run.json"},
    ],
}
SUCCESS_CONFIG = {"test": "success"}


@pytest.fixture
def pipe_config_fp(tmp_path):
    """Add a sample pipeline config to a temp directory."""
    pipe_config_fp = tmp_path / "config_pipe.json"
    with open(pipe_config_fp, "w") as config_file:
        json.dump(SAMPLE_CONFIG, config_file)

    yield pipe_config_fp


@pytest.fixture
def runnable_pipeline(pipe_config_fp):
    """Add run to pipeline commands for test only."""
    try:
        Pipeline.COMMANDS["run"] = run
        yield pipe_config_fp
    finally:
        Pipeline.COMMANDS.pop("run")


@click.command()
@click.option("--config", "-c", default=".", help="Path to config file")
def run(config):
    """Test command."""
    config_fp = Path(config)
    with open(config_fp, "w") as config_file:
        json.dump(SUCCESS_CONFIG, config_file)

    attrs = {StatusField.JOB_STATUS: StatusOption.SUCCESSFUL}
    Status.make_single_job_file(config_fp.parent, "run", "test", attrs)


# pylint: disable=no-value-for-parameter
def test_can_run_background(monkeypatch, test_ctx, pipe_config_fp):
    """Test the `_can_run_background` method"""

    monkeypatch.setattr(os, "setsid", lambda: None, raising=False)
    monkeypatch.setattr(os, "fork", lambda: None, raising=False)

    assert _can_run_background()

    monkeypatch.delattr(os, "setsid", raising=False)
    monkeypatch.delattr(os, "fork", raising=False)

    assert not _can_run_background()

    with pytest.raises(gapsExecutionError) as exc_info:
        pipeline(pipe_config_fp, cancel=False, monitor=False, background=True)

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
    target_config_fp.touch()

    pipe = pipeline_command({})
    if _can_run_background():
        assert "background" in [opt.name for opt in pipe.params]
    else:
        assert "background" not in [opt.name for opt in pipe.params]
    cli_runner.invoke(pipe, ["-c", runnable_pipeline.as_posix()] + extra_args)

    if not extra_args:
        cli_runner.invoke(pipe, ["-c", runnable_pipeline.as_posix()])
    else:
        assert Status(tmp_path).get(StatusField.MONITOR_PID) == os.getpid()

    with open(target_config_fp, "r") as config:
        assert json.load(config) == SUCCESS_CONFIG

    assert_message_was_logged("Pipeline job", "INFO")
    assert_message_was_logged("is complete.", "INFO")


@pytest.mark.parametrize(
    "extra_args", [["--background"], ["--monitor", "--background"]]
)
def test_pipeline_command_with_background(
    extra_args, pipe_config_fp, cli_runner, monkeypatch
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

    pipe = pipeline_command({})
    assert "background" in [opt.name for opt in pipe.params]
    assert not kickoff_background_cache
    cli_runner.invoke(
        pipe, ["-c", pipe_config_fp.as_posix()] + extra_args, obj={}
    )

    assert len(kickoff_background_cache) == 1
    assert pipe_config_fp.as_posix() in kickoff_background_cache[0]


def test_pipeline_command_cancel(pipe_config_fp, cli_runner, monkeypatch):
    """Test the pipeline_command with --cancel."""

    def _new_cancel(config):
        assert config == pipe_config_fp.as_posix()

    monkeypatch.setattr(Pipeline, "cancel_all", _new_cancel, raising=True)

    pipe = pipeline_command({})
    cli_runner.invoke(pipe, ["-c", pipe_config_fp.as_posix(), "--cancel"])


def test_ppl_command_no_config_arg(
    tmp_cwd,
    cli_runner,
    runnable_pipeline,
    assert_message_was_logged,
):
    """Test pipeline command without explicit config input."""

    target_config_fp = tmp_cwd / "config_run.json"
    pipe_config_fp = tmp_cwd / "config_pipeline.json"

    target_config_fp.touch()
    assert not pipe_config_fp.exists()
    pipe = pipeline_command({})
    result = cli_runner.invoke(pipe)

    assert result.exit_code == 1
    assert "Could not determine config file" in str(result.exception)

    with open(pipe_config_fp, "w") as config_file:
        json.dump(SAMPLE_CONFIG, config_file)

    cli_runner.invoke(pipe)
    with open(target_config_fp, "r") as config:
        assert json.load(config) == SUCCESS_CONFIG

    cli_runner.invoke(pipe)
    assert_message_was_logged("Pipeline job", "INFO")
    assert_message_was_logged("is complete.", "INFO")

    (tmp_cwd / "config_pipeline_2.json").touch()
    result = cli_runner.invoke(pipe)

    assert result.exit_code == 1
    assert "Could not determine config file" in str(result.exception)


def test_template_pipeline_config():
    """Test generating the template pipeline config"""

    def _test_func():
        pass

    commands = [
        CLICommandFromFunction(_test_func, name="run"),
        CLICommandFromFunction(_test_func, name="collect-run"),
        CLICommandFromFunction(_test_func, name="analysis_and_qa"),
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


def test_pipeline_command_with_running_pid(
    pipe_config_fp, cli_runner, monkeypatch
):
    """Assert pipeline_command does not start processing if existing pid."""

    monkeypatch.setattr(
        gaps.cli.pipeline.Status,
        "get",
        lambda *__, **___: os.getpid(),
        raising=True,
    )

    pipe = pipeline_command({})
    with pytest.warns(gapsWarning) as warn_info:
        cli_runner.invoke(pipe, ["-c", pipe_config_fp.as_posix()], obj={})

    assert "Another pipeline" in warn_info[0].message.args[0]
    assert "is running on monitor PID:" in warn_info[0].message.args[0]
    assert f"{os.getpid()}" in warn_info[0].message.args[0]
    assert (
        "Not starting a new pipeline execution" in warn_info[0].message.args[0]
    )


def test_pipeline_command_recursive(
    tmp_cwd, cli_runner, runnable_pipeline, assert_message_was_logged
):
    """Test the pipeline command with recursive directories."""

    target_config_fp = tmp_cwd / "config_run.json"
    target_config_fp.touch()
    runnable_pipeline.rename(runnable_pipeline.parent / "config_pipeline.json")

    test_dirs = [
        tmp_cwd,
        tmp_cwd / "test_run_2",
        tmp_cwd / "test_run_2" / "test_run_3",
        tmp_cwd / "test_run_4",
        tmp_cwd / "test_run_5",
    ]

    for prev_dir, next_dir in zip(test_dirs[0:-1], test_dirs[1:]):
        shutil.copytree(prev_dir, next_dir)

    for test_dir in test_dirs:
        expected_out_fp = test_dir / "config_run.json"
        assert expected_out_fp.exists()
        with open(expected_out_fp, "r") as config:
            assert not config.read()

    shutil.copy(
        test_dirs[-1] / "config_pipeline.json",
        test_dirs[-1] / "config_pipeline2.json",
    )

    pipe = pipeline_command({})
    cli_runner.invoke(pipe, ["-r"])
    cli_runner.invoke(pipe, ["-r"])

    for test_dir in test_dirs[:-1]:
        assert_message_was_logged(test_dir.name, "INFO")
        with open(test_dir / "config_run.json", "r") as config:
            assert json.load(config) == SUCCESS_CONFIG

    with open(test_dirs[-1] / "config_run.json", "r") as config:
        assert not config.read()

    assert_message_was_logged("Pipeline job", "INFO")
    assert_message_was_logged("is complete.", "INFO")
    assert_message_was_logged(
        "Could not determine config file - multiple files detected", "WARNING"
    )
    assert_message_was_logged(test_dirs[-1].name, "WARNING")


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
