# -*- coding: utf-8 -*-
# pylint: disable=redefined-outer-name, protected-access, unused-argument
# pylint: disable=too-few-public-methods
"""
GAPs Pipeline tests.
"""
import time
import json
import random
import logging
from pathlib import Path

import pytest

from gaps.hpc import SLURM
from gaps.config import ConfigType
from gaps.status import (
    Status,
    StatusField,
    StatusOption,
    HardwareStatusRetriever,
)
from gaps.pipeline import (
    Pipeline,
    _dump_sorted,
    _check_jobs_submitted,
    _parse_code_array,
    parse_previous_status,
)
from gaps.exceptions import (
    gapsConfigError,
    gapsExecutionError,
    gapsKeyError,
    gapsValueError,
)
from gaps.warnings import gapsWarning

SAMPLE_CONFIG = {
    "logging": {"log_level": "INFO"},
    "pipeline": [
        {"run": "./config.{config_type}"},
        {"collect-run": "./collect_config.{config_type}"},
    ],
}


@pytest.fixture
def submit_call_cache():
    """Cache subprocess calls."""
    return []


@pytest.fixture
def sample_config_fp_type(tmp_path):
    """Generate a sample config type and corresponding fp."""
    types = list(ConfigType)
    config_type = random.choice(types)
    config_fp = tmp_path / f"pipe_config.{config_type}"

    sample_config = {
        "logging": {"log_level": "INFO"},
        "pipeline": [
            {"run": (tmp_path / "config.json").as_posix()},
            {"collect-run": (tmp_path / "collect_config.json").as_posix()},
        ],
    }
    return config_type, config_fp, sample_config


@pytest.fixture
def sample_pipeline_config(sample_config_fp_type):
    """Write a sample pipeline config for use in tests."""
    config_type, config_fp, sample_config = sample_config_fp_type
    with open(config_fp, "w") as file_:
        config_type.dump(sample_config, file_)

    for step_dict in sample_config["pipeline"]:
        for step_config_fp in step_dict.values():
            Path(step_config_fp).touch()

    return config_fp


@pytest.fixture
def mock_command(submit_call_cache):
    """Create a mock command that caches config filepath."""

    class MockCommand:
        """Mock command used for testing - caches config filepath."""

        @classmethod
        def callback(cls, config_filepath):
            """Mock callback function that only caches config filepath."""
            submit_call_cache.append(config_filepath)

    return MockCommand


@pytest.fixture
def runnable_pipeline(monkeypatch, mock_command, submit_call_cache):
    """Monkeypatch pipeline COMMANDS."""
    monkeypatch.setattr(
        Pipeline,
        "COMMANDS",
        {"run": mock_command, "collect-run": mock_command},
        raising=True,
    )
    return submit_call_cache


def test_pipeline_init_bad_config(tmp_path):
    """Test initializing Pipeline from bad config."""
    config_fp = tmp_path / "pipe_config.json"
    with open(config_fp, "w") as file_:
        json.dump({}, file_)

    with pytest.raises(gapsConfigError) as exc_info:
        Pipeline(config_fp)

    assert "Could not find required key" in str(exc_info)

    with open(config_fp, "w") as file_:
        json.dump({"pipeline": "./run_config.json"}, file_)

    with pytest.raises(gapsConfigError) as exc_info:
        Pipeline(config_fp)

    assert "must be a list" in str(exc_info)

    with open(config_fp, "w") as file_:
        json.dump({"pipeline": [{"run": "./dne_config.json"}]}, file_)

    with pytest.raises(gapsConfigError) as exc_info:
        Pipeline(config_fp)

    assert "depends on non-existent file" in str(exc_info)


def test_pipeline_init(sample_pipeline_config, assert_message_was_logged):
    """Test initializing Pipeline."""
    config_dir = sample_pipeline_config.parent

    assert not list(config_dir.glob("*status.json"))

    pipeline = Pipeline(sample_pipeline_config)
    assert pipeline._out_dir == sample_pipeline_config.parent.as_posix()
    assert pipeline.name == sample_pipeline_config.parent.name
    assert pipeline._run_list == [
        {"run": (config_dir / "config.json").as_posix()},
        {"collect-run": (config_dir / "collect_config.json").as_posix()},
    ]

    assert list(config_dir.glob("*status.json"))

    status = Status(config_dir)
    assert status == {
        "run": {StatusField.PIPELINE_INDEX: 0},
        "collect-run": {StatusField.PIPELINE_INDEX: 1},
    }
    logging.getLogger("gaps").info("A test message")
    assert_message_was_logged("A test message", "INFO")


def test_pipeline_get_command_config(sample_pipeline_config):
    """Test `_get_command_config` method."""
    config_dir = sample_pipeline_config.parent
    pipeline = Pipeline(sample_pipeline_config)
    assert pipeline._get_command_config(0) == (
        "run",
        (config_dir / "config.json").as_posix(),
    )
    assert pipeline._get_command_config(1) == (
        "collect-run",
        (config_dir / "collect_config.json").as_posix(),
    )


def test_pipeline_submit(tmp_path, runnable_pipeline):
    """Test _submit method."""
    config_type = random.choice(list(ConfigType))
    config_fp = tmp_path / f"pipe_config.{config_type}"
    sample_config = {
        "pipeline": [
            {"run": "./config.json"},
            {"collect-run": "./collect_config.json"},
            {"dne": "./dne_config.json"},
        ]
    }
    with open(config_fp, "w") as file_:
        config_type.dump(sample_config, file_)

    for fn in ["config.json", "collect_config.json", "dne_config.json"]:
        (tmp_path / fn).touch()

    pipeline = Pipeline(config_fp)
    pipeline._submit(0)
    assert runnable_pipeline[-1] == (tmp_path / "config.json").as_posix()

    with pytest.raises(gapsKeyError) as exc_info:
        pipeline._submit(2)

    assert "Could not recognize command" in str(exc_info)


def test_pipeline_get_command_return_code(
    sample_pipeline_config, monkeypatch, assert_message_was_logged
):
    """Test the _get_command_return_code function."""
    pipeline = Pipeline(sample_pipeline_config)
    status = Status(sample_pipeline_config.parent)
    status.data = {}
    assert (
        pipeline._get_command_return_code(status, "run")
        == StatusOption.RUNNING
    )
    assert_message_was_logged("is running", "INFO")

    monkeypatch.setattr(
        HardwareStatusRetriever,
        "__getitem__",
        lambda *__, **___: "pending",
        raising=True,
    )
    Status.mark_job_as_submitted(
        sample_pipeline_config.parent,
        "run",
        "test",
        job_attrs={StatusField.JOB_ID: 0},
    )
    status.reload()
    assert (
        pipeline._get_command_return_code(status, "run")
        == StatusOption.SUBMITTED
    )
    assert_message_was_logged("is submitted", "INFO", clear_records=True)

    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.RUNNING},
    )
    assert (
        pipeline._get_command_return_code(status, "run")
        == StatusOption.RUNNING
    )
    assert_message_was_logged("is running", "INFO")

    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    assert (
        pipeline._get_command_return_code(status, "run")
        == StatusOption.SUCCESSFUL
    )
    assert_message_was_logged("is successful", "INFO")

    Status.mark_job_as_submitted(
        sample_pipeline_config.parent,
        "collect-run",
        "test",
        job_attrs={StatusField.JOB_ID: 1},
    )
    status.reload()
    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.FAILED},
    )
    assert (
        pipeline._get_command_return_code(status, "collect-run")
        == StatusOption.FAILED
    )
    assert_message_was_logged("has failed", "INFO", clear_records=True)

    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    assert (
        pipeline._get_command_return_code(status, "collect-run")
        == StatusOption.SUCCESSFUL
    )
    assert_message_was_logged("is successful", "INFO")

    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: None},
    )
    assert (
        pipeline._get_command_return_code(status, "collect-run")
        == StatusOption.COMPLETE
    )
    assert_message_was_logged("is complete", "INFO")

    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.RUNNING},
    )
    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="collect-run",
        job_name="test1",
        attrs={StatusField.JOB_STATUS: StatusOption.FAILED},
    )
    status.reload()
    assert (
        pipeline._get_command_return_code(status, "collect-run")
        == StatusOption.RUNNING
    )
    assert_message_was_logged("is running, but some jobs have failed", "INFO")

    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: "DNE"},
    )
    with pytest.raises(gapsValueError) as exc_info:
        pipeline._get_command_return_code(status, "collect-run")

    assert "Job status code" in str(exc_info)
    assert "not understood!" in str(exc_info)


def test_pipeline_status(sample_pipeline_config, monkeypatch):
    """Test the `_status` function."""
    pipeline = Pipeline(sample_pipeline_config)
    assert pipeline._status(0) == StatusOption.NOT_SUBMITTED
    monkeypatch.setattr(
        HardwareStatusRetriever,
        "__getitem__",
        lambda *__, **___: "pending",
        raising=True,
    )

    Status.mark_job_as_submitted(
        sample_pipeline_config.parent,
        "run",
        "test",
        job_attrs={StatusField.JOB_ID: 0},
    )
    assert pipeline._status(0) == StatusOption.SUBMITTED

    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.RUNNING},
    )
    assert pipeline._status(0) == StatusOption.RUNNING

    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    assert pipeline._status(0) == StatusOption.SUCCESSFUL

    Status.mark_job_as_submitted(
        sample_pipeline_config.parent,
        "collect-run",
        "test",
        job_attrs={StatusField.JOB_ID: 1},
    )
    assert pipeline._status(1) == StatusOption.SUBMITTED

    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.FAILED},
    )
    assert pipeline._status(1) == StatusOption.FAILED

    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    assert pipeline._status(1) == StatusOption.SUCCESSFUL


def test_pipeline_run(
    runnable_pipeline,
    sample_pipeline_config,
    monkeypatch,
    assert_message_was_logged,
):
    """Test the _run function."""
    config_dir = sample_pipeline_config.parent
    Pipeline.run(sample_pipeline_config, monitor=False)
    assert len(runnable_pipeline) == 1
    assert runnable_pipeline[-1] == (config_dir / "config.json").as_posix()

    Status.mark_job_as_submitted(
        sample_pipeline_config.parent,
        "run",
        "test",
        job_attrs={StatusField.JOB_ID: 0},
    )
    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    Pipeline.run(sample_pipeline_config, monitor=False)
    assert len(runnable_pipeline) == 2
    assert (
        runnable_pipeline[-1]
        == (config_dir / "collect_config.json").as_posix()
    )

    Status.mark_job_as_submitted(
        sample_pipeline_config.parent,
        "collect-run",
        "test",
        job_attrs={StatusField.JOB_ID: 1},
    )
    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.FAILED},
    )
    monkeypatch.setattr(
        HardwareStatusRetriever,
        "__getitem__",
        lambda *__, **___: None,
        raising=True,
    )
    Pipeline.run(sample_pipeline_config, monitor=False)
    assert len(runnable_pipeline) == 3
    assert (
        runnable_pipeline[-1]
        == (config_dir / "collect_config.json").as_posix()
    )
    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    Pipeline.run(sample_pipeline_config, monitor=False)
    assert_message_was_logged("Pipeline job", "INFO")
    assert_message_was_logged("is complete", "INFO")


def test_pipeline_monitor(sample_pipeline_config, monkeypatch):
    """Test the _run function with monitor=True."""
    monkeypatch.setattr(time, "sleep", lambda *__, **___: None, raising=True)

    steps_submitted = []

    def log_step(__, step):
        steps_submitted.append(step)
        return StatusOption.RUNNING

    n_calls = 0
    test_err_msg = "Something went wrong during test setup"

    # pylint: disable=inconsistent-return-statements
    def return_status(__, step):
        nonlocal n_calls
        n_calls += 1
        if n_calls == 1:
            assert step == 0, test_err_msg
            return StatusOption.RUNNING
        if n_calls == 2:
            assert step == 0, test_err_msg
            return StatusOption.SUCCESSFUL
        if n_calls == 3:
            assert step == 1, test_err_msg
            return StatusOption.RUNNING
        if n_calls == 4:
            assert step == 1, test_err_msg
            return StatusOption.FAILED
        if n_calls >= 5:
            raise Exception(test_err_msg)

    monkeypatch.setattr(Pipeline, "_submit", log_step, raising=True)
    monkeypatch.setattr(Pipeline, "_status", return_status, raising=True)

    with pytest.raises(gapsExecutionError) as exc_info:
        Pipeline.run(sample_pipeline_config, monitor=True)

    assert "Pipeline failed at step 1" in str(exc_info.value)
    assert steps_submitted == [0, 1]


def test_pipeline_cancel_all(
    sample_pipeline_config, monkeypatch, assert_message_was_logged
):
    """Test the _check_jobs_submitted function."""
    cancelled_jobs = []

    def cache_cancel_calls(__, job_id):
        cancelled_jobs.append(job_id)

    # cspell:disable-next-line
    monkeypatch.setattr(SLURM, "cancel", cache_cancel_calls, raising=True)

    Pipeline.cancel_all(sample_pipeline_config)
    assert_message_was_logged("cancelled", "INFO", clear_records=True)
    assert not cancelled_jobs

    Status.mark_job_as_submitted(
        sample_pipeline_config.parent,
        "run",
        "test1",
        job_attrs={StatusField.JOB_ID: 0},
    )
    Status.mark_job_as_submitted(
        sample_pipeline_config.parent,
        "run",
        "test2",
        job_attrs={StatusField.JOB_ID: 1},
    )
    Status.mark_job_as_submitted(
        sample_pipeline_config.parent,
        "run",
        "test3",
        job_attrs={StatusField.JOB_ID: 12},
    )
    Pipeline.cancel_all(sample_pipeline_config)
    assert set(cancelled_jobs) == {0, 1, 12}
    assert_message_was_logged("Pipeline job", "INFO")
    assert_message_was_logged("cancelled", "INFO")


def test_check_jobs_submitted(sample_pipeline_config):
    """Test the _check_jobs_submitted function."""
    Pipeline(sample_pipeline_config)
    status = Status(sample_pipeline_config.parent)
    assert not _check_jobs_submitted(status, "run")

    Status.mark_job_as_submitted(
        sample_pipeline_config.parent,
        "run",
        "test",
        job_attrs={StatusField.JOB_ID: 0},
    )
    status.reload()
    assert _check_jobs_submitted(status, "run")


def test_dump_sorted(tmp_path):
    """Test _dump_sorted function"""
    status = Status(tmp_path)
    status.data = {
        "command1": {StatusField.PIPELINE_INDEX: 3},
        "command2": {StatusField.PIPELINE_INDEX: 1},
        "command3": {StatusField.PIPELINE_INDEX: 2},
        "command4": {StatusField.PIPELINE_INDEX: 0},
        "monitor_pid": 12345,
        "another_value": {"hello": "there"},
    }
    vals = [
        v[StatusField.PIPELINE_INDEX]
        for k, v in status.data.items()
        if k.startswith("command")
    ]
    assert vals == [3, 1, 2, 0]
    expected_order = [
        "command1",
        "command2",
        "command3",
        "command4",
        "monitor_pid",
        "another_value",
    ]
    assert list(status) == expected_order

    assert not list(tmp_path.glob("*.json"))
    _dump_sorted(status)
    dumped_files = list(tmp_path.glob("*.json"))
    assert len(dumped_files) == 1

    with open(dumped_files[0]) as file_:
        data = json.load(file_)

    observed_values = [
        v[StatusField.PIPELINE_INDEX]
        for k, v in data.items()
        if k.startswith("command")
    ]
    expected_values = [0, 1, 2, 3]
    assert observed_values == expected_values
    expected_order = [
        "another_value",
        "monitor_pid",
        "command4",
        "command2",
        "command3",
        "command1",
    ]
    assert list(status) == expected_order


def test_parse_code_array():
    """Test the _parse_code_array function."""

    assert _parse_code_array([]) == StatusOption.SUCCESSFUL
    assert (
        _parse_code_array([StatusOption.SUCCESSFUL]) == StatusOption.SUCCESSFUL
    )
    assert (
        _parse_code_array([StatusOption.SUCCESSFUL, StatusOption.SUCCESSFUL])
        == StatusOption.SUCCESSFUL
    )
    assert _parse_code_array([StatusOption.COMPLETE]) == StatusOption.COMPLETE
    assert (
        _parse_code_array([StatusOption.SUCCESSFUL, StatusOption.COMPLETE])
        == StatusOption.COMPLETE
    )
    assert (
        _parse_code_array([StatusOption.SUCCESSFUL, StatusOption.FAILED])
        == StatusOption.FAILED
    )
    assert _parse_code_array([StatusOption.FAILED]) == StatusOption.FAILED
    assert _parse_code_array([StatusOption.RUNNING]) == StatusOption.RUNNING
    assert (
        _parse_code_array([StatusOption.SUCCESSFUL, StatusOption.RUNNING])
        == StatusOption.RUNNING
    )
    assert (
        _parse_code_array([StatusOption.COMPLETE, StatusOption.RUNNING])
        == StatusOption.RUNNING
    )
    assert (
        _parse_code_array([StatusOption.FAILED, StatusOption.RUNNING])
        == StatusOption.RUNNING
    )


def test_parse_previous_status(sample_pipeline_config):
    """Test the `parse_previous_status` function."""
    Pipeline(sample_pipeline_config)

    with pytest.warns(gapsWarning):
        assert not parse_previous_status(sample_pipeline_config.parent, "run")

    assert len(list(sample_pipeline_config.parent.glob("*"))) == 4
    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="run",
        job_name="test_1",
        attrs={
            StatusField.JOB_ID: 0,
            StatusField.JOB_STATUS: StatusOption.SUCCESSFUL,
            StatusField.OUT_FILE: ["test.h5"],
        },
    )
    Status.make_single_job_file(
        sample_pipeline_config.parent,
        command="run",
        job_name="test_2",
        attrs={
            StatusField.JOB_ID: 1,
            StatusField.JOB_STATUS: StatusOption.SUCCESSFUL,
            StatusField.OUT_FILE: ["another_test.h5", "a_third.h5"],
        },
    )
    assert len(list(sample_pipeline_config.parent.glob("*"))) == 6

    out_files = parse_previous_status(
        sample_pipeline_config.parent, "collect-run"
    )
    assert set(out_files) == {"test.h5", "another_test.h5", "a_third.h5"}
    assert len(list(sample_pipeline_config.parent.glob("*"))) == 6
    ids = parse_previous_status(
        sample_pipeline_config.parent, "collect-run", key=StatusField.JOB_ID
    )
    assert set(ids) == {0, 1}

    with pytest.raises(gapsKeyError) as exc_info:
        parse_previous_status(sample_pipeline_config.parent, "DNE")

    assert "Could not parse data for command" in str(exc_info)

    status = Status(sample_pipeline_config.parent)
    status["monitor_id"] = 1234
    status["collect-run"] = {StatusField.PIPELINE_INDEX: 2}
    status.dump()

    assert not parse_previous_status(
        sample_pipeline_config.parent, "collect-run"
    )


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
