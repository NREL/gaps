# -*- coding: utf-8 -*-
# pylint: disable=redefined-outer-name
"""
GAPs HPC job managers tests.
"""
from pathlib import Path

import pytest

import gaps.hpc
from gaps.status import Status, StatusField, StatusOption
from gaps.cli.execution import kickoff_job, _should_run
from gaps.exceptions import gapsConfigError


@pytest.fixture(autouse=True)
def update_ctx(test_ctx):
    """Update context dict to contain out dir and command name"""
    test_ctx.obj["OUT_DIR"] = test_ctx.obj["TMP_PATH"]
    test_ctx.obj["COMMAND_NAME"] = "run"

    yield

    test_ctx.obj.pop("OUT_DIR")
    test_ctx.obj.pop("COMMAND_NAME")


def test_should_run(test_ctx, caplog, assert_message_was_logged):
    """Test the `_should_run` function."""
    assert _should_run(test_ctx)
    assert not caplog.records

    Status.make_single_job_file(
        test_ctx.obj["OUT_DIR"],
        test_ctx.obj["COMMAND_NAME"],
        test_ctx.obj["NAME"],
        {StatusField.JOB_STATUS: StatusOption.FAILED},
    )

    assert _should_run(test_ctx)
    assert not caplog.records

    Status.make_single_job_file(
        test_ctx.obj["OUT_DIR"],
        test_ctx.obj["COMMAND_NAME"],
        test_ctx.obj["NAME"],
        {StatusField.JOB_STATUS: StatusOption.RUNNING},
    )

    assert not _should_run(test_ctx)
    assert_message_was_logged(test_ctx.obj["NAME"], "INFO")
    assert_message_was_logged("was found with status", "INFO")
    assert_message_was_logged("running", "INFO", clear_records=True)
    assert not caplog.records

    Status.make_single_job_file(
        test_ctx.obj["OUT_DIR"],
        test_ctx.obj["COMMAND_NAME"],
        test_ctx.obj["NAME"],
        {StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )

    assert not _should_run(test_ctx)
    assert_message_was_logged(test_ctx.obj["NAME"], "INFO")
    assert_message_was_logged("is successful", "INFO")
    assert_message_was_logged("not re-running", "INFO")


def test_kickoff_job_local_basic(test_ctx, assert_message_was_logged):
    """Test kickoff for a basic command for local job."""

    exec_kwargs = {"option": "local"}
    cmd = "python -c \"print('hello world')\""
    kickoff_job(test_ctx, cmd, exec_kwargs)
    assert_message_was_logged("hello world")


def test_kickoff_job_local(test_ctx, assert_message_was_logged):
    """Test kickoff command for local job."""

    exec_kwargs = {"option": "local"}
    cmd = "python -c \"import warnings; warnings.warn('a test warning')\""
    kickoff_job(test_ctx, cmd, exec_kwargs)
    assert_message_was_logged("Running locally with job name", "INFO")
    assert_message_was_logged(test_ctx.obj["NAME"], "INFO")
    assert_message_was_logged(cmd, "DEBUG")
    assert_message_was_logged("Subprocess received stderr")
    assert_message_was_logged("a test warning")
    assert_message_was_logged("Completed job", "INFO", clear_records=True)

    Status.make_single_job_file(
        test_ctx.obj["OUT_DIR"],
        test_ctx.obj["COMMAND_NAME"],
        test_ctx.obj["NAME"],
        {StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    kickoff_job(test_ctx, cmd, exec_kwargs)
    assert_message_was_logged("not re-running", "INFO")


def test_kickoff_job_hpc(test_ctx, monkeypatch, assert_message_was_logged):
    """Test kickoff command for HPC job."""

    exec_kwargs = {
        "option": "eagle",
        "dne_arg": 0,
        "allocation": "test",
        "walltime": 0.43,
        "stdout_path": (test_ctx.obj["TMP_PATH"] / "stdout").as_posix(),
    }
    cmd = (
        "python -c \"import warnings; print('hello world'); "
        "warnings.warn('a test warning')\""
    )
    cmd_cache = []

    def _test_submit(cmd):
        cmd_cache.append(cmd)
        return "9999", None

    monkeypatch.setattr(gaps.hpc, "submit", _test_submit, raising=True)
    assert not cmd_cache
    assert not list(test_ctx.obj["TMP_PATH"].glob("*"))

    kickoff_job(test_ctx, cmd, exec_kwargs)

    assert cmd_cache
    assert_message_was_logged(
        "Found extra keys in 'execution_control'! ", "WARNING"
    )
    assert_message_was_logged("dne_arg", "WARNING")
    assert_message_was_logged("Running on HPC with job name", "INFO")
    assert_message_was_logged(test_ctx.obj["NAME"], "INFO")
    assert_message_was_logged("Kicked off job")
    assert_message_was_logged("(Job ID #9999)", clear_records=True)
    assert len(list(test_ctx.obj["TMP_PATH"].glob("*"))) == 2

    exec_kwargs = {"option": "eagle", "allocation": "test", "walltime": 0.43}
    kickoff_job(test_ctx, cmd, exec_kwargs)
    assert_message_was_logged("not resubmitting", "INFO")

    exec_kwargs = {"option": "eagle", "walltime": 0.43}
    with pytest.raises(gapsConfigError):
        kickoff_job(test_ctx, cmd, exec_kwargs)


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
