# -*- coding: utf-8 -*-
# pylint: disable=redefined-outer-name
"""
GAPs HPC job managers tests.
"""
import json
from pathlib import Path

import pytest

import gaps.hpc
from gaps.status import Status, StatusField, StatusOption, HardwareOption
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

    for option in [StatusOption.NOT_SUBMITTED, StatusOption.FAILED]:
        Status.make_single_job_file(
            test_ctx.obj["OUT_DIR"],
            test_ctx.obj["COMMAND_NAME"],
            test_ctx.obj["NAME"],
            {StatusField.JOB_STATUS: option},
        )

        assert _should_run(test_ctx)
        assert not caplog.records

    for option in [StatusOption.SUBMITTED, StatusOption.RUNNING]:
        Status.make_single_job_file(
            test_ctx.obj["OUT_DIR"],
            test_ctx.obj["COMMAND_NAME"],
            test_ctx.obj["NAME"],
            {StatusField.JOB_STATUS: option},
        )

        assert not _should_run(test_ctx)
        assert_message_was_logged(test_ctx.obj["NAME"], "INFO")
        assert_message_was_logged(
            "was found with status", "INFO", clear_records=True
        )
        assert not caplog.records

    for option in [StatusOption.SUCCESSFUL, StatusOption.COMPLETE]:
        Status.make_single_job_file(
            test_ctx.obj["OUT_DIR"],
            test_ctx.obj["COMMAND_NAME"],
            test_ctx.obj["NAME"],
            {StatusField.JOB_STATUS: option},
        )

        assert not _should_run(test_ctx)
        assert_message_was_logged(test_ctx.obj["NAME"], "INFO")
        assert_message_was_logged("is successful", "INFO")
        assert_message_was_logged("not re-running", "INFO", clear_records=True)
        assert not caplog.records


def test_kickoff_job_local_basic(test_ctx, assert_message_was_logged):
    """Test kickoff for a basic command for local job."""

    run_dir = test_ctx.obj["TMP_PATH"]
    assert not list(run_dir.glob("*"))

    exec_kwargs = {"option": "local"}
    cmd = "python -c \"print('hello world')\""
    kickoff_job(test_ctx, cmd, exec_kwargs)
    assert_message_was_logged("hello world")

    files = list(run_dir.glob("*"))
    assert len(files) == 1
    status_file = files[0]
    assert status_file.name.endswith(".json")

    with open(status_file, "r") as status_fh:
        status = json.load(status_fh)

    assert StatusField.HARDWARE in status["run"]["test"]
    assert StatusField.QOS not in status["run"]["test"]


def test_kickoff_job_local(test_ctx, assert_message_was_logged):
    """Test kickoff command for local job."""

    exec_kwargs = {"option": "local"}
    cmd = "python -c \"import warnings; warnings.warn('a test warning')\""
    kickoff_job(test_ctx, cmd, exec_kwargs)
    assert_message_was_logged("Running 'run' locally with job name", "INFO")
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


@pytest.mark.parametrize("high_qos", [False, True])
def test_kickoff_job_hpc(
    test_ctx, monkeypatch, assert_message_was_logged, high_qos
):
    """Test kickoff command for HPC job."""

    test_ctx.obj.pop("MANAGER", None)
    run_dir = test_ctx.obj["TMP_PATH"]
    assert not list(run_dir.glob("*"))
    job_name = "_".join([test_ctx.obj["NAME"], str(high_qos)])
    test_ctx.obj["NAME"] = job_name

    exec_kwargs = {
        "option": "eagle",
        "dne_arg": 0,
        "allocation": "test",
        "walltime": 0.43,
        "stdout_path": (test_ctx.obj["TMP_PATH"] / "stdout").as_posix(),
    }
    if high_qos:
        exec_kwargs["qos"] = "high"

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
    test_ctx.obj.pop("MANAGER", None)

    assert len(cmd_cache) >= 1, str(cmd_cache)
    assert_message_was_logged(
        "Found extra keys in 'execution_control'! ", "WARNING"
    )
    assert_message_was_logged("dne_arg", "WARNING")
    assert_message_was_logged(test_ctx.obj["NAME"])
    assert_message_was_logged("Kicked off ")
    assert_message_was_logged("(Job ID #9999)", clear_records=True)
    assert len(list(test_ctx.obj["TMP_PATH"].glob("*"))) == 2

    status_file = list(run_dir.glob("*.json"))
    assert len(status_file) == 1
    status_file = status_file[0]
    assert status_file.name.endswith(".json")

    with open(status_file, "r") as status_fh:
        status = json.load(status_fh)

    assert status["run"][job_name][StatusField.HARDWARE] == "eagle"
    if high_qos:
        assert status["run"][job_name][StatusField.QOS] == "high"
    else:
        assert status["run"][job_name][StatusField.QOS] == "normal"

    exec_kwargs = {"option": "eagle", "allocation": "test", "walltime": 0.43}
    kickoff_job(test_ctx, cmd, exec_kwargs)
    assert_message_was_logged("not resubmitting", "INFO")
    assert len(cmd_cache) == 2

    Status.make_single_job_file(
        run_dir,
        "run",
        job_name,
        {StatusField.JOB_STATUS: StatusOption.RUNNING},
    )
    kickoff_job(test_ctx, cmd, exec_kwargs)
    assert len(cmd_cache) == 2

    # check repeated call does not requeue HPC
    kickoff_job(test_ctx, cmd, exec_kwargs)
    assert len(cmd_cache) == 2

    exec_kwargs = {"option": "eagle", "walltime": 0.43}
    with pytest.raises(gapsConfigError):
        kickoff_job(test_ctx, cmd, exec_kwargs)

    HardwareOption.EAGLE.manager = gaps.hpc.SLURM()
    test_ctx.obj.pop("MANAGER", None)


def test_qos_values(test_ctx, monkeypatch):
    """Test kickoff command for HPC job."""

    test_ctx.obj.pop("MANAGER", None)
    run_dir = test_ctx.obj["TMP_PATH"]
    assert not list(run_dir.glob("*"))

    exec_kwargs = {
        "option": "eagle",
        "dne_arg": 0,
        "allocation": "test",
        "walltime": 0.43,
        "stdout_path": (test_ctx.obj["TMP_PATH"] / "stdout").as_posix(),
        "qos": "dne",
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
    monkeypatch.setattr(
        gaps.hpc.PBS,
        "_job_is_running",
        lambda *__, **___: True,
        raising=True,
    )
    assert not cmd_cache
    assert not list(test_ctx.obj["TMP_PATH"].glob("*"))

    with pytest.raises(gapsConfigError):
        kickoff_job(test_ctx, cmd, exec_kwargs)

    exec_kwargs["option"] = "peregrine"
    kickoff_job(test_ctx, cmd, exec_kwargs)

    status_file = list(run_dir.glob("*.json"))
    assert len(status_file) == 1
    status_file = status_file[0]
    assert status_file.name.endswith(".json")

    with open(status_file, "r") as status_fh:
        status = json.load(status_fh)

    assert status["run"][test_ctx.obj["NAME"]][StatusField.QOS] == "dne"

    HardwareOption.EAGLE.manager = gaps.hpc.SLURM()
    test_ctx.obj.pop("MANAGER", None)


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
