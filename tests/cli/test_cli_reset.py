# -*- coding: utf-8 -*-
"""
GAPs reset status command tests.
"""
import shutil
from pathlib import Path

import pytest

from gaps.status import (
    Status,
    StatusField,
    StatusOption,
    HardwareStatusRetriever,
)
from gaps.cli.reset import reset_command


@pytest.mark.parametrize("add_dir", [True, False])
def test_reset_nominal(
    tmp_cwd, test_data_dir, cli_runner, add_dir, assert_message_was_logged
):
    """Test the reset command with and without directory input."""

    reset = reset_command()

    if add_dir:
        cli_runner.invoke(reset, [tmp_cwd.as_posix()], obj={"VERBOSE": True})
    else:
        cli_runner.invoke(reset, obj={"VERBOSE": True})

    assert_message_was_logged("No status info detected in", "DEBUG")
    assert_message_was_logged(tmp_cwd.name, "DEBUG", clear_records=True)

    assert not list(tmp_cwd.glob("*")), list(tmp_cwd.glob("*"))

    shutil.copytree(
        test_data_dir / "test_run" / Status.HIDDEN_SUB_DIR,
        tmp_cwd / Status.HIDDEN_SUB_DIR,
    )

    assert list(tmp_cwd.glob(Status.HIDDEN_SUB_DIR)), list(tmp_cwd.glob("*"))

    if add_dir:
        cli_runner.invoke(reset, [tmp_cwd.as_posix()], obj={"VERBOSE": True})
    else:
        cli_runner.invoke(reset, obj={"VERBOSE": True})

    assert_message_was_logged("Removing status info for directory", "INFO")
    assert_message_was_logged(tmp_cwd.name, "INFO")
    assert not list(tmp_cwd.glob(Status.HIDDEN_SUB_DIR))


def test_reset_force_option(
    tmp_cwd, test_data_dir, cli_runner, assert_message_was_logged, monkeypatch
):
    """Test the force option for reset command."""

    reset = reset_command()
    shutil.copytree(
        test_data_dir / "test_run" / Status.HIDDEN_SUB_DIR,
        tmp_cwd / Status.HIDDEN_SUB_DIR,
    )

    monkeypatch.setattr(
        HardwareStatusRetriever,
        "__getitem__",
        lambda *__, **___: "running",
        raising=True,
    )

    assert list(tmp_cwd.glob(Status.HIDDEN_SUB_DIR)), list(tmp_cwd.glob("*"))

    cli_runner.invoke(reset, obj={"VERBOSE": True})

    assert_message_was_logged("Found queued/running jobs", "WARNING")
    assert_message_was_logged(tmp_cwd.name, "WARNING")
    assert_message_was_logged("Not resetting..", "WARNING", clear_records=True)
    assert list(tmp_cwd.glob(Status.HIDDEN_SUB_DIR))

    cli_runner.invoke(reset, ["--force"], obj={"VERBOSE": True})
    assert_message_was_logged("Removing status info for directory", "INFO")
    assert_message_was_logged(tmp_cwd.name, "INFO")
    assert not list(tmp_cwd.glob(Status.HIDDEN_SUB_DIR))


def test_reset_keep_thru_option(
    tmp_cwd, test_data_dir, cli_runner, assert_message_was_logged, monkeypatch
):
    """Test the force option for reset command."""

    reset = reset_command()
    shutil.copytree(
        test_data_dir / "test_run" / Status.HIDDEN_SUB_DIR,
        tmp_cwd / Status.HIDDEN_SUB_DIR,
    )

    monkeypatch.setattr(
        HardwareStatusRetriever,
        "__getitem__",
        lambda *__, **___: "running",
        raising=True,
    )

    Status.mark_job_as_submitted(
        tmp_cwd,
        command="collect-run",
        job_name="collect_job_0",
        replace=False,
        job_attrs={StatusField.JOB_STATUS: StatusOption.SUBMITTED},
    )

    Status.mark_job_as_submitted(
        tmp_cwd,
        command="collect-run",
        job_name="collect_job_1",
        replace=False,
        job_attrs={StatusField.JOB_STATUS: StatusOption.SUBMITTED},
    )

    Status.make_single_job_file(
        tmp_cwd,
        command="collect-run",
        job_name="collect_job_0",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    Status.make_single_job_file(
        tmp_cwd,
        command="collect-run",
        job_name="collect_job_1",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )

    status_df = Status(tmp_cwd).as_df()
    collect_status = status_df[status_df.index.str.startswith("collect")]
    assert len(collect_status) == 2
    assert (collect_status.job_status == StatusOption.SUCCESSFUL).all()

    assert list(tmp_cwd.glob(Status.HIDDEN_SUB_DIR))
    assert len(list((tmp_cwd / Status.HIDDEN_SUB_DIR).glob("*"))) > 1
    assert len(list((tmp_cwd / Status.HIDDEN_SUB_DIR).glob("*collect*"))) == 2

    # cli_runner.invoke(reset, obj={"VERBOSE": True})

    # assert_message_was_logged("Found queued/running jobs", "WARNING")
    # assert_message_was_logged(tmp_cwd.name, "WARNING")
    # assert_message_was_logged("Not resetting..", "WARNING", clear_records=True)
    # assert list(tmp_cwd.glob(Status.HIDDEN_SUB_DIR))

    # cli_runner.invoke(reset, ["--force"], obj={"VERBOSE": True})
    # assert_message_was_logged("Removing status info for directory", "INFO")
    # assert_message_was_logged(tmp_cwd.name, "INFO")
    # assert not list(tmp_cwd.glob(Status.HIDDEN_SUB_DIR))


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
