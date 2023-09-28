# -*- coding: utf-8 -*-
"""
GAPs reset status command tests.
"""
from pathlib import Path
from copy import deepcopy

import pytest

from gaps.status import (
    Status,
    StatusField,
    StatusOption,
    HardwareStatusRetriever,
)
from gaps.cli.reset import reset_command


def test_reset_no_status(tmp_cwd, cli_runner, assert_message_was_logged):
    """Test reset command for dir with no status."""
    reset = reset_command()
    cli_runner.invoke(reset, obj={"VERBOSE": True})
    assert_message_was_logged("No status info detected in", "DEBUG")
    assert_message_was_logged(tmp_cwd.name, "DEBUG", clear_records=True)
    assert not list(tmp_cwd.glob("*"))


@pytest.mark.parametrize("add_dir", [True, False])
def test_reset_nominal(
    temp_status_dir, cli_runner, add_dir, assert_message_was_logged
):
    """Test the reset command with and without directory input."""

    reset = reset_command()
    assert list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))

    if add_dir:
        cli_runner.invoke(
            reset, [temp_status_dir.as_posix()], obj={"VERBOSE": True}
        )
    else:
        cli_runner.invoke(reset, obj={"VERBOSE": True})

    assert_message_was_logged("Removing status info for directory", "INFO")
    assert_message_was_logged(temp_status_dir.name, "INFO")
    assert not list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))


def test_reset_force_option(
    temp_status_dir, cli_runner, assert_message_was_logged, monkeypatch
):
    """Test the force option for reset command."""

    reset = reset_command()

    monkeypatch.setattr(
        HardwareStatusRetriever,
        "__getitem__",
        lambda *__, **___: "running",
        raising=True,
    )

    assert list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))

    cli_runner.invoke(reset, obj={"VERBOSE": True})

    assert_message_was_logged("Found queued/running jobs", "WARNING")
    assert_message_was_logged(temp_status_dir.name, "WARNING")
    assert_message_was_logged("Not resetting..", "WARNING", clear_records=True)
    assert list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))

    cli_runner.invoke(reset, ["--force"], obj={"VERBOSE": True})
    assert_message_was_logged("Removing status info for directory", "INFO")
    assert_message_was_logged(temp_status_dir.name, "INFO")
    assert not list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))


def test_reset_keep_thru_option(
    temp_status_dir, cli_runner, assert_message_was_logged, monkeypatch
):
    """Test the force option for reset command."""

    reset = reset_command()
    monkeypatch.setattr(
        HardwareStatusRetriever,
        "__getitem__",
        lambda *__, **___: "running",
        raising=True,
    )

    Status.mark_job_as_submitted(
        temp_status_dir,
        pipeline_step="collect-run",
        job_name="collect_job_0",
        replace=False,
        job_attrs={StatusField.JOB_STATUS: StatusOption.SUBMITTED},
    )

    Status.mark_job_as_submitted(
        temp_status_dir,
        pipeline_step="collect-run",
        job_name="collect_job_1",
        replace=False,
        job_attrs={StatusField.JOB_STATUS: StatusOption.SUBMITTED},
    )

    assert list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))
    assert len(list((temp_status_dir / Status.HIDDEN_SUB_DIR).glob("*"))) > 1

    cli_runner.invoke(reset, ["-f", "-a", "DNE"], obj={"VERBOSE": True})

    assert_message_was_logged("not found as part of pipeline")
    assert_message_was_logged("DNE", "WARNING")
    assert_message_was_logged("Not resetting..", "WARNING", clear_records=True)

    assert list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))
    assert len(list((temp_status_dir / Status.HIDDEN_SUB_DIR).glob("*"))) > 1

    cli_runner.invoke(reset, ["-a", "run"], obj={"VERBOSE": True})

    assert_message_was_logged("Found queued/running jobs", "WARNING")
    assert_message_was_logged(temp_status_dir.name, "WARNING")
    assert_message_was_logged("Not resetting..", "WARNING", clear_records=True)

    assert list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))
    assert len(list((temp_status_dir / Status.HIDDEN_SUB_DIR).glob("*"))) > 1

    Status.make_single_job_file(
        temp_status_dir,
        pipeline_step="collect-run",
        job_name="collect_job_0",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    Status.make_single_job_file(
        temp_status_dir,
        pipeline_step="collect-run",
        job_name="collect_job_1",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    assert (
        len(list((temp_status_dir / Status.HIDDEN_SUB_DIR).glob("*collect*")))
        == 2
    )

    status = Status(temp_status_dir)
    status.update_from_all_job_files(purge=False)
    status_df = status.as_df()
    collect_status = status_df[status_df.index.str.startswith("collect")]
    assert len(collect_status) == 2
    assert (collect_status.job_status == StatusOption.SUCCESSFUL).all()
    assert list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))
    assert len(list((temp_status_dir / Status.HIDDEN_SUB_DIR).glob("*"))) > 1
    assert (
        len(list((temp_status_dir / Status.HIDDEN_SUB_DIR).glob("*collect*")))
        == 2
    )

    original_status = deepcopy(status.data)

    cli_runner.invoke(
        reset, ["-f", "-a", "collect-run"], obj={"VERBOSE": True}
    )

    assert list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))
    assert len(list((temp_status_dir / Status.HIDDEN_SUB_DIR).glob("*"))) == 1
    assert (
        len(list((temp_status_dir / Status.HIDDEN_SUB_DIR).glob("*collect*")))
        == 0
    )

    status = Status(temp_status_dir)
    assert status.data == original_status

    cli_runner.invoke(reset, ["-f", "-a", "run"], obj={"VERBOSE": True})

    assert_message_was_logged("Resetting status for all steps after", "INFO")
    assert_message_was_logged("run", "INFO")
    assert list(temp_status_dir.glob(Status.HIDDEN_SUB_DIR))
    assert len(list((temp_status_dir / Status.HIDDEN_SUB_DIR).glob("*"))) == 1
    assert (
        len(list((temp_status_dir / Status.HIDDEN_SUB_DIR).glob("*collect*")))
        == 0
    )

    original_status["collect-run"] = {StatusField.PIPELINE_INDEX: 1}
    status = Status(temp_status_dir)
    assert status.data == original_status


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
