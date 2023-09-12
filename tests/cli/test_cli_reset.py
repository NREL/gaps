# -*- coding: utf-8 -*-
"""
GAPs reset status command tests.
"""
import shutil
from pathlib import Path

import pytest

from gaps.status import Status
from gaps.cli.reset import reset_command


@pytest.mark.parametrize("add_dir", [True, False])
def test_recursive_status(
    tmp_cwd, test_data_dir, cli_runner, add_dir, assert_message_was_logged
):
    """Test the status command for recursive directories."""

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


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
