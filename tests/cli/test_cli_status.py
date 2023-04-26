# -*- coding: utf-8 -*-
"""
GAPs status command tests.
"""
from pathlib import Path

import psutil
import pytest

from gaps.cli.status import status_command
from gaps.warnings import gapsWarning


@pytest.mark.parametrize(
    "extra_args",
    [
        [],
        "-i out_dir".split(),
        "-c run".split() + "-c collect-run".split(),
        "-s successful".split()
        + "-s fail".split()
        + "-s r".split()
        + "-s pending".split()
        + "-s u".split()
        + "-s dne".split(),
        "-c run".split()
        + "-c collect-run".split()
        + "-s successful".split()
        + "-s fail".split()
        + "-s r".split()
        + "-s pending".split()
        + "-s u".split()
        + "-s dne".split(),
    ],
)
def test_status(test_data_dir, cli_runner, extra_args, monkeypatch):
    """Test the status command."""

    monkeypatch.setattr(psutil, "pid_exists", lambda *__: True, raising=True)

    status = status_command()
    if "dne" in extra_args:
        with pytest.warns(gapsWarning):
            result = cli_runner.invoke(
                status,
                [(test_data_dir / "test_run").as_posix()] + extra_args,
            )
    else:
        result = cli_runner.invoke(
            status,
            [(test_data_dir / "test_run").as_posix()] + extra_args,
        )
    lines = result.stdout.split("\n")
    cols = [
        "job_status",
        "time_submitted",
        "time_start",
        "time_end",
        "total_runtime",
        "hardware",
        "qos",
    ]
    if "out_dir" in extra_args:
        cols += ["out_dir"]
    cols = " ".join(cols)

    expected_partial_lines = [
        "test_run",
        "MONITOR PID: 1234",
        cols,
        "--",
        "gaps_test_run_j0 successful 0:03:38 local",
        "gaps_test_run_j1 failed 0:01:05 eagle high",
        "gaps_test_run_j2 running (r) local unspecified",
        "gaps_test_run_j3 submitted local",
        "collect-run not submitted",
    ]

    for ind, partial in enumerate(expected_partial_lines):
        assert all(
            string in lines[-15 + ind] for string in partial.split()
        ), partial

    assert lines[-4] == "Total AUs spent: 6"

if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
