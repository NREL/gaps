# -*- coding: utf-8 -*-
"""
GAPs status command tests.
"""
import json
import shutil
from pathlib import Path

import psutil
import pytest

from gaps.status import HardwareStatusRetriever, StatusOption
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
    monkeypatch.setattr(
        HardwareStatusRetriever,
        "__getitem__",
        lambda *__, **___: StatusOption.SUBMITTED,
        raising=True,
    )

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
            string in lines[-16 + ind] for string in partial.split()
        ), partial

    assert "Total Runtime" in lines[-6]
    assert "Total project wall time" in lines[-5]
    assert lines[-4] == "Total AUs spent: 6"


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
def test_status_with_hardware_check(
    test_data_dir, cli_runner, extra_args, monkeypatch
):
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
        "gaps_test_run_j2 failed local unspecified",
        "gaps_test_run_j3 failed local",
        "collect-run not submitted",
    ]

    for ind, partial in enumerate(expected_partial_lines):
        assert all(
            string in lines[-16 + ind] for string in partial.split()
        ), partial
        if "failed" in lines[-16 + ind]:
            assert not "(r)" in lines[-16 + ind]

    assert "Total Runtime" in lines[-6]
    assert "Total project wall time" in lines[-5]
    assert lines[-4] == "Total AUs spent: 6"


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
@pytest.mark.parametrize("single_command", [True, False])
def test_failed_run(
    tmp_path,
    test_data_dir,
    cli_runner,
    extra_args,
    monkeypatch,
    single_command,
):
    """Test the status command."""

    monkeypatch.setattr(psutil, "pid_exists", lambda *__: True, raising=True)

    status = status_command()
    if single_command:
        shutil.copytree(
            test_data_dir / "test_failed_run", tmp_path / "test_failed_run"
        )
        run_dir = (tmp_path / "test_failed_run").as_posix()
        pipe_json = Path(run_dir) / "test_failed_run_status.json"
        with open(pipe_json, "r") as config_file:
            config = json.load(config_file)
        config.pop("collect-run")
        with open(pipe_json, "w") as config_file:
            json.dump(config, config_file)
    else:
        run_dir = (test_data_dir / "test_failed_run").as_posix()

    if "dne" in extra_args:
        with pytest.warns(gapsWarning):
            result = cli_runner.invoke(status, [run_dir] + extra_args)
    else:
        result = cli_runner.invoke(status, [run_dir] + extra_args)

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
        "test_failed_run",
        "MONITOR PID: 1234",
        cols,
        "--",
        "gaps_test_failed_run_j0 failed local",
        "gaps_test_failed_run_j1 failed local high",
        "gaps_test_failed_run_j2 failed local unspecified",
    ]
    if single_command:
        start_ind = -12
    else:
        expected_partial_lines += ["collect-run not submitted"]
        start_ind = -14

    for ind, partial in enumerate(expected_partial_lines):
        assert all(
            string in lines[start_ind + ind] for string in partial.split()
        ), partial
        assert not "(r)" in lines[start_ind + ind]
        assert "Total AUs spent" not in lines[start_ind + ind]
        if single_command:
            assert "Total project wall time" not in lines[start_ind + 10]

    if single_command:
        assert "Total Runtime: 0:00:00" in lines[-4]
    else:
        assert "Total Runtime: 0:00:00" in lines[-5]
        assert "Total project wall time" in lines[-4]


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
