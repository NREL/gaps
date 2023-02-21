# -*- coding: utf-8 -*-
# pylint: disable=redefined-outer-name
"""Fixtures for use across all tests."""
import os
import sys
from pathlib import Path

import h5py
import numpy as np
import click
import pytest
from click.testing import CliRunner

from gaps import TEST_DATA_DIR
from gaps.collection import find_h5_files


LOGGING_META_FILES = {"log.py", "exceptions.py", "warnings.py"}


@pytest.fixture
def assert_message_was_logged(caplog):
    """Assert that a particular (partial) message was logged."""
    caplog.clear()

    def assert_message(msg, log_level=None, clear_records=False):
        """Assert that a message was logged."""
        assert caplog.records

        for record in caplog.records:
            if msg in record.message:
                break
        else:
            raise AssertionError(f"{msg!r} not found in log records")

        # record guaranteed to be defined b/c of "assert caplog.records"
        # pylint: disable=undefined-loop-variable
        if log_level:
            assert record.levelname == log_level
        if sys.version_info[1] >= 8:  # pragma: no cover
            assert record.filename not in LOGGING_META_FILES
            assert record.funcName != "__init__"
        assert "gaps" in record.name

        if clear_records:
            caplog.clear()

    return assert_message


@pytest.fixture(scope="module")
def cli_runner():
    """Cli runner helper utility."""
    return CliRunner()


@pytest.fixture
def collect_pattern(test_data_dir):
    """Return collect dir path and test collect pattern."""
    collect_dir = test_data_dir / "collect"
    pattern = "peregrine_2012*.h5"
    return collect_dir, pattern


@pytest.fixture
def manual_collect():
    """Manually collect dataset from .h5 files in h5_dir.

    Parameters
    ----------
    collect_pattern : str
        /Filepath/pattern*.h5 to collect
    dataset : str
        Dataset to collect

    Results
    -------
    arr : ndarray
        Collected dataset array
    """

    def _manual_collect(collect_pattern, dataset):
        """Function that manually collects data."""
        h5_files = find_h5_files(collect_pattern)
        arr = []
        for h5_file in h5_files:
            with h5py.File(h5_file, "r") as file_:
                arr.append(file_[dataset][...])

        return np.hstack(arr)

    return _manual_collect


@pytest.fixture
def points_path(test_data_dir):
    """Return path to sample project points file."""
    return test_data_dir / "project_points_100.csv"


@pytest.fixture(autouse=True)
def save_test_dir():
    """Return to the starting dir after running a test.

    In particular, persisting the batch dir change that happens during
    a BatchJob run can mess up downstream tests.
    """
    previous_dir = os.getcwd()
    yield
    os.chdir(previous_dir)


@pytest.fixture
def test_ctx(tmp_path):
    """Test context."""
    with click.Context(click.Command("run"), obj={}) as ctx:
        ctx.obj["NAME"] = "test"
        ctx.obj["TMP_PATH"] = tmp_path
        ctx.obj["VERBOSE"] = False
        yield ctx


@pytest.fixture
def test_data_dir():
    """Return TEST_DATA_DIR as a `Path` object."""
    return Path(TEST_DATA_DIR)


@pytest.fixture
def tmp_cwd(tmp_path):
    """Change working dir to temporary dir."""
    original_directory = os.getcwd()
    try:
        os.chdir(tmp_path)
        yield tmp_path
    finally:
        os.chdir(original_directory)


def pytest_configure(config):
    """Configure tests."""

    config.addinivalue_line(  # cspell:disable-line
        "markers", "integration: mark test and an integration test"
    )
