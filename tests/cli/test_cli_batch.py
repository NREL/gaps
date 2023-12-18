# -*- coding: utf-8 -*-
# pylint: disable=unused-argument,redefined-outer-name,invalid-name
"""
GAPs batch command tests.
"""
import json
import shutil
from pathlib import Path

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from gaps.batch import BatchJob
from gaps.cli.batch import batch_command


TEST_CONFIG = {
    "pipeline_config": "./config_batch.json",
    "sets": [
        {"set_tag": "1", "args": {}, "files": []},
        {"set_tag": "2", "args": {}, "files": []},
    ],
}


def test_batch_command_cancel_delete(tmp_path, cli_runner, monkeypatch):
    """Test the batch_command cancel+delete subcommands."""

    batch_config_fp = tmp_path / "config_batch.json"
    with open(batch_config_fp, "w") as config_file:
        json.dump(TEST_CONFIG, config_file)
    batch_config_fp = batch_config_fp.as_posix()

    call_cache = []

    def _cache_fp_cancel(*__, **___):
        call_cache.append("Called")

    def _cache_fp_delete(*__, **___):
        call_cache.append("Called")

    monkeypatch.setattr(BatchJob, "cancel", _cache_fp_cancel, raising=True)
    monkeypatch.setattr(BatchJob, "delete", _cache_fp_delete, raising=True)

    bc = batch_command()
    assert not call_cache
    cli_runner.invoke(bc, ["-c", batch_config_fp, "--cancel"])
    assert len(call_cache) == 1

    cli_runner.invoke(bc, ["-c", batch_config_fp, "--delete"])
    assert len(call_cache) == 2


def test_batch_command_run(tmp_path, cli_runner, monkeypatch):
    """Test the batch_command run subcommand."""

    batch_config_fp = tmp_path / "config_batch.json"
    with open(batch_config_fp, "w") as config_file:
        json.dump(TEST_CONFIG, config_file)
    batch_config_fp = batch_config_fp.as_posix()

    arg_cache = []

    def _cache_args_kwargs(self, dry_run, monitor_background):
        arg_cache.append((dry_run, monitor_background))

    monkeypatch.setattr(BatchJob, "run", _cache_args_kwargs, raising=True)

    bc = batch_command()
    assert not arg_cache
    cli_runner.invoke(bc, ["-c", batch_config_fp, "--dry"])
    assert len(arg_cache) == 1
    assert arg_cache[-1][0]
    assert not arg_cache[-1][1]

    cli_runner.invoke(bc, ["-c", batch_config_fp, "--monitor-background"])
    assert len(arg_cache) == 2
    assert not arg_cache[-1][0]
    assert arg_cache[-1][1]


# pylint: disable=too-many-locals
def test_batch_csv(test_data_dir, tmp_path, cli_runner):
    """Test a batch project setup from csv config"""

    src_dir = test_data_dir / "batch_project_1"
    batch_dir = tmp_path / "batch_project_1"
    shutil.copytree(src_dir, batch_dir)
    csv_batch_config = batch_dir / "config_batch.csv"

    config_table = pd.read_csv(csv_batch_config)
    count_0 = len(list(batch_dir.glob("*")))
    assert count_0 == 5, "Unknown starting files detected!"

    bc = batch_command()
    cli_runner.invoke(bc, ["-c", csv_batch_config, "--dry"])

    dirs = set(fp.name for fp in batch_dir.glob("*"))
    count_1 = len(dirs)
    assert (count_1 - count_0) == len(config_table) + 1

    job_table = pd.read_csv(batch_dir / "batch_jobs.csv", index_col=0)
    for job in job_table.index.values:
        assert job in dirs

    job_table.index.name = "index"
    compare_cols = set(config_table.columns)
    compare_cols -= {"pipeline_config", "files"}
    compare_cols = list(compare_cols)
    assert_frame_equal(
        config_table[compare_cols].reset_index(drop=True),
        job_table[compare_cols].reset_index(drop=True),
    )

    # test that the dict was input properly
    fp_agg = batch_dir / "blanket_cf0_sd0" / "config_aggregation.json"
    with open(fp_agg, "r") as config_file:
        config_agg = json.load(config_file)
    arg = config_agg["data_layers"]["big_brown_bat"]
    assert isinstance(arg, dict)
    assert arg["dset"] == "big_brown_bat"  # cspell: disable-line
    assert arg["method"] == "sum"

    cli_runner.invoke(bc, ["-c", csv_batch_config, "--delete"])
    count_2 = len(list(batch_dir.glob("*")))
    assert count_2 == count_0, "Batch did not clear all job files!"


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
