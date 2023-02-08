# -*- coding: utf-8 -*-
# pylint: disable=unused-argument,redefined-outer-name,invalid-name
"""
GAPs batch command tests.
"""
import json
from pathlib import Path

import pytest

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
    cli_runner.invoke(bc, ["-c", batch_config_fp, "-dry"])
    assert len(arg_cache) == 1
    assert arg_cache[-1][0]
    assert not arg_cache[-1][1]

    cli_runner.invoke(bc, ["-c", batch_config_fp, "--monitor-background"])
    assert len(arg_cache) == 2
    assert not arg_cache[-1][0]
    assert arg_cache[-1][1]


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
