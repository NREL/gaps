# -*- coding: utf-8 -*-
# pylint: disable=too-many-locals,unused-argument, unused-variable
# pylint: disable=redefined-outer-name, no-value-for-parameter
"""
GAPs CLI preprocessing tests.
"""
import json
from pathlib import Path

import pytest

from gaps.status import Status, StatusField
from gaps.pipeline import Pipeline
from gaps.cli.preprocessing import (
    preprocess_collect_config,
    split_project_points_into_ranges,
)
from gaps.exceptions import gapsConfigError
from gaps.warnings import gapsWarning

SAMPLE_CONFIG = {
    "logging": {"log_level": "INFO"},
    "pipeline": [
        {"run": "./config.json"},
        {"collect-run": "./collect_config.json"},
    ],
}


def test_preprocess_collect_config(tmp_path):
    """Test `preprocess_collect_config` function"""

    with pytest.raises(gapsConfigError) as exc_info:
        preprocess_collect_config({}, tmp_path, "run")

    assert "Found no files to collect!" in str(exc_info)

    test_pattern = "pattern*.h5"
    (tmp_path / "pattern_sample_job_file.h5").touch()
    dne_pattern = "pattern_dne*.h5"
    expected_out_file = tmp_path / "pattern.h5"

    config = {}
    config["collect_pattern"] = (tmp_path / "." / test_pattern).as_posix()

    config = preprocess_collect_config(config, tmp_path, "run")
    assert config["collect_pattern"] == [
        (expected_out_file.as_posix(), (tmp_path / test_pattern).as_posix())
    ]

    config["collect_pattern"] = [
        (tmp_path / test_pattern).as_posix(),
        (tmp_path / dne_pattern).as_posix(),
    ]

    with pytest.warns(gapsWarning):
        config = preprocess_collect_config(config, tmp_path, "run")

    assert config["collect_pattern"] == [
        (expected_out_file.as_posix(), (tmp_path / test_pattern).as_posix())
    ]


def test_preprocess_collect_config_dict_input(tmp_path):
    """Test `preprocess_collect_config` function with dict input"""

    expected_out_file = tmp_path / "pattern.h5"
    expected_out_file.touch()
    config = {}

    for out_fp in ["pattern.h5", "./pattern.h5", expected_out_file.as_posix()]:
        config["collect_pattern"] = {out_fp: expected_out_file.as_posix()}
        config = preprocess_collect_config(config, tmp_path, "run")
        assert config["collect_pattern"] == [
            tuple([expected_out_file.as_posix()] * 2)
        ]


def test_preprocess_collect_config_pipeline_input(tmp_path):
    """Test `preprocess_collect_config` function with "PIPELINE" input"""
    config_fp = tmp_path / "pipe_config.json"
    with open(config_fp, "w") as file_:
        json.dump(SAMPLE_CONFIG, file_)

    Pipeline(config_fp)

    job_files = [
        tmp_path / "pattern_j0.h5",
        tmp_path / "another_pattern_j1.h5",
    ]
    for ind, job_file in enumerate(job_files):
        job_file.touch()
        Status.make_single_job_file(
            tmp_path,
            command="run",
            job_name=f"test_{ind}",
            attrs={StatusField.OUT_FILE: job_file.as_posix()},
        )

    config = {"collect_pattern": "PIPELINE"}
    config = preprocess_collect_config(config, tmp_path, "collect-run")

    allowed_out_fn = {"pattern.h5", "another_pattern.h5"}
    assert len(config["collect_pattern"]) == 2
    for out_fp, pattern in config["collect_pattern"]:
        assert any(name in out_fp for name in allowed_out_fn)
        assert out_fp == pattern.replace("*", "")


def test_split_project_points_into_ranges():
    """Test the `split_project_points_into_ranges` function."""

    config = {"project_points": [0, 1, 2, 3]}
    config = split_project_points_into_ranges(config)
    assert config["project_points_split_range"] == [(0, 4)]
    config.pop("project_points_split_range")

    config["execution_control"] = {}
    config = split_project_points_into_ranges(config)
    assert config["project_points_split_range"] == [(0, 4)]
    config.pop("project_points_split_range")

    config["execution_control"] = {"option": "local", "nodes": 2}
    config = split_project_points_into_ranges(config)
    assert config["project_points_split_range"] == [(0, 4)]
    config.pop("project_points_split_range")

    config["execution_control"] = {"nodes": 2}
    config = split_project_points_into_ranges(config)
    assert config["project_points_split_range"] == [(0, 2), (2, 4)]
    assert "nodes" not in config["execution_control"]


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
