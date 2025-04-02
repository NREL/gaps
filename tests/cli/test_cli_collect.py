# -*- coding: utf-8 -*-
# pylint: disable=too-many-locals,unused-argument, unused-variable
# pylint: disable=redefined-outer-name, no-value-for-parameter
"""
GAPs CLI config tests.
"""

import shutil
from pathlib import Path

import h5py
import numpy as np
import pytest

from gaps.cli.collect import collect
from gaps.warn import gapsWarning


@pytest.mark.parametrize(
    "datasets", [None, ["cf_profile", "cf_mean", "lcoe_fcr"]]
)
def test_collect(
    tmp_path,
    datasets,
    collect_pattern,
    points_path,
    manual_collect,
    assert_message_was_logged,
):
    """Test basic collect call."""

    assert collect("test.h5", "test.h5") == "test.h5"
    assert_message_was_logged("No collection performed", "INFO")

    collect_dir, pattern = collect_pattern
    out_file = tmp_path / "cf.h5"
    profiles = manual_collect(collect_dir / pattern, "cf_profile")

    assert not list(tmp_path.glob("*"))
    for file_ind, h5_file in enumerate(collect_dir.glob(pattern)):
        shutil.copy(h5_file, tmp_path / h5_file.name)
        if file_ind == 0:
            shutil.copy(h5_file, out_file)

    files = list(tmp_path.glob("*"))
    assert len(files) == 5
    assert tmp_path / "chunk_files" not in files
    assert out_file in files

    pattern = (tmp_path / pattern).as_posix()
    with pytest.warns(gapsWarning) as warning_info:
        collect(
            out_file,
            pattern,
            project_points=points_path,
            datasets=datasets,
            purge_chunks=True,
        )

    expected_message = "already exists and is being replaced"
    assert expected_message in warning_info[0].message.args[0]

    files = list(tmp_path.glob("*"))
    assert tmp_path / "chunk_files" not in files
    assert len(files) == 1
    assert out_file in files

    with h5py.File(out_file, "r") as collected_outputs:
        assert len(collected_outputs.keys()) == 5
        assert "cf_mean" in collected_outputs
        assert "lcoe_fcr" in collected_outputs
        cf_profiles = collected_outputs["cf_profile"][...]

    assert np.allclose(profiles, cf_profiles)


def test_collect_other_inputs(
    tmp_path, collect_pattern, points_path, manual_collect
):
    """Test basic collect call."""

    collect_dir, pattern = collect_pattern
    out_file = tmp_path / "cf.h5"
    profiles = manual_collect(collect_dir / pattern, "cf_profile")

    assert not list(tmp_path.glob("*"))
    for h5_file in collect_dir.glob(pattern):
        shutil.copy(h5_file, tmp_path / h5_file.name)

    files = list(tmp_path.glob("*"))
    assert len(files) == 4
    assert tmp_path / "chunk_files" not in files
    assert out_file not in files

    pattern = (tmp_path / pattern).as_posix()
    with pytest.warns(gapsWarning) as warning_info:
        collect(
            out_file,
            pattern,
            project_points=points_path,
            datasets=["cf_profile", "dne_dataset"],
        )

    expected_message = (
        "Could not find the following datasets in the output files"
    )
    assert expected_message in warning_info[0].message.args[0]
    assert "dne_dataset" in warning_info[0].message.args[0]

    files = list(tmp_path.glob("*"))
    assert tmp_path / "chunk_files" in files
    assert out_file in files

    with h5py.File(out_file, "r") as collected_outputs:
        assert len(collected_outputs.keys()) == 3
        assert "cf_mean" not in collected_outputs
        assert "lcoe_fcr" not in collected_outputs
        cf_profiles = collected_outputs["cf_profile"][...]

    assert np.allclose(profiles, cf_profiles)


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
