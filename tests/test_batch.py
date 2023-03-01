# -*- coding: utf-8 -*-
# pylint: disable=too-many-locals,too-many-statements,redefined-outer-name
"""
PyTest file for batch jobs.
"""
import os
import json
import time
import shutil
import warnings
from pathlib import Path

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from gaps.batch import (
    BATCH_CSV_FN,
    BatchJob,
    _check_pipeline,
    _check_sets,
    _clean_arg,
    _enumerated_product,
    _load_batch_config,
    _parse_config,
    _source_needs_copying,
    _validate_batch_table,
)
import gaps.pipeline
import gaps.batch
import gaps.cli.pipeline
from gaps.config import ConfigType
from gaps.exceptions import gapsValueError, gapsConfigError


@pytest.fixture
def typical_batch_config(test_data_dir, tmp_path):
    """All batch configs to be used in tests"""

    src_dir = test_data_dir / "batch_project_0"
    shutil.copytree(src_dir, tmp_path / "batch_project_0")
    return tmp_path / "batch_project_0" / "config_batch.json"


@pytest.fixture
def csv_batch_config(test_data_dir, tmp_path):
    """Batch directory with yaml config files."""

    src_dir = test_data_dir / "batch_project_1"
    shutil.copytree(src_dir, tmp_path / "batch_project_1")
    return tmp_path / "batch_project_1" / "config_batch.csv"


@pytest.fixture
def batch_config_with_yaml(test_data_dir, tmp_path):
    """Batch directory with yaml config files."""

    src_dir = test_data_dir / "batch_project_2"
    shutil.copytree(src_dir, tmp_path / "batch_project_2")
    return tmp_path / "batch_project_2" / "config_batch.json"


def test_clean_arg():
    """Test that `_clean_arg` throws for bad str input."""

    with pytest.raises(json.decoder.JSONDecodeError):
        _clean_arg('{"a"}')


def test_source_needs_copying(tmp_path):
    """Test the `_source_needs_copying` function."""
    test_source_file = tmp_path / "test.txt"
    test_destination_file = tmp_path / "test_copy.txt"

    test_source_file.touch()
    assert _source_needs_copying(test_source_file, test_destination_file)

    test_destination_file.touch()
    assert not _source_needs_copying(test_source_file, test_destination_file)

    test_source_file.unlink()
    time.sleep(1)
    test_source_file.touch()
    assert _source_needs_copying(test_source_file, test_destination_file)


def test_enumerated_product():
    """Test `_enumerated_product` function."""

    vals1 = [1, 2, 3]
    vals2 = [1]
    vals3 = list("Hello")

    for inds, prod_vals in _enumerated_product([vals1, vals2, vals3]):
        assert vals1[inds[0]] == prod_vals[0]
        assert vals2[inds[1]] == prod_vals[1]
        assert vals3[inds[2]] == prod_vals[2]


def test_validate_batch_table():
    """Test `_validate_batch_table`"""
    test_table = pd.DataFrame()
    with pytest.raises(gapsConfigError) as exc_info:
        _validate_batch_table(test_table)

    assert 'must have "job" as the first column' in str(exc_info)

    test_table.index.name = "job"
    with pytest.raises(gapsConfigError) as exc_info:
        _validate_batch_table(test_table)

    assert 'must have "set_tag" and "files" columns' in str(exc_info)

    test_table["set_tag"] = ["1", "1"]
    test_table["files"] = ["fp", "fp"]
    with pytest.raises(gapsConfigError) as exc_info:
        _validate_batch_table(test_table)

    assert 'must have completely unique "set_tag"' in str(exc_info)

    test_table["set_tag"] = ["1", "2"]
    test_table["files"] = ["fp1", "fp2"]
    with pytest.raises(gapsConfigError) as exc_info:
        _validate_batch_table(test_table)

    assert 'must have "pipeline_config" columns' in str(exc_info)


def test_check_pipeline():
    """Test `_check_pipeline`"""
    test_config = {}
    with pytest.raises(gapsConfigError) as exc_info:
        _check_pipeline(test_config, Path.home())

    assert 'Batch config needs "pipeline_config" arg!' in str(exc_info)

    test_config["pipeline_config"] = "DNE_file.json"
    with pytest.raises(gapsConfigError) as exc_info:
        _check_pipeline(test_config, Path.home())

    assert "Could not find the pipeline config file" in str(exc_info)


def test_check_sets():
    """Test `_check_sets`"""
    test_config = {}
    with pytest.raises(gapsConfigError) as exc_info:
        _check_sets(test_config, Path.home())

    assert 'Batch config needs "sets" arg!' in str(exc_info)

    test_config["sets"] = [[]]
    with pytest.raises(gapsConfigError) as exc_info:
        _check_sets(test_config, Path.home())

    assert "Batch sets must be dictionaries." in str(exc_info)

    test_config["sets"] = [{}]
    with pytest.raises(gapsConfigError) as exc_info:
        _check_sets(test_config, Path.home())

    assert 'All batch sets must have "args" key.' in str(exc_info)

    test_config["sets"] = [{"args": []}]
    with pytest.raises(gapsConfigError) as exc_info:
        _check_sets(test_config, Path.home())

    assert 'All batch sets must have "files" key.' in str(exc_info)

    test_config["sets"] = [{"args": [], "files": ["DNE_file.json"]}]
    with pytest.raises(gapsConfigError) as exc_info:
        _check_sets(test_config, Path.home())

    assert "Could not find file to modify in batch jobs" in str(exc_info)


def test_parse_config_duplicate_set_tags():
    """Test the `_parse_config` with duplicate "set_tag"."""
    test_config = {
        "sets": [
            {"set_tag": "1", "args": {}, "files": []},
            {"set_tag": "1", "args": {}, "files": []},
        ]
    }
    with pytest.raises(gapsValueError) as exc_info:
        _parse_config(test_config)

    assert "Found multiple sets with the same set_tag" in str(exc_info)


def test_batch_job_setup_with_yaml_files_no_sort(batch_config_with_yaml):
    """Test the creation and deletion of a batch job directory with yaml files,
    and ensure that the output yaml files are NOT sorted."""

    batch_dir = batch_config_with_yaml.parent
    count_0 = len(list(batch_dir.glob("*")))
    assert count_0 == 7, "Unknown starting files detected!"

    BatchJob(batch_config_with_yaml).run(dry_run=True)
    job_dir = batch_dir / "set1_ic10_ic31"
    with open(job_dir / "test.yaml", "r") as test_file:
        key_order = [line.split(":")[0] for line in test_file]

    correct_key_order = [
        "input_constant_1",
        "input_constant_2",
        "another_input_constant",
        "some_equation",
    ]
    e_msg = "Output YAML file does not have correct key order!"
    assert key_order == correct_key_order, e_msg

    BatchJob(batch_config_with_yaml).delete()
    count_1 = len(list(batch_dir.glob("*")))
    assert count_1 == count_0, "Batch did not clear all job files!"


def test_batch_job_setup_with_yaml_files(batch_config_with_yaml):
    """Test the creation and deletion of a batch job directory with yaml files.
    Does not test batch execution which will require slurm."""

    batch_dir = batch_config_with_yaml.parent

    config = ConfigType.JSON.load(batch_config_with_yaml)

    count_0 = len(list(batch_dir.glob("*")))
    assert count_0 == 7, "Unknown starting files detected!"

    BatchJob(batch_config_with_yaml).run(dry_run=True)

    dir_list = set(fp.name for fp in batch_dir.glob("*"))
    set1_count = len([fn for fn in dir_list if fn.startswith("set1_")])
    set2_count = len([fn for fn in dir_list if fn.startswith("ic2")])
    assert set1_count == 6
    assert set2_count == 18

    assert "set1_ic10_ic30" in dir_list
    assert "set1_ic11_ic31" in dir_list
    assert "set1_ic12_ic31" in dir_list
    assert "ic218020_se0_se20" in dir_list
    assert "ic218020_se1_se21" in dir_list
    assert "ic219040_se2_se20" in dir_list
    assert "ic219040_se2_se22" in dir_list
    assert "batch_jobs.csv" in dir_list

    args = config["sets"][0]["args"]
    job_dir = batch_dir / "set1_ic10_ic31"
    test_yaml = ConfigType.YAML.load(job_dir / "test.yaml")
    test_yml = ConfigType.YAML.load(job_dir / "test.yml")
    assert test_yaml["input_constant_1"] == args["input_constant_1"][0]
    assert test_yaml["input_constant_2"] == args["input_constant_2"][0]
    assert test_yml["input_constant_3"] == args["input_constant_3"][1]

    args = config["sets"][1]["args"]
    job_dir = batch_dir / "ic219040_se1_se21"
    test_yaml = ConfigType.YAML.load(job_dir / "test.yaml")
    test_yml = ConfigType.YAML.load(job_dir / "test.yml")
    assert test_yaml["input_constant_2"] == args["input_constant_2"][1]
    assert test_yaml["some_equation"] == args["some_equation"][1]
    assert test_yml["some_equation_2"] == args["some_equation_2"][1]

    count_1 = len(set(batch_dir.glob("*")))
    assert count_1 == 32, "Batch generated unexpected files or directories!"

    BatchJob(batch_config_with_yaml).delete()
    count_2 = len(set(batch_dir.glob("*")))
    assert count_2 == count_0, "Batch did not clear all job files!"


def test_invalid_mod_file_input(batch_config_with_yaml):
    """Test that error is raised for unknown file input type."""

    batch_dir = batch_config_with_yaml.parent

    bad_config_file = batch_dir / "config_batch_bad_fpath.json"
    with pytest.raises(ValueError) as exc_info:
        BatchJob(bad_config_file).run(dry_run=True)

    # cspell: disable-next-line
    assert "'yamlet' is not a valid ConfigType" in str(exc_info.value)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        BatchJob(bad_config_file).delete()


def test_batch_job_setup(typical_batch_config, monkeypatch):
    """Test the creation and deletion of a batch job directory.
    Does not test batch execution which will require slurm."""

    batch_dir = typical_batch_config.parent

    config = ConfigType.JSON.load(typical_batch_config)

    count_0 = len(list(batch_dir.glob("*")))
    assert count_0 == 8, "Unknown starting files detected!"

    BatchJob(typical_batch_config).run(dry_run=True)

    dir_list = set(fp.name for fp in batch_dir.glob("*"))
    set1_count = len([fn for fn in dir_list if fn.startswith("set1_")])
    set2_count = len([fn for fn in dir_list if fn.startswith("set2_")])
    assert set1_count == 6
    assert set2_count == 3

    assert "set1_wthh80_wtpp0" in dir_list  # cspell: disable-line
    assert "set1_wthh110_wtpp1" in dir_list  # cspell: disable-line
    assert "set1_wthh140_wtpp1" in dir_list  # cspell: disable-line
    assert "set2_wthh80" in dir_list  # cspell: disable-line
    assert "set2_wthh110" in dir_list  # cspell: disable-line
    assert "batch_jobs.csv" in dir_list

    args = config["sets"][0]["args"]
    job_dir = batch_dir / "set1_wthh140_wtpp1"  # cspell: disable-line
    config_gen = ConfigType.JSON.load(job_dir / "config_gen.json")
    config_col = ConfigType.JSON.load(job_dir / "config_collect.json")
    turbine_base = ConfigType.JSON.load(batch_dir / "sam_configs/turbine.json")
    turbine = ConfigType.JSON.load(job_dir / "sam_configs/turbine.json")
    assert config_gen["project_points"] == args["project_points"][0]
    assert config_col["project_points"] == args["project_points"][0]
    assert turbine["wind_turbine_hub_ht"] == args["wind_turbine_hub_ht"][2]
    assert (
        turbine["wind_turbine_powercurve_powerout"]  # cspell: disable-line
        == args["wind_turbine_powercurve_powerout"][1]  # cspell: disable-line
    )
    assert (
        turbine["wind_resource_shear"] == turbine_base["wind_resource_shear"]
    )
    assert (
        turbine["wind_resource_turbulence_coeff"]  # cspell: disable-line
        # cspell: disable-next-line
        == turbine_base["wind_resource_turbulence_coeff"]
    )
    assert (
        turbine["wind_turbine_rotor_diameter"]
        == turbine_base["wind_turbine_rotor_diameter"]
    )

    args = config["sets"][1]["args"]
    job_dir = batch_dir / "set2_wthh140"  # cspell: disable-line
    config_gen = ConfigType.JSON.load(job_dir / "config_gen.json")
    config_col = ConfigType.JSON.load(job_dir / "config_collect.json")
    config_agg = ConfigType.JSON.load(job_dir / "config_aggregation.json")
    turbine = ConfigType.JSON.load(job_dir / "sam_configs/turbine.json")
    assert config_gen["project_points"] == args["project_points"][0]
    assert config_gen["resource_file"] == args["resource_file"][0]
    assert config_col["project_points"] == args["project_points"][0]
    assert isinstance(config_agg["data_layers"]["big_brown_bat"], dict)
    assert config_agg["data_layers"]["big_brown_bat"] == json.loads(
        args["big_brown_bat"][0].replace("'", '"')
    )
    assert turbine["wind_turbine_hub_ht"] == args["wind_turbine_hub_ht"][2]
    assert (
        turbine["wind_turbine_powercurve_powerout"]  # cspell: disable-line
        # cspell: disable-next-line
        == turbine_base["wind_turbine_powercurve_powerout"]
    )
    assert (
        turbine["wind_resource_shear"] == turbine_base["wind_resource_shear"]
    )
    assert (
        turbine["wind_resource_turbulence_coeff"]  # cspell: disable-line
        # cspell: disable-next-line
        == turbine_base["wind_resource_turbulence_coeff"]
    )
    assert (
        turbine["wind_turbine_rotor_diameter"]
        == turbine_base["wind_turbine_rotor_diameter"]
    )

    count_1 = len(list(batch_dir.glob("*")))
    assert count_1 == 18, "Batch generated unexpected files or directories!"

    call_cache = []

    def _test_call(*cmd):
        call_cache.append(cmd)

    monkeypatch.setattr(os, "makedirs", _test_call, raising=True)
    monkeypatch.setattr(shutil, "copy", _test_call, raising=True)

    BatchJob(typical_batch_config).run(dry_run=True)
    assert not call_cache

    BatchJob(typical_batch_config).delete()
    count_2 = len(list(batch_dir.glob("*")))
    assert count_2 == count_0, "Batch did not clear all job files!"


def test_batch_job_run(typical_batch_config, monkeypatch):
    """Test a batch job run."""

    batch_dir = typical_batch_config.parent

    count_0 = len(list(batch_dir.glob("*")))
    assert count_0 == 8, "Unknown starting files detected!"

    config_cache = []

    def _test_call(config, monitor, *__, **___):
        assert not monitor
        config_cache.append(config)

    monkeypatch.setattr(
        gaps.pipeline.Pipeline, "run", _test_call, raising=True
    )

    BatchJob(typical_batch_config).run()
    assert len(config_cache) == 9
    assert set(fp.name for fp in config_cache) == {"config_pipeline.json"}
    assert len(set(fp.parent for fp in config_cache)) == 9

    BatchJob(typical_batch_config).delete()
    count_2 = len(list(batch_dir.glob("*")))
    assert count_2 == count_0, "Batch did not clear all job files!"

    monitor_cache = []

    def _test_call(pipeline_config, cancel, monitor, background):
        assert not cancel
        assert monitor
        assert background
        monitor_cache.append(pipeline_config)

    monkeypatch.setattr(
        gaps.cli.pipeline, "pipeline", _test_call, raising=True
    )

    BatchJob(typical_batch_config).run(monitor_background=True)
    assert len(monitor_cache) == 9
    assert set(fp.name for fp in monitor_cache) == {"config_pipeline.json"}
    assert len(set(fp.parent for fp in monitor_cache)) == 9

    cancel_cache = []

    def _test_cancel_call(config, *__, **___):
        cancel_cache.append(config)

    monkeypatch.setattr(
        gaps.pipeline.Pipeline, "cancel_all", _test_cancel_call, raising=True
    )

    # cspell: disable-next-line
    (batch_dir / "set2_wthh110" / "config_pipeline.json").unlink()

    BatchJob(typical_batch_config).cancel()

    assert len(cancel_cache) == 8
    assert set(fp.name for fp in cancel_cache) == {"config_pipeline.json"}
    assert len(set(fp.parent for fp in cancel_cache)) == 8

    monkeypatch.setattr(
        gaps.batch.BatchJob, "_make_job_dirs", lambda *__: None, raising=True
    )

    with pytest.raises(gapsConfigError) as exc_info:
        BatchJob(typical_batch_config).run()

    assert "Could not find pipeline config to run" in str(exc_info)

    BatchJob(typical_batch_config).delete()
    count_2 = len(list(batch_dir.glob("*")))
    assert count_2 == count_0, "Batch did not clear all job files!"

    with pytest.raises(FileNotFoundError) as exc_info:
        BatchJob(typical_batch_config).delete()

    assert "Cannot delete batch jobs without jobs summary table" in str(
        exc_info
    )

    pd.DataFrame().to_csv(batch_dir / BATCH_CSV_FN)

    with pytest.raises(gapsValueError) as exc_info:
        BatchJob(typical_batch_config).delete()

    assert 'batch summary table does not have "job" as the index key' in str(
        exc_info
    )
    (batch_dir / BATCH_CSV_FN).unlink()


def test_batch_csv_config(csv_batch_config):
    """Test the batch job csv parser."""
    table = pd.read_csv(csv_batch_config, index_col=0)
    __, config = _load_batch_config(csv_batch_config)
    assert "logging" in config
    assert "pipeline_config" in config
    assert "sets" in config
    sets = config["sets"]
    assert len(sets) == len(table)
    for _, row in table.iterrows():
        row = row.to_dict()
        set_tag = row["set_tag"]
        found = False
        for job_set in sets:
            if job_set["set_tag"] == set_tag:
                found = True
                for key, val in row.items():
                    if key not in ("set_tag", "files"):
                        assert [val] == job_set["args"][key]
                break

        assert found


def test_batch_csv_setup(csv_batch_config):
    """Test a batch project setup from csv config"""

    batch_dir = csv_batch_config.parent

    config_table = pd.read_csv(csv_batch_config, index_col=0)
    count_0 = len(list(batch_dir.glob("*")))
    assert count_0 == 5, "Unknown starting files detected!"

    BatchJob(csv_batch_config).run(dry_run=True)

    dirs = set(fp.name for fp in batch_dir.glob("*"))
    count_1 = len(dirs)
    assert (count_1 - count_0) == len(config_table) + 1
    for job in config_table.index.values:
        assert job in dirs

    job_table = pd.read_csv(batch_dir / "batch_jobs.csv", index_col=0)
    # pylint: disable=no-member
    compare_cols = set(config_table.columns)
    compare_cols -= {"pipeline_config", "files"}
    compare_cols = list(compare_cols)
    assert_frame_equal(config_table[compare_cols], job_table[compare_cols])

    # test that the dict was input properly
    fp_agg = batch_dir / "blanket_cf0_sd0" / "config_aggregation.json"
    with open(fp_agg, "r") as config_file:
        config_agg = json.load(config_file)
    arg = config_agg["data_layers"]["big_brown_bat"]
    assert isinstance(arg, dict)
    assert arg["dset"] == "big_brown_bat"  # cspell: disable-line
    assert arg["method"] == "sum"

    BatchJob(csv_batch_config).delete()
    count_2 = len(list(batch_dir.glob("*")))
    assert count_2 == count_0, "Batch did not clear all job files!"


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
