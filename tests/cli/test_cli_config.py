# -*- coding: utf-8 -*-
# pylint: disable=too-many-locals,unused-argument, unused-variable
# pylint: disable=redefined-outer-name, no-value-for-parameter
"""
GAPs CLI config tests.
"""
import json
from pathlib import Path

import pytest

import gaps.cli.execution
from gaps import ProjectPoints
from gaps.status import Status, StatusField, StatusOption
from gaps.cli.command import CLICommandConfiguration
from gaps.cli.documentation import FunctionDocumentation
from gaps.cli.config import (
    as_script_str,
    from_config,
    run_with_status_updates,
    _validate_config,
    _CMD_LIST,
)
from gaps.exceptions import gapsKeyError
from gaps.warnings import gapsWarning

TEST_FILE_DIR = Path(__file__).parent.as_posix()


def _testing_function(
    project_points, input1, input2, input3, out_dir, tag, max_workers
):
    """Test function to make CLI around."""
    is_pp = isinstance(project_points, ProjectPoints)
    out_fp = Path(out_dir) / f"out{tag}.json"
    out_vals = {
        "is_pp": is_pp,
        "len_pp": len(project_points),
        "input1": input1,
        "input2": input2,
        "input3": input3,
        "max_workers": max_workers,
    }
    with open(out_fp, "w") as out_file:
        json.dump(out_vals, out_file)

    return out_fp.as_posix()


@pytest.fixture
def runnable_script():
    """Written test script now locally runnable."""
    try:
        _CMD_LIST.insert(
            0, f'import sys; sys.path.insert(0, "{TEST_FILE_DIR}")'
        )
        yield
    finally:
        _CMD_LIST.pop(0)


@pytest.mark.parametrize(
    ("extra_input", "none_list"),
    [
        ({"execution_control": {"max_workers": 100}}, None),
        ({"max_workers": 100}, []),
    ],
)
def test_run_local(test_ctx, extra_input, none_list, runnable_script):
    """Test the `run` function locally."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    def pre_processing(config):
        config["input2"] = config.pop("a_value") * config.pop("a_multiplier")
        return config

    command_config = CLICommandConfiguration(
        "run",
        _testing_function,
        split_keys={"project_points", "input3"},
        config_preprocessor=pre_processing,
    )
    config = {
        "input1": 1,
        "a_value": 5,
        "a_multiplier": 2,
        "input2": 7,
        "input3": none_list,
        "project_points": [0, 1, 2],
    }
    config.update(extra_input)
    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    if "max_workers" in extra_input:
        with pytest.warns(gapsWarning):
            from_config(config_fp, command_config)
    else:
        from_config(config_fp, command_config)

    expected_file = tmp_path / "out.json"
    assert expected_file.exists()
    with open(expected_file, "r") as output_file:
        outputs = json.load(output_file)

    expected_log_dir = tmp_path / "logs"
    assert expected_log_dir.exists()

    assert outputs["is_pp"]
    assert outputs["len_pp"] == 3
    assert outputs["input1"] == 1
    assert outputs["input2"] == 10
    assert outputs["input3"] is None
    assert outputs["max_workers"] == 100

    status = Status(tmp_path).update_from_all_job_files()
    assert len(status["run"]) == 1
    job_name = list(status["run"])[0]
    assert job_name == f"{tmp_path.name}_run"
    job_attrs = status["run"][job_name]

    assert job_attrs[StatusField.JOB_STATUS] == StatusOption.SUCCESSFUL
    assert job_attrs[StatusField.OUT_FILE] == expected_file.as_posix()
    assert "project_points" not in job_attrs
    assert "tag" in job_attrs


def test_run_multiple_nodes(test_ctx, runnable_script, monkeypatch):
    """Test the `run` function calls `_kickoff_hpc_job` for multiple nodes."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    command_config = CLICommandConfiguration(
        "run", _testing_function, split_keys={"project_points"}
    )

    config = {
        "execution_control": {
            "option": "eagle",
            "allocation": "test",
            "walltime": 1,
            "nodes": 2,
            "max_workers": 1,
        },
        "input1": 1,
        "input2": 7,
        "input3": None,
        "project_points": [0, 1, 2, 4],
    }
    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    job_names_cache = []

    def _test_kickoff(ctx, cmd, **kwargs):
        job_names_cache.append(ctx.obj["NAME"])

    monkeypatch.setattr(
        gaps.cli.execution, "_kickoff_hpc_job", _test_kickoff, raising=True
    )

    assert len(job_names_cache) == 0
    from_config(config_fp, command_config)
    assert len(job_names_cache) == 2
    assert job_names_cache[0] != job_names_cache[1]


def test_run_local_empty_split_key(test_ctx, runnable_script):
    """Test the `run` function locally with empty split key."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    command_config = CLICommandConfiguration(
        "run",
        _testing_function,
        split_keys={"input3"},
    )
    config = {
        "input1": 1,
        "a_value": 5,
        "a_multiplier": 2,
        "input2": 7,
        "input3": [],
        "max_workers": 100,
        "project_points": "/a/path",
    }
    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    with pytest.warns(gapsWarning):
        from_config(config_fp, command_config)

    expected_file = tmp_path / "out.json"
    assert expected_file.exists()
    with open(expected_file, "r") as output_file:
        outputs = json.load(output_file)

    assert not outputs["is_pp"]
    assert outputs["len_pp"] == 7
    assert outputs["input1"] == 1
    assert outputs["input2"] == 7
    assert outputs["input3"] is None
    assert outputs["max_workers"] == 100


def test_run_local_multiple_out_files(test_ctx, runnable_script):
    """Test the `run` function locally with empty split key."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    command_config = CLICommandConfiguration(
        "run",
        _testing_function,
        split_keys={"input3"},
    )
    config = {
        "input1": 1,
        "input2": 7,
        "input3": ["Hello", "world"],
        "max_workers": 100,
        "project_points": "/a/path",
    }
    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    from_config(config_fp, command_config)

    for out_fn, in3 in zip(["out_j0.json", "out_j1.json"], config["input3"]):
        expected_file = tmp_path / out_fn
        assert expected_file.exists()
        with open(expected_file, "r") as output_file:
            outputs = json.load(output_file)

        assert not outputs["is_pp"]
        assert outputs["len_pp"] == 7
        assert outputs["input1"] == 1
        assert outputs["input2"] == 7
        assert outputs["input3"] == in3
        assert outputs["max_workers"] == 100

    status = Status(tmp_path).update_from_all_job_files()
    assert len(status["run"]) == 2
    for job_name in status["run"]:
        assert f"{tmp_path.name}_run" in job_name
        assert "_j" in job_name


def test_validate_config():
    """Test the `_validate_config` function."""

    def func(input1, input2, input3=None, input4=None):
        pass

    func_doc = FunctionDocumentation(func, skip_params={"input2", "input4"})

    with pytest.raises(gapsKeyError):
        _validate_config({}, func_doc)

    with pytest.warns(gapsWarning) as warning_info:
        _validate_config({"input1": 1, "input3": 3, "dne": -1}, func_doc)

    expected_message = "Found unused keys in the configuration file: {'dne'}"
    assert expected_message in warning_info[0].message.args

    def func2(max_workers):
        pass

    func_doc = FunctionDocumentation(func2)

    with pytest.raises(gapsKeyError):
        _validate_config({"execution_control": {}}, func_doc)

    # ensure no errors are thrown
    _validate_config({"execution_control": {"max_workers": 10}}, func_doc)


def test_as_script_str():
    """Test the `as_script_str` function."""

    assert as_script_str("a") == '"a"'
    assert as_script_str(None) == "None"
    assert as_script_str(True) == "True"
    assert as_script_str(False) == "False"

    input_dict = {"a": None, "b": True, "c": False, "d": 3, "e": [{"t": "hi"}]}
    expected_string = (
        '{"a": None, "b": True, "c": False, "d": 3, "e": [{"t": "hi"}]}'
    )
    assert as_script_str(input_dict) == expected_string


@pytest.mark.parametrize(
    "points",
    [
        {"project_points": ProjectPoints(0)},
        {
            "project_points_split_range": list(
                ProjectPoints([0, 1]).split(sites_per_split=1)
            )[0].split_range
        },
    ],
)
def test_run_with_status_updates(points, tmp_path):
    """Test the running a function with status updates."""

    input_cache = []

    def test_func(project_points, input1, input2, tag):
        assert Status.job_exists(tmp_path, "test", "run")
        input_cache.append((project_points, input1, input2, tag))

    config = {
        "input1": 1,
        "input2": "config_input",
        "project_points": [0, 1, 2],
    }
    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    logging_options = {
        "name": "test",
        "log_directory": None,
        "verbose": True,
        "node": False,
    }
    node_specific_config = {
        "input1": 1,
        "input2": "overwritten",
        "tag": "node0",
        "project_points": [0, 1, 2],
    }
    node_specific_config.update(points)
    node_args = (node_specific_config, logging_options)
    status_update_args = tmp_path, "run", "test"
    exclude = {"project_points", "input1"}

    assert not input_cache
    run_with_status_updates(test_func, node_args, status_update_args, exclude)
    assert len(input_cache) == 1
    project_points, input1, input2, tag = input_cache[0]
    assert len(project_points) == 1
    assert project_points.df.gid.values[0] == 0
    assert input1 == 1
    assert input2 == "overwritten"
    assert tag == "node0"

    status = Status(tmp_path).update_from_all_job_files()
    assert (
        status["run"]["test"][StatusField.JOB_STATUS]
        == StatusOption.SUCCESSFUL
    )

    assert status["run"]["test"][StatusField.OUT_FILE] is None
    assert status["run"]["test"]["input2"] == "overwritten"
    assert all(key not in status["run"]["test"] for key in exclude)


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
