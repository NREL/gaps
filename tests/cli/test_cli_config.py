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
from gaps.cli.command import (
    CLICommandFromClass,
    CLICommandFromFunction,
)
from gaps.cli.documentation import CommandDocumentation
from gaps.cli.config import (
    TAG,
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
    project_points,
    input1,
    input3,
    tag,
    command_name,
    config_file,
    project_dir,
    job_name,
    out_dir,
    out_fpath,
    max_workers,
    pool_size=16,
    _input2=None,
    _z_0=None,
):
    """Test function to make CLI around.

    Parameters
    ----------
    project_points : path-like
        Path to project points.
    input1 : int
        Input 1.
    input3 : str
        Input 3.
    tag : str
        Internal GAPs tag.
    command_name : str
        Internal GAPs command name.
    config_file : str
        Internal GAPs path to config file.
    project_dir : str
        Internal GAPs Path to project dir.
    job_name : str
        Internal GAPs job name.
    out_dir : str
        Internal GAPs path to out dir.
    out_fpath : str
        Internal GAPs out filepath.
    max_workers : int
        Max workers.
    pool_size : int, optional
            Pool size. By default, ``16``.
    _input2 : float, optional
        Secret input 2. By default, ``None``.
    _z_0 : str, optional
        Secret str. By default, ``None``.
    """
    is_pp = isinstance(project_points, ProjectPoints)
    out_fp = Path(out_dir) / f"out{tag}.json"
    out_vals = {
        "is_pp": is_pp,
        "len_pp": len(project_points),
        "input1": input1,
        "_input2": _input2,
        "input3": input3,
        "max_workers": max_workers,
        "pool_size": pool_size,
        "_z_0": _z_0,
        "out_fpath": out_fpath,
        "out_dir": out_dir,
        "tag": tag,
        "command_name": command_name,
        "config_file": config_file,
        "project_dir": project_dir,
        "job_name": job_name,
    }
    with open(out_fp, "w") as out_file:
        json.dump(out_vals, out_file)

    return out_fp.as_posix()


def _testing_function_no_pp(
    input1,
    input3,
    tag,
    command_name,
    config_file,
    project_dir,
    job_name,
    out_dir,
    out_fpath,
    max_workers,
    pool_size=16,
    _input2=None,
    _z_0=None,
):
    """Test function to make CLI around.

    Parameters
    ----------
    input1 : int
        Input 1.
    input3 : str
        Input 3.
    tag : str
        Internal GAPs tag.
    command_name : str
        Internal GAPs command name.
    config_file : str
        Internal GAPs path to config file.
    project_dir : str
        Internal GAPs Path to project dir.
    job_name : str
        Internal GAPs job name.
    out_dir : str
        Internal GAPs path to out dir.
    out_fpath : str
        Internal GAPs out filepath.
    max_workers : int
        Max workers.
    pool_size : int, optional
            Pool size. By default, ``16``.
    _input2 : float, optional
        Secret input 2. By default, ``None``.
    _z_0 : str, optional
        Secret str. By default, ``None``.
    """
    out_fp = Path(out_dir) / f"out{tag}.json"
    out_vals = {
        "input1": input1,
        "_input2": _input2,
        "input3": input3,
        "max_workers": max_workers,
        "pool_size": pool_size,
        "_z_0": _z_0,
        "out_fpath": out_fpath,
        "out_dir": out_dir,
        "tag": tag,
        "command_name": command_name,
        "config_file": config_file,
        "project_dir": project_dir,
        "job_name": job_name,
    }
    with open(out_fp, "w") as out_file:
        json.dump(out_vals, out_file)

    return out_fp.as_posix()


class TestCommand:
    """Test command class."""

    def __init__(self, input1, input3, _input2=None):
        """est function to make CLI around.

        Parameters
        ----------
        input1 : int
            Input 1.
        input3 : str
            Input 3.
        _input2 : float, optional
            Secret input 2. By default, ``None``.
        """
        self._input1 = input1
        self._input2 = _input2
        self._input3 = input3

    def run(
        self,
        project_points,
        tag,
        command_name,
        config_file,
        project_dir,
        job_name,
        out_dir,
        out_fpath,
        max_workers,
        pool_size=16,
        _z_0=None,
    ):
        """Test run function for CLI around.

        Parameters
        ----------
        project_points : path-like
            Path to project points.
        tag : str
            Internal GAPs tag.
        command_name : str
            Internal GAPs command name.
        config_file : str
            Internal GAPs path to config file.
        project_dir : str
            Internal GAPs Path to project dir.
        job_name : str
            Internal GAPs job name.
        out_dir : str
            Internal GAPs path to out dir.
        out_fpath : str
            Internal GAPs out filepath.
        max_workers : int
            Max workers.
        pool_size : int, optional
            Pool size. By default, ``16``.
        _z_0 : str, optional
            Secret str. By default, ``None``.
        """
        is_pp = isinstance(project_points, ProjectPoints)
        out_fp = Path(out_dir) / f"out{tag}.json"
        out_vals = {
            "is_pp": is_pp,
            "len_pp": len(project_points),
            "input1": self._input1,
            "_input2": self._input2,
            "input3": self._input3,
            "max_workers": max_workers,
            "pool_size": pool_size,
            "_z_0": _z_0,
            "out_fpath": out_fpath,
            "out_dir": out_dir,
            "tag": tag,
            "command_name": command_name,
            "config_file": config_file,
            "project_dir": project_dir,
            "job_name": job_name,
        }
        with open(out_fp, "w") as out_file:
            json.dump(out_vals, out_file)

        return out_fp.as_posix()

    def run_no_pp(
        self,
        tag,
        command_name,
        config_file,
        project_dir,
        job_name,
        out_dir,
        out_fpath,
        max_workers,
        pool_size=16,
        _z_0=None,
    ):
        """Test run function for CLI around.

        Parameters
        ----------
        tag : str
            Internal GAPs tag.
        command_name : str
            Internal GAPs command name.
        config_file : str
            Internal GAPs path to config file.
        project_dir : str
            Internal GAPs Path to project dir.
        job_name : str
            Internal GAPs job name.
        out_dir : str
            Internal GAPs path to out dir.
        out_fpath : str
            Internal GAPs out filepath.
        max_workers : int
            Max workers.
        pool_size : int, optional
            Pool size. By default, ``16``.
        _z_0 : str, optional
            Secret str. By default, ``None``.
        """
        out_fp = Path(out_dir) / f"out{tag}.json"
        out_vals = {
            "input1": self._input1,
            "_input2": self._input2,
            "input3": self._input3,
            "max_workers": max_workers,
            "pool_size": pool_size,
            "_z_0": _z_0,
            "out_fpath": out_fpath,
            "out_dir": out_dir,
            "tag": tag,
            "command_name": command_name,
            "config_file": config_file,
            "project_dir": project_dir,
            "job_name": job_name,
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


@pytest.fixture
def job_names_cache(monkeypatch):
    """Monkeypatch `_kickoff_hpc_job` and cache call"""
    cache = {}

    def _test_kickoff(ctx, cmd, __, **kwargs):
        cache[ctx.obj["NAME"]] = cmd

    monkeypatch.setattr(
        gaps.cli.execution, "_kickoff_hpc_job", _test_kickoff, raising=True
    )
    return cache


@pytest.mark.parametrize(
    ("extra_input", "none_list"),
    [
        ({"execution_control": {"max_workers": 100}}, None),
        ({"max_workers": 100}, []),
    ],
)
@pytest.mark.parametrize("test_class", [False, True])
def test_run_local(
    test_ctx, extra_input, none_list, runnable_script, test_class, caplog
):
    """Test the `run` function locally."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    def pre_processing(config, a_value, a_multiplier):
        config["_input2"] = a_value * a_multiplier
        return config

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            split_keys={"project_points", "input3"},
            config_preprocessor=pre_processing,
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys={"project_points", "input3"},
            config_preprocessor=pre_processing,
        )
    config = {
        "input1": 1,
        "a_value": 5,
        "a_multiplier": 2,
        "input2": 7,
        "_input2": 8,
        "input3": none_list,
        "project_points": [0, 1, 2],
    }
    config.update(extra_input)
    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    with pytest.warns(gapsWarning) as warning_info:
        from_config(config_fp, command_config)

    expected_message = "Found unused keys in the configuration file"
    assert expected_message in warning_info[0].message.args[0]
    assert "input2" in warning_info[0].message.args[0]
    assert "_input2" in warning_info[0].message.args[0]

    expected_log_starts = [
        "Running run from config file: '",
        "Target output directory: '",
        "Target logging directory: '",
    ]
    for expected in expected_log_starts:
        assert any(expected in record.message for record in caplog.records)
    assert not any("Path(" in record.message for record in caplog.records)

    if "max_workers" in extra_input:
        expected_message = (
            "Found key(s) {'max_workers'} outside of 'execution_control'. "
            "Moving these keys into 'execution_control' block."
        )
        assert expected_message in warning_info[1].message.args[0]

    expected_file = tmp_path / "out.json"
    assert expected_file.exists()
    with open(expected_file, "r") as output_file:
        outputs = json.load(output_file)

    expected_log_dir = tmp_path / "logs"
    assert expected_log_dir.exists()

    assert outputs["is_pp"]
    assert outputs["len_pp"] == 3
    assert outputs["input1"] == 1
    assert outputs["_input2"] == 10
    assert outputs["input3"] is None
    assert outputs["max_workers"] == 100
    assert outputs["pool_size"] == 16

    status = Status(tmp_path).update_from_all_job_files()
    assert len(status["run"]) == 1
    job_name = list(status["run"])[0]
    assert job_name == f"{tmp_path.name}_run"
    job_attrs = status["run"][job_name]

    assert job_attrs[StatusField.JOB_STATUS] == StatusOption.SUCCESSFUL
    assert job_attrs[StatusField.OUT_FILE] == expected_file.as_posix()
    assert "project_points" not in job_attrs
    assert "tag" in job_attrs

    assert outputs["out_fpath"] == (tmp_path / job_name).as_posix()
    assert outputs["out_dir"] == tmp_path.as_posix()
    assert outputs["project_dir"] == tmp_path.as_posix()
    assert outputs["config_file"] == config_fp.as_posix()
    assert isinstance(outputs["tag"], str)
    assert outputs["command_name"] == "run"
    assert outputs["job_name"] == f"{tmp_path.name}_run{outputs['tag']}"


@pytest.mark.parametrize(
    "option", ["eagle", "EAGLE", "Eagle", "EaGlE", "kestrel", "KESTREL"]
)
@pytest.mark.parametrize("test_class", [False, True])
def test_run_multiple_nodes(
    test_ctx, runnable_script, test_class, option, job_names_cache
):
    """Test the `run` function calls `_kickoff_hpc_job` for multiple nodes."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            name="run",
            split_keys={"project_points", "_z_0"},
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys={"project_points", "_z_0"},
        )

    config = {
        "execution_control": {
            "option": option,
            "allocation": "test",
            "walltime": 1,
            "nodes": 2,
            "max_workers": 1,
        },
        "input1": 1,
        "input2": 7,
        "input3": 8,
        "_z_0": ["unsorted", "strings"],
        "project_points": [0, 1, 2, 4],
    }

    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    assert len(job_names_cache) == 0
    from_config(config_fp, command_config)
    assert len(job_names_cache) == 4
    assert len(set(job_names_cache)) == 4

    for job_name, script in job_names_cache.items():
        if f"{TAG}0" in job_name or f"{TAG}1" in job_name:
            assert '"_z_0": "strings"' in script
        elif f"{TAG}2" in job_name or f"{TAG}3" in job_name:
            assert '"_z_0": "unsorted"' in script
        else:
            raise ValueError(
                f"Could not find expected tag in job name: {job_name!r}"
            )


@pytest.mark.parametrize("test_class", [False, True])
def test_run_multiple_nodes_correct_zfill(
    test_ctx, runnable_script, test_class, job_names_cache
):
    """Test the `run` function calls `_kickoff_hpc_job` for multiple nodes."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            name="run",
            split_keys={"project_points", "_z_0"},
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys={"project_points", "_z_0"},
        )

    config = {
        "execution_control": {
            "option": "eagle",
            "allocation": "test",
            "walltime": 1,
            "nodes": 5,
            "max_workers": 1,
        },
        "input1": 1,
        "input2": 7,
        "input3": 8,
        "_z_0": ["unsorted", "strings"],
        "project_points": [0, 1, 2, 4, 5, 6, 7, 8, 9],
    }

    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    assert len(job_names_cache) == 0
    from_config(config_fp, command_config)
    assert len(job_names_cache) == 10
    assert len(set(job_names_cache)) == 10

    assert not any("j00" in job_name for job_name in job_names_cache)
    assert any("j0" in job_name for job_name in job_names_cache)


@pytest.mark.parametrize("test_class", [False, True])
def test_run_no_split_keys(
    test_ctx, runnable_script, test_class, job_names_cache
):
    """Test the `run` function with no split keys specified."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            name="run",
            split_keys=None,
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys=None,
        )

    config = {
        "execution_control": {
            "option": "eagle",
            "allocation": "test",
            "walltime": 1,
            "max_workers": 1,
        },
        "input1": 1,
        "input2": 7,
        "input3": 8,
        "_z_0": ["unsorted", "strings"],
        "project_points": [0, 1, 2, 4],
    }

    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    assert len(job_names_cache) == 0
    from_config(config_fp, command_config)
    assert len(job_names_cache) == 1
    assert len(set(job_names_cache)) == 1

    for job_name, script in job_names_cache.items():
        assert f"{TAG}0" not in job_name
        assert "[0, 1, 2, 4]" in script
        assert '["unsorted", "strings"]' in script


@pytest.mark.parametrize("test_class", [False, True])
def test_run_single_node_out_fpath(
    test_ctx, runnable_script, test_class, job_names_cache
):
    """Test the `run` function with no split keys specified."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            name="run",
            split_keys={"project_points", "_z_0"},
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys={"project_points", "_z_0"},
        )

    config = {
        "execution_control": {
            "option": "eagle",
            "allocation": "test",
            "walltime": 1,
            "nodes": 1,
            "max_workers": 1,
        },
        "input1": 1,
        "input2": 7,
        "input3": 8,
        "_z_0": ["unsorted", "strings"],
        "project_points": [0, 1, 2, 4],
    }

    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    assert len(job_names_cache) == 0
    from_config(config_fp, command_config)
    assert len(job_names_cache) == 2
    assert len(set(job_names_cache)) == 2

    for job_name, script in job_names_cache.items():
        for substr in script.split(","):
            if '"out_fpath"' not in substr:
                continue
            fn = Path(substr.split(":")[-1].strip()).name
            assert f"{TAG}" not in fn
            assert f"{TAG}" in job_name


@pytest.mark.parametrize("test_class", [False, True])
def test_run_split_key_only(
    test_ctx, runnable_script, test_class, job_names_cache
):
    """Test the `run` function with no split keys specified."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run_no_pp",
            name="run",
            split_keys={"_z_0"},
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function_no_pp,
            name="run",
            split_keys={"_z_0"},
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
        "input3": 8,
        "_z_0": ["unsorted", "strings"],
    }

    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    assert len(job_names_cache) == 0
    from_config(config_fp, command_config)
    assert len(job_names_cache) == 2
    assert len(set(job_names_cache)) == 2

    for job_name, script in job_names_cache.items():
        for substr in script.split(","):
            if '"out_fpath"' not in substr:
                continue
            fn = Path(substr.split(":")[-1].strip()).name
            assert f"{TAG}" not in fn
            assert f"{TAG}" in job_name


@pytest.mark.parametrize("test_class", [False, True])
def test_run_empty_split_keys(
    test_ctx, runnable_script, test_class, job_names_cache
):
    """Test the `run` function with empty split keys input."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            name="run",
            split_keys=["_z_0"],
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys=["_z_0"],
        )

    config = {
        "execution_control": {
            "option": "eagle",
            "allocation": "test",
            "walltime": 1,
            "max_workers": 1,
        },
        "input1": 1,
        "input2": 7,
        "input3": 8,
        "_z_0": [],
        "project_points": [0, 1, 2, 4],
    }

    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    assert len(job_names_cache) == 0
    from_config(config_fp, command_config)
    assert len(job_names_cache) == 1
    assert len(set(job_names_cache)) == 1

    for job_name, script in job_names_cache.items():
        assert f"{TAG}0" not in job_name
        assert "[0, 1, 2, 4]" in script
        assert '"_z_0": None' in script


@pytest.mark.parametrize("test_class", [False, True])
@pytest.mark.parametrize("num_nodes", [30, 100])
@pytest.mark.parametrize(
    "option", ["eagle", "EAGLE", "Eagle", "EaGlE", "kestrel", "KESTREL", "dne"]
)
def test_warning_about_au_usage(
    test_ctx,
    runnable_script,
    test_class,
    caplog,
    num_nodes,
    option,
    job_names_cache,
):
    """Test the `run` function calls `_kickoff_hpc_job` for multiple nodes."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            name="run",
            split_keys={"input3"},
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys={"input3"},
        )

    config = {
        "execution_control": {
            "option": option,
            "allocation": "test",
            "qos": "high",
            "walltime": 50,
            "max_workers": 1,
        },
        "input1": 1,
        "input3": ["input"] * num_nodes,
        "project_points": [0, 1, 2, 4],
    }

    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    assert not caplog.records
    assert len(job_names_cache) == 0
    try:
        from_config(config_fp, command_config)
    except ValueError:
        pass

    if option != "dne":
        assert len(job_names_cache) == num_nodes
        assert len(set(job_names_cache)) == num_nodes

    warnings = [
        record for record in caplog.records if record.levelname == "WARNING"
    ]
    if num_nodes < 33 or option.casefold() == "dne":
        assert not warnings
    else:
        assert warnings
        assert any("Job may use up to" in record.msg for record in warnings)


@pytest.mark.parametrize("test_class", [False, True])
@pytest.mark.parametrize("option", ["slurm", "SLURM", "Slurm", "SlUrM"])
def test_hardware_slurm_raises_warning(
    test_ctx,
    runnable_script,
    test_class,
    caplog,
    option,
    job_names_cache,
):
    """Test that a "slurm" hardware option raises a warning."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            name="run",
            split_keys={"input3"},
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys={"input3"},
        )

    config = {
        "execution_control": {
            "option": option,
            "allocation": "test",
            "qos": "high",
            "walltime": 50,
            "max_workers": 1,
        },
        "input1": 1,
        "input3": ["input"] * 100,
        "project_points": [0, 1, 2, 4],
    }

    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    assert not caplog.records
    assert len(job_names_cache) == 0
    try:
        from_config(config_fp, command_config)
    except ValueError:
        pass

    assert len(job_names_cache) == 100
    assert len(set(job_names_cache)) == 100

    warnings = [
        record for record in caplog.records if record.levelname == "WARNING"
    ]

    assert warnings
    assert any(
        "Detected option='slurm' in execution control. Please do not"
        in record.msg
        for record in warnings
    )
    assert any(
        "use this option unless your HPC is explicitly not supported"
        in record.msg
        for record in warnings
    )
    assert any(
        "Available HPC options are:" in record.msg for record in warnings
    )
    assert any("eagle" in record.msg for record in warnings)
    assert any("kestrel" in record.msg for record in warnings)


@pytest.mark.parametrize("test_class", [False, True])
def test_run_parallel_split_keys(
    test_ctx, runnable_script, test_class, job_names_cache
):
    """Test the `run` function calls `_kickoff_hpc_job` for multiple nodes."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            split_keys={"_z_0", ("input1", "input3")},
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys={"_z_0", ("input1", "input3")},
        )

    config = {
        "execution_control": {
            "option": "eagle",
            "allocation": "test",
            "walltime": 1,
            "nodes": 2,
            "max_workers": 1,
        },
        "input1": [1, 2, 3],
        "input3": [4, 5, 6],
        "_z_0": ["unsorted", "strings"],
        "project_points": [0, 1, 2, 4],
    }

    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    assert len(job_names_cache) == 0
    from_config(config_fp, command_config)
    assert len(job_names_cache) == 6
    assert len(set(job_names_cache)) == 6

    for job_name, script in job_names_cache.items():
        if f"{TAG}0" in job_name:
            assert '"_z_0": "strings"' in script
            assert '"input1": 1' in script
            assert '"input3": 4' in script
        elif f"{TAG}1" in job_name:
            assert '"_z_0": "strings"' in script
            assert '"input1": 2' in script
            assert '"input3": 5' in script
        elif f"{TAG}2" in job_name:
            assert '"_z_0": "strings"' in script
            assert '"input1": 3' in script
            assert '"input3": 6' in script
        elif f"{TAG}3" in job_name:
            assert '"_z_0": "unsorted"' in script
            assert '"input1": 1' in script
            assert '"input3": 4' in script
        elif f"{TAG}4" in job_name:
            assert '"_z_0": "unsorted"' in script
            assert '"input1": 2' in script
            assert '"input3": 5' in script
        elif f"{TAG}5" in job_name:
            assert '"_z_0": "unsorted"' in script
            assert '"input1": 3' in script
            assert '"input3": 6' in script

        else:
            raise ValueError(
                f"Could not find expected tag in job name: {job_name!r}"
            )


@pytest.mark.parametrize("test_class", [False, True])
def test_run_local_empty_split_key(test_ctx, runnable_script, test_class):
    """Test the `run` function locally with empty split key."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            split_keys={"input3"},
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
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
    assert outputs["_input2"] is None
    assert outputs["input3"] is None
    assert outputs["max_workers"] == 100


@pytest.mark.parametrize("test_class", [False, True])
def test_run_local_multiple_out_files(test_ctx, runnable_script, test_class):
    """Test the `run` function locally with empty split key."""

    tmp_path = test_ctx.obj["TMP_PATH"]

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            name="run",
            split_keys={"input3"},
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys={"input3"},
        )
    config = {
        "input1": 1,
        "input3": ["Hello", "world"],
        "max_workers": 100,
        "project_points": "/a/path",
    }
    config_fp = tmp_path / "config.json"
    with open(config_fp, "w") as config_file:
        json.dump(config, config_file)

    from_config(config_fp, command_config)
    out_fns = [f"out{TAG}0.json", f"out{TAG}1.json"]

    for out_fn, in3 in zip(out_fns, config["input3"]):
        expected_file = tmp_path / out_fn
        assert expected_file.exists()
        with open(expected_file, "r") as output_file:
            outputs = json.load(output_file)

        assert not outputs["is_pp"]
        assert outputs["len_pp"] == 7
        assert outputs["input1"] == 1
        assert outputs["_input2"] is None
        assert outputs["input3"] == in3
        assert outputs["max_workers"] == 100

    status = Status(tmp_path).update_from_all_job_files()
    assert len(status["run"]) == 2
    for job_name in status["run"]:
        assert f"{tmp_path.name}_run" in job_name
        assert f"{TAG}" in job_name


@pytest.mark.parametrize("test_class", [False, True])
def test_command_skip_doc_params(test_class):
    """Test the `command` class with skip params."""

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            split_keys={"input3"},
            skip_doc_params={"input1"},
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys={"input3"},
            skip_doc_params={"input1"},
        )

    assert "input1" not in command_config.documentation.parameter_help
    assert "input1" not in command_config.documentation.template_config

    assert "_input2" not in command_config.documentation.parameter_help
    assert "_input2" not in command_config.documentation.template_config

    assert "input3" in command_config.documentation.parameter_help
    assert "input3" in command_config.documentation.template_config


def test_validate_config():
    """Test the `_validate_config` function."""

    def func(input1, input2, input3=None, input4=None):
        pass

    func_doc = CommandDocumentation(func, skip_params={"input2", "input4"})

    with pytest.raises(gapsKeyError):
        _validate_config({}, func_doc)

    with pytest.warns(gapsWarning) as warning_info:
        _validate_config({"input1": 1, "input3": 3, "dne": -1}, func_doc)

    expected_message = "Found unused keys in the configuration file: {'dne'}"
    assert expected_message in warning_info[0].message.args[0]

    def func2(max_workers):
        pass

    func_doc = CommandDocumentation(func2)

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
    status_update_args = tmp_path, "run", "test"
    exclude = {"project_points", "input1"}

    assert not input_cache
    run_with_status_updates(
        test_func,
        node_specific_config,
        logging_options,
        status_update_args,
        exclude,
    )
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


@pytest.mark.parametrize("test_extras", [False, True])
@pytest.mark.parametrize("test_class", [False, True])
def test_args_passed_to_pre_processor(
    tmp_path, test_ctx, test_extras, test_class, runnable_script
):
    """Test that correct arguments are passed to the pre-processor."""

    input_config = {
        "execution_control": {"max_workers": 100},
        "input1": 1,
        "a_value": 5,
        "a_multiplier": 2,
        "input2": 7,
        "_input2": 8,
        "input3": None,
        "project_points": [0, 1, 2],
    }
    config_fp = tmp_path / "config.json"

    if test_extras:
        input_config["log_directory"] = str(tmp_path / "other_logs")
        input_config["log_level"] = "DEBUG"

    with open(config_fp, "w") as config_file:
        json.dump(input_config, config_file)

    # pylint: disable=too-many-arguments
    def pre_processing(
        config,
        a_value,
        a_multiplier,
        command_name,
        config_file,
        project_dir,
        job_name,
        out_dir,
        out_fpath,
        log_directory,
        verbose,
    ):
        assert a_value == 5
        assert a_multiplier == 2
        assert command_name == "run"
        assert config_file == config_fp
        assert project_dir == tmp_path
        assert tmp_path.name in job_name
        assert "run" in job_name
        assert out_dir == tmp_path
        assert out_fpath == out_dir / job_name
        if test_extras:
            assert log_directory == tmp_path / "other_logs"
            assert verbose
        else:
            assert config == input_config
            assert log_directory == tmp_path / "logs"
            assert not verbose

        return config

    if test_class:
        command_config = CLICommandFromClass(
            TestCommand,
            "run",
            split_keys={"project_points", "input3"},
            config_preprocessor=pre_processing,
        )
    else:
        command_config = CLICommandFromFunction(
            _testing_function,
            name="run",
            split_keys={"project_points", "input3"},
            config_preprocessor=pre_processing,
        )

    with pytest.warns(gapsWarning) as warning_info:
        from_config(config_fp, command_config)


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
