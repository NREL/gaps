"""GAPs CLI tests"""

import os
import time
import json
import shutil
from pathlib import Path

import h5py
import psutil
import pytest
import numpy as np

from gaps import Pipeline
from gaps.status import Status, StatusOption
from gaps.cli import CLICommandFromFunction, make_cli
from gaps.cli.config import TAG
from gaps.cli.documentation import CommandDocumentation
from gaps.cli.pipeline import _can_run_background


PROJECT_POINTS = [0, 1, 2]
MAX_WORKERS = 10


def _copy_single_file(
    project_points, source_dir, dest_dir, file_pattern, max_workers
):
    """Test function that copies over data files"""
    time.sleep(3)
    assert project_points.gids == PROJECT_POINTS
    assert max_workers == MAX_WORKERS
    out_files = []
    for in_file in sorted(Path(source_dir).glob(file_pattern)):
        out_file_name = file_pattern.replace("*", "")
        out_file = str(Path(dest_dir) / out_file_name)
        shutil.copy(in_file, out_file)
        out_files.append(out_file)
        break
    return out_files


def _copy_files(
    project_points, source_dir, dest_dir, file_pattern, max_workers
):
    """Test function that copies over data files"""
    time.sleep(3)
    assert project_points.gids == PROJECT_POINTS
    assert max_workers == MAX_WORKERS
    out_files = []
    for ind, in_file in enumerate(sorted(Path(source_dir).glob(file_pattern))):
        out_file_name = file_pattern.replace("*", f"{TAG}{ind}")
        out_file = str(Path(dest_dir) / out_file_name)
        shutil.copy(in_file, out_file)
        out_files.append(out_file)
    return out_files


def _make_test_cli(run_func, data_dir, file_pattern):
    """Make CLI for testing"""

    def preprocess_run_config(config, project_dir, out_dir):
        assert project_dir == out_dir
        config["dest_dir"] = str(project_dir)
        config["source_dir"] = str(data_dir)
        config["file_pattern"] = file_pattern
        return config

    commands = [
        CLICommandFromFunction(
            run_func,
            name="run",
            add_collect=True,
            split_keys=["project_points"],
            config_preprocessor=preprocess_run_config,
        )
    ]

    return make_cli(commands)


def _check_make_templates(cwd, cli_runner, main):
    """Make templates in cwd while performing checks"""

    assert not set(cwd.glob("*"))
    cli_runner.invoke(main, ["template-configs"])
    files = set(cwd.glob("*"))
    assert len(files) == 4
    for config_type in ["pipeline", "run", "collect_run"]:
        assert cwd / f"config_{config_type}.json" in files

    pipe_config_fp = cwd / "config_pipeline.json"
    run_config_fp = cwd / "config_run.json"
    collect_config_fp = cwd / "config_collect_run.json"
    with run_config_fp.open(encoding="utf-8") as config_file:
        config = json.load(config_file)

    assert config["project_points"] == CommandDocumentation.REQUIRED_TAG
    exec_control = config["execution_control"]
    assert exec_control["max_workers"] == CommandDocumentation.REQUIRED_TAG
    assert exec_control["nodes"] == 1
    config["project_points"] = PROJECT_POINTS
    config["execution_control"]["option"] = "local"
    config["execution_control"]["max_workers"] = MAX_WORKERS

    with run_config_fp.open("w", encoding="utf-8") as config_file:
        json.dump(config, config_file)

    with pipe_config_fp.open(encoding="utf-8") as config_file:
        config = json.load(config_file)

    config["pipeline"] = config["pipeline"][:-1]

    with pipe_config_fp.open("w", encoding="utf-8") as config_file:
        json.dump(config, config_file)

    return pipe_config_fp, collect_config_fp


def test_make_cli():
    """Test that `make_cli` generates the correct commands"""

    assert not Pipeline.COMMANDS
    commands = [
        CLICommandFromFunction(
            _copy_files,
            name="run",
            split_keys=["project_points"],
        ),
        CLICommandFromFunction(
            _copy_files,
            name="analyze",
            add_collect=True,
            split_keys=["a_key"],
        ),
    ]

    main = make_cli(commands, info={"name": "test", "version": "0.1.0"})
    assert "run" in Pipeline.COMMANDS
    assert "collect-run" not in Pipeline.COMMANDS
    assert "analyze" in Pipeline.COMMANDS
    assert "collect-analyze" in Pipeline.COMMANDS

    for expected_command in [
        "pipeline",
        "run",
        "analyze",
        "collect-analyze",
        "batch",
        "status",
        "template-configs",
    ]:
        assert expected_command in main.commands

    assert "test" in main.help
    assert "$ test --help" in main.help
    assert "$ test run --help" in main.help
    assert "$ test analyze --help" in main.help
    assert "$ test collect-analyze --help" in main.help
    assert "$ test collect-run --help" not in main.help


@pytest.mark.integration
@pytest.mark.parametrize("test_single_file", [True, False])
def test_cli(
    tmp_cwd,
    cli_runner,
    collect_pattern,
    manual_collect,
    runnable_script,
    test_single_file,
    assert_message_was_logged,
):
    """Integration test of `make_cli`"""

    data_dir, file_pattern = collect_pattern
    run_func = _copy_single_file if test_single_file else _copy_files
    main = _make_test_cli(run_func, data_dir, file_pattern)

    assert main.help == "Command Line Interface"

    config_files = _check_make_templates(tmp_cwd, cli_runner, main)
    pipe_config_fp, collect_config_fp = config_files

    assert not set(tmp_cwd.glob(file_pattern))
    assert tmp_cwd / "logs" not in set(tmp_cwd.glob("*"))
    result = cli_runner.invoke(
        main, ["pipeline", "-c", pipe_config_fp.as_posix()]
    )
    assert len(set((tmp_cwd / "logs").glob("*run*"))) == 1
    assert len(set(tmp_cwd.glob(file_pattern))) == 1 if test_single_file else 4
    assert tmp_cwd / "logs" in set(tmp_cwd.glob("*"))

    result = cli_runner.invoke(main, ["status"])
    lines = result.stdout.split("\n")

    expected_partial_lines = [
        "test_cli",
        "job_status time_submitted time_start time_end total_runtime",
        "--",
        "test_cli run successful",
        "collect-run not submitted",
    ]

    for ind, partial in enumerate(expected_partial_lines[::-1], start=9):
        err_msg = f"{partial!r} not in {lines[-ind]!r}. All lines: {lines}"
        assert all(string in lines[-ind] for string in partial.split()), (
            err_msg
        )

    with collect_config_fp.open("r", encoding="utf-8") as config_file:
        config = json.load(config_file)
    config["collect_pattern"] = {"out.h5": "./*.h5"}
    with collect_config_fp.open("w", encoding="utf-8") as config_file:
        json.dump(config, config_file)

    assert tmp_cwd / "chunk_files" not in set(tmp_cwd.glob("*"))
    result = cli_runner.invoke(
        main,
        ["-v", "collect-run", "-c", collect_config_fp.as_posix()],
    )
    assert tmp_cwd / "chunk_files" in set(tmp_cwd.glob("*"))

    log_file = set((tmp_cwd / "logs").glob("*collect_run*"))
    assert len(log_file) == 1

    with Path(log_file.pop()).open("r", encoding="utf-8") as log:
        assert "DEBUG" in log.read()

    h5_files = set(tmp_cwd.glob("*.h5"))
    assert len(h5_files) == 1

    if not test_single_file:
        with h5py.File(h5_files.pop(), "r") as collected_outputs:
            assert len(collected_outputs.keys()) == 5
            assert "cf_mean" in collected_outputs
            assert "lcoe_fcr" in collected_outputs
            cf_profiles = collected_outputs["cf_profile"][...]

            assert "collect-run_config_fp" in collected_outputs.attrs
            assert (
                Path(collected_outputs.attrs["collect-run_config_fp"])
                == collect_config_fp
            )
            assert "collect-run_config" in collected_outputs.attrs
            collect_config = json.loads(
                collected_outputs.attrs["collect-run_config"]
            )

            expected_keys = {
                "execution_control",
                "log_directory",
                "log_level",
                "project_points",
                "datasets",
                "purge_chunks",
                "clobber",
                "collect_pattern",
            }
            assert set(collect_config) == expected_keys
            assert collect_config["collect_pattern"] == {"out.h5": "./*.h5"}

            assert "collect-run_collect_pattern" in collected_outputs.attrs
            assert (
                Path(collected_outputs.attrs["collect-run_collect_pattern"])
                == tmp_cwd / "*.h5"
            )

        profiles = manual_collect(data_dir / file_pattern, "cf_profile")
        assert np.allclose(profiles, cf_profiles)

    result = cli_runner.invoke(
        main, ["pipeline", "-c", pipe_config_fp.as_posix()]
    )

    assert_message_was_logged("Pipeline job", "INFO")
    assert_message_was_logged("is complete.", "INFO")


@pytest.mark.integration
def test_cli_monitor(
    tmp_cwd,
    cli_runner,
    collect_pattern,
    manual_collect,
    runnable_script,
    assert_message_was_logged,
):
    """Integration test of `make_cli` with monitor"""

    data_dir, file_pattern = collect_pattern
    main = _make_test_cli(_copy_files, data_dir, f"./{file_pattern}")

    config_files = _check_make_templates(tmp_cwd, cli_runner, main)
    pipe_config_fp, collect_config_fp = config_files

    assert not set(tmp_cwd.glob(file_pattern))
    assert tmp_cwd / "logs" not in set(tmp_cwd.glob("*"))
    assert tmp_cwd / "chunk_files" not in set(tmp_cwd.glob("*"))

    cli_runner.invoke(
        main, ["pipeline", "-c", pipe_config_fp.as_posix(), "--monitor"]
    )
    assert len(set((tmp_cwd / "logs").glob("*run*"))) == 2
    assert len(set(tmp_cwd.glob(file_pattern))) == 1
    assert tmp_cwd / "logs" in set(tmp_cwd.glob("*"))
    assert tmp_cwd / "chunk_files" in set(tmp_cwd.glob("*"))
    assert len(set((tmp_cwd / "chunk_files").glob(file_pattern))) == 4

    log_file = set((tmp_cwd / "logs").glob("*collect_run*"))
    assert len(log_file) == 1

    with Path(log_file.pop()).open("r", encoding="utf-8") as log:
        assert "DEBUG" not in log.read()

    h5_files = set(tmp_cwd.glob("*.h5"))
    assert len(h5_files) == 1

    with h5py.File(h5_files.pop(), "r") as collected_outputs:
        assert len(collected_outputs.keys()) == 5
        assert "cf_mean" in collected_outputs
        assert "lcoe_fcr" in collected_outputs
        cf_profiles = collected_outputs["cf_profile"][...]

        assert "collect-run_config_fp" in collected_outputs.attrs
        assert (
            Path(collected_outputs.attrs["collect-run_config_fp"])
            == collect_config_fp
        )
        assert "collect-run_config" in collected_outputs.attrs
        collect_config = json.loads(
            collected_outputs.attrs["collect-run_config"]
        )

        expected_keys = {
            "execution_control",
            "log_directory",
            "log_level",
            "project_points",
            "datasets",
            "purge_chunks",
            "clobber",
            "collect_pattern",
        }
        assert set(collect_config) == expected_keys
        assert collect_config["collect_pattern"] == "PIPELINE"

        assert "collect-run_collect_pattern" in collected_outputs.attrs
        assert (
            Path(collected_outputs.attrs["collect-run_collect_pattern"])
            == tmp_cwd / file_pattern
        )

    profiles = manual_collect(data_dir / file_pattern, "cf_profile")
    assert np.allclose(profiles, cf_profiles)

    assert_message_was_logged("Pipeline job", "INFO")
    assert_message_was_logged("is complete.", "INFO")


@pytest.mark.skipif(
    not _can_run_background(),
    reason="Can't run pipeline in background on system that does not "
    "implement os.fork/setsid",
)
@pytest.mark.integration
@pytest.mark.flaky(max_runs=5, min_passes=1)
def test_cli_background(
    tmp_cwd,
    cli_runner,
    collect_pattern,
    manual_collect,
    runnable_script,
):
    """Integration test of `make_cli` with background"""

    data_dir, file_pattern = collect_pattern
    main = _make_test_cli(_copy_files, data_dir, file_pattern)

    config_files = _check_make_templates(tmp_cwd, cli_runner, main)
    pipe_config_fp, collect_config_fp = config_files

    assert not set(tmp_cwd.glob(file_pattern))
    assert tmp_cwd / "logs" not in set(tmp_cwd.glob("*"))
    assert tmp_cwd / "chunk_files" not in set(tmp_cwd.glob("*"))

    cli_runner.invoke(
        main, ["pipeline", "-c", pipe_config_fp.as_posix(), "--background"]
    )

    time.sleep(10)  # give job enough time to run a little

    status = Status(tmp_cwd).update_from_all_job_files(purge=False)
    assert "monitor_pid" in status

    if os.getpid() == status["monitor_pid"]:
        # Wait to die
        for __ in range(10):
            time.sleep(60)
        pytest.exit(0)

    assert (
        Status.retrieve_job_status(
            tmp_cwd, "collect-run", f"{tmp_cwd.name}_collect_run"
        )
        != StatusOption.SUCCESSFUL
    )

    for __ in range(10):
        time.sleep(60)
        collect_status = Status.retrieve_job_status(
            tmp_cwd, "collect-run", f"{tmp_cwd.name}_collect_run"
        )
        if collect_status == StatusOption.SUCCESSFUL:
            break
    else:
        msg = "Collection step timed out"
        raise AssertionError(msg)

    psutil.Process(status["monitor_pid"]).kill()

    assert len(set((tmp_cwd / "logs").glob("*run*"))) == 2
    assert len(set(tmp_cwd.glob(file_pattern))) == 1
    assert tmp_cwd / "logs" in set(tmp_cwd.glob("*"))
    assert tmp_cwd / "chunk_files" in set(tmp_cwd.glob("*"))
    assert len(set((tmp_cwd / "chunk_files").glob(file_pattern))) == 4

    log_file = set((tmp_cwd / "logs").glob("*collect_run*"))
    assert len(log_file) == 1

    with Path(log_file.pop()).open("r", encoding="utf-8") as log:
        assert "DEBUG" not in log.read()

    h5_files = set(tmp_cwd.glob("*.h5"))
    assert len(h5_files) == 1

    with h5py.File(h5_files.pop(), "r") as collected_outputs:
        assert len(collected_outputs.keys()) == 5
        assert "cf_mean" in collected_outputs
        assert "lcoe_fcr" in collected_outputs
        cf_profiles = collected_outputs["cf_profile"][...]

        assert "collect-run_config_fp" in collected_outputs.attrs
        assert (
            Path(collected_outputs.attrs["collect-run_config_fp"])
            == collect_config_fp
        )
        assert "collect-run_config" in collected_outputs.attrs
        collect_config = json.loads(
            collected_outputs.attrs["collect-run_config"]
        )

        expected_keys = {
            "execution_control",
            "log_directory",
            "log_level",
            "project_points",
            "datasets",
            "purge_chunks",
            "clobber",
            "collect_pattern",
        }
        assert set(collect_config) == expected_keys
        assert collect_config["collect_pattern"] == "PIPELINE"

        assert "collect-run_collect_pattern" in collected_outputs.attrs
        assert (
            Path(collected_outputs.attrs["collect-run_collect_pattern"])
            == tmp_cwd / file_pattern
        )

    profiles = manual_collect(data_dir / file_pattern, "cf_profile")
    assert np.allclose(profiles, cf_profiles)


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
