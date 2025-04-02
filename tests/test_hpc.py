# -*- coding: utf-8 -*-
# pylint: disable=too-many-locals
"""
GAPs HPC job managers tests.
"""

import shlex
import subprocess
from pathlib import Path

import pytest

from gaps import TEST_DATA_DIR
import gaps.hpc
from gaps.hpc import (
    PBS,
    SLURM,
    _skip_q_rows,
    DEFAULT_STDOUT_PATH,
    submit,
    format_env,
    format_walltime,
)
from gaps.exceptions import gapsHPCError, gapsExecutionError, gapsValueError
from gaps.warn import gapsHPCWarning

with open(TEST_DATA_DIR / "hpc" / "qstat.txt", "r") as f:
    Q_STAT_RAW = f.read()
Q_STAT = _skip_q_rows(Q_STAT_RAW, (0, 1))

with open(TEST_DATA_DIR / "hpc" / "squeue.txt", "r") as f:
    SQUEUE_RAW = f.read()

JOB_IDS = (12345, 12346, 12347)
PBS_HEADER = ("Job id", "Name", "User", "Time Use", "S", "Queue")
SLURM_HEADER = (
    "JOBID",  # cspell:disable-line
    "PARTITION",
    "NAME",
    "USER",
    "ST",
    "TIME",
    "NODELIST(REASON)",
)


@pytest.mark.parametrize("manager", [PBS, SLURM])
def test_bad_hpc_init(manager):
    """Test that initializing with bad dict input throws error."""
    with pytest.raises(gapsHPCError):
        manager(queue_dict=[])


@pytest.mark.parametrize(
    ("manager", "raw_queue", "header"),
    [(PBS, Q_STAT_RAW, PBS_HEADER), (SLURM, SQUEUE_RAW, SLURM_HEADER)],
)
def test_querying_queue(manager, raw_queue, header, monkeypatch):
    """Test querying the queue function."""

    cmd_cache = []

    def _test_submit(cmd):
        cmd_cache.append(cmd)
        return raw_queue, None

    monkeypatch.setattr(gaps.hpc, "submit", _test_submit, raising=True)

    assert not cmd_cache
    hpc_manager = manager(user="test_user")
    queue_dict = hpc_manager.queue
    assert len(cmd_cache) == 1
    assert "test_user" in cmd_cache[-1]
    assert len(queue_dict) == 3
    assert all(i in queue_dict for i in JOB_IDS)
    for job_props in queue_dict.values():
        assert all(col in job_props for col in header)


@pytest.mark.parametrize(
    ("manager", "raw_queue"), [(PBS, Q_STAT_RAW), (SLURM, SQUEUE_RAW)]
)
def test_job_cancel(manager, raw_queue, monkeypatch):
    """Test that jobs are correctly canceled."""

    call_cache = []

    def _test_call(cmd):
        call_cache.append(cmd)

    def _test_submit(_):
        return raw_queue, None

    monkeypatch.setattr(gaps.hpc, "submit", _test_submit, raising=True)
    monkeypatch.setattr(subprocess, "call", _test_call, raising=True)
    monkeypatch.setattr(shlex, "split", lambda x: x, raising=True)

    assert not call_cache
    hpc_manager = manager()
    hpc_manager.cancel(JOB_IDS[0])
    assert len(call_cache) == 1
    assert str(JOB_IDS[0]) in call_cache[-1]

    hpc_manager.cancel(JOB_IDS[1:])
    assert len(call_cache) == 3
    assert all(
        str(job_id) in " ".join(call_cache[-2:]) for job_id in JOB_IDS[1:]
    )

    hpc_manager.cancel("aLL")
    assert len(call_cache) == 6
    assert all(str(job_id) in " ".join(call_cache[-3:]) for job_id in JOB_IDS)

    with pytest.raises(gapsExecutionError):
        hpc_manager.cancel({"job_id": JOB_IDS[0]})


@pytest.mark.parametrize("manager", [PBS, SLURM])
def test_job_name_too_long(manager):
    """Test submission fails if name too long."""
    hpc_manager = manager()
    with pytest.raises(gapsValueError):
        hpc_manager.submit("".join(["a"] * (manager.MAX_NAME_LEN * 2)))


@pytest.mark.parametrize(
    ("manager", "q_str", "header"),
    [(PBS, Q_STAT, PBS_HEADER), (SLURM, SQUEUE_RAW, SLURM_HEADER)],
)
def test_queue_parsing(manager, q_str, header):
    """Test the PBS job handler qstat parsing utility"""
    queue_dict = manager.parse_queue_str(q_str)

    assert len(queue_dict) == 3
    assert all(i in queue_dict for i in JOB_IDS)

    for attrs in queue_dict.values():
        assert all(key in attrs for key in header)

    assert queue_dict[JOB_IDS[0]][manager.COLUMN_HEADERS.NAME] == "job1"
    assert queue_dict[JOB_IDS[0]][manager.COLUMN_HEADERS.STATUS] == "R"
    assert (
        queue_dict[JOB_IDS[2]][manager.COLUMN_HEADERS.STATUS]
        == manager.Q_SUBMITTED_STATUS
    )


@pytest.mark.parametrize(
    ("manager", "q_str"),
    [(PBS, Q_STAT), (SLURM, SQUEUE_RAW)],
)
def test_check_job(manager, q_str):
    """Test the HPC job status checker utility"""
    qstat = manager.parse_queue_str(q_str)
    hpc_manager = manager(user="usr0", queue_dict=qstat)

    assert hpc_manager.check_status_using_job_name("job1") == "R"
    assert (
        hpc_manager.check_status_using_job_name("job2")
        == hpc_manager.Q_SUBMITTED_STATUS
    )
    assert hpc_manager.check_status_using_job_name("bad") is None
    assert (
        hpc_manager.check_status_using_job_id(JOB_IDS[-1])
        == hpc_manager.Q_SUBMITTED_STATUS
    )
    assert hpc_manager.check_status_using_job_id(1) is None


@pytest.mark.parametrize(
    ("manager", "q_str", "kwargs", "expectation", "add_qos"),
    [
        (
            PBS,
            Q_STAT,
            {"queue": "batch-h", "sh_script": "echo Hello!"},
            {
                "#PBS -N submit_test": 1,
                "#PBS -A rev": 2,
                "#PBS -q batch-h": 3,
                # cspell:disable-next-line
                "#PBS -o ./stdout/submit_test_$PBS_JOBID.o": 4,
                # cspell:disable-next-line
                "#PBS -e ./stdout/submit_test_$PBS_JOBID.e": 5,
                "#PBS -l walltime=00:26:00,qos=high": 6,
                # cspell:disable-next-line
                "echo Running on: $HOSTNAME, Machine Type: $MACHTYPE": 7,
                "echo Running python in directory `which python`": 8,
                "echo Hello!": 9,
            },
            True,
        ),
        (
            PBS,
            Q_STAT,
            {"queue": "batch-h", "sh_script": "echo Hello!"},
            {
                "#PBS -N submit_test": 1,
                "#PBS -A rev": 2,
                "#PBS -q batch-h": 3,
                # cspell:disable-next-line
                "#PBS -o ./stdout/submit_test_$PBS_JOBID.o": 4,
                # cspell:disable-next-line
                "#PBS -e ./stdout/submit_test_$PBS_JOBID.e": 5,
                "#PBS -l walltime=00:26:00": 6,
                # cspell:disable-next-line
                "echo Running on: $HOSTNAME, Machine Type: $MACHTYPE": 7,
                "echo Running python in directory `which python`": 8,
                "echo Hello!": 9,
            },
            False,
        ),
        (
            SLURM,
            SQUEUE_RAW,
            {"sh_script": "echo Hello!"},
            {
                "#SBATCH --account=rev": 1,
                "#SBATCH --time=00:26:00": 2,
                "#SBATCH --job-name=submit_test": 3,
                "#SBATCH --nodes=1": 4,
                "#SBATCH --output=./stdout/submit_test_%j.o": 5,
                "#SBATCH --error=./stdout/submit_test_%j.e": 6,
                "#SBATCH --qos=high": 7,
                # cspell:disable-next-line
                "echo Running on: $HOSTNAME, Machine Type: $MACHTYPE": 8,
                "echo Running python in directory `which python`": 9,
                "echo Hello!": 10,
            },
            True,
        ),
    ],
)
def test_hpc_submit(manager, q_str, kwargs, expectation, add_qos, monkeypatch):
    """Test the HPC job submission utility"""

    queue_dict = manager.parse_queue_str(q_str)
    hpc_manager = manager(user="usr0", queue_dict=queue_dict)
    kwargs["cmd"] = "python -c \"print('hello world')\""
    kwargs["allocation"] = "rev"
    kwargs["walltime"] = 0.43
    if add_qos:
        kwargs["qos"] = "high"
    out, err = hpc_manager.submit("job1", **kwargs)
    assert out is None
    assert err == "already_running"

    name = "submit_test"
    cmd_cache = []
    fn_sh = Path(manager.SHELL_FILENAME_FMT.format(name))

    def _test_submit(cmd):
        cmd_cache.append(cmd)
        assert fn_sh.exists()
        return "9999", "A test Err"

    monkeypatch.setattr(gaps.hpc, "submit", _test_submit, raising=True)

    assert not cmd_cache
    with pytest.warns(gapsHPCWarning):
        out, err = hpc_manager.submit(name, **kwargs)
    assert not fn_sh.exists()
    assert len(cmd_cache) == 1
    assert fn_sh.name in cmd_cache[-1]
    assert 9999 not in hpc_manager.queue
    assert out == "9999"
    assert err == "A test Err"

    cmd_cache = []

    def _test_submit(cmd):
        cmd_cache.append(cmd)
        assert fn_sh.exists()
        return "Job ID 9999", None

    monkeypatch.setattr(gaps.hpc, "submit", _test_submit, raising=True)

    assert not cmd_cache
    out, err = hpc_manager.submit(name, keep_sh=True, **kwargs)
    assert fn_sh.exists()
    assert len(cmd_cache) == 1
    assert fn_sh.name in cmd_cache[-1]
    assert 9999 in hpc_manager.queue

    job_props = hpc_manager.queue[9999]
    assert job_props[hpc_manager.COLUMN_HEADERS.ID] == 9999
    assert job_props[hpc_manager.COLUMN_HEADERS.NAME] == name
    assert (
        job_props[hpc_manager.COLUMN_HEADERS.STATUS]
        == hpc_manager.Q_SUBMITTED_STATUS
    )
    assert out == "9999"
    assert err is None

    with open(fn_sh, "r") as submission_file:
        shell_script = submission_file.readlines()

    for expected_str, line_no in expectation.items():
        assert expected_str in shell_script[line_no]
    assert kwargs["cmd"] in shell_script[-1]

    fn_sh.unlink()
    stdout_dir = Path(DEFAULT_STDOUT_PATH)
    assert stdout_dir.exists()
    stdout_dir.rmdir()


def test_submit(monkeypatch):
    """Test the submission utility function"""

    cmd = "python -c \"print('hello world')\""
    cmd_id = "python -c \"print('Job ID is 12342')\""
    cmd_err = "python -c \"raise ValueError('An error occurred')\""
    cmd_squeue = f'python -c "print({SQUEUE_RAW!r})"'

    out, err = submit(cmd, background=False, background_stdout=False)
    assert out == "hello world"
    assert not err

    out, err = submit(cmd, background=False, background_stdout=True)
    assert out == "hello world"
    assert not err

    out, err = submit(cmd_id, background=False, background_stdout=False)
    assert out == "Job ID is 12342"
    assert not err

    out, err = submit(cmd_squeue, background=False, background_stdout=False)
    out = out.replace("\n", "").replace("\r", "")
    expected = SQUEUE_RAW.replace("\n", "").replace("\r", "")
    assert out == expected
    assert not err

    with pytest.raises(OSError):
        submit(cmd_err, background=False, background_stdout=False)

    run_cache = []

    def _test_run(cmd, *__, **___):
        run_cache.append(cmd)

    monkeypatch.setattr(subprocess, "run", _test_run, raising=True)

    assert not run_cache
    out, err = submit(cmd, background=False, background_stdout=False)
    assert not run_cache
    assert out == "hello world"
    assert not err

    assert not run_cache
    out, err = submit(cmd, background=False, background_stdout=True)
    assert not run_cache
    assert out == "hello world"
    assert not err

    assert not run_cache
    out, err = submit(cmd, background=True, background_stdout=False)
    assert len(run_cache) == 1
    assert "nohup" in run_cache[-1]
    assert not out
    assert not err

    out, err = submit(cmd, background=True, background_stdout=True)
    assert len(run_cache) == 2
    assert "nohup" in run_cache[-1]
    assert not out
    assert not err


def test_format_methods():
    """Misc tests for format methods."""
    assert not format_walltime()
    assert "source activate test_env" in format_env("test_env")


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
