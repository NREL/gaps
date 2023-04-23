# pylint: disable=protected-access,redefined-outer-name
"""
GAPs Status tests.
"""
import json
import shutil
import datetime as dt
from pathlib import Path
from copy import deepcopy

import pytest

from gaps.hpc import SLURM, PBS
from gaps.config import ConfigType
from gaps.status import (
    DT_FMT,
    JOB_STATUS_FILE,
    MONITOR_PID_FILE,
    NAMED_STATUS_FILE,
    HardwareOption,
    HardwareStatusRetriever,
    Status,
    StatusField,
    StatusOption,
    StatusUpdates,
    _get_attr_flat_list,
    _load_job_file,
    _elapsed_time_as_str,
)
from gaps.exceptions import gapsTypeError, gapsKeyError
from gaps.warnings import gapsWarning


TEST_1_ATTRS_1 = {
    "job_name": "test1",
    StatusField.JOB_STATUS: "R",
    "run_id": 1234,
}
TEST_1_ATTRS_2 = {
    "job_name": "test1",
    StatusField.JOB_STATUS: StatusOption.SUCCESSFUL,
}
TEST_2_ATTRS_1 = {
    "job_name": "test2",
    StatusField.JOB_STATUS: "R",
}
TEST_2_ATTRS_2 = {
    "job_name": "test2",
    StatusField.JOB_STATUS: "R",
    StatusField.JOB_ID: 123,
}


@pytest.fixture
def temp_job_dir(tmp_path):
    """Create a temp dir and temp status filename for mock job directory."""
    return tmp_path, tmp_path / NAMED_STATUS_FILE.format(tmp_path.name)


# pylint: disable=no-member
def test_hardware_option():
    """Test the HardwareOption Enum"""

    slurm_cs = SLURM().check_status_using_job_id
    pbs_cs = PBS().check_status_using_job_id

    assert not HardwareOption.LOCAL.is_hpc
    assert HardwareOption.SLURM.is_hpc
    assert HardwareOption.EAGLE.is_hpc
    assert HardwareOption.PEREGRINE.is_hpc
    assert HardwareOption.PBS.is_hpc

    assert HardwareOption.LOCAL.check_status_using_job_id() is None
    assert (
        HardwareOption.SLURM.check_status_using_job_id.__name__
        == slurm_cs.__name__
    )
    assert (
        HardwareOption.EAGLE.check_status_using_job_id.__name__
        == slurm_cs.__name__
    )
    assert (
        HardwareOption.PEREGRINE.check_status_using_job_id.__name__
        == pbs_cs.__name__
    )
    assert (
        HardwareOption.PBS.check_status_using_job_id.__name__
        == pbs_cs.__name__
    )

    assert (
        HardwareOption.SLURM.check_status_using_job_id.__module__
        == slurm_cs.__module__
    )
    assert (
        HardwareOption.EAGLE.check_status_using_job_id.__module__
        == slurm_cs.__module__
    )
    assert (
        HardwareOption.PEREGRINE.check_status_using_job_id.__module__
        == pbs_cs.__module__
    )
    assert (
        HardwareOption.PBS.check_status_using_job_id.__module__
        == pbs_cs.__module__
    )


def test_get_attr_flat_list():
    """Test _get_attr_flat_list"""

    assert _get_attr_flat_list([]) == []
    assert _get_attr_flat_list({}) == []
    assert _get_attr_flat_list({1, 2, 3}) == []
    assert _get_attr_flat_list((1, 2, 3)) == []
    assert _get_attr_flat_list({StatusField.JOB_ID: 5}) == [5]

    nested_status = {
        "run1": {StatusField.JOB_ID: 5},
        "run3": {StatusField.JOB_ID: 7},
    }
    assert _get_attr_flat_list(nested_status) == [5, 7]

    nested_status = {
        "run1": {"job_id2": [5, 6]},
        "run2": {"job_id2": [7, 8]},
        "run3": {"job_id2": 9},
    }
    assert _get_attr_flat_list(nested_status, key="job_id2") == [5, 6, 7, 8, 9]


def test_elapsed_time_as_str():
    """Test the `_elapsed_time_as_str` function."""
    assert _elapsed_time_as_str(0.569448) == "0:00:00"
    assert _elapsed_time_as_str(1.1) == "0:00:01"
    assert _elapsed_time_as_str(30) == "0:00:30"
    assert _elapsed_time_as_str(1 * 60 + 30) == "0:01:30"
    assert _elapsed_time_as_str(1 * 3600 + 1 * 60 + 30) == "1:01:30"
    assert _elapsed_time_as_str(23 * 3600 + 1 * 60 + 30) == "23:01:30"
    assert _elapsed_time_as_str(24 * 3600 + 1 * 60 + 30) == "1 day, 0:01:30"
    assert _elapsed_time_as_str(71 * 3600 + 1 * 60 + 30) == "2 days, 23:01:30"


def test_status_init(temp_job_dir):
    """Test initializing Status object"""
    tmp_path, status_fp = temp_job_dir
    status = Status(tmp_path)

    assert status._fpath == status_fp
    assert status.name == tmp_path.name
    assert not status.data

    status = Status(tmp_path.as_posix())
    assert status._fpath == status_fp
    assert status.name == tmp_path.name
    assert not status.data

    for file_type in ConfigType.members_as_str():
        with pytest.raises(gapsTypeError) as exc_info:
            Status(f"test.{file_type}")

        expected_msg = (
            f"Need a directory containing a status {file_type}, not a "
            f"status {file_type}:"
        )
        assert expected_msg in str(exc_info)


def test_status_job_ids(temp_job_dir):
    """Test test_status job_ids."""
    tmp_path, status_fp = temp_job_dir
    with open(status_fp, "w") as file_:
        json.dump(TEST_2_ATTRS_2, file_)
    status = Status(tmp_path)
    assert status.job_ids == [123]


@pytest.mark.parametrize("nested_dir", [False, True])
def test_status_dump(tmp_path, nested_dir):
    """Test Status dump functionality"""
    if nested_dir:
        status_dir = tmp_path / "nested" / "DNE"
    else:
        status_dir = tmp_path / "DNE"
    status = Status(status_dir)
    status.data = TEST_2_ATTRS_2

    assert "DNE" not in [f.name for f in status_dir.parent.glob("*")]

    status.dump()
    assert "DNE" in [f.name for f in status_dir.parent.glob("*")]
    expected_status_fn = NAMED_STATUS_FILE.format("DNE")
    assert expected_status_fn in [f.name for f in (status_dir).glob("*")]

    backup = expected_status_fn.replace(".json", "_backup.json")
    assert backup not in [f.name for f in (status_dir).glob("*")]

    status = Status(status_dir)
    assert status.data == TEST_2_ATTRS_2


def test_hardware_status_retriever():
    """Test `HardwareStatusRetriever` method"""
    hsr = HardwareStatusRetriever()

    assert hsr[None, "local"] is None
    assert hsr[1, "local"] is None

    with pytest.raises(gapsKeyError) as exc_info:
        __ = hsr[1, "DNE"]

    assert "Requested hardware" in str(exc_info.value)
    assert "not recognized!" in str(exc_info.value)

    hsr = HardwareStatusRetriever(HardwareOption.LOCAL)
    assert hsr[1, "local"] is None


def test_load_job_file(tmp_path):
    """Test _load_job_file function."""
    command, job_name = "run", "test1"
    status_fname = JOB_STATUS_FILE.format(job_name)
    assert status_fname not in [f.name for f in tmp_path.glob("*")]
    Status.make_single_job_file(tmp_path, command, job_name, TEST_1_ATTRS_1)
    assert status_fname in [f.name for f in tmp_path.glob("*")]

    status = _load_job_file(tmp_path, job_name, purge=False)
    assert status == {command: {job_name: TEST_1_ATTRS_1}}
    assert status_fname in [f.name for f in tmp_path.glob("*")]

    status2 = _load_job_file(tmp_path, job_name)
    assert status2 == {command: {job_name: TEST_1_ATTRS_1}}
    assert status_fname not in [f.name for f in tmp_path.glob("*")]

    Status.make_single_job_file(tmp_path, command, job_name, TEST_1_ATTRS_1)
    assert status_fname in [f.name for f in tmp_path.glob("*")]
    assert _load_job_file(tmp_path, "DNE") is None
    assert status_fname in [f.name for f in tmp_path.glob("*")]

    shutil.move(tmp_path / status_fname, tmp_path / "new_file_status.json")
    assert _load_job_file(tmp_path, job_name) is None
    assert "new_file_status.json" in [f.name for f in tmp_path.glob("*")]


@pytest.mark.parametrize("job_name", ["test1", "test1.h5"])
def test_make_file(temp_job_dir, job_name):
    """Test file creation and reading"""
    tmp_path, status_fp = temp_job_dir
    assert not list(tmp_path.glob("*"))

    assert Status.retrieve_job_status(tmp_path, "generation", "test1") is None

    Status.make_single_job_file(
        tmp_path, "generation", job_name, TEST_1_ATTRS_1
    )

    expected_fn = JOB_STATUS_FILE.format("test1")
    assert expected_fn in [f.name for f in tmp_path.glob("*")]

    status = Status.retrieve_job_status(tmp_path, "generation", "test1")
    assert expected_fn not in [f.name for f in tmp_path.glob("*")]
    assert status_fp in tmp_path.glob("*")

    assert status == "R", "Failed, status is {status!r}"

    status = Status.retrieve_job_status(tmp_path, "generation", "test1.h5")
    assert expected_fn not in [f.name for f in tmp_path.glob("*")]
    assert status_fp in tmp_path.glob("*")
    assert status == StatusOption.FAILED, f"Failed, status is {status!r}"

    for file_ in tmp_path.glob("*"):
        file_.unlink()
    Status.make_single_job_file(tmp_path, "generation", job_name, {})
    assert Status.retrieve_job_status(tmp_path, "generation", "test1") is None
    assert Status.retrieve_job_status(tmp_path, "generation", "test2") is None
    assert Status.retrieve_job_status(tmp_path, "run", "test1") is None


def test_update_from_all_job_files(temp_job_dir):
    """Test file creation and collection"""
    tmp_path, status_fp = temp_job_dir
    Status.make_single_job_file(
        tmp_path, "generation", "test1", TEST_1_ATTRS_1
    )
    Status.make_single_job_file(
        tmp_path, "generation", "test2", TEST_2_ATTRS_1
    )

    status = Status(tmp_path).update_from_all_job_files()
    status.dump()
    with open(status_fp, "r") as file_:
        data = json.load(file_)
    assert json.dumps(TEST_1_ATTRS_1) in json.dumps(data)
    assert json.dumps(TEST_2_ATTRS_1) in json.dumps(data)


def test_update_job_status(tmp_path, monkeypatch):
    """Test updating job status"""
    job_name = "test0"
    status = Status(tmp_path)
    status.data = {"run": {StatusField.PIPELINE_INDEX: 0}}
    assert status.data["run"].get(job_name) is None
    status.update_job_status("run", job_name)
    assert status.data["run"].get(job_name) == {}

    Status.make_single_job_file(
        tmp_path, "generation", "test1", TEST_1_ATTRS_1
    )
    Status.make_single_job_file(
        tmp_path, "generation", "test2", TEST_2_ATTRS_1
    )

    status = Status(tmp_path)
    status.update_job_status("run", "test")
    assert not status

    status.update_job_status("generation", "test0")
    assert not status

    status.update_job_status("generation", "test1")
    assert json.dumps(TEST_1_ATTRS_1) in json.dumps(status.data)
    assert json.dumps(TEST_2_ATTRS_1) not in json.dumps(status.data)

    status.update_job_status("generation", "test2")
    assert json.dumps(TEST_1_ATTRS_1) in json.dumps(status.data)
    assert json.dumps(TEST_2_ATTRS_1) in json.dumps(status.data)

    Status.make_single_job_file(
        tmp_path, "generation", "test1", TEST_1_ATTRS_2
    )
    status.update_job_status("generation", "test1")
    assert (
        status.data["generation"]["test1"][StatusField.JOB_STATUS]
        == StatusOption.SUCCESSFUL
    )
    new_attrs = {
        "job_name": "test1",
        StatusField.JOB_STATUS: "some status",
        StatusField.HARDWARE: "local",
    }
    Status.make_single_job_file(tmp_path, "generation", "test1", new_attrs)
    status.update_job_status("generation", "test1")
    assert (
        status.data["generation"]["test1"][StatusField.JOB_STATUS]
        == "some status"
    )

    monkeypatch.setattr(
        HardwareStatusRetriever,
        "__getitem__",
        lambda *__, **___: StatusOption.RUNNING,
        raising=True,
    )
    status.update_job_status("generation", "test1")
    assert (
        status.data["generation"]["test1"][StatusField.JOB_STATUS]
        == StatusOption.RUNNING
    )

    # test a repeated call to hardware
    status.update_job_status("generation", "test1")
    assert (
        status.data["generation"]["test1"][StatusField.JOB_STATUS]
        == StatusOption.RUNNING
    )


def test_status_reload(tmp_path):
    """Test re-loading data from disk."""

    status = Status(tmp_path)
    assert not status

    Status.mark_job_as_submitted(
        tmp_path, "run", "test1", job_attrs=TEST_1_ATTRS_1
    )
    assert not status
    status.reload()
    assert json.dumps(TEST_1_ATTRS_1) in json.dumps(status.data)


def test_job_exists(tmp_path):
    """Test job addition and exist check"""
    Status.mark_job_as_submitted(
        tmp_path,
        "generation",
        "test1",
        job_attrs={StatusField.JOB_STATUS: StatusOption.SUBMITTED},
    )
    assert Status.job_exists(tmp_path, "test1")
    assert Status.job_exists(tmp_path, "test1", "generation")
    assert Status.job_exists(tmp_path, "test1.h5", "generation")
    assert not Status.job_exists(tmp_path, "test1", "run")
    assert not Status.job_exists(tmp_path, "test2", "generation")
    assert not Status.job_exists(tmp_path, "test2.h5", "generation")


def test_single_job_exists(tmp_path):
    """Test single job addition and exist check"""
    Status.make_single_job_file(
        tmp_path,
        command="generation",
        job_name="test1",
        attrs={StatusField.JOB_STATUS: StatusOption.RUNNING},
    )
    assert Status.job_exists(tmp_path, "test1")
    assert Status.job_exists(tmp_path, "test1", "generation")
    assert Status.job_exists(tmp_path, "test1.h5", "generation")
    assert not Status.job_exists(tmp_path, "test1", "run")
    assert not Status.job_exists(tmp_path, "test2", "generation")
    assert not Status.job_exists(tmp_path, "test2.h5", "generation")


def test_mark_job_as_submitted(temp_job_dir):
    """Test job addition and exist check"""
    tmp_path, status_fp = temp_job_dir

    assert not status_fp.exists()
    Status.mark_job_as_submitted(tmp_path, "gen", "test1")
    assert status_fp.exists()

    status1 = Status(tmp_path).data["gen"]["test1"][StatusField.JOB_STATUS]
    assert status1 == StatusOption.SUBMITTED

    Status.mark_job_as_submitted(
        tmp_path,
        "gen",
        "test1",
        job_attrs={StatusField.JOB_STATUS: "finished", "additional": "test"},
        replace=False,
    )
    status2 = Status(tmp_path).data["gen"]["test1"][StatusField.JOB_STATUS]

    assert status2 == status1


def test_record_monitor_pid(tmp_path):
    """Test recording monitor job PID"""
    expected_pid_file = tmp_path / MONITOR_PID_FILE

    assert not expected_pid_file.exists()
    Status.record_monitor_pid(tmp_path, 1234)
    assert expected_pid_file.exists()

    status1 = Status(tmp_path).update_from_all_job_files()
    assert status1.data == {StatusField.MONITOR_PID: 1234}

    Status.mark_job_as_submitted(
        tmp_path,
        "gen",
        "test1",
        job_attrs={StatusField.JOB_STATUS: "finished", "additional": "test"},
        replace=False,
    )
    status1 = Status(tmp_path).update_from_all_job_files()
    assert status1.data != {StatusField.MONITOR_PID: 1234}
    assert status1.data[StatusField.MONITOR_PID] == 1234


def test_job_replacement(tmp_path):
    """Test job addition and replacement"""
    Status.mark_job_as_submitted(
        tmp_path,
        "generation",
        "test1",
        job_attrs={StatusField.JOB_STATUS: StatusOption.SUBMITTED},
    )
    job_data = Status(tmp_path).data["generation"]["test1"]
    status = job_data[StatusField.JOB_STATUS]
    assert status == StatusOption.SUBMITTED
    assert "addition" not in job_data

    with pytest.warns(gapsWarning):
        Status.mark_job_as_submitted(
            tmp_path,
            "generation",
            "test1",
            job_attrs={"addition": "test", StatusField.JOB_STATUS: "finished"},
            replace=True,
        )

    job_data = Status(tmp_path).data["generation"]["test1"]
    status = job_data[StatusField.JOB_STATUS]
    assert "addition" in job_data
    addition = job_data["addition"]
    assert status == StatusOption.SUBMITTED
    assert addition == "test"

    with pytest.warns(gapsWarning):
        Status.mark_job_as_submitted(
            tmp_path,
            "generation",
            "test1.h5",
            job_attrs={
                StatusField.HARDWARE: "eagle",
                StatusField.JOB_STATUS: StatusOption.SUBMITTED,
            },
            replace=True,
        )


def test_status_as_df(tmp_path):
    """Test converting status to a DataFrame"""
    status = Status(tmp_path)
    status_df = status.as_df()
    assert status_df.empty
    assert len(status_df.columns) > 0

    status.data = {"run": {StatusField.PIPELINE_INDEX: 0}}

    status_df = status.as_df()
    assert not status_df.empty
    assert len(status_df) == 1
    assert (
        status_df.loc["run", StatusField.JOB_STATUS]
        == StatusOption.NOT_SUBMITTED
    )

    Status.make_single_job_file(
        tmp_path, "generation", "job1", {StatusField.JOB_ID: "123456789"}
    )
    Status.make_single_job_file(tmp_path, "generation", "job2", {})
    assert len(list(tmp_path.glob("*.json"))) == 2

    status_df = Status(tmp_path).as_df()
    assert not status_df.empty
    assert len(list(tmp_path.glob("*.json"))) == 2
    assert not status_df[StatusField.JOB_STATUS].isna().any()

    assert (
        status_df[StatusField.JOB_STATUS] == StatusOption.NOT_SUBMITTED
    ).all()

    status_df = Status(tmp_path).as_df(commands=["DNE"])
    assert status_df.empty

    status_df = Status(tmp_path).as_df(commands=["generation", "DNE"])
    assert not status_df.empty

    started_job_attrs = {
        StatusField.TIME_START: dt.datetime.now().strftime(DT_FMT)
    }
    Status.make_single_job_file(
        tmp_path, "generation", "job3", started_job_attrs
    )
    status_df = Status(tmp_path).as_df()
    assert status_df[StatusField.TIME_END].isna().all()
    assert status_df.loc["job3"][StatusField.TIME_START]
    assert status_df.loc["job3"][StatusField.TOTAL_RUNTIME]
    assert status_df.loc["job3"][StatusField.TOTAL_RUNTIME].endswith("(r)")


def test_parse_command_status(tmp_path):
    """Test `parse_command_status` command"""
    Status.make_single_job_file(
        tmp_path, "generation", "test1", TEST_1_ATTRS_1
    )
    Status.make_single_job_file(
        tmp_path, "generation", "test2", TEST_2_ATTRS_1
    )

    assert len(list(tmp_path.glob("*"))) == 2

    job_names = Status.parse_command_status(
        tmp_path, "generation", key="job_name"
    )

    assert sorted(job_names) == ["test1", "test2"]
    assert len(list(tmp_path.glob("*"))) == 2

    assert not Status.parse_command_status(tmp_path, "generation", key="dne")


def test_status_updates(tmp_path):
    """Test `StatusUpdates` context manager"""

    assert not list(tmp_path.glob("*"))
    test_attrs = deepcopy(TEST_1_ATTRS_1)

    with StatusUpdates(tmp_path, "generation", "test0", TEST_1_ATTRS_1) as stu:
        job_files = list(tmp_path.glob("*"))
        assert len(job_files) == 1
        with open(job_files[0]) as job_status:
            status = json.load(job_status)

        assert "generation" in status
        assert "test0" in status["generation"]
        status = status["generation"]["test0"]
        assert status.get(StatusField.JOB_STATUS) == StatusOption.RUNNING
        assert StatusField.TIME_START in status
        assert StatusField.TIME_END not in status
        assert StatusField.TOTAL_RUNTIME not in status
        assert StatusField.RUNTIME_SECONDS not in status
        assert StatusField.OUT_FILE not in status

        stu.out_file = "my_test_file.h5"

    job_files = list(tmp_path.glob("*"))
    assert len(job_files) == 1
    with open(job_files[0]) as job_status:
        status = json.load(job_status)["generation"]["test0"]

    assert status.get(StatusField.JOB_STATUS) == StatusOption.SUCCESSFUL
    assert StatusField.TIME_START in status
    assert StatusField.TIME_END in status
    assert StatusField.TOTAL_RUNTIME in status
    assert StatusField.RUNTIME_SECONDS in status
    assert status.get(StatusField.OUT_FILE) == "my_test_file.h5"
    assert test_attrs == TEST_1_ATTRS_1


def test_status_for_failed_job(tmp_path):
    """Test `StatusUpdates` context manager for a failed job"""

    class _TestError(ValueError):
        """Test error class so that a real ValueError is not silenced."""

    assert not list(tmp_path.glob("*"))

    try:
        with StatusUpdates(tmp_path, "generation", "test0", TEST_1_ATTRS_1):
            status = Status(tmp_path)
            status.update_from_all_job_files()
            status = status["generation"]["test0"]

            assert status.get(StatusField.JOB_STATUS) == StatusOption.RUNNING
            assert StatusField.TIME_START in status
            assert StatusField.TIME_END not in status
            assert StatusField.TOTAL_RUNTIME not in status
            assert StatusField.RUNTIME_SECONDS not in status
            assert StatusField.OUT_FILE not in status
            raise _TestError
    except _TestError:
        pass

    status = Status(tmp_path).update_from_all_job_files()
    status = status["generation"]["test0"]

    assert status.get(StatusField.JOB_STATUS) == StatusOption.FAILED
    assert StatusField.TIME_START in status
    assert StatusField.TIME_END in status
    assert StatusField.TOTAL_RUNTIME in status
    assert StatusField.RUNTIME_SECONDS in status
    assert StatusField.OUT_FILE not in status


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
