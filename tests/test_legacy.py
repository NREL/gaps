# -*- coding: utf-8 -*-
# pylint: disable=redefined-outer-name,protected-access,too-many-locals
"""
GAPs Legacy API tests.
"""
import types
from pathlib import Path

import pytest

from rex.utilities.loggers import init_logger
import gaps.hpc
from gaps.status import (
    JOB_STATUS_FILE,
    NAMED_STATUS_FILE,
    HardwareOption,
    StatusField,
    StatusOption,
)
from gaps.legacy import (
    HardwareStatusRetriever,
    Status,
    Pipeline,
    BatchJob,
    PipelineError,
)
from gaps.exceptions import gapsKeyError


TEST_ATTRS = {
    "job_name": "test1",
    StatusField.JOB_STATUS: "R",
    "run_id": 1234,
}


@pytest.fixture
def submit_call_cache():
    """Cache subprocess calls."""
    return []


@pytest.fixture
def cache_submit_call(monkeypatch, submit_call_cache):
    """Monkeypatch `gaps.hpc` to cache submit command instead of running it."""

    def _new_submit(cmd):
        submit_call_cache.append(cmd)
        return None, None

    monkeypatch.setattr(gaps.hpc, "submit", _new_submit, raising=True)
    return submit_call_cache


# pylint: disable=invalid-name,super-init-not-called,too-few-public-methods
class reVStyleTestPipeline(Pipeline):
    """Basic implementation of Pipeline as used by other repos."""

    CMD_BASE = "python -m reV.cli -c {fp_config} {command}"
    COMMANDS = ["run", "collect-run"]

    def __init__(self, pipeline, monitor=True, verbose=False):
        self.monitor = monitor
        self.verbose = verbose
        self._config = types.SimpleNamespace(
            name=pipeline.name,
            dirout=pipeline,  # cspell:disable-line
            hardware="local",
        )
        self._run_list = [
            {"run": "./config.json"},
            {"collect-run": "./collect_config.json"},
        ]
        self._init_status()
        init_logger("gaps", log_level="DEBUG")


@pytest.fixture
def temp_job_dir(tmp_path):
    """Create a temp dir and temp status filename for mock job directory."""
    return tmp_path, tmp_path / NAMED_STATUS_FILE.format(tmp_path.name)


def test_hardware_status_retriever(monkeypatch):
    """Test `HardwareStatusRetriever` method"""
    job_id_queries = []

    def capture_job_id(job_id):
        job_id_queries.append(job_id)

    monkeypatch.setattr(
        HardwareOption.SLURM,
        "check_status_using_job_id",
        capture_job_id,
        raising=True,
    )

    hsr = HardwareStatusRetriever()

    assert hsr[None, "local"] is None
    assert len(job_id_queries) == 0

    assert hsr[1, "local"] is None
    assert len(job_id_queries) == 1
    assert job_id_queries[-1] == 1

    hsr = HardwareStatusRetriever(
        hardware="PBS", subprocess_manager=HardwareOption.SLURM
    )

    assert hsr[None, "local"] is None
    assert hsr[123, "local"] is None
    assert len(job_id_queries) == 2
    assert job_id_queries[-1] == 123

    with pytest.raises(gapsKeyError) as exc_info:
        HardwareStatusRetriever("DNE")

    assert "Requested hardware" in str(exc_info.value)
    assert "not recognized!" in str(exc_info.value)


@pytest.mark.parametrize("job_name", ["test1", "test1.h5"])
def test_make_job_file_and_retrieve_job_status(temp_job_dir, job_name):
    """Test `make_job_file` and `retrieve_job_status` methods."""
    tmp_path, status_fp = temp_job_dir
    assert not list(tmp_path.glob("*"))

    # kwargs are a must in order to ensure legacy API in tact
    Status.make_job_file(
        status_dir=tmp_path,
        module="generation",
        job_name=job_name,
        attrs=TEST_ATTRS,
    )

    expected_fn = JOB_STATUS_FILE.format("test1")
    assert expected_fn in [f.name for f in tmp_path.glob("*")]

    status = Status.retrieve_job_status(
        status_dir=tmp_path, module="generation", job_name="test1"
    )
    assert expected_fn not in [f.name for f in tmp_path.glob("*")]
    assert status_fp in tmp_path.glob("*")

    assert status == "R", "Failed, status is {status!r}"

    status = Status.retrieve_job_status(
        status_dir=tmp_path, module="generation", job_name="test1.h5"
    )
    assert expected_fn not in [f.name for f in tmp_path.glob("*")]
    assert status_fp in tmp_path.glob("*")
    assert status == StatusOption.FAILED, f"Failed, status is {status!r}"

    for file_ in tmp_path.glob("*"):
        file_.unlink()
    Status.make_job_file(tmp_path, "generation", job_name, {})
    assert (
        Status.retrieve_job_status(
            status_dir=tmp_path, module="generation", job_name="test1"
        )
        is None
    )
    assert (
        Status.retrieve_job_status(
            status_dir=tmp_path, module="generation", job_name="test2"
        )
        is None
    )
    assert (
        Status.retrieve_job_status(
            status_dir=tmp_path, module="run", job_name="test1"
        )
        is None
    )


def test_add_job(temp_job_dir):
    """Test job addition and exist check"""
    tmp_path, status_fp = temp_job_dir
    assert not status_fp.exists()

    # kwargs are a must in order to ensure legacy API in tact
    Status.add_job(status_dir=tmp_path, module="gen", job_name="test1")
    assert status_fp.exists()

    status1 = Status(tmp_path).data["gen"]["test1"][StatusField.JOB_STATUS]
    assert status1 == StatusOption.SUBMITTED

    Status.add_job(
        status_dir=tmp_path,
        module="gen",
        job_name="test1",
        job_attrs={StatusField.JOB_STATUS: "finished", "additional": "test"},
        replace=False,
    )
    status2 = Status(tmp_path).data["gen"]["test1"][StatusField.JOB_STATUS]

    assert status2 == status1


def test_pipeline_init():
    """Test initializing the pipeline."""
    pipeline = reVStyleTestPipeline(Path.home())
    assert pipeline._out_dir == Path.home()
    assert pipeline._name == Path.home().name
    assert pipeline._hardware == "local"


def test_pipeline_submit(cache_submit_call):
    """Test `_submit` method."""
    pipeline = reVStyleTestPipeline(Path.home())
    pipeline._submit(0)
    formatted_cmd = reVStyleTestPipeline.CMD_BASE.format(
        command="run", fp_config="./config.json"
    )
    assert cache_submit_call[-1] == formatted_cmd


def test_pipeline_get_cmd():
    """Test the command formatting function."""
    pipeline = reVStyleTestPipeline(Path.home())
    with pytest.raises(KeyError):
        pipeline._get_cmd("DNE", "config.json")

    expected = reVStyleTestPipeline.CMD_BASE.format(
        fp_config="config.json", command="run"
    )
    assert pipeline._get_cmd("run", "config.json") == expected
    expected_verbose = " ".join([expected, "-v"])
    pipeline.verbose = True
    assert pipeline._get_cmd("run", "config.json") == expected_verbose


def test_pipeline_run(
    cache_submit_call, tmp_path, monkeypatch, assert_message_was_logged
):
    """Test the _run function."""
    reVStyleTestPipeline.run(tmp_path, monitor=False)
    assert len(cache_submit_call) == 1
    expected = reVStyleTestPipeline.CMD_BASE.format(
        fp_config="./config.json", command="run"
    )
    assert cache_submit_call[-1] == expected

    Status.mark_job_as_submitted(
        tmp_path,
        "run",
        "test",
        job_attrs={StatusField.JOB_ID: 0},
    )
    Status.make_single_job_file(
        tmp_path,
        command="run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    reVStyleTestPipeline.run(tmp_path, monitor=False)
    assert len(cache_submit_call) == 2
    expected = reVStyleTestPipeline.CMD_BASE.format(
        fp_config="./collect_config.json", command="collect-run"
    )
    assert cache_submit_call[-1] == expected

    Status.mark_job_as_submitted(
        tmp_path,
        "collect-run",
        "test",
        job_attrs={StatusField.JOB_ID: 1},
    )
    Status.make_single_job_file(
        tmp_path,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.FAILED},
    )
    monkeypatch.setattr(
        HardwareStatusRetriever,
        "__getitem__",
        lambda *__, **___: None,
        raising=True,
    )
    reVStyleTestPipeline.run(tmp_path, monitor=False)
    assert len(cache_submit_call) == 3
    expected = reVStyleTestPipeline.CMD_BASE.format(
        fp_config="./collect_config.json", command="collect-run"
    )
    assert cache_submit_call[-1] == expected
    Status.make_single_job_file(
        tmp_path,
        command="collect-run",
        job_name="test",
        attrs={StatusField.JOB_STATUS: StatusOption.SUCCESSFUL},
    )
    reVStyleTestPipeline.run(tmp_path, monitor=False)
    assert_message_was_logged("Pipeline job", "INFO")
    assert_message_was_logged("is complete", "INFO")
    assert_message_was_logged("Output directory is", "DEBUG")


def test_pipeline_logs_error(monkeypatch, tmp_path, assert_message_was_logged):
    """Test that pipeline logs error if subprocess error is raised."""

    def _new_submit(*__, **___):
        raise OSError("A message")

    monkeypatch.setattr(gaps.hpc, "submit", _new_submit, raising=True)

    with pytest.raises(OSError) as exc_info:
        reVStyleTestPipeline.run(tmp_path, monitor=False)

    assert "A message" in str(exc_info)

    assert_message_was_logged(
        "Pipeline subprocess submission returned an error", "ERROR"
    )


def test_pipeline_logs_warning(monkeypatch, assert_message_was_logged):
    """Test that pipeline logs error if subprocess error is raised."""

    def _new_submit(*__, **___):
        return None, "A sample err msg"

    monkeypatch.setattr(gaps.hpc, "submit", _new_submit, raising=True)

    reVStyleTestPipeline.run(Path.home(), monitor=False)
    assert_message_was_logged("Subprocess received stderr", "WARNING")


def test_batch_job_run(test_data_dir):
    """Test a legacy batch job run."""

    config_cache = []
    verbose_cache = []
    cancel_cache = []
    bg_cache = []

    class TestPipeline:
        """Test pipeline class."""

        @classmethod
        def run(cls, pipeline_config, monitor, verbose):
            """Test run method (legacy)"""
            assert not monitor
            config_cache.append(pipeline_config)
            verbose_cache.append(verbose)

        @classmethod
        def cancel_all(cls, pipeline_config):
            """Test cancel_all method (legacy)"""
            cancel_cache.append(pipeline_config)

    def save_bg_args(__, pipeline_config, verbose):
        """Save args to cache."""
        bg_cache.append((pipeline_config, verbose))

    class TestBatchJob(BatchJob):
        """Sample legacy batch job class."""

        PIPELINE_CLASS = TestPipeline
        PIPELINE_BACKGROUND_METHOD = save_bg_args

    batch_dir = test_data_dir / "batch_project_0"
    batch_fp = (batch_dir / "config_batch.json").as_posix()

    count_0 = len(list(batch_dir.glob("*")))
    assert count_0 == 8, "Unknown starting files detected!"
    TestBatchJob.run(batch_fp, dry_run=True)
    assert not config_cache
    assert not verbose_cache
    count_1 = len(list(batch_dir.glob("*")))
    assert count_1 > count_0, "Batch did not create new files."
    TestBatchJob.run(batch_fp, delete=True)
    count_2 = len(list(batch_dir.glob("*")))
    assert count_2 == count_0, "Batch did not clear all job files!"

    TestBatchJob.run(batch_fp, verbose=True)
    assert len(config_cache) == 9
    assert all(fp.endswith("config_pipeline.json") for fp in config_cache)
    assert len(verbose_cache) == 9
    assert all(verbose_cache)

    TestBatchJob.run(batch_fp, monitor_background=True, verbose=False)
    assert len(bg_cache) == 9
    assert all(arg[0].endswith("config_pipeline.json") for arg in bg_cache)
    assert not any(arg[1] for arg in bg_cache)

    (batch_dir / "set2_wthh110" / "config_pipeline.json").unlink()
    TestBatchJob.cancel_all(batch_fp, verbose=True)
    assert len(cancel_cache) == 8
    assert all(fp.endswith("config_pipeline.json") for fp in cancel_cache)

    class TestBatchJobNoNewFiles(TestBatchJob):
        """Sample legacy batch job class."""

        def _make_job_dirs(self, *__, **___):
            """Do not make any extra files."""
            return

    with pytest.raises(PipelineError) as exc_info:
        TestBatchJobNoNewFiles.run(batch_fp, verbose=True)

    assert "Could not find pipeline config to run" in str(exc_info)

    TestBatchJob.delete_all(batch_fp, verbose=True)
    count_3 = len(list(batch_dir.glob("*")))
    assert count_3 == count_0, "Batch did not clear all job files!"


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
