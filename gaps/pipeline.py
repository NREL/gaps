"""GAPs Pipeline architecture"""

import time
import logging
from pathlib import Path
from warnings import warn

from gaps.status import Status, StatusOption, StatusField, HardwareOption
from gaps.utilities import recursively_update_dict
from gaps.config import load_config
from gaps.exceptions import (
    gapsConfigError,
    gapsExecutionError,
    gapsKeyError,
    gapsValueError,
)
from gaps.warn import gapsWarning


logger = logging.getLogger(__name__)


class PipelineStep:
    """A Pipeline Config step"""

    COMMAND_KEY = "command"
    _KEYS_PER_STEP = 2

    def __init__(self, step_dict):
        self.name = self.config_path = self._command = None
        self._parse_step_dict(step_dict.copy())

    def _parse_step_dict(self, step_dict):
        """Parse the input step dictionary into name, command, and fp"""
        if len(step_dict) > self._KEYS_PER_STEP:
            msg = (
                f"Pipeline step dictionary can have at most two keys. Got: "
                f"{step_dict}"
            )
            raise gapsConfigError(msg)

        if len(step_dict) > 1 and self.COMMAND_KEY not in step_dict:
            msg = (
                f"The only extra key allowed in pipeline step dictionary "
                f"is {self.COMMAND_KEY!r}. Got dictionary: {step_dict}"
            )
            raise gapsConfigError(msg)

        self._command = step_dict.pop(self.COMMAND_KEY, None)
        self.name, self.config_path = next(iter(step_dict.items()))

    @property
    def command(self):
        """Pipeline command to call"""
        if self._command is None:
            self._command = self.name
        return self._command


class Pipeline:
    """gaps pipeline execution framework"""

    COMMANDS = {}

    def __init__(self, pipeline, monitor=True):
        """

        Parameters
        ----------
        pipeline : path-like
            Pipeline config file path.
        monitor : bool, optional
            Flag to perform continuous monitoring of the pipeline.
            By default, ``True``.

        Raises
        ------
        gapsConfigError
            If "pipeline" key not in config file.
        """
        self.monitor = monitor
        self._out_dir = Path(pipeline).expanduser().parent.resolve()
        self._out_dir = self._out_dir.as_posix()

        config = load_config(pipeline)
        self._run_list = _check_pipeline(config)
        self._init_status()

    def _init_status(self):
        """Initialize the status json in the output directory"""
        status = self.status
        self._run_list = [
            PipelineStep(step_dict) for step_dict in self._run_list
        ]
        for pipe_index, pipe_step in enumerate(self._run_list):
            step_dict = {
                pipe_step.name: {StatusField.PIPELINE_INDEX: pipe_index}
            }
            status.data = recursively_update_dict(status.data, step_dict)

        _dump_sorted(status)

    @property
    def status(self):
        """:class:`~gaps.status.Status`: A pipeline status object"""
        return Status(self._out_dir).update_from_all_job_files(purge=False)

    @property
    def name(self):
        """str: Name of the pipeline job (directory of status file)"""
        return self.status.name

    def _cancel_all_jobs(self):
        """Cancel all jobs in this pipeline"""
        status = self.status
        for job_id, hardware in zip(status.job_ids, status.job_hardware):
            if job_id is None:
                continue

            manager = HardwareOption(hardware).manager
            if manager is None:
                continue

            manager.cancel(job_id)

        logger.info("Pipeline job %r cancelled.", self.name)

    def _main(self):
        """Submitting run steps while monitoring status"""

        for step, pipe_step in enumerate(self._run_list):
            step_status = self._status(step)

            if step_status == StatusOption.SUCCESSFUL:
                logger.debug("Successful: %r.", pipe_step.name)
                continue

            # the submit function handles individual job success/failure
            self._submit(step)

            # do not enter while loop for continuous monitoring
            if not self.monitor:
                break

            time.sleep(1)
            try:
                self._monitor(step)
            except Exception:
                self._cancel_all_jobs()
                raise

        else:
            logger.info("Pipeline job %r is complete.", self.name)
            logger.debug("Output directory is: %r", self._out_dir)

    def _monitor(self, step, seconds=5, step_status=StatusOption.RUNNING):
        """Continuously monitor job until success or failure"""

        while step_status.is_processing:
            time.sleep(seconds)
            HardwareOption.reset_all_cached_queries()
            step_status = self._status(step)

            if step_status == StatusOption.FAILED:
                pipe_step = self._run_list[step]
                msg = (
                    f"Pipeline failed at step {step}: {pipe_step.name!r} "
                    f"for {pipe_step.config_path!r}"
                )
                raise gapsExecutionError(msg)

    def _submit(self, step):
        """Submit a step in the pipeline"""

        pipe_step = self._run_list[step]
        if pipe_step.command not in self.COMMANDS:
            msg = (
                f"Could not recognize command {pipe_step.command!r}. "
                f"Available commands are: {set(self.COMMANDS)!r}"
            )
            raise gapsKeyError(msg) from None

        self.COMMANDS[pipe_step.command].callback(
            pipe_step.config_path, pipeline_step=pipe_step.name
        )

    def _status(self, step):
        """Get a pipeline step status"""

        pipe_step = self._run_list[step]
        status = self.status
        submitted = _check_jobs_submitted(status, pipe_step.name)
        if not submitted:
            return StatusOption.NOT_SUBMITTED

        return self._get_step_return_code(status, pipe_step.name)

    def _get_step_return_code(self, status, step_name):  # noqa: C901
        """Get a return code for a pipeline step based on status object

        Note that it is assumed a job has been submitted before this
        function is called, otherwise the return values make no sense!
        """

        # initialize return code array
        arr = []
        check_failed = False
        status.update_from_all_job_files(check_hardware=True)

        if step_name not in status.data:
            # assume running
            arr = [StatusOption.RUNNING]
        else:
            for job_name, job_info in status.data[step_name].items():
                if job_name == StatusField.PIPELINE_INDEX:
                    continue

                job_status = job_info.get(StatusField.JOB_STATUS)

                if job_status == "successful":
                    arr.append(StatusOption.SUCCESSFUL)
                elif job_status == "failed":
                    arr.append(StatusOption.FAILED)
                    check_failed = True
                elif job_status == "submitted":
                    arr.append(StatusOption.SUBMITTED)
                elif job_status == "not submitted":
                    arr.append(StatusOption.NOT_SUBMITTED)
                elif job_status == "running":
                    arr.append(StatusOption.RUNNING)
                elif job_status is None:
                    arr.append(StatusOption.COMPLETE)
                else:
                    msg = f"Job status code {job_status!r} not understood!"
                    raise gapsValueError(msg)

            _dump_sorted(status)

        return_code = _parse_code_array(arr)

        fail_str = ""
        if return_code != StatusOption.FAILED and check_failed:
            fail_str = ", but some jobs have failed"
        logger.info(
            "Pipeline step %r for job %r %s%s. (%s)",
            step_name,
            self.name,
            return_code.with_verb,
            fail_str,
            time.ctime(),
        )

        return return_code

    @classmethod
    def cancel_all(cls, pipeline):
        """Cancel all jobs corresponding to pipeline

        Parameters
        ----------
        pipeline : path-like
            Pipeline config file path.
        """
        cls(pipeline)._cancel_all_jobs()  # noqa: SLF001

    @classmethod
    def run(cls, pipeline, monitor=True):
        """Run the pipeline

        Parameters
        ----------
        pipeline : path-like
            Pipeline config file path.
        monitor : bool
            Flag to perform continuous monitoring of the pipeline.
        """
        cls(pipeline, monitor=monitor)._main()  # noqa: SLF001


def _check_pipeline(config):
    """Check pipeline steps input"""

    if "pipeline" not in config:
        msg = 'Could not find required key "pipeline" in the pipeline config.'
        raise gapsConfigError(msg)

    pipeline = config["pipeline"]

    if not isinstance(pipeline, list):
        msg = (
            'Config arg "pipeline" must be a list of '
            f"{{command: f_config}} pairs, but received {type(pipeline)}."
        )
        raise gapsConfigError(msg)

    step_names = set()
    duplicate_names = []
    for pipe_step in pipeline:
        pipe_step = PipelineStep(pipe_step)  # noqa: PLW2901

        if pipe_step.name in step_names:
            duplicate_names.append(pipe_step.name)
        step_names.add(pipe_step.name)

        if not Path(pipe_step.config_path).expanduser().resolve().exists():
            msg = (
                "Pipeline step depends on non-existent "
                f"file: {pipe_step.config_path}"
            )
            raise gapsConfigError(msg)

    if duplicate_names:
        msg = (
            f"Pipeline contains duplicate step names: {duplicate_names}. "
            "Please specify unique step names for all steps (use the "
            "'command' key to specify duplicate commands to execute)"
        )
        raise gapsConfigError(msg)

    return pipeline


def _parse_code_array(arr):
    """Parse array of return codes to get single return code for step"""

    # check to see if all have completed, or any have failed
    all_successful = all(status == StatusOption.SUCCESSFUL for status in arr)
    all_completed = not any(status.is_processing for status in arr)
    any_failed = any(status == StatusOption.FAILED for status in arr)
    any_submitted = any(status == StatusOption.SUBMITTED for status in arr)

    # only return success if all have succeeded.
    if all_successful:
        return StatusOption.SUCCESSFUL
    # Only return failed when all have finished.
    if all_completed and any_failed:
        return StatusOption.FAILED
    # only return complete when all have completed
    # (but some should have succeeded or failed)
    if all_completed:
        return StatusOption.COMPLETE
    # only return "running" if all jobs running, else return "submitted"
    if any_submitted:
        return StatusOption.SUBMITTED
    # otherwise, all jobs are still running
    return StatusOption.RUNNING


def _check_jobs_submitted(status, step_name):
    """Check whether jobs have been submitted for a pipeline step"""
    return any(
        job != StatusField.PIPELINE_INDEX for job in status.data.get(step_name)
    )


def _dump_sorted(status):
    """Dump status dict after sorting on PIPELINE_INDEX"""
    pi_key = StatusField.PIPELINE_INDEX

    def _sort_key(status_entry):
        """Sort on pipeline index and key name; non-pipeline at top"""
        try:
            pipe_index = status[status_entry].get(pi_key, -1)
        except AttributeError:
            pipe_index = -1
        return pipe_index, status_entry

    sorted_keys = sorted(status, key=_sort_key)
    status.data = {k: status[k] for k in sorted_keys}
    status.dump()


def parse_previous_status(status_dir, command, key=StatusField.OUT_FILE):
    """Parse key from job status(es) from the previous pipeline step

    Parameters
    ----------
    status_dir : path-like
        Directory containing the status file to parse.
    command : str
        Current CLI command (i.e. current pipeline step).
    key : `StatusField` | str, optional
        Parsing target of previous command. By default,
        `StatusField.OUT_FILE`.

    Returns
    -------
    out : list
        Arguments parsed from the status file in status_dir from
        the command preceding the input command arg. This list is
        empty if the `key` is not found in the job status, or if
        the previous step index does not exist in status.

    Raises
    ------
    gapsKeyError
        If ``command`` not in status.

    Warnings
    --------
    This command **DOES NOT** check the HPC queue for jobs and therefore
    **DOES NOT** update the status of previously running jobs that have
    errored out of the HPC queue. For best results, ensure that all
    previous steps of a pipeline have finished processing before calling
    this function.

    This command will not function properly for pipelines with duplicate
    command calls (i.e. multiple collect calls under different names,
    etc.).
    """

    status = Status(status_dir).update_from_all_job_files(purge=False)

    if (
        command not in status
        or StatusField.PIPELINE_INDEX not in status[command]
    ):
        msg = (
            f"Could not parse data for command {command!r} from status "
            f"file in {status_dir!r}"
        )
        raise gapsKeyError(msg)

    index = int(status.data[command][StatusField.PIPELINE_INDEX]) - 1

    if index < 0:
        index = 0
        msg = (
            f"CLI command {command!r} is attempting to parse a previous "
            f"pipeline step, but it appears to be the first step. "
            f"Attempting to parse data from {command!r}."
        )
        warn(msg, gapsWarning)

    for cmd, cmd_status in status.items():
        try:
            command_index = cmd_status.get(StatusField.PIPELINE_INDEX)
        except AttributeError:
            continue

        if str(index) == str(command_index):
            return Status.parse_step_status(status_dir, cmd, key)

    return []
