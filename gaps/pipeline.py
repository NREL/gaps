# -*- coding: utf-8 -*-
"""
GAPs Pipeline architecture.
"""
import time
import logging
from pathlib import Path
from warnings import warn

from gaps.hpc import SLURM
from gaps.status import Status, StatusOption, StatusField
from gaps.utilities import recursively_update_dict
from gaps.config import load_config, init_logging_from_config
from gaps.exceptions import (
    gapsConfigError,
    gapsExecutionError,
    gapsKeyError,
    gapsValueError,
)
from gaps.warnings import gapsWarning


logger = logging.getLogger(__name__)


class Pipeline:
    """gaps pipeline execution framework."""

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

        if "pipeline" not in config:
            raise gapsConfigError(
                'Could not find required key "pipeline" in the '
                "pipeline config."
            )
        self._run_list = config["pipeline"]
        self._init_status()

        init_logging_from_config(config)

    def _init_status(self):
        """Initialize the status json in the output directory."""
        status = self.status
        for pipe_index, step in enumerate(self._run_list):
            for command in step.keys():
                command_dict = {
                    command: {StatusField.PIPELINE_INDEX: pipe_index}
                }
                status.data = recursively_update_dict(
                    status.data, command_dict
                )

        _dump_sorted(status)

    @property
    def status(self):
        """:class:`~gaps.status.Status`: A gaps pipeline status object."""
        return Status(self._out_dir).update_from_all_job_files(purge=False)

    @property
    def name(self):
        """str: Name of the pipeline job (directory of status file)."""
        return self.status.name

    def _cancel_all_jobs(self):
        """Cancel all jobs in this pipeline."""
        slurm_manager = SLURM()
        for job_id in self.status.job_ids:
            slurm_manager.cancel(job_id)
        logger.info("Pipeline job %r cancelled.", self.name)

    def _main(self):
        """Iterate through run list submitting steps while monitoring status"""

        for step, command in enumerate(self._run_list):
            step_status = self._status(step)

            if step_status == StatusOption.SUCCESSFUL:
                logger.debug("Successful: %r.", list(command.keys())[0])
                continue

            # the submit function handles individual job success/failure
            self._submit(step)

            # do not enter while loop for continuous monitoring
            if not self.monitor:
                break

            time.sleep(1)
            self._monitor(step)

        else:
            logger.info("Pipeline job %r is complete.", self.name)
            logger.debug("Output directory is: %r", self._out_dir)

    def _monitor(self, step, seconds=5, step_status=StatusOption.RUNNING):
        """Continuously monitor job until success or failure."""

        while step_status.is_processing:
            time.sleep(seconds)
            step_status = self._status(step)

            if step_status == StatusOption.FAILED:
                command, f_config = self._get_command_config(step)
                raise gapsExecutionError(
                    f"Pipeline failed at step {step}: {command!r} "
                    f"for {f_config!r}"
                )

    def _submit(self, step):
        """Submit a step in the pipeline."""

        command, f_config = self._get_command_config(step)
        if command not in self.COMMANDS:
            raise gapsKeyError(
                f"Could not recognize command {command!r}. "
                f"Available commands are: {set(self.COMMANDS)!r}"
            ) from None

        self.COMMANDS[command].callback(f_config)

    def _status(self, step):
        """Get a pipeline step status."""

        command, _ = self._get_command_config(step)
        status = self.status
        submitted = _check_jobs_submitted(status, command)
        if not submitted:
            return StatusOption.RUNNING

        return self._get_command_return_code(status, command)

    def _get_command_return_code(self, status, command):
        """Get a return code for a command based on a status object.

        Note that it is assumed a job has been submitted before this
        function is called, otherwise the return values make no sense!
        """

        # initialize return code array
        arr = []
        check_failed = False
        status.update_from_all_job_files()

        if command not in status.data:
            # assume running
            arr = [StatusOption.RUNNING]
        else:
            for job_name, job_info in status.data[command].items():
                if job_name == StatusField.PIPELINE_INDEX:
                    continue

                job_status = job_info[StatusField.JOB_STATUS]

                if job_status == "successful":
                    arr.append(StatusOption.SUCCESSFUL)
                elif job_status == "failed":
                    arr.append(StatusOption.FAILED)
                    check_failed = True
                elif job_status == "submitted":
                    arr.append(StatusOption.SUBMITTED)
                elif job_status == "running":
                    arr.append(StatusOption.RUNNING)
                elif job_status is None:
                    arr.append(StatusOption.COMPLETE)
                else:
                    msg = "Job status code {job_status!r} not understood!"
                    raise gapsValueError(msg)

            _dump_sorted(status)

        return_code = _parse_code_array(arr)

        fail_str = ""
        if return_code != StatusOption.FAILED and check_failed:
            fail_str = ", but some jobs have failed"
        logger.info(
            "CLI command %r for job %r %s%s.",
            command,
            self.name,
            return_code.with_verb,  # pylint: disable=no-member
            fail_str,
        )

        return return_code

    def _get_command_config(self, step):
        """Get the (command, config) key pair."""
        return list(self._run_list[step].items())[0]

    @classmethod
    def cancel_all(cls, pipeline):
        """Cancel all jobs corresponding to pipeline.

        Parameters
        ----------
        pipeline : path-like
            Pipeline config file path.
        """
        cls(pipeline)._cancel_all_jobs()

    @classmethod
    def run(cls, pipeline, monitor=True):
        """Run the pipeline.

        Parameters
        ----------
        pipeline : path-like
            Pipeline config file path.
        monitor : bool
            Flag to perform continuous monitoring of the pipeline.
        """
        cls(pipeline, monitor=monitor)._main()


def _parse_code_array(arr):
    """Parse array of return codes to get single return code for command."""

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


def _check_jobs_submitted(status, command):
    """Check whether jobs have been submitted for a given command."""
    return any(
        job != StatusField.PIPELINE_INDEX for job in status.data.get(command)
    )


def _dump_sorted(status):
    """Dump status dict after sorting on PIPELINE_INDEX."""
    pi_key = StatusField.PIPELINE_INDEX

    def _sort_key(status_entry):
        """Sort on pipeline index and key name, putting non-pipeline at top."""
        try:
            pipe_index = status[status_entry].get(pi_key, -1)
        except AttributeError:
            pipe_index = -1
        return pipe_index, status_entry

    sorted_keys = sorted(status, key=_sort_key)
    status.data = {k: status[k] for k in sorted_keys}
    status.dump()


def parse_previous_status(status_dir, command, key=StatusField.OUT_FILE):
    """Parse key from job status(es) from the previous pipeline step.

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

    for cmd, status in status.items():
        try:
            command_index = status.get(StatusField.PIPELINE_INDEX)
        except AttributeError:
            continue

        if str(index) == str(command_index):
            return Status.parse_command_status(status_dir, cmd, key)

    return []
