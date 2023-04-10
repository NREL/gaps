# -*- coding: utf-8 -*-
"""gaps Job status manager. """
import json
import logging
import time
import shutil
import datetime as dt
from pathlib import Path
from copy import deepcopy
from warnings import warn
from itertools import chain
from collections import UserDict, abc

import pandas as pd
import numpy as np

from rex.utilities import safe_json_load
from gaps.hpc import SLURM, PBS
from gaps.config import ConfigType
from gaps.utilities import recursively_update_dict, CaseInsensitiveEnum
from gaps.exceptions import gapsKeyError, gapsTypeError
from gaps.warnings import gapsWarning


logger = logging.getLogger(__name__)
JOB_STATUS_FILE = "jobstatus_{}.json"
MONITOR_PID_FILE = "monitor_pid.json"
NAMED_STATUS_FILE = "{}_status.json"
DT_FMT = "%d-%b-%Y %H:%M:%S"


class StatusField(CaseInsensitiveEnum):
    """A collection of required status fields in a status file."""

    JOB_ID = "job_id"
    JOB_STATUS = "job_status"
    PIPELINE_INDEX = "pipeline_index"
    HARDWARE = "hardware"
    OUT_FILE = "out_file"
    TIME_SUBMITTED = "time_submitted"
    TIME_START = "time_start"
    TIME_END = "time_end"
    TOTAL_RUNTIME = "total_runtime"
    RUNTIME_SECONDS = "runtime_seconds"
    MONITOR_PID = "monitor_pid"


class HardwareOption(CaseInsensitiveEnum):
    """A collection of hardware options."""

    LOCAL = "local"
    SLURM = "slurm"
    EAGLE = "eagle"
    PEREGRINE = "peregrine"
    PBS = "pbs"

    @classmethod
    def _new_post_hook(cls, obj, value):
        """Hook for post-processing after __new__"""
        obj.is_hpc = value != "local"
        if value in {"slurm", "eagle"}:
            obj.manager = SLURM()
            obj.check_status_using_job_id = (
                obj.manager.check_status_using_job_id
            )
        elif value in {"pbs", "peregrine"}:
            obj.manager = PBS()
            obj.check_status_using_job_id = (
                obj.manager.check_status_using_job_id
            )
        else:
            obj.manager = None
            obj.check_status_using_job_id = lambda *__, **___: None
        return obj


class StatusOption(CaseInsensitiveEnum):
    """A collection of job status options."""

    NOT_SUBMITTED = "not submitted"
    SUBMITTED = "submitted"
    RUNNING = "running"
    SUCCESSFUL = "successful"
    FAILED = "failed"
    COMPLETE = "complete"

    @classmethod
    def _new_post_hook(cls, obj, value):
        """Hook for post-processing after __new__"""
        verb = "has" if value == "failed" else "is"
        obj.with_verb = " ".join([verb, value])
        obj.is_processing = value in {"submitted", "running"}
        return obj


# pylint: disable=too-few-public-methods
class HardwareStatusRetriever:
    """Query hardware for job status."""

    def __init__(self, subprocess_manager=None):
        """Initialize `HardwareStatusRetriever`.

        Parameters
        ----------
        subprocess_manager : PBS | SLURM | None, optional
            Optional initialized subprocess manager to use to check job
            statuses. This can be input with cached queue data to avoid
            constantly querying the HPC. By default, `None`.
        """
        self.subprocess_manager = subprocess_manager

    def __getitem__(self, key):
        """Get the job status using pre-defined hardware-specific methods."""
        job_id, hardware = key
        if not job_id:
            return None

        if self.subprocess_manager is not None:
            return self.subprocess_manager.check_status_using_job_id(job_id)

        return _validate_hardware(hardware).check_status_using_job_id(job_id)


class Status(UserDict):
    """Base class for data pipeline health and status information.

    This class facilitates the creating of individual job status files
    which the main `Status` object can collect.
    """

    _DF_COLUMNS = [
        StatusField.JOB_STATUS.value,
        StatusField.PIPELINE_INDEX.value,
        StatusField.JOB_ID.value,
        StatusField.TIME_SUBMITTED.value,
        StatusField.TIME_START.value,
        StatusField.TIME_END.value,
        StatusField.TOTAL_RUNTIME.value,
        StatusField.HARDWARE.value,
    ]

    def __init__(self, status_dir):
        """Initialize `Status`.

        Parameters
        ----------
        status_dir : path-like
            Directory containing zero or more job json status files.
        """
        super().__init__()
        self.dir = Path(status_dir).expanduser().resolve()
        self._validate_dir()
        self.name = self.dir.name
        self._fpath = self.dir / NAMED_STATUS_FILE.format(self.name)
        self.data = _load(self._fpath)

    def _validate_dir(self):
        """Validate that the directory name is not a config file type."""
        for file_type in ConfigType.members_as_str():
            if self.dir.name.endswith(f".{file_type}"):
                raise gapsTypeError(
                    f"Need a directory containing a status {file_type}, "
                    f"not a status {file_type}: {self.dir!r}"
                )

    @property
    def job_ids(self):
        """list: Flat list of job ids."""
        return _get_attr_flat_list(self.data, key=StatusField.JOB_ID)

    def as_df(self, commands=None, index_name="job_name", include_cols=None):
        """Format status as pandas DataFrame.

        Parameters
        ----------
        commands : container, optional
            A container of command names to collect. If `None`, all
            commands in the status file are collected.
            By default, `None`.
        index_name : str, optional
            Name to assign to index of DataFrame.
            By default, `"job_name"`.

        Returns
        -------
        pd.DataFrame
            Pandas DataFrame containing status information.
        """
        include_cols = include_cols or []
        output_cols = self._DF_COLUMNS + list(include_cols)

        self.update_from_all_job_files(purge=False)
        if not self.data:
            return pd.DataFrame(columns=output_cols)

        requested_commands = commands or self.keys()
        commands = []
        for command, status in self.items():
            if command not in requested_commands:
                continue
            try:
                command_index = status.pop(StatusField.PIPELINE_INDEX, None)
            except (AttributeError, TypeError):
                continue
            if not status:
                status = {command: {}}
            try:
                command_df = pd.DataFrame(status).T
            except ValueError:
                continue
            command_df[f"{StatusField.PIPELINE_INDEX}"] = command_index
            commands.append(command_df)
        try:
            command_df = pd.concat(commands, sort=False)
        except ValueError:
            return pd.DataFrame(columns=output_cols)

        for field in chain(StatusField, include_cols):
            if field not in command_df.columns:
                command_df[f"{field}"] = np.nan

        command_df.loc[
            command_df[StatusField.JOB_STATUS].isna(),
            StatusField.JOB_STATUS.value,
        ] = StatusOption.NOT_SUBMITTED.value

        command_df = _add_elapsed_time(command_df)

        command_df.index.name = index_name
        command_df = command_df[output_cols]
        return command_df

    def reload(self):
        """Re-load the data from disk."""
        self.data = _load(self._fpath)

    def dump(self):
        """Dump status json w/ backup file in case process gets killed."""

        self._fpath.parent.mkdir(exist_ok=True)

        backup = self._fpath.name.replace(".json", "_backup.json")
        backup = self._fpath.parent / backup
        if self._fpath.exists():
            shutil.copy(self._fpath, backup)

        with open(self._fpath, "w") as status:
            json.dump(self.data, status, indent=4, separators=(",", ": "))

        backup.unlink(missing_ok=True)

    def update_from_all_job_files(self, purge=True):
        """Update status from all single-job job status files.

        This method loads all single-job status files in the target
        directory and updates the `Status` object with the single-job
        statuses.

        Parameters
        ----------
        purge : bool, optional
            Option to purge the individual status files.
            By default,`True`.

        Returns
        -------
        `Status`
            This instance of `Status` with updated job properties.
        """
        file_pattern = JOB_STATUS_FILE.format("*")
        for file_ in Path(self.dir).glob(file_pattern):
            status = _safe_load(file_, purge=purge)
            self.data = recursively_update_dict(self.data, status)

        monitor_pid_file = Path(self.dir) / MONITOR_PID_FILE
        if monitor_pid_file.exists():
            monitor_pid_info = _safe_load(monitor_pid_file, purge=purge)
            self.data.update(monitor_pid_info)

        if purge:
            self.dump()

        return self

    def update_job_status(
        self, command, job_name, hardware_status_retriever=None
    ):
        """Update single-job job status from single-job job status file.

        If the status for a given command/job name combination is not
        found, the status object remains unchanged.

        Parameters
        ----------
        command : str
            CLI command that the job belongs to.
        job_name : str
            Unique job name identification.
        hardware_status_retriever : `HardwareStatusRetriever`, optional
            Hardware status retriever. By default, `None`, which creates
            an instance internally.
        """
        hardware_status_retriever = (
            hardware_status_retriever or HardwareStatusRetriever()
        )
        # look for job completion file.
        current = _load_job_file(self.dir, job_name)

        # Update status data dict and file if job file was found
        if current is not None:
            self.data = recursively_update_dict(self.data, current)
            self.dump()

        # check job status via hardware if job file not found.
        elif command in self.data:
            # job exists
            if job_name in self.data[command]:
                job_data = self.data[command][job_name]

                # init defaults in case job/command not in status file yet
                previous = job_data.get(StatusField.JOB_STATUS, None)
                job_id = job_data.get(StatusField.JOB_ID, None)
                job_hardware = job_data.get(StatusField.HARDWARE, None)

                # get job status from hardware
                current = hardware_status_retriever[job_id, job_hardware]

                # No current status and job was not successful: failed!
                if current is None and previous != StatusOption.SUCCESSFUL:
                    job_data[StatusField.JOB_STATUS] = StatusOption.FAILED

                # do not overwrite a successful or failed job status.
                elif (
                    current != previous
                    and previous not in StatusOption.members_as_str()
                ):
                    job_data[StatusField.JOB_STATUS] = current

            # job does not yet exist
            else:
                self.data[command][job_name] = {}

    def _retrieve_job_status(
        self, command, job_name, hardware_status_retriever
    ):
        """Update and retrieve job status."""
        if job_name.endswith(".h5"):
            job_name = job_name.replace(".h5", "")

        self.update_job_status(command, job_name, hardware_status_retriever)

        try:
            job_data = self[command][job_name]
        except KeyError:
            return None

        return job_data.get(StatusField.JOB_STATUS)

    @staticmethod
    def record_monitor_pid(status_dir, pid):
        """Make a json file recording the PID of the monitor process.

        Parameters
        ----------
        status_dir : path-like
            Directory to put json status file.
        pid : int
            PID of the monitoring process.
        """
        pid = {StatusField.MONITOR_PID: pid}
        fpath = Path(status_dir) / MONITOR_PID_FILE
        with open(fpath, "w") as pid_file:
            json.dump(
                pid,
                pid_file,
                sort_keys=True,
                indent=4,
                separators=(",", ": "),
            )

    @staticmethod
    def make_single_job_file(status_dir, command, job_name, attrs):
        """Make a json file recording the status of a single job.

        This method should primarily be used by HPC nodes to mark the
        status of individual jobs.

        Parameters
        ----------
        status_dir : path-like
            Directory to put json status file.
        command : str
            CLI command that the job belongs to.
        job_name : str
            Unique job name identification.
        attrs : dict
            Dictionary of job attributes that represent the job status
            attributes.
        """
        if job_name.endswith(".h5"):
            job_name = job_name.replace(".h5", "")

        status = {command: {job_name: attrs}}
        fpath = Path(status_dir) / JOB_STATUS_FILE.format(job_name)
        with open(fpath, "w") as job_status:
            json.dump(
                status,
                job_status,
                sort_keys=True,
                indent=4,
                separators=(",", ": "),
            )

    @classmethod
    def mark_job_as_submitted(
        cls, status_dir, command, job_name, replace=False, job_attrs=None
    ):
        """Mark a job in the status json as "submitted".

        Status json does not have to exist - it is created if missing.
        If it exists, the job will only be updated if it does not
        exist (i.e. not submitted), unless replace is set to `True`.

        Parameters
        ----------
        status_dir : path-like
            Directory containing json status file.
        command : str
            CLI command that the job belongs to.
        job_name : str
            Unique job name identification.
        replace : bool, optional
            Flag to force replacement of pre-existing job status.
            By default, `False`.
        job_attrs : dict, optional
            Job attributes. Should include 'job_id' if running on HPC.
            By default, `None`.
        """
        if job_name.endswith(".h5"):
            job_name = job_name.replace(".h5", "")

        obj = cls(status_dir)

        job_attrs = job_attrs or {}
        hardware = job_attrs.get(StatusField.HARDWARE)
        try:
            job_on_hpc = HardwareOption(hardware).is_hpc
        except ValueError:
            job_on_hpc = False

        if job_on_hpc and StatusField.JOB_ID not in job_attrs:
            msg = (
                f'Key "job_id" should be in kwargs for {job_name!r} if '
                f"adding job from an HPC node."
            )
            warn(msg, gapsWarning)

        exists = obj.job_exists(status_dir, job_name, command=command)

        if exists and not replace:
            return

        job_status = job_attrs.get(StatusField.JOB_STATUS)
        if job_status != StatusOption.SUBMITTED:
            if job_status is not None:
                msg = (
                    f"Attempting to mark a job as submitted but included a "
                    f"{StatusField.JOB_STATUS} value of {job_status!r} in the "
                    f"job_attrs dictionary! Setting the job status to "
                    f"{StatusOption.SUBMITTED!r} before writing."
                )
                warn(msg, gapsWarning)
            job_attrs[StatusField.JOB_STATUS] = StatusOption.SUBMITTED

        if command not in obj.data:
            obj.data[command] = {job_name: job_attrs}
        else:
            obj.data[command][job_name] = job_attrs

        obj.dump()

    @classmethod
    def job_exists(cls, status_dir, job_name, command=None):
        """Check whether a job exists and return a bool.

        This method will return `True` if the job name is found as a
        key in the dictionary under the `command` keyword, or any
        command if a `None` value is passed.

        Parameters
        ----------
        status_dir : str
            Directory containing json status file.
        job_name : str
            Unique job name identification.
        command : str, optional
            CLI command that the job belongs to. By default, `None`,
            which checks all commands for the job name.

        Returns
        -------
        exists : bool
            `True` if the job exists in the status json.
        """
        if job_name.endswith(".h5"):
            job_name = job_name.replace(".h5", "")

        obj = cls(status_dir).update_from_all_job_files(purge=False)
        if not obj.data:
            return False
        if command is not None:
            jobs = [obj.data.get(command)]
        else:
            jobs = obj.data.values()

        for job in jobs:
            if not job:
                continue
            for name in job.keys():
                if name == job_name:
                    return True

        return False

    @classmethod
    def retrieve_job_status(
        cls,
        status_dir,
        command,
        job_name,
        subprocess_manager=None,
    ):
        """Update and retrieve job status.

        Parameters
        ----------
        status_dir : str
            Directory containing json status file.
        command : str
            CLI command that the job belongs to.
        job_name : str
            Unique job name identification.
        subprocess_manager : None | SLURM, optional
            Optional initialized subprocess manager to use to check job
            statuses. This can be input with cached queue data to avoid
            constantly querying the HPC.

        Returns
        -------
        status : str | None
            Status string or `None` if job/command not found.
        """
        hsr = HardwareStatusRetriever(subprocess_manager)
        return cls(status_dir)._retrieve_job_status(command, job_name, hsr)

    @classmethod
    def parse_command_status(
        cls,
        status_dir,
        command,
        key=StatusField.OUT_FILE,
    ):
        """Parse key from job status(es) from the given command.

        Parameters
        ----------
        status_dir : path-like
            Directory containing the status file to parse.
        command : str
            Target CLI command to parse.
        key : StatusField | str, optional
            Parsing target of previous command. By default,
            `StatusField.OUT_FILE`.

        Returns
        -------
        list
            Arguments parsed from the status file in status_dir from
            the input command. This list is empty if the `key` is not
            found in the job status, or if the command does not exist in
            status.
        """
        status = cls(status_dir).update_from_all_job_files(purge=False)
        command_status = status.get(command, {})
        command_status.pop(StatusField.PIPELINE_INDEX, None)
        return _get_attr_flat_list(command_status, key=key)


class StatusUpdates:
    """Context manager to track run function progress.

    When this context is entered, a status file is written for the given
    command/job combination, with the given job attributes. The job
    status is set to "running", and the start time is recorded. When the
    context is exited, another status file is written with the end time
    and total runtime values added. The status is also set to
    "successful", unless an uncaught exception was raised during the
    function runtime, in which case the status is set to "failed". If
    the `out_file` attribute of this context manager is set before the
    context is exited, that value is also written to the status file.
    """

    def __init__(self, directory, command, job_name, job_attrs):
        """Initialize `StatusUpdates`.

        Parameters
        ----------
        directory : path-like
            Directory to write status files to.
        command : str
            Name of the command being run.
        job_name : str
            Name of the job being run.
        job_attrs : dict
            A dictionary containing job attributes that should be
            written to the status file.
        """
        self.directory = directory
        self.command = command
        self.job_name = job_name
        self.job_attrs = deepcopy(job_attrs)
        self.start_time = None
        self.out_file = None

    def __enter__(self):
        self.start_time = dt.datetime.now()
        self.job_attrs.update(
            {
                StatusField.JOB_STATUS: StatusOption.RUNNING,
                StatusField.TIME_START: self.start_time.strftime(DT_FMT),
            }
        )
        Status.make_single_job_file(
            self.directory,
            command=self.command,
            job_name=self.job_name,
            attrs=self.job_attrs,
        )
        return self

    def __exit__(self, exc_type, exc, traceback):
        end_time = dt.datetime.now()
        time_elapsed_s = (end_time - self.start_time).total_seconds()
        time_elapsed = _elapsed_time_as_str(time_elapsed_s)
        self.job_attrs.update(
            {
                StatusField.TIME_END: end_time.strftime(DT_FMT),
                StatusField.TOTAL_RUNTIME: time_elapsed,
                StatusField.RUNTIME_SECONDS: time_elapsed_s,
            }
        )
        if exc is None:
            self.job_attrs.update(
                {
                    StatusField.OUT_FILE: self.out_file,
                    StatusField.JOB_STATUS: StatusOption.SUCCESSFUL,
                }
            )
        else:
            self.job_attrs.update(
                {StatusField.JOB_STATUS: StatusOption.FAILED}
            )
        Status.make_single_job_file(
            self.directory,
            command=self.command,
            job_name=self.job_name,
            attrs=self.job_attrs,
        )


def _elapsed_time_as_str(seconds_elapsed):
    """Format elapsed time into human readable string"""
    days, seconds = divmod(int(seconds_elapsed), 24 * 3600)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    time_str = f"{hours:d}:{minutes:02d}:{seconds:02d}"
    if days:
        time_str = f"{days:d} day{'s' if abs(days) != 1 else ''}, {time_str}"
    return time_str


def _add_elapsed_time(status_df):
    """Add elapsed time to status DataFrame"""
    mask = (
        ~status_df[StatusField.TIME_START].isna()
        & status_df[StatusField.TIME_END].isna()
    )
    start_times = status_df.loc[mask, StatusField.TIME_START]
    start_times = pd.to_datetime(start_times, format=DT_FMT)
    elapsed_times = dt.datetime.now() - start_times
    elapsed_times = elapsed_times.apply(lambda dt: dt.total_seconds())
    elapsed_times = elapsed_times.apply(_elapsed_time_as_str)
    elapsed_times = elapsed_times.apply(lambda time_str: f"{time_str} (r)")
    status_df.loc[mask, StatusField.TOTAL_RUNTIME] = elapsed_times
    return status_df


def _load(fpath):
    """Load status json."""
    if fpath.is_file():
        return safe_json_load(fpath.as_posix())
    return {}


def _load_job_file(status_dir, job_name, purge=True):
    """Load a single-job job status file in the target status_dir."""
    status_dir = Path(status_dir)
    status_fname = status_dir / JOB_STATUS_FILE.format(job_name)
    if status_fname not in status_dir.glob("*"):
        return None
    return _safe_load(status_fname, purge=purge)


def _safe_load(file_path, purge=True):
    """Safe load json file and purge if needed."""
    # wait one second to make sure file is finished being written
    time.sleep(0.01)
    status = safe_json_load(file_path.as_posix())
    if purge:
        file_path.unlink()
    return status


def _get_attr_flat_list(inp, key=StatusField.JOB_ID):
    """Get all job attribute values from the status data dict."""

    out = []
    if not isinstance(inp, abc.Mapping):
        return out

    if key in inp:
        out = _combine_iterables_and_numerics(out, inp[key])
    else:
        for val in inp.values():
            temp = _get_attr_flat_list(val, key=key)
            out = _combine_iterables_and_numerics(out, temp)

    return out


def _combine_iterables_and_numerics(iterable, new_vals):
    """Combine new_vals into the "iterable" list"""
    try:
        return iterable + new_vals
    except TypeError:
        pass

    return iterable + [new_vals]


def _validate_hardware(hardware):
    """Verify that the selected hardware is a valid option."""
    try:
        return HardwareOption(hardware)
    except ValueError as err:
        msg = (
            f"Requested hardware ({hardware!r}) not recognized! "
            f"Available options are: {HardwareOption.members_as_str()}."
        )
        raise gapsKeyError(msg) from err
