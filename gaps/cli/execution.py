# -*- coding: utf-8 -*-
"""
GAPs execution CLI utils.
"""
import logging
import datetime as dt
from warnings import warn
from inspect import signature

from gaps.hpc import submit
from gaps.status import (
    DT_FMT,
    Status,
    HardwareOption,
    StatusOption,
    StatusField,
)
from gaps.warnings import gapsWarning
from gaps.exceptions import gapsConfigError

logger = logging.getLogger(__name__)


def kickoff_job(ctx, cmd, exec_kwargs):
    """Kickoff a single job (a single command execution).

    Parameters
    ----------
    ctx : click.Context
        Context object with a `.obj` attribute that contains at least
        the following keys:

            NAME : str
                Job name.
            OUT_DIR : path-like
                Path to output directory.
            COMMAND_NAME : str
                Name of command being run.

    cmd : str
        String form of command to kickoff.
    exec_kwargs : dict
        Keyword-value pairs to pass to the respective `submit` function.
        These will be filtered, so they may contain extra values. If
        some required inputs are missing from this dictionary, a
        `gapsConfigError` is raised.

    Raises
    ------
    gapsConfigError
        If `exec_kwargs` is missing some arguments required by the
        respective `submit` function.
    """
    hardware_option = HardwareOption(exec_kwargs.pop("option", "local"))
    if hardware_option.manager is None:
        _kickoff_local_job(ctx, cmd)
        return

    ctx.obj["MANAGER"] = hardware_option.manager
    exec_kwargs = _filter_exec_kwargs(
        exec_kwargs, hardware_option.manager.make_script_str
    )
    _kickoff_hpc_job(ctx, cmd, **exec_kwargs)


def _filter_exec_kwargs(kwargs, func):
    """Filter out extra keywords and raise error if any are missing."""
    sig = signature(func)
    kwargs_to_use = {
        k: v for k, v in kwargs.items() if k in sig.parameters.keys()
    }
    extra_keys = set(kwargs) - set(kwargs_to_use)
    if extra_keys:
        msg = (
            f"Found extra keys in 'execution_control'! The following "
            f"inputs will be ignored: {extra_keys}"
        )
        warn(msg, gapsWarning)

    required = {
        name for name, p in sig.parameters.items() if p.default == p.empty
    }
    required -= {"self", "cmd", "name"}
    missing = {k for k in required if k not in kwargs_to_use}
    if missing:
        msg = (
            f"The 'execution_control' block is missing the following "
            f"required keys: {missing}"
        )
        raise gapsConfigError(msg)

    return kwargs_to_use


def _kickoff_local_job(ctx, cmd):
    """Run a job (command) locally."""

    if not _should_run(ctx):
        return

    name = ctx.obj["NAME"]
    logger.info("Running locally with job name %r.", name)
    logger.debug("Submitting the following command:\n%s", cmd)
    Status.mark_job_as_submitted(
        ctx.obj["OUT_DIR"],
        command=ctx.obj["COMMAND_NAME"],
        job_name=name,
        replace=True,
        job_attrs={
            StatusField.JOB_STATUS: StatusOption.SUBMITTED,
            StatusField.HARDWARE: HardwareOption.LOCAL,
            StatusField.TIME_SUBMITTED: dt.datetime.now().strftime(DT_FMT),
        },
    )
    stdout, stderr = submit(cmd)
    if stdout:
        logger.info("Subprocess received stdout: \n%s", stdout)
    if stderr:
        logger.warning("Subprocess received stderr: \n%s", stderr)
    msg = f"Completed job {name!r}."
    logger.info(msg)


def _kickoff_hpc_job(ctx, cmd, **kwargs):
    """Run a job (command) on the HPC."""

    if not _should_run(ctx):
        return

    name = ctx.obj["NAME"]
    logger.info("Running on HPC with job name %r.", name)
    logger.debug("Submitting the following command:\n%s", cmd)
    out = ctx.obj["MANAGER"].submit(name, cmd=cmd, **kwargs)[0]
    id_msg = f" (Job ID #{out})" if out else ""
    msg = f"Kicked off job {name!r}{id_msg}"

    Status.mark_job_as_submitted(
        ctx.obj["OUT_DIR"],
        command=ctx.obj["COMMAND_NAME"],
        job_name=name,
        replace=True,
        job_attrs={
            StatusField.JOB_ID: out,
            StatusField.HARDWARE: HardwareOption.SLURM,
            StatusField.JOB_STATUS: StatusOption.SUBMITTED,
            StatusField.TIME_SUBMITTED: dt.datetime.now().strftime(DT_FMT),
        },
    )
    logger.info(msg)


def _should_run(ctx):
    """Determine wether a command should be run based on status."""
    name = ctx.obj["NAME"]
    out_dir = ctx.obj["OUT_DIR"]
    status = Status.retrieve_job_status(
        out_dir,
        command=ctx.obj["COMMAND_NAME"],
        job_name=name,
        subprocess_manager=ctx.obj.get("MANAGER"),
    )
    if status == StatusOption.SUCCESSFUL:
        msg = (
            f"Job {name!r} is successful in status json found in {out_dir!r}, "
            f"not re-running."
        )
        logger.info(msg)
        return False

    if status is not None and "fail" not in str(status).lower():
        msg = (
            f"Job {name!r} was found with status {status!r}, not resubmitting"
        )
        logger.info(msg)
        return False

    return True
