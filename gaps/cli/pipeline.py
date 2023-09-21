# -*- coding: utf-8 -*-
"""
GAPs pipeline CLI entry points.
"""
import os
import logging
from pathlib import Path
from warnings import warn

import click
import psutil

from gaps import Pipeline
from gaps.config import ConfigType, init_logging_from_config_file
from gaps.cli.documentation import _pipeline_command_help
from gaps.cli.command import _WrappedCommand
from gaps.status import Status, StatusField
from gaps.exceptions import gapsExecutionError
from gaps.warnings import gapsWarning


logger = logging.getLogger(__name__)


def _can_run_background():
    """Determine if GAPs can execute the pipeline in the background."""
    return hasattr(os, "setsid") and hasattr(os, "fork")


def template_pipeline_config(commands):
    """Generate a template pipeline config based on the commands."""
    _pipeline = []
    for command in commands:
        command_name_config = command.name.replace("-", "_")
        sample_config_filename = (
            f"./config_{command_name_config}.{ConfigType.JSON}"
        )
        _pipeline.append({command.name: sample_config_filename})
    return {
        "pipeline": _pipeline,
        "logging": {"log_file": None, "log_level": "INFO"},
    }


@click.pass_context
def pipeline(ctx, config_file, cancel, monitor, background=False):
    """Execute multiple steps in an analysis pipeline."""

    if config_file is None:
        config_file = [
            fp
            for fp in Path(".").glob("*")
            if fp.is_file() and "pipeline" in fp.name
        ]
        if len(config_file) != 1:
            msg = (
                f"Could not determine config file - multiple (or no) files "
                f" detected with 'pipeline' in the name exist: {config_file}"
            )
            raise gapsExecutionError(msg)

        config_file = config_file[0]

    _run_pipeline(ctx, config_file, cancel, monitor, background)


def _run_pipeline(ctx, config_file, cancel, monitor, background):
    """Run a GAPs pipeline for an existing config file."""

    init_logging_from_config_file(config_file, background=background)

    if cancel:
        Pipeline.cancel_all(config_file)
        return

    if background:
        if not _can_run_background():
            msg = (
                "Cannot run pipeline in background on system that does not "
                "implement os.fork and os.setsid"
            )
            raise gapsExecutionError(msg)
        ctx.obj["LOG_STREAM"] = False
        _kickoff_background(config_file)
        return

    project_dir = str(Path(config_file).parent.expanduser().resolve())
    status = Status(project_dir).update_from_all_job_files(purge=False)
    monitor_pid = status.get(StatusField.MONITOR_PID)
    if monitor_pid is not None and psutil.pid_exists(monitor_pid):
        msg = (
            f"Another pipeline in {project_dir!r} is running on monitor PID: "
            f"{monitor_pid}. Not starting a new pipeline execution."
        )
        warn(msg, gapsWarning)
        return

    if monitor:
        Status.record_monitor_pid(Path(config_file).parent, os.getpid())

    Pipeline.run(config_file, monitor=monitor)


# pylint: disable=no-member
def _kickoff_background(config_file):  # pragma: no cover
    """Kickoff a child process that runs pipeline in the background."""
    pid = os.fork()
    if pid == 0:
        os.setsid()  # This creates a new session
        Pipeline.run(config_file, monitor=True)
    else:
        Status.record_monitor_pid(Path(config_file).parent, pid)
        click.echo(
            f"Kicking off pipeline job in the background. Monitor PID: {pid}"
        )


def pipeline_command(template_config):
    """Generate a pipeline command."""
    params = [
        click.Option(
            param_decls=["--config_file", "-c"],
            default=None,
            help=_pipeline_command_help(template_config),
        ),
        click.Option(
            param_decls=["--cancel"],
            is_flag=True,
            help="Flag to cancel all jobs associated with a given pipeline.",
        ),
        click.Option(
            param_decls=["--monitor"],
            is_flag=True,
            help="Flag to monitor pipeline jobs continuously. Default is not "
            "to monitor (kick off jobs and exit).",
        ),
    ]
    if _can_run_background():
        params += [
            click.Option(
                param_decls=["--background"],
                is_flag=True,
                help="Flag to monitor pipeline jobs continuously in the "
                "background. Note that the stdout/stderr will not be "
                "captured, but you can set a pipeline 'log_file' to "
                "capture logs.",
            ),
        ]

    return _WrappedCommand(
        "pipeline",
        context_settings=None,
        callback=pipeline,
        params=params,
        help=(
            "Execute multiple steps in an analysis pipeline.\n\n"
            "The general structure for calling this CLI command is given "
            "below (add ``--help`` to print help info to the terminal)."
        ),
        epilog=None,
        short_help=None,
        options_metavar="[OPTIONS]",
        add_help_option=True,
        no_args_is_help=False,
        hidden=False,
        deprecated=False,
    )
