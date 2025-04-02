"""GAPs pipeline CLI entry points"""

import os
import sys
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
from gaps.warn import gapsWarning


logger = logging.getLogger(__name__)
FORKED_PID = 0


def _can_run_background():
    """Determine if GAPs can execute the pipeline in the background"""
    return hasattr(os, "setsid") and hasattr(os, "fork")


def template_pipeline_config(commands):
    """Generate a template pipeline config based on the commands"""
    pipeline = []
    for command in commands:
        command_name_config = command.name.replace("-", "_")
        sample_config_filename = (
            f"./config_{command_name_config}.{ConfigType.JSON}"
        )
        pipeline.append({command.name: sample_config_filename})
    return {
        "pipeline": pipeline,
        "logging": {"log_file": None, "log_level": "INFO"},
    }


@click.pass_context
def pipeline(
    ctx, config_file, cancel, monitor, background=False, recursive=False
):
    """Execute multiple steps in an analysis pipeline"""

    if recursive:
        _submit_recursive_pipelines(ctx, cancel, monitor, background)
        return

    if config_file is None:
        config_files = _find_pipeline_config_files(Path())
        if len(config_files) != 1:
            msg = (
                f"Could not determine config file - multiple (or no) files "
                f" detected with 'pipeline' in the name exist: {config_file}"
            )
            raise gapsExecutionError(msg)

        config_file = config_files[0]

    init_logging_from_config_file(config_file, background=background)
    _run_pipeline(ctx, config_file, cancel, monitor, background)


def _submit_recursive_pipelines(ctx, cancel, monitor, background):
    """Submit pipelines in all recursive subdirectories"""
    start_dir = Path()
    for sub_dir in start_dir.glob("**/"):
        config_files = _find_pipeline_config_files(sub_dir)
        if sub_dir.name == Status.HIDDEN_SUB_DIR:
            continue

        if len(config_files) > 1:
            msg = (
                f"Could not determine config file - multiple files detected "
                f"with 'pipeline' in the name in the {str(sub_dir)!r} "
                "directory!"
            )
            warn(msg, gapsWarning)
            continue
        if len(config_files) == 0:
            continue

        init_logging_from_config_file(config_files[0], background=background)
        _run_pipeline(ctx, config_files[0], cancel, monitor, background)


def _find_pipeline_config_files(directory):
    """Find all files matching *pipeline* in directory"""
    return [fp for fp in Path(directory).glob("*pipeline*") if fp.is_file()]


def _run_pipeline(ctx, config_file, cancel, monitor, background):
    """Run a GAPs pipeline for an existing config file"""

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
        pid = _kickoff_background(config_file)
        if pid == FORKED_PID:
            sys.exit()
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


def _kickoff_background(config_file):  # pragma: no cover
    """Kickoff a child process that runs pipeline in the background"""
    pid = os.fork()
    if pid == FORKED_PID:
        os.setsid()  # This creates a new session
        Pipeline.run(config_file, monitor=True)
    else:
        Status.record_monitor_pid(Path(config_file).parent, pid)
        click.echo(
            f"Kicking off pipeline job in the background. Monitor PID: {pid}"
        )
    return pid


def pipeline_command(template_config):
    """Generate a pipeline command"""
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
        click.Option(
            param_decls=["--recursive", "-r"],
            is_flag=True,
            help="Flag to recursively submit pipelines, starting from the "
            "current directory and checking every sub-directory therein. The "
            "`-c` option will be *completely ignored* if you use this option. "
            "Instead, the code will check every sub-directory for exactly one "
            "file with the word `pipeline` in it. If found, that file is "
            "assumed to be the pipeline config and is used to kick off the "
            "pipeline. In any other case, the directory is skipped.",
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
