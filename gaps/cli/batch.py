"""Batch Job CLI entry points"""

import click

import gaps.batch
from gaps.log import init_logger
from gaps.config import init_logging_from_config_file
from gaps.cli.command import _WrappedCommand
from gaps.cli.documentation import _batch_command_help


def _batch(config_file, dry, cancel, delete, monitor_background):
    """Execute an analysis pipeline over a parametric set of inputs"""
    if str(config_file).endswith("csv"):
        init_logger(stream=not monitor_background, level="INFO", file=None)
    else:
        init_logging_from_config_file(
            config_file, background=monitor_background
        )

    if cancel:
        gaps.batch.BatchJob(config_file).cancel()
    elif delete:
        gaps.batch.BatchJob(config_file).delete()
    else:
        gaps.batch.BatchJob(config_file).run(
            dry_run=dry,
            monitor_background=monitor_background,
        )


def batch_command():
    """Generate a batch command"""
    params = [
        click.Option(
            param_decls=["--config_file", "-c"],
            required=True,
            type=click.Path(exists=True),
            help=_batch_command_help(),
        ),
        click.Option(
            param_decls=["--dry"],
            is_flag=True,
            help="Flag to do a dry run (make batch dirs and update files "
            "without running the pipeline).",
        ),
        click.Option(
            param_decls=["--cancel"],
            is_flag=True,
            help="Flag to cancel all jobs associated associated with the "
            "``batch_jobs.csv`` file in the current batch config directory.",
        ),
        click.Option(
            param_decls=["--delete"],
            is_flag=True,
            help="Flag to delete all batch job sub directories associated "
            "with the ``batch_jobs.csv`` file in the current batch config "
            "directory.",
        ),
        click.Option(
            param_decls=["--monitor-background"],
            is_flag=True,
            help="Flag to monitor all batch pipelines continuously in the "
            "background. Note that the ``stdout/stderr`` will not be "
            'captured, but you can set a pipeline ``"log_file"`` to capture '
            "logs.",
        ),
    ]
    return _WrappedCommand(
        "batch",
        context_settings=None,
        callback=_batch,
        params=params,
        help=(
            "Execute an analysis pipeline over a parametric set of inputs.\n\n"
            "The general structure for calling this CLI command is given "
            "below (add ``--help`` to print help info to the terminal)."
        ),
        epilog=None,
        short_help=None,
        options_metavar="[OPTIONS]",
        add_help_option=True,
        no_args_is_help=True,
        hidden=False,
        deprecated=False,
    )
