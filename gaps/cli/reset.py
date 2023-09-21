# -*- coding: utf-8 -*-
"""
GAPs CLI template config generation command.
"""
import logging
import shutil
from pathlib import Path
from warnings import warn

import click

from rex.utilities.loggers import init_logger
from gaps.status import Status, StatusOption
from gaps.cli.command import _WrappedCommand
from gaps.warnings import gapsHPCWarning


logger = logging.getLogger(__name__)


# pylint: disable=redefined-builtin
@click.pass_context
def _reset_status(ctx, directory, force=False, keep_thru=None):
    """Filter configs and write to file based on type."""
    if ctx.obj.get("VERBOSE"):
        init_logger("gaps")

    if not directory:
        directory = [Path("./")]

    for status_dir in directory:
        status_dir = Path(status_dir).expanduser().resolve()
        status_file_dir = status_dir / Status.HIDDEN_SUB_DIR
        if not status_file_dir.exists():
            logger.debug(
                "No status info detected in %r. Skipping...", str(status_dir)
            )
            continue

        status = Status(status_dir)
        is_processing = (
            status.as_df()
            .job_status.isin({StatusOption.SUBMITTED, StatusOption.RUNNING})
            .any()
        )
        if is_processing and not force:
            msg = (
                f"Found queued/running jobs in {str(status_dir)}. "
                "Not resetting... (override this behavior with --force)"
            )
            warn(msg, gapsHPCWarning)
            continue

        if keep_thru:
            pass
        else:
            logger.info(
                "Removing status info for directory %r", str(status_dir)
            )
            shutil.rmtree(status_file_dir)


def reset_command():
    """A status reset CLI command."""
    params = [
        click.Argument(
            param_decls=["directory"],
            required=False,
            nargs=-1,
        ),
        click.Option(
            param_decls=["--force", "-f"],
            is_flag=True,
            help="Force pipeline status reset even if jobs are queued/running",
        ),
        click.Option(
            param_decls=["--keep-thru", "-k"],
            multiple=False,
            default=None,
            help="Reset pipeline status up to the given command. This command "
            "will remain completed, but the status of commands following it "
            "will be reset completely.",
        ),
    ]
    return _WrappedCommand(
        "reset-status",
        context_settings=None,
        callback=_reset_status,
        params=params,
        help=(
            "Reset the pipeline/job status (progress) for a given directory "
            "(defaults to ``./``). Multiple directories can be supplied to "
            "reset the status of each. \n\nThe general structure for calling "
            "this CLI command is given below (add ``--help`` to print help "
            "info to the terminal)."
        ),
        epilog=None,
        short_help=None,
        options_metavar="",
        add_help_option=True,
        no_args_is_help=False,
        hidden=False,
        deprecated=False,
    )
