# -*- coding: utf-8 -*-
"""
GAPs CLI template config generation command.
"""
import logging
import shutil
from pathlib import Path

import click

from rex.utilities.loggers import init_logger
from gaps.status import Status
from gaps.cli.command import _WrappedCommand


logger = logging.getLogger(__name__)


# pylint: disable=redefined-builtin
@click.pass_context
def _reset_status(ctx, directory):
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

        logger.info("Removing status info for directory %r", str(status_dir))
        shutil.rmtree(status_file_dir)


def reset_command():
    """A status reset CLI command."""
    params = [
        click.Argument(
            param_decls=["directory"],
            required=False,
            nargs=-1,
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
