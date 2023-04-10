# -*- coding: utf-8 -*-
"""GAPs Status Monitor"""
from pathlib import Path
from warnings import warn
from itertools import chain

import click
import psutil
from colorama import init, Fore, Style
from tabulate import tabulate

from gaps.status import Status, StatusField, StatusOption, _elapsed_time_as_str
from gaps.warnings import gapsWarning
from gaps.cli.command import _WrappedCommand


FAILURE_STRINGS = ["failure", "fail", "failed", "f"]
RUNNING_STRINGS = ["running", "run", "r"]
SUBMITTED_STRINGS = ["submitted", "submit", "sb", "pending", "pend", "p"]
SUCCESS_STRINGS = ["successful", "success", "s"]
# cspell:disable-next-line
NOT_SUBMITTED_STRINGS = ["unsubmitted", "unsubmit", "u", "not_submitted", "ns"]
FILTER_HELP = """
Filter jobs for the requested status(es). Allowed options (case-insensitive)
include:

    - Failed: {fail_options}
    - Running: {running_options}
    - Submitted: {submitted_options}
    - Success: {success_options}
    - Not submitted: {ns_options}

Multiple status keys can be specified by repeating this option
(e.g. :code:`-s status1 -s status2 ...`). By default, all status values are
displayed.
"""
STATUS_HELP = """
Display the status of a project FOLDER.

By default, the status of the current working directory is displayed.
"""


def _filter_df_for_status(df, status_request):
    """Check for a specific status."""

    filter_statuses = set()
    for request in status_request:
        request = request.lower()
        if request in FAILURE_STRINGS:
            filter_statuses |= {StatusOption.FAILED}
        elif request in SUCCESS_STRINGS:
            filter_statuses |= {StatusOption.SUCCESSFUL}
        elif request in RUNNING_STRINGS:
            filter_statuses |= {StatusOption.RUNNING}
        elif request in SUBMITTED_STRINGS:
            filter_statuses |= {StatusOption.SUBMITTED}
        elif request in NOT_SUBMITTED_STRINGS:
            filter_statuses |= {StatusOption.NOT_SUBMITTED}
        else:
            msg = (
                f"Requested status not recognized: {status_request!r}. "
                "No additional filtering performed!"
            )
            warn(msg, gapsWarning)

    df = df[df[StatusField.JOB_STATUS].isin(filter_statuses)]
    return df.copy()


def _color_print(df, print_folder, commands, status):
    """Color the status portion of the print out."""

    def color_string(string):
        if string == StatusOption.FAILED:
            string = f"{Fore.RED}{string}{Style.RESET_ALL}"
        elif string == StatusOption.SUCCESSFUL:
            string = f"{Fore.GREEN}{string}{Style.RESET_ALL}"
        elif string == StatusOption.RUNNING:
            string = f"{Fore.BLUE}{string}{Style.RESET_ALL}"
        else:
            string = f"{Fore.YELLOW}{string}{Style.RESET_ALL}"
        return string

    extras = []
    commands = ", ".join(commands or [])
    if commands:
        extras.append(f"commands={commands}")
    status = ", ".join(status or [])
    if status:
        extras.append(f"status={status}")
    if extras:
        extras = "; ".join(extras)
        extras = f" ({extras})"
    else:
        extras = ""

    pid = df.monitor_pid
    total_runtime_seconds = df.total_runtime_seconds
    name = f"\n{Fore.CYAN}{print_folder}{extras}{Style.RESET_ALL}:"
    job_status = StatusField.JOB_STATUS
    df[job_status.value] = df[job_status].apply(color_string)
    df = df.fillna("--")
    pdf = tabulate(
        df,
        showindex=True,  # cspell:disable-line
        headers=df.columns,
        tablefmt="simple",  # cspell:disable-line
        disable_numparse=[3],  # cspell:disable-line
    )
    print(name)
    if pid is not None and psutil.pid_exists(pid):
        print(f"{Fore.MAGENTA}MONITOR PID: {pid}{Style.RESET_ALL}")
    print(pdf)
    if total_runtime_seconds is not None:
        runtime_str = (
            f"Total Runtime: {_elapsed_time_as_str(total_runtime_seconds)}"
        )
        # TODO: make prettier
        # divider = "".join(["-"] * len(runtime_str))
        # divider = "".join(["~"] * 3)
        divider = ""
        print(divider)
        print(f"{Style.BRIGHT}{runtime_str}{Style.RESET_ALL}")
        print(
            f"{Style.BRIGHT}**Statistics only include shown jobs, excluding "
            f"failed or duplicate runs**{Style.RESET_ALL}"
        )
    print()


def main_monitor(folder, commands, status, include):
    """Run the appropriate monitor functions for a folder.

    Parameters
    ----------
    folder : path-like
        Path to folder for which to print status.
    commands : container of str
        Container with the commands to display.
    status : container of str
        Container with the statuses to display.
    include : container of str
        Container of extra keys to pull from the status files.
    """

    init()
    folder = Path(folder).expanduser().resolve()
    for directory in chain([folder], folder.rglob("*")):
        if not directory.is_dir():
            continue

        pipe_status = Status(directory)
        if not pipe_status:
            continue

        include_with_runtime = list(include) + [StatusField.RUNTIME_SECONDS]
        df = pipe_status.as_df(
            commands=commands, include_cols=include_with_runtime
        )
        if status:
            df = _filter_df_for_status(df, status)

        total_runtime = df[StatusField.RUNTIME_SECONDS].sum()
        df = df[list(df.columns)[:-1]]
        df.monitor_pid = pipe_status.get(StatusField.MONITOR_PID)
        df.total_runtime_seconds = total_runtime
        _color_print(df, directory.name, commands, status)


def status_command():
    """A status CLI command."""
    filter_help = FILTER_HELP.format(
        fail_options=" ".join([f"``{s}``" for s in FAILURE_STRINGS]),
        running_options=" ".join([f"``{s}``" for s in RUNNING_STRINGS]),
        submitted_options=" ".join([f"``{s}``" for s in SUBMITTED_STRINGS]),
        success_options=" ".join([f"``{s}``" for s in SUCCESS_STRINGS]),
        ns_options=" ".join([f"``{s}``" for s in NOT_SUBMITTED_STRINGS]),
    )

    params = [
        click.Argument(
            param_decls=["folder"],
            default=Path.cwd(),
            type=click.Path(exists=True),
        ),
        click.Option(
            param_decls=["--commands", "-c"],
            multiple=True,
            default=None,
            help="Filter status for the given command(s). Multiple commands "
            "can be specified by repeating this option (e.g. :code:`-c "
            "command1 -c command2 ...`) By default, the status of all "
            "commands is displayed.",
        ),
        click.Option(
            param_decls=["--status", "-s"],
            multiple=True,
            default=None,
            help=filter_help,
        ),
        click.Option(
            param_decls=["--include", "-i"],
            multiple=True,
            default=None,
            help="Extra status keys to include in the print output for each "
            "job. Multiple status keys can be specified by repeating "
            "this option (e.g. :code:`-i key1 -i key2 ...`) By default, no "
            "extra keys are displayed.",
        ),
    ]

    return _WrappedCommand(
        "status",
        context_settings=None,
        callback=main_monitor,
        params=params,
        help=STATUS_HELP,
        epilog=None,
        short_help=None,
        options_metavar="[OPTIONS]",
        add_help_option=True,
        no_args_is_help=False,
        hidden=False,
        deprecated=False,
    )
