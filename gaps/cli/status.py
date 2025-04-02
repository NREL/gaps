"""GAPs Status Monitor"""

import datetime as dt
from pathlib import Path
from warnings import warn
from itertools import chain

import click
import psutil
import pandas as pd
from colorama import init, Fore, Style
from tabulate import tabulate, SEPARATING_LINE

from gaps.status import (
    DT_FMT,
    Status,
    StatusField,
    StatusOption,
    HardwareOption,
    QOSOption,
    _elapsed_time_as_str,
)
from gaps.warn import gapsWarning
from gaps.cli.command import _WrappedCommand


JOB_STATUS_COL = StatusField.JOB_STATUS.value
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

The general structure for calling this CLI command is given below
(add ``--help`` to print help info to the terminal)."
"""


def _extract_qos_charge_factor(option, enum_class):
    """Get the charge factor of a value in a row"""
    try:
        return enum_class(str(option)).charge_factor
    except ValueError:
        return 1


def _filter_df_for_status(df, status_request):
    """Check for a specific status"""

    filter_statuses = set()
    for request in status_request:
        request = request.lower()  # noqa: PLW2901
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

    return df[df[StatusField.JOB_STATUS].isin(filter_statuses)].copy()


def _calculate_runtime_stats(df):
    """Calculate df runtime statistics"""

    run_times_seconds = df[StatusField.RUNTIME_SECONDS]
    if run_times_seconds.isna().any():
        return pd.DataFrame()
    runtime_stats = pd.DataFrame(run_times_seconds.astype(float).describe())
    runtime_stats = runtime_stats.T[["min", "mean", "50%", "max"]]
    runtime_stats = runtime_stats.rename(columns={"50%": "median"})
    # runtime_stats = runtime_stats.rename(
    #     columns={
    #         col: f"{Fore.MAGENTA}{col}{Style.RESET_ALL}"
    #         for col in runtime_stats.columns
    #     }
    # )
    runtime_stats.index = ["Node runtime"]
    runtime_stats = runtime_stats.T
    runtime_stats["Node runtime"] = runtime_stats["Node runtime"].apply(
        _elapsed_time_as_str
    )
    return pd.DataFrame(runtime_stats).reset_index()


def _calculate_walltime(df):
    """Calculate total project walltime"""
    all_jobs_failed = (df[StatusField.JOB_STATUS] == StatusOption.FAILED).all()
    all_end_times_missing = df[StatusField.TIME_END].isna().all()
    if all_jobs_failed and all_end_times_missing:
        return 0

    start_time = df[StatusField.TIME_SUBMITTED].fillna(
        dt.datetime.now().strftime(DT_FMT)
    )
    start_time = pd.to_datetime(start_time, format=DT_FMT).min()

    end_time = df[StatusField.TIME_END].fillna(
        dt.datetime.now().strftime(DT_FMT)
    )
    end_time = pd.to_datetime(end_time, format=DT_FMT).max()
    return (end_time - start_time).total_seconds()


def _calculate_aus(df):
    """Calculate the number of AU's spent by jobs"""
    run_times_seconds = df[StatusField.RUNTIME_SECONDS]

    hardware_charge_factors = df[StatusField.HARDWARE].apply(
        _extract_qos_charge_factor, enum_class=HardwareOption
    )
    qos_charge_factors = df[StatusField.QOS].apply(
        _extract_qos_charge_factor, enum_class=QOSOption
    )
    aus_used = (
        run_times_seconds / 3600 * hardware_charge_factors * qos_charge_factors
    )
    return int(aus_used.sum())


def _color_string(string):
    """Color string value based on status option"""
    if string == StatusOption.FAILED:
        string = f"{Fore.RED}{string}{Style.RESET_ALL}"
    elif string == StatusOption.SUCCESSFUL:
        string = f"{Fore.GREEN}{string}{Style.RESET_ALL}"
    elif string == StatusOption.RUNNING:
        string = f"{Fore.BLUE}{string}{Style.RESET_ALL}"
    else:
        string = f"{Fore.YELLOW}{string}{Style.RESET_ALL}"
    return string


def _print_intro(print_folder, steps, status, monitor_pid):
    """Print intro including project folder and steps/status filters"""
    extras = []
    steps = ", ".join(steps or [])
    if steps:
        extras.append(f"steps={steps}")
    status = ", ".join(status or [])
    if status:
        extras.append(f"status={status}")
    if extras:
        extras = "; ".join(extras)
        extras = f" ({extras})"
    else:
        extras = ""

    print(f"\n{Fore.CYAN}{print_folder}{extras}{Style.RESET_ALL}:")
    if monitor_pid is not None and psutil.pid_exists(monitor_pid):
        print(f"{Fore.MAGENTA}MONITOR PID: {monitor_pid}{Style.RESET_ALL}")


def _print_df(df):
    """Print main status body (table of job statuses)"""
    df[JOB_STATUS_COL] = df[JOB_STATUS_COL].apply(_color_string)
    df = df.fillna("--")  # noqa: PD901

    pdf = pd.concat([df, pd.DataFrame({JOB_STATUS_COL: [SEPARATING_LINE]})])
    pdf = tabulate(
        pdf,
        showindex=True,  # cspell:disable-line
        headers=df.columns,
        tablefmt="simple",  # cspell:disable-line
        disable_numparse=[3],  # cspell:disable-line
    )
    pdf = pdf.split("\n")
    pdf[-1] = pdf[-1].replace(" ", "-")
    pdf = "\n".join(pdf)
    print(pdf)


def _print_job_status_statistics(df):
    """Print job status statistics"""
    statistics_str = f"Total number of jobs: {df.shape[0]}"
    counts = pd.DataFrame(df[JOB_STATUS_COL].value_counts()).reset_index()
    counts = tabulate(
        counts[["count", JOB_STATUS_COL]].values,
        showindex=False,  # cspell:disable-line
        headers=counts.columns,
        tablefmt="simple",  # cspell:disable-line
        disable_numparse=[1],  # cspell:disable-line
    )
    counts = "\n".join(
        [f"  {substr.lstrip()}" for substr in counts.split("\n")[2:]]
    )
    print(f"{Style.BRIGHT}{statistics_str}{Style.RESET_ALL}")
    print(counts)


def _print_runtime_stats(runtime_stats, total_runtime_seconds):
    """Print node runtime statistics"""

    runtime_str = (
        f"Total node runtime: {_elapsed_time_as_str(total_runtime_seconds)}"
    )
    print(f"{Style.BRIGHT}{runtime_str}{Style.RESET_ALL}")
    if runtime_stats.empty:
        return

    runtime_stats = tabulate(
        runtime_stats,
        showindex=False,  # cspell:disable-line
        headers=runtime_stats.columns,
        tablefmt="simple",  # cspell:disable-line
    )
    runtime_stats = "\n".join(
        [f"  {substr}" for substr in runtime_stats.split("\n")[2:]]
    )
    print(runtime_stats)


def _print_au_usage(total_aus_used):
    """Print the job AU usage"""
    if total_aus_used <= 0:
        return

    au_str = f"Total AUs spent: {total_aus_used:,}"
    print(f"{Style.BRIGHT}{au_str}{Style.RESET_ALL}")


def _print_total_walltime(walltime):
    """Print the total project walltime"""
    if walltime <= 2:  # noqa: PLR2004
        return
    walltime_str = (
        f"Total project wall time (including queue and downtime "
        f"between steps): {_elapsed_time_as_str(walltime)}"
    )
    print(f"{Style.BRIGHT}{walltime_str}{Style.RESET_ALL}")


def _print_disclaimer():
    """Print disclaimer about statistics"""
    print(
        f"{Style.BRIGHT}**Statistics only include shown jobs (excluding "
        f"any previous runs or other steps)**{Style.RESET_ALL}"
    )


def _color_print(
    df,
    print_folder,
    steps,
    status,
    walltime,
    runtime_stats,
    total_aus_used,
    monitor_pid=None,
    total_runtime_seconds=None,
):
    """Color the status portion of the print out"""

    _print_intro(print_folder, steps, status, monitor_pid)
    _print_df(df)
    _print_job_status_statistics(df)

    if total_runtime_seconds is None:
        return

    _print_runtime_stats(runtime_stats, total_runtime_seconds)
    _print_au_usage(total_aus_used)
    _print_total_walltime(walltime)
    _print_disclaimer()


def main_monitor(folder, pipe_steps, status, include, recursive):
    """Run the appropriate monitor functions for a folder.

    Parameters
    ----------
    folder : path-like
        Path to folder for which to print status.
    pipe_steps : container of str
        Container with the pipeline steps to display.
    status : container of str
        Container with the statuses to display.
    include : container of str
        Container of extra keys to pull from the status files.
    recursive : bool
        Option to perform recursive search of directories.
    """

    init()
    folders = [Path(folder).expanduser().resolve()]
    if recursive:
        folders = chain(folders, folders[0].rglob("*"))

    for directory in folders:
        if not directory.is_dir():
            continue
        if directory.name == Status.HIDDEN_SUB_DIR:
            continue

        pipe_status = Status(directory)
        if not pipe_status:
            print(f"No non-empty status file found in {str(directory)!r}. ")
            continue

        include_with_runtime = [*list(include), StatusField.RUNTIME_SECONDS]
        df = pipe_status.as_df(  # noqa: PD901
            pipe_steps=pipe_steps, include_cols=include_with_runtime
        )
        if status:
            df = _filter_df_for_status(df, status)  # noqa: PD901

        if df.empty:
            print(
                f"No status data found to display for {str(directory)!r}. "
                "Please check your filters and try again."
            )
            continue

        runtime_stats = _calculate_runtime_stats(df)
        aus_used = _calculate_aus(df)
        walltime = _calculate_walltime(df)

        _color_print(
            df[list(df.columns)[:-1]].copy(),
            directory.name,
            pipe_steps,
            status,
            walltime,
            runtime_stats,
            total_aus_used=aus_used,
            monitor_pid=pipe_status.get(StatusField.MONITOR_PID),
            total_runtime_seconds=df[StatusField.RUNTIME_SECONDS].sum(),
        )


def status_command():
    """A status CLI command"""
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
            param_decls=["--pipe_steps", "-ps"],
            multiple=True,
            default=None,
            help="Filter status for the given pipeline step(s). Multiple "
            "steps can be specified by repeating this option (e.g. :code:`-ps "
            "step1 -ps step2 ...`) By default, the status of all "
            "pipeline steps is displayed.",
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
        click.Option(
            param_decls=["--recursive", "-r"],
            is_flag=True,
            help="Option to perform a recursive search of directories "
            "(starting with the input directory). The status of every nested "
            "directory is reported.",
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
