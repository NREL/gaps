"""GAPs config preprocessing functions"""

import re
import glob
from math import ceil
from warnings import warn
from collections import abc

from gaps.project_points import ProjectPoints
from gaps.cli.config import TAG
from gaps.pipeline import parse_previous_status
from gaps.utilities import resolve_path
from gaps.exceptions import gapsConfigError

from gaps.warn import gapsWarning


def split_project_points_into_ranges(config):
    """Split project points into ranges inside of config

    Parameters
    ----------
    config : dict
        Run config. This config must have a "project_points" input that
        can be used to initialize :class:`ProjectPoints`.

    Returns
    -------
    dict
        Run config with a "project_points_split_range" key that split
        the project points into multiple ranges based on node input.
    """
    project_points_file = config["project_points"]
    exec_control = config.get("execution_control", {})
    if exec_control.get("option") == "local":
        num_nodes = 1
    else:
        num_nodes = exec_control.pop("nodes", 1)

    points = ProjectPoints(project_points_file)
    sites_per_split = ceil(len(points) / num_nodes)
    config["project_points_split_range"] = [
        sub_points.split_range
        for sub_points in points.split(sites_per_split=sites_per_split)
    ]
    return config


def preprocess_script_config(config, cmd):
    """Pre-process script config.

    Parameters
    ----------
    config : dict
        Script config. This config will be updated such that the "cmd"
        key is always a list.
    cmd : str | list
        A single command represented as a string or a list of command
        strings to execute on a node. If the input is a list, each
        command string in the list will be executed on a separate node.
        For example, to run a python script, simply specify
        ::

            "cmd": "python my_script.py"

        This will run the python file "my_script.py" (in the project
        directory) on a single node.

        .. Important:: It is inefficient to run scripts that only use a
           single processor on HPC nodes for extended periods of time.
           Always make sure your long-running scripts use Python's
           multiprocessing library wherever possible to make the most
           use of shared HPC resources.

        To run multiple commands in parallel, supply them as a list:
        ::

            "cmd": [
                "python /path/to/my_script/py -a -out out_file.txt",
                "wget https://website.org/latest.zip"
            ]

        This input will run two commands (a python script with the
        specified arguments and a ``wget`` command to download a file
        from the web), each on their own node and in parallel as part of
        this pipeline step. Note that commands are always executed from
        the project directory.

    Returns
    -------
    dict
        Updated script config.
    """
    if isinstance(cmd, str):
        cmd = [cmd]

    config["_cmd"] = cmd
    return config


def preprocess_collect_config(
    config, project_dir, command_name, collect_pattern="PIPELINE"
):
    """Pre-process collection config.

    Specifically, the "collect_pattern" key is resolved into a list of
    2-tuples, where the first element in each tuple is a path to the
    collection output file, while the second element is the
    corresponding filepath-pattern representing the files to be
    collected into the output file.

    Parameters
    ----------
    config : dict
        Collection config. This config will be updated to include a
        "collect_pattern" key if it doesn't already have one. If the
        "collect_pattern" key exists, it can be a string, a collection,
        or a mapping with a `.items()` method.
    project_dir : path-like
        Path to project directory. This path is used to resolve the
        out filepath input from the user.
    command_name : str
        Name of the command being run. This is used to parse the
        pipeline status for output files if
        ``collect_pattern="PIPELINE"`` in the input `config`.
    collect_pattern : str | list | dict, optional
        Unix-style ``/filepath/pattern*.h5`` representing the files to
        be collected into a single output HDF5 file. If no output file
        path is specified (i.e. this input is a single pattern or a list
        of patterns), the output file path will be inferred from the
        pattern itself (specifically, the wildcard will be removed
        and the result will be the output file path). If a list of
        patterns is provided, each pattern will be collected into a
        separate output file. To specify the name of the output file(s),
        set this input to a dictionary where the keys are paths to the
        output file (including the filename itself; relative paths are
        allowed) and the values are patterns representing the files that
        should be collected into the output file. If running a collect
        job as part of a pipeline, this input can be set to
        ``"PIPELINE"``, which will parse the output of the previous step
        and generate the input file pattern and output file name
        automatically. By default, ``"PIPELINE"``.

    Returns
    -------
    dict
        Updated collection config.
    """
    files = collect_pattern
    if files == "PIPELINE":
        files = parse_previous_status(project_dir, command_name)
        files = [re.sub(f"{TAG}\\d+", "*", fname) for fname in files]

    if isinstance(files, str):
        files = [files]

    if isinstance(files, abc.Sequence):
        files = {pattern.replace("*", ""): pattern for pattern in files}

    files = [
        (
            resolve_path(
                out_path if out_path.startswith("/") else f"./{out_path}",
                project_dir,
            ),
            pattern,
        )
        for out_path, pattern in files.items()
    ]
    config["_out_path"], config["_pattern"] = zip(*_validate_patterns(files))
    return config


def _validate_patterns(files):
    """Remove any patterns that have no corresponding files"""

    patterns_with_no_files = []
    files_to_collect = []
    for out, pattern in files:
        if glob.glob(pattern):  # noqa: PTH207
            files_to_collect.append((out, pattern))
        else:
            patterns_with_no_files.append(pattern)

    if any(patterns_with_no_files):
        msg = (
            f"Could not find any files for the following patterns: "
            f"{patterns_with_no_files}. Skipping..."
        )
        warn(msg, gapsWarning)

    if not files_to_collect:
        msg = (
            "Found no files to collect! Please double check your config input "
            "and verify that the files to be collected exist on disk!"
        )
        raise gapsConfigError(msg)

    return files_to_collect
