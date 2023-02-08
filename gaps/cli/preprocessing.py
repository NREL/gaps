# -*- coding: utf-8 -*-
"""
GAPs config preprocessing functions.
"""
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

from gaps.warnings import gapsWarning


def split_project_points_into_ranges(config):
    """Split project points into ranges inside of config.

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


def preprocess_collect_config(config, project_dir, command_name):
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
        pipeline status for output files if "collect_pattern"="PIPELINE"
        in the input `config`.

    Returns
    -------
    dict
        Updated collection config.
    """
    files = config.get("collect_pattern", {})
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
    config["collect_pattern"] = _validate_patterns(files)
    return config


def _validate_patterns(files):
    """Remove any patterns that have no corresponding files."""

    patterns_with_no_files = []
    files_to_collect = []
    for out, pattern in files:
        if glob.glob(pattern):
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
