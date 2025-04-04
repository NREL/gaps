"""GAPs batching framework for parametric runs

Based on reV-batch.
"""

import os
import copy
import json
import shutil
import logging
from warnings import warn
from pathlib import Path
from itertools import product
from collections import namedtuple

import pandas as pd

from rex.utilities import parse_year

from gaps.config import load_config, ConfigType, resolve_all_paths
import gaps.cli.pipeline
from gaps.pipeline import Pipeline
from gaps.exceptions import (
    gapsValueError,
    gapsConfigError,
    gapsFileNotFoundError,
)
from gaps.warn import gapsWarning


logger = logging.getLogger(__name__)

_TOO_MANY_JOBS_WARNING_THRESH = 1_000
BATCH_CSV_FN = "batch_jobs.csv"
BatchSet = namedtuple("BatchSet", ["arg_combo", "file_set", "tag"])


class BatchJob:
    """Framework for building a batched job suite

    Based on reV-batch.

    This framework allows users to modify key-value pairs in input
    configuration files based on a top-level batch config file. This
    framework will create run directories for all combinations of input
    parametrics and run the corresponding GAPs pipelines for each job.
    """

    def __init__(self, config):
        """
        Parameters
        ----------
        config : str
            File path to config json or csv (str).
        """

        self._job_tags = None
        self._base_dir, config = _load_batch_config(config)
        self._pipeline_fp = Path(config["pipeline_config"])
        self._sets = _parse_config(config)

        logger.info("Batch job initialized with %d sub jobs.", len(self._sets))

    @property
    def job_table(self):
        """pd.DataFrame: Batch job summary table"""
        jobs = []
        for job_tag, (arg_comb, file_set, set_tag) in self._sets.items():
            job_info = {k: str(v) for k, v in arg_comb.items()}
            job_info["set_tag"] = str(set_tag)
            job_info["files"] = str(file_set)
            job_info = pd.DataFrame(job_info, index=[job_tag])
            jobs.append(job_info)

        table = pd.concat(jobs)

        table.index.name = "job"
        table["pipeline_config"] = self._pipeline_fp.as_posix()

        return table

    @property
    def sub_dirs(self):
        """list: Job sub directory paths"""
        return [self._base_dir / tag for tag in self._sets]

    def _make_job_dirs(self):
        """Copy job files from the batch config dir into sub job dirs"""

        table = self.job_table
        table.to_csv(self._base_dir / BATCH_CSV_FN)
        logger.debug(
            "Batch jobs list: %s", sorted(table.index.to_numpy().tolist())
        )
        logger.debug("Using the following batch sets: %s", self._sets)
        logger.info("Preparing batch job directories...")

        # walk through current directory getting everything to copy
        for source_dir, _, filenames in os.walk(self._base_dir):
            logger.debug("Processing files in : %s", source_dir)
            logger.debug(
                "    - Is dupe dir: %s",
                any(job_tag in source_dir for job_tag in self._sets),
            )

            # don't make additional copies of job sub directories.
            if any(job_tag in source_dir for job_tag in self._sets):
                continue

            # For each dir level, iterate through the batch arg combos
            for tag, (arg_comb, mod_files, __) in self._sets.items():
                mod_files = {Path(fp) for fp in mod_files}  # noqa: PLW2901
                # Add the job tag to the directory path.
                # This will copy config subdirs into the job subdirs
                source_dir = Path(source_dir)  # noqa: PLW2901
                destination_dir = (
                    self._base_dir
                    / tag
                    / source_dir.relative_to(self._base_dir)
                )
                logger.debug("Creating dir: %s", destination_dir)
                destination_dir.mkdir(parents=True, exist_ok=True)

                for name in filenames:
                    if BATCH_CSV_FN in name:
                        continue
                    fp_source = source_dir / name
                    fp_target = destination_dir / name
                    if fp_source in mod_files:
                        _mod_file(fp_source, fp_target, arg_comb)
                    else:
                        _copy_batch_file(fp_source, destination_dir / name)

        for tag in self._sets:
            destination_dir = self._base_dir / tag
            pipeline_file_target = (
                destination_dir / self._pipeline_fp.relative_to(self._base_dir)
            )
            pipeline_file_target.parent.mkdir(parents=True, exist_ok=True)
            _copy_batch_file(
                self._pipeline_fp,
                destination_dir
                / self._pipeline_fp.relative_to(self._base_dir),
            )

        logger.info("Batch job directories ready for execution.")

    def _run_pipelines(self, monitor_background=False):
        """Run the pipeline modules for each batch job"""

        for sub_directory in self.sub_dirs:
            os.chdir(sub_directory)
            pipeline_config = sub_directory / self._pipeline_fp.name
            if not pipeline_config.is_file():
                msg = (
                    f"Could not find pipeline config to run: "
                    f"{pipeline_config!r}"
                )
                raise gapsConfigError(msg)
            if monitor_background:
                gaps.cli.pipeline.pipeline(
                    pipeline_config,
                    cancel=False,
                    monitor=True,
                    background=True,
                )
            else:
                Pipeline.run(pipeline_config, monitor=False)

    def cancel(self):
        """Cancel all pipeline modules for all batch jobs"""
        for sub_directory in self.sub_dirs:
            pipeline_config = sub_directory / self._pipeline_fp.name
            if pipeline_config.is_file():
                Pipeline.cancel_all(pipeline_config)

    def delete(self):
        """Clear all of the batch sub job folders.

        Only the batch sub folders listed in the job summary csv file
        in the batch config directory are deleted.
        """

        fp_job_table = self._base_dir / BATCH_CSV_FN
        if not fp_job_table.exists():
            msg = (
                f"Cannot delete batch jobs without jobs summary table: "
                f"{fp_job_table!r}"
            )
            raise gapsFileNotFoundError(msg)

        job_table = pd.read_csv(fp_job_table, index_col=0)

        if job_table.index.name != "job":
            msg = (
                "Cannot delete batch jobs when the batch summary table "
                'does not have "job" as the index key'
            )
            raise gapsValueError(msg)

        self._remove_sub_dirs(job_table)
        fp_job_table.unlink()

    def _remove_sub_dirs(self, job_table):
        """Remove all the sub-directories tracked in the job table"""
        for sub_dir in job_table.index:
            job_dir = self._base_dir / sub_dir
            if job_dir.exists():
                logger.info("Removing batch job directory: %r", sub_dir)
                shutil.rmtree(job_dir)
            else:
                msg = f"Cannot find batch job directory: {sub_dir!r}"
                warn(msg, gapsWarning)

    def run(self, dry_run=False, monitor_background=False):
        """Run the batch job from a config file.

        Parameters
        ----------
        dry_run : bool
            Flag to make job directories without running.
        monitor_background : bool
            Flag to monitor all batch pipelines continuously
            in the background. Note that the stdout/stderr will not be
            captured, but you can set a pipeline "log_file" to capture
            logs.
        """
        self._make_job_dirs()
        if dry_run:
            return

        cwd = os.getcwd()  # noqa: PTH109
        try:
            self._run_pipelines(monitor_background=monitor_background)
        finally:
            os.chdir(cwd)


def _load_batch_config(config_fp):
    """Load and validate the batch config file (CSV or JSON)"""
    base_dir = Path(config_fp).expanduser().parent.resolve()
    config = _load_batch_config_to_dict(config_fp)
    config = _validate_batch_config(config, base_dir)
    return base_dir, config


def _load_batch_config_to_dict(config_fp):
    """Load the batch file to dict"""
    if Path(config_fp).name.endswith(".csv"):
        return _load_batch_csv(config_fp)
    return load_config(config_fp, resolve_paths=False)


def _load_batch_csv(config_fp):
    """Load batch csv file to dict"""
    table = pd.read_csv(config_fp)
    table = table.where(pd.notna(table), None)
    _validate_batch_table(table)
    return _convert_batch_table_to_dict(table)


def _validate_batch_table(table):
    """Validate batch file CSV table"""
    if "set_tag" not in table or "files" not in table:
        msg = 'Batch CSV config must have "set_tag" and "files" columns'
        raise gapsConfigError(msg)

    set_tags_not_unique = len(table.set_tag.unique()) != len(table)
    if set_tags_not_unique:
        msg = 'Batch CSV config must have completely unique "set_tag" column'
        raise gapsConfigError(msg)

    if "pipeline_config" not in table:
        msg = (
            'Batch CSV config must have "pipeline_config" columns specifying '
            "the pipeline config filename."
        )
        raise gapsConfigError(msg)


def _convert_batch_table_to_dict(table):
    """Convert validated batch csv file to dict"""
    sets = []
    for _, job in table.iterrows():
        job_dict = job.to_dict()
        args = {
            k: [v]
            for k, v in job_dict.items()
            if k not in {"set_tag", "files", "pipeline_config"}
        }
        files = _json_load_with_cleaning(job_dict["files"])
        set_config = {
            "args": args,
            "set_tag": job_dict["set_tag"],
            "files": files,
        }
        sets.append(set_config)

    return {
        "logging": {"log_file": None, "log_level": "INFO"},
        "pipeline_config": table["pipeline_config"].to_numpy()[0],
        "sets": sets,
    }


def _validate_batch_config(config, base_dir):
    """Validate the batch config dict"""
    config = _check_pipeline(config, base_dir)
    return _check_sets(config, base_dir)


def _check_pipeline(config, base_dir):
    """Check the pipeline config file in the batch config"""

    if "pipeline_config" not in config:
        msg = 'Batch config needs "pipeline_config" arg!'
        raise gapsConfigError(msg)

    config["pipeline_config"] = resolve_all_paths(
        config["pipeline_config"], base_dir
    )
    if not Path(config["pipeline_config"]).exists():
        msg = (
            f"Could not find the pipeline config file: "
            f"{config['pipeline_config']!r}"
        )
        raise gapsConfigError(msg)
    return config


def _check_sets(config, base_dir):
    """Check the batch sets for required inputs and valid files"""

    if "sets" not in config:
        msg = 'Batch config needs "sets" arg!'
        raise gapsConfigError(msg)

    batch_sets = []
    for batch_set in config["sets"]:
        if not isinstance(batch_set, dict):
            msg = "Batch sets must be dictionaries."
            raise gapsConfigError(msg)
        if "args" not in batch_set:
            msg = 'All batch sets must have "args" key.'
            raise gapsConfigError(msg)
        if "files" not in batch_set:
            msg = 'All batch sets must have "files" key.'
            raise gapsConfigError(msg)
        batch_set["files"] = resolve_all_paths(batch_set["files"], base_dir)
        for fpath in batch_set["files"]:
            if not Path(fpath).exists():
                msg = f"Could not find file to modify in batch jobs: {fpath!r}"
                raise gapsConfigError(msg)
        batch_sets.append(batch_set)

    config["sets"] = batch_sets
    return config


def _enumerated_product(args):
    """An enumerated product function"""
    return list(zip(product(*(range(len(x)) for x in args)), product(*args)))


def _parse_config(config):
    """Parse batch config object for useful data"""

    sets = set()
    batch_sets = {}

    for batch_set in config["sets"]:
        set_tag = batch_set.get("set_tag", "")
        args = batch_set["args"]

        if set_tag in sets:
            msg = f"Found multiple sets with the same set_tag: {set_tag!r}"
            raise gapsValueError(msg)

        for key, value in args.items():
            if isinstance(value, str):
                msg = (
                    "Batch arguments should be lists but found "
                    f"{key!r}: {value!r}"
                )
                raise gapsValueError(msg)

        sets.add(set_tag)

        products = _enumerated_product(args.values())
        num_batch_jobs = len(products)
        set_str = f" in set {set_tag!r}" if set_tag else ""
        logger.info(
            "Found %d batch projects%s. Creating jobs...",
            num_batch_jobs,
            set_str,
        )
        if num_batch_jobs > _TOO_MANY_JOBS_WARNING_THRESH:
            msg = (
                f"Large number of batch jobs found: {num_batch_jobs:,}! "
                "Proceeding, but consider double checking your config."
            )
            warn(msg, gapsWarning)

        for inds, comb in products:
            arg_combo = dict(zip(args, comb))
            arg_inds = dict(zip(args, inds))
            tag_arg_comb = {
                k: v for k, v in arg_combo.items() if len(args[k]) > 1
            }
            job_tag = _make_job_tag(set_tag, tag_arg_comb, arg_inds)
            batch_sets[job_tag] = BatchSet(
                arg_combo, batch_set["files"], set_tag
            )

    return batch_sets


def _make_job_tag(set_tag, arg_comb, arg_inds):
    """Make a job tags from a unique combination of args + values"""

    job_tag = [set_tag] if set_tag else []

    for arg, value in arg_comb.items():
        arg_tag = "".join(
            [s[0] for s in arg.split("_")]
            + (
                [_format_value(value)]
                if isinstance(value, (int, float))
                else [str(arg_inds[arg])]
            )
        )

        job_tag.append(arg_tag)

    return "_".join(job_tag).rstrip("_")


def _format_value(value):
    """Format the input value as a string"""

    value = str(value).replace(".", "")

    if parse_year(f"_{value}", option="bool"):
        value = f"{value}0"

    return value


def _mod_file(fpath_in, fpath_out, arg_mods):
    """Import and modify the contents of a json. Dump to new file"""
    logger.debug(
        "Copying and modifying run file %r to job: %r", fpath_in, fpath_out
    )
    config_type = ConfigType(fpath_in.name.split(".")[-1])
    config = config_type.load(fpath_in)
    config_type.write(fpath_out, _mod_dict(config, arg_mods))


def _mod_dict(inp, arg_mods):
    """Recursively modify key/value pairs in a dictionary"""

    out = copy.deepcopy(inp)

    if isinstance(inp, dict):
        for key, val in inp.items():
            if key in arg_mods:
                out[key] = _clean_arg(arg_mods[key])
            elif isinstance(val, (list, dict)):
                out[key] = _mod_dict(val, arg_mods)

    elif isinstance(inp, list):
        return [_mod_dict(entry, arg_mods) for entry in inp]

    return out


def _clean_arg(arg):
    """Perform any cleaning steps required before writing to a json"""

    if not isinstance(arg, str):
        return arg

    try:
        return _json_load_with_cleaning(arg)
    except json.decoder.JSONDecodeError:
        logger.debug("Could not load json string: %s", arg)

    return arg


def _copy_batch_file(fp_source, fp_target):
    """Copy file in the batch directory into job directory if needed"""
    if not _source_needs_copying(fp_source, fp_target):
        return

    logger.debug("Copying run file %r to %r", fp_source, fp_target)
    shutil.copyfile(fp_source, fp_target)


def _source_needs_copying(fp_source, fp_target):
    """Determine if source needs to be copied to dest"""
    if not fp_target.exists():
        return True
    return fp_source.lstat().st_mtime > fp_target.lstat().st_mtime


def _json_load_with_cleaning(input_str):
    return json.loads(
        input_str.replace("'", '"')
        .removesuffix('"""')
        .removeprefix('"""')
        .rstrip('"')
        .lstrip('"')
    )
