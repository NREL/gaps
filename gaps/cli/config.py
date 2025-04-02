"""GAPs CLI command for spatially distributed function runs"""

import json
import logging
from copy import deepcopy
from warnings import warn
from pathlib import Path
from itertools import product
from inspect import signature, isclass

import click

from rex.utilities.loggers import init_mult  # cspell:disable-line

from gaps import ProjectPoints
from gaps.status import StatusUpdates, QOSOption, HardwareOption
from gaps.config import load_config
from gaps.log import init_logger
from gaps.cli.execution import kickoff_job
from gaps.cli.documentation import EXTRA_EXEC_PARAMS
from gaps.exceptions import gapsKeyError
from gaps.warn import gapsWarning

logger = logging.getLogger(__name__)

_CMD_LIST = [
    "from gaps.cli.config import run_with_status_updates",
    "from {run_func_module} import {run_func_name}",
    'su_args = "{project_dir}", "{pipeline_step}", "{job_name}"',
    "run_with_status_updates("
    "   {run_func_name}, {node_specific_config}, {logging_options}, su_args, "
    "   {exclude_from_status}"
    ")",
]
TAG = "_j"
MAX_AU_BEFORE_WARNING = {
    "eagle": 10_000,
    "kestrel": 35_000,
}
GAPS_SUPPLIED_ARGS = {
    "tag",
    "command_name",
    "pipeline_step",
    "config_file",
    "project_dir",
    "job_name",
    "out_dir",
    "out_fpath",
    "config",
    "log_directory",
    "verbose",
}


class _FromConfig:
    """Utility class for running a function from a config file"""

    def __init__(self, ctx, config_file, command_config):
        """

        Parameters
        ----------
        ctx : click.Context
            Click context for the invoked command.
        config_file : path-like
            Path to input file containing key-value pairs as input to
            function.
        step : str
            Name of step being run. This will be used to key the status
            dictionary, so it must be unique to the pipeline.
        command_config : `gaps.cli.cli.CLICommandFromFunction`
            A command configuration object containing info such as the
            command name, run function, pre-processing function,
            function documentation, etc.
        """
        self.ctx = ctx
        self.config_file = Path(config_file).expanduser().resolve()
        self.command_config = command_config
        self.config = load_config(config_file)
        self.log_directory = None
        self.verbose = None
        self.exec_kwargs = None
        self.logging_options = None
        self.exclude_from_status = None
        self._include_tag_in_out_fpath = (
            self.command_config.is_split_spatially
            and self.config.get("execution_control", {}).get("nodes", 1) > 1
        )

    @property
    def project_dir(self):
        """`Path`: Path to project directory"""
        return self.config_file.parent

    @property
    def command_name(self):
        """str: Name of command being run"""
        return self.command_config.name

    @property
    def pipeline_step(self):
        """str: Name of pipeline_step being run"""
        return self.ctx.obj.get("PIPELINE_STEP", self.command_name)

    @property
    def job_name(self):
        """str: Name of job being run"""
        return "_".join(
            [self.project_dir.name, self.command_name.replace("-", "_")]
        )

    def enable_logging(self):
        """Enable logging based on config file input"""
        self.log_directory = self.config.pop(
            "log_directory", (self.project_dir / "logs").as_posix()
        )
        self.log_directory = Path(self.log_directory)
        self.verbose = self.config.pop(
            "log_level", "INFO"
        ) == "DEBUG" or self.ctx.obj.get("VERBOSE", False)
        pipe_log_file = self.log_directory / f"{self.job_name}.log"
        init_logger(
            stream=self.ctx.obj.get("LOG_STREAM", True),
            level="DEBUG" if self.verbose else "INFO",
            file=pipe_log_file.as_posix(),
        )
        return self

    def validate_config(self):
        """Validate the user input config file"""
        logger.debug("Validating %r", str(self.config_file))
        _validate_config(self.config, self.command_config.documentation)
        return self

    def preprocess_config(self):
        """Apply preprocessing function to config file"""

        preprocessor_kwargs = {
            "config": self.config,
            "command_name": self.command_name,
            "pipeline_step": self.pipeline_step,
            "config_file": self.config_file,
            "project_dir": self.project_dir,
            "job_name": self.job_name,
            "out_dir": self.project_dir,
            "out_fpath": self.project_dir / self.job_name,
            "log_directory": self.log_directory,
            "verbose": self.verbose,
        }
        extra_preprocessor_kwargs = {
            k: self.config[k]
            for k in self.command_config.preprocessor_args
            if k not in preprocessor_kwargs and k in self.config
        }
        preprocessor_kwargs.update(extra_preprocessor_kwargs)
        preprocessor_defaults = {
            k: v
            for k, v in self.command_config.preprocessor_defaults.items()
            if k not in preprocessor_kwargs
        }
        preprocessor_kwargs.update(preprocessor_defaults)
        preprocessor_kwargs = {
            k: v
            for k, v in preprocessor_kwargs.items()
            if k in self.command_config.preprocessor_args
        }
        self.config = self.command_config.config_preprocessor(
            **preprocessor_kwargs
        )
        return self

    def set_exec_kwargs(self):
        """Extract the execution control dictionary"""
        self.exec_kwargs = {
            "option": "local",
            "sh_script": "",
            "stdout_path": (self.log_directory / "stdout").as_posix(),
        }

        self.exec_kwargs.update(self.config.get("execution_control", {}))
        extra_params = set()
        for extra_exec_param in EXTRA_EXEC_PARAMS:
            if extra_exec_param in self.config:
                extra_params.add(extra_exec_param)
                param = self.config.pop(extra_exec_param)
                self.exec_kwargs[extra_exec_param] = param

        if extra_params:
            msg = (
                f"Found key(s) {extra_params} outside of 'execution_control'. "
                "Moving these keys into 'execution_control' block. To "
                "silence this warning, please specify these keys inside of "
                "the 'execution_control' block."
            )
            warn(msg, gapsWarning)

        return self

    def set_logging_options(self):
        """Assemble the logging options dictionary"""
        self.logging_options = {
            "name": self.job_name,
            "log_directory": self.log_directory.as_posix(),
            "verbose": self.verbose,
            "node": self.exec_kwargs.get("option", "local") != "local",
        }
        return self

    def set_exclude_from_status(self):
        """Assemble the exclusion keyword set"""
        self.exclude_from_status = {"project_points"}
        self.exclude_from_status |= set(
            self.config.pop("exclude_from_status", set())
        )
        self.exclude_from_status = [
            key.lower()
            for key in self.exclude_from_status
            if key.lower() != "tag"
        ]
        return self

    def prepare_context(self):
        """Add required key-val;ue pairs to context object"""
        self.ctx.obj["COMMAND_NAME"] = self.command_name
        self.ctx.obj["PIPELINE_STEP"] = self.pipeline_step
        self.ctx.obj["OUT_DIR"] = self.project_dir
        return self

    def log_job_info(self):
        """Log information about job submission"""
        logger.info(
            "Running %s from config file: %r",
            self.command_name,
            str(self.config_file),
        )
        logger.info("Target output directory: %r", str(self.project_dir))
        logger.info("Target logging directory: %r", str(self.log_directory))
        return self

    def kickoff_jobs(self):
        """Kickoff jobs across nodes based on config and run function"""
        keys_to_run, lists_to_run = self._keys_and_lists_to_run()

        jobs = sorted(product(*lists_to_run))
        self._warn_about_excessive_au_usage(len(jobs))
        extra_exec_args = self._extract_extra_exec_args_for_command()

        for tag, values, exec_kwargs in self._with_tagged_context(jobs):
            node_specific_config = self._compile_node_config(tag)
            node_specific_config.update(extra_exec_args)

            for key, val in zip(keys_to_run, values):
                if isinstance(key, str):
                    node_specific_config.update({key: val})
                else:
                    node_specific_config.update(dict(zip(key, val)))

            cmd = self._compile_run_command(node_specific_config)
            kickoff_job(self.ctx, cmd, exec_kwargs)

        return self

    def _with_tagged_context(self, jobs):
        """Iterate over jobs and populate context with job name"""
        num_jobs_submit = len(jobs)

        exec_kwargs = deepcopy(self.exec_kwargs)
        num_test_nodes = exec_kwargs.pop("num_test_nodes", None)
        if num_test_nodes is None:
            num_test_nodes = float("inf")

        for node_index, values in enumerate(jobs):
            if node_index >= num_test_nodes:
                return

            tag = _tag(node_index, num_jobs_submit)
            self.ctx.obj["NAME"] = f"{self.job_name}{tag}"
            yield tag, values, exec_kwargs

    def _compile_node_config(self, tag):
        """Compile initial node-specific config"""
        job_name = self.ctx.obj["NAME"]
        node_specific_config = deepcopy(self.config)
        node_specific_config.pop("execution_control", None)
        node_specific_config.update(
            {
                "tag": tag,
                "command_name": self.command_name,
                "pipeline_step": self.pipeline_step,
                "config_file": self.config_file.as_posix(),
                "project_dir": self.project_dir.as_posix(),
                "job_name": job_name,
                "out_dir": self.project_dir.as_posix(),
                "out_fpath": self._suggested_stem(job_name).as_posix(),
                "run_method": getattr(self.command_config, "run_method", None),
            }
        )
        return node_specific_config

    def _compile_run_command(self, node_specific_config):
        """Create run command from config and job name"""
        job_name = self.ctx.obj["NAME"]
        cmd = "; ".join(_CMD_LIST).format(
            run_func_module=self.command_config.runner.__module__,
            run_func_name=self.command_config.runner.__name__,
            node_specific_config=as_script_str(node_specific_config),
            project_dir=self.project_dir.as_posix(),
            logging_options=as_script_str(self.logging_options),
            exclude_from_status=as_script_str(self.exclude_from_status),
            pipeline_step=self.pipeline_step,
            job_name=job_name,
        )
        return f"python -c {cmd!r}"

    def _suggested_stem(self, job_name_with_tag):
        """Determine suggested filepath with filename stem"""
        if self._include_tag_in_out_fpath:
            return self.project_dir / job_name_with_tag
        return self.project_dir / self.job_name

    def _extract_extra_exec_args_for_command(self):
        """Dictionary of function args from the exec block"""
        extra_exec_args = {}
        for param in EXTRA_EXEC_PARAMS:
            if param not in self.exec_kwargs:
                continue
            extra_exec_args[param] = self.exec_kwargs.pop(param)
        return extra_exec_args

    def _keys_and_lists_to_run(self):
        """Compile run lists based on `command_config.split_keys`"""
        keys_to_run = []
        lists_to_run = []
        keys = sorted(self.command_config.split_keys, key=_project_points_last)
        for key_group in keys:
            keys_to_run.append(key_group)
            if isinstance(key_group, str):
                lists_to_run.append(self.config.get(key_group) or [None])
            else:
                lists_to_run.append(
                    list(
                        zip(*[self.config.get(k) or [None] for k in key_group])
                    )
                )
        return keys_to_run, lists_to_run

    def _warn_about_excessive_au_usage(self, num_jobs):
        """Warn if max job runtime exceeds AU threshold"""
        try:
            max_walltime_per_job = float(self.exec_kwargs.get("walltime"))
        except (TypeError, ValueError):
            return

        qos = self.exec_kwargs.get("qos") or str(QOSOption.UNSPECIFIED)
        try:
            qos_charge_factor = QOSOption(str(qos)).charge_factor
        except ValueError:
            qos_charge_factor = 1

        hardware = self.exec_kwargs.get("option", "local")
        if hardware.casefold() == HardwareOption.SLURM:
            available_opts = [
                f"{opt}"
                for opt in HardwareOption
                if opt != HardwareOption.SLURM and opt.is_hpc
            ]
            msg = (
                "Detected option='slurm' in execution control. Please do not "
                "use this option unless your HPC is explicitly not supported. "
                f"Available HPC options are: {available_opts}"
            )
            warn(msg, gapsWarning)
            return

        try:
            hardware_charge_factor = HardwareOption(hardware).charge_factor
        except ValueError:
            return

        max_au_usage = int(
            num_jobs
            * float(max_walltime_per_job)
            * qos_charge_factor
            * hardware_charge_factor
        )
        max_au_thresh = MAX_AU_BEFORE_WARNING.get(
            hardware.casefold(), float("inf")
        )
        if max_au_usage > max_au_thresh:
            msg = f"Job may use up to {max_au_usage:,} AUs!"
            warn(msg, gapsWarning)

    def run(self):
        """Run the entire config pipeline"""
        try:
            return (
                self.enable_logging()
                .validate_config()
                .log_job_info()
                .preprocess_config()
                .set_exec_kwargs()
                .set_logging_options()
                .set_exclude_from_status()
                .prepare_context()
                .kickoff_jobs()
            )
        except Exception:
            logger.exception("Encountered error while kicking off jobs")
            raise


@click.pass_context
def from_config(ctx, config_file, command_config, pipeline_step=None):
    """Run command from a config file"""
    if pipeline_step is not None:
        ctx.obj["PIPELINE_STEP"] = pipeline_step
    _FromConfig(ctx, config_file, command_config).run()


def _validate_config(config, documentation):
    """Ensure required keys exist and warn user about extra keys"""
    _ensure_required_args_exist(config, documentation)
    _warn_about_extra_args(config, documentation)


def _ensure_required_args_exist(config, documentation):
    """Make sure that args required for func to run exist in config"""
    missing = {
        name for name in documentation.required_args if name not in config
    }

    missing = _mark_extra_exec_params_missing_if_needed(
        documentation, config, missing
    )

    if any(missing):
        msg = (
            f"The following required keys are missing from the configuration "
            f"file: {missing}"
        )
        raise gapsKeyError(msg)


def _mark_extra_exec_params_missing_if_needed(documentation, config, missing):
    """Add extra exec params as missing if not in `execution_control"""
    exec_control = config.get("execution_control", {})
    for param in EXTRA_EXEC_PARAMS:
        param_required = documentation.param_required(param)
        param_missing = param not in config and param not in exec_control
        if param_required and param_missing:
            missing |= {param}
    return missing


def _warn_about_extra_args(config, documentation):
    """Warn user about extra unused keys in the config file"""
    extra = {name for name in config if not _param_in_sig(name, documentation)}
    extra -= {"execution_control", "project_points_split_range"}
    if any(extra):
        msg = (
            "Found unused keys in the configuration file: %s. To silence "
            "this warning, please remove these keys from the input "
            "configuration file."
        )
        warn(msg % extra, gapsWarning)


def _param_in_sig(param, documentation):
    """Determine if ``name`` is an argument in any func signatures"""
    return any(
        param in _public_args(signature)
        for signature in documentation.signatures
    )


def _public_args(func_signature):
    """Gather set of all "public" function args"""
    return {
        param
        for param in func_signature.parameters
        if not param.startswith("_")
    }


def _project_points_last(key):
    """Sorting that always puts "project_points_split_range" last"""
    if isinstance(key, str):
        if key.casefold() == "project_points_split_range":
            return (chr(0x10FFFF),)  # PEP 393
        return (key,)
    return key


def _tag(node_index, num_jobs):
    """Determine node tag based on total number of jobs"""
    n_zfill = len(str(max(0, num_jobs - 1)))
    if num_jobs > 1:
        return f"{TAG}{str(node_index).zfill(n_zfill)}"
    return ""


def as_script_str(input_):
    """Convert input to how it would appear in a python script.

    Essentially this means the input is dumped to json format with some
    minor replacements (e.g. null -> None, etc.). Importantly, all
    string inputs are wrapped in double quotes.

    Parameters
    ----------
    input_ : obj
        Any object that can be serialized with :func:`json.dumps`.

    Returns
    -------
    str
        Input object as it would appear in a python script.

    Examples
    --------
    >>> as_script_str("a")
    "a"

    >>> as_script_str({"a": None, "b": True, "c": False, "d": 3,
    ...                "e": [{"t": "hi"}]})
    {"a": None, "b": True, "c": False, "d": 3, "e": [{"t": "hi"}]}
    """
    return (
        json.dumps(input_)
        .replace("null", "None")
        .replace("false", "False")
        .replace("true", "True")
    )


def run_with_status_updates(
    run_func, config, logging_options, status_update_args, exclude
):
    """Run a function and write status updated before/after execution.

    Parameters
    ----------
    run_func : callable
        A function to run.
    config : dict
        Dictionary of node-specific inputs to `run_func`.
    logging_options : dict
        Dictionary of logging options containing at least the following
        key-value pairs:

            name : str
                Job name; name of log file.
            log_directory : path-like
                Path to log file directory.
            verbose : bool
                Option to turn on debug logging.
            node : bool
                Flag for whether this is a node-level logger. If this is
                a node logger, and the log level is info, the log_file
                will be `None` (sent to stdout).

    status_update_args : iterable
        An iterable containing the first three initializer arguments for
        :class:`StatusUpdates`.
    exclude : collection | None
        A collection (list, set, dict, etc.) of keys that should be
        excluded from the job status file that is written before/after
        the function runs.
    """

    # initialize loggers for multiple modules
    init_mult(  # cspell:disable-line
        logging_options["name"],
        logging_options["log_directory"],
        modules=[run_func.__module__.split(".")[0], "gaps", "rex"],
        verbose=logging_options["verbose"],
        node=logging_options["node"],
    )

    run_kwargs = node_kwargs(run_func, config)
    exclude = exclude or set()
    job_attrs = {
        key: value for key, value in run_kwargs.items() if key not in exclude
    }
    status_update_args = *status_update_args, job_attrs
    with StatusUpdates(*status_update_args) as status:
        out = run_func(**run_kwargs)
        if method := config.get("run_method"):
            func = getattr(out, method)
            run_kwargs = node_kwargs(func, config)
            status.job_attrs.update(
                {
                    key: value
                    for key, value in run_kwargs.items()
                    if key not in exclude
                }
            )
            out = func(**run_kwargs)
        status.out_file = out


def node_kwargs(run_func, config):
    """Compile the function inputs arguments for a particular node.

    Parameters
    ----------
    run_func : callable
        A function to run.
    config : dict
        Dictionary of node-specific inputs to `run_func`.
    logging_options : dict
        Dictionary of logging options containing at least the following
        key-value pairs:

            name : str
                Job name; name of log file.
            log_directory : path-like
                Path to log file directory.
            verbose : bool
                Option to turn on debug logging.
            node : bool
                Flag for whether this is a node-level logger. If this is
                a node logger, and the log level is info, the log_file
                will be `None` (sent to stdout).


    Returns
    -------
    dict
        Function run kwargs to be used on this node.
    """

    split_range = config.pop("project_points_split_range", None)
    if split_range is not None:
        config["project_points"] = ProjectPoints.from_range(
            split_range, config["project_points"]
        )

    sig = signature(run_func)
    run_kwargs = {k: v for k, v in config.items() if k in sig.parameters}
    verb = "Initializing" if isclass(run_func) else "Running"
    logger.debug("%s %r with kwargs: %s", verb, run_func.__name__, run_kwargs)
    return run_kwargs
