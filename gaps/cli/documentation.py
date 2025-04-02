"""CLI documentation utilities"""

from copy import deepcopy
from itertools import chain
from functools import lru_cache
from inspect import signature, isclass

from numpydoc.docscrape import NumpyDocString

from gaps.status import HardwareOption
from gaps.config import config_as_str_for_docstring, ConfigType
from gaps.utilities import _is_sphinx_build


_TAB_LENGTH = 4
DEFAULT_EXEC_VALUES = {
    "option": "local",
    "allocation": "[REQUIRED IF ON HPC]",
    "walltime": "[REQUIRED IF ON HPC]",
    "qos": "normal",
    "memory": None,
    "nodes": 1,
    "queue": None,
    "feature": None,
    "conda_env": None,
    "module": None,
    "sh_script": None,
    "num_test_nodes": None,
}

EXTRA_EXEC_PARAMS = {
    "max_workers": "Maximum number of parallel workers run on each node.",
    "sites_per_worker": "Number of sites to run in series on a worker.",
    "memory_utilization_limit": """Memory utilization limit (fractional).
                Must be a value between 0 and 100. This input sets
                how much data will be stored in-memory at any
                given time before flushing to disk.
    """,
    "timeout": """Number of seconds to wait for parallel run iteration
                to complete before early termination.
    """,
    "pool_size": """Number of futures to submit to a single process pool
                for parallel futures.
    """,
}

CONFIG_TYPES = [
    ConfigType.JSON,
    ConfigType.YAML,
    ConfigType.TOML,
]

MAIN_DOC = """{name} Command Line Interface.

Typically, a good place to start is to set up a {name} job with a pipeline
config that points to several {name} modules that you want to run in serial.

To begin, you can generate some template configuration files using::

    $ {name} template-configs

By default, this generates template JSON configuration files, though you
can request JSON5, YAML, or TOML configuration files instead. You can run
``$ {name} template-configs --help`` on the command line to see all available
options for the ``template-configs`` command. Once the template configuration
files have been generated, you can fill them out by referring to the
module CLI documentation (if available) or the help pages of the module CLIs
for more details on the config options for each CLI command::

    {cli_help_str}

After appropriately filling our the configuration files for each module you
want to run, you can call the {name} pipeline CLI using::

    $ {name} pipeline -c config_pipeline.json

This command will run each pipeline step in sequence.

.. Note:: You will need to re-submit the ``pipeline`` command above after
          each completed pipeline step.

To check the status of the pipeline, you can run::

    $ {name} status

This will print a report to the command line detailing the progress of the
current pipeline. See ``$ {name} status --help`` for all status command
options.

If you need to parameterize the pipeline execution, you can use the ``batch``
command. For details on setting up a batch config file, see the documentation
or run::

    $ {name} batch --help

on the command line. Once you set up a batch config file, you can execute
it using::

    $ {name} batch -c config_batch.json


For more information on getting started, see the
`How to Run a Model Powered by GAPs
<https://nrel.github.io/gaps/misc/examples.users.html>`_ guide.

The general structure of the {name} CLI is given below.
"""

PIPELINE_CONFIG_DOC = """
Path to the ``pipeline`` configuration file. This argument can be
left out, but *one and only one file* with "pipeline" in the
name should exist in the directory and contain the config
information. {sample_config}

Parameters
----------
pipeline : list of dicts
    A list of dictionaries, where each dictionary represents one
    step in the pipeline. Each dictionary should have one of two
    configurations:

        - A single key-value pair, where the key is the name of
          the CLI command to run, and the value is the path to
          a config file containing the configuration for that
          command
        - Exactly two key-value pairs, where one of the keys is
          ``"command"``, with a value that points to the name of
          a command to execute, while the second key is a _unique_
          user-defined name of the pipeline step to execute, with
          a value that points to the path to a config file
          containing the configuration for the command specified
          by the other key. This configuration allows users to
          specify duplicate commands as part of their pipeline
          execution.

logging : dict, optional
    Dictionary containing keyword-argument pairs to pass to
    `init_logger <https://tinyurl.com/47hakp7f/>`_. This
    initializes logging for the submission portion of the
    pipeline. Note, however, that each step (command) will
    **also** record the submission step log output to a
    common "project" log file, so it's only ever necessary
    to use this input if you want a different (lower) level
    of verbosity than the `log_level` specified in the
    config for the step of the pipeline being executed.

"""

BATCH_CONFIG_DOC = """
Path to the ``batch`` configuration file. {sample_config}

Parameters
----------
logging : dict, optional
    Dictionary containing keyword-argument pairs to pass to
    `init_logger <https://tinyurl.com/47hakp7f/>`_. This
    initializes logging for the batch command. Note that
    each pipeline job submitted via batch has it's own
    ``logging`` key that will initialize pipeline step
    logging. Therefore, it's only ever necessary to use
    this input if you want logging information about the
    batching portion of the execution.
pipeline_config : str
    Path to the pipeline configuration defining the commands to
    run for every parametric set.
sets : list of dicts
    A list of dictionaries, where each dictionary defines a
    "set" of parametric runs. Each dictionary should have
    the following keys:

        args : dict
            A dictionary defining the arguments across all input
            configuration files to parameterize. Each argument
            to be parametrized should be a key in this
            dictionary, and the value should be a **list** of the
            parameter values to run for this argument (single-item lists
            are allowed and can be used to vary a parameter value across
            sets).

            {batch_args_dict}

            Remember that the keys in the ``args`` dictionary
            should be part of (at least) one of your other
            configuration files.
        files : list
            A list of paths to the configuration files that
            contain the arguments to be updated for every
            parametric run. Arguments can be spread out over
            multiple files. {batch_files}
        set_tag : str, optional
            Optional string defining a set tag that will prefix
            each job tag for this set. This tag does not need to
            include an underscore, as that is provided during
            concatenation.

"""

_BATCH_ARGS_DICT = """.. tabs::

                .. group-tab:: JSON/JSON5
                    ::

                        {sample_json_args_dict}

                .. group-tab:: YAML
                    ::

                        {sample_yaml_args_dict}

                .. group-tab:: TOML
                    ::

                        {sample_toml_args_dict}


            This example would run a total of six pipelines, one
            with each of the following arg combinations:
            ::

                input_constant_1=18.20, path_to_a_file="/first/path.h5"
                input_constant_1=18.20, path_to_a_file="/second/path.h5"
                input_constant_1=18.20, path_to_a_file="/third/path.h5"
                input_constant_1=19.04, path_to_a_file="/first/path.h5"
                input_constant_1=19.04, path_to_a_file="/second/path.h5"
                input_constant_1=19.04, path_to_a_file="/third/path.h5"


"""
_BATCH_FILES = """For example:

            .. tabs::

                .. group-tab:: JSON/JSON5
                    ::

                        {sample_json_files}

                .. group-tab:: YAML
                    ::

                        {sample_yaml_files}

                .. group-tab:: TOML
                    ::

                        {sample_toml_files}


"""

CONFIG_DOC = """
Path to the ``{name}`` configuration file. {sample_config}

{docstring}

Note that you may remove any keys with a ``null`` value if you do not intend to update them yourself.
"""  # noqa: E501
SAMPLE_CONFIG_DOC = """Below is a sample template config

.. tabs::

    .. group-tab:: JSON/JSON5
        ::

            {template_json_config}

    .. group-tab:: YAML
        ::

            {template_yaml_config}

    .. group-tab:: TOML
        ::

            {template_toml_config}

"""
COMMAND_DOC = """
Execute the ``{name}`` step from a config file.

{desc}

The general structure for calling this CLI command is given below
(add ``--help`` to print help info to the terminal).

"""
EXEC_CONTROL_DOC = """
Parameters
----------
    execution_control : dict
        Dictionary containing execution control arguments. Allowed
        arguments are:

        :option: ({opts})
            Hardware run option. Determines the type of job
            scheduler to use as well as the base AU cost. The
            "slurm" option is a catchall for HPC systems
            that use the SLURM scheduler and **should only be
            used if desired hardware is not listed above**. If
            "local", no other HPC-specific keys in are
            required in `execution_control` (they are ignored
            if provided).
        :allocation: (str)
            HPC project (allocation) handle.
        :walltime: (int)
            Node walltime request in hours.
        :qos: (str, optional)
            Quality-of-service specifier. For Kestrel users:
            This should be one of {{'standby', 'normal',
            'high'}}. Note that 'high' priority doubles the AU
            cost. By default, ``"normal"``.
        :memory: (int, optional)
            Node memory max limit (in GB). By default, ``None``,
            which uses the scheduler's default memory limit.
            For Kestrel users: If you would like to use the
            full node memory, leave this argument unspecified
            (or set to ``None``) if you are running on standard
            nodes. However, if you would like to use the bigmem
            nodes, you must specify the full upper limit of
            memory you would like for your job, otherwise you
            will be limited to the standard node memory size
            (250GB).{n}{eep}
        :queue: (str, optional; PBS ONLY)
            HPC queue to submit job to. Examples include: 'debug',
            'short', 'batch', 'batch-h', 'long', etc.
            By default, ``None``, which uses "test_queue".
        :feature: (str, optional)
            Additional flags for SLURM job (e.g. "-p debug").
            By default, ``None``, which does not specify any
            additional flags.
        :conda_env: (str, optional)
            Name of conda environment to activate. By default,
            ``None``, which does not load any environments.
        :module: (str, optional)
            Module to load. By default, ``None``, which does not
            load any modules.
        :sh_script: (str, optional)
            Extra shell script to run before command call.
            By default, ``None``, which does not run any
            scripts.
        :num_test_nodes: (str, optional)
            Number of nodes to submit before terminating the
            submission process. This can be used to test a
            new submission configuration without submitting
            all nodes (i.e. only running a handful to ensure
            the inputs are specified correctly and the
            outputs look reasonable). By default, ``None``,
            which submits all node jobs.

        Only the `option` key is required for local execution. For
        execution on the HPC, the `allocation` and `walltime` keys are also
        required. All other options are populated with default values,
        as seen above.
    log_directory : str
        Path to directory where logs should be written. Path can be relative
        and does not have to exist on disk (it will be created if missing).
        By default, ``"./logs"``.
    log_level : {{"DEBUG", "INFO", "WARNING", "ERROR"}}
        String representation of desired logger verbosity. Suitable options
        are ``DEBUG`` (most verbose), ``INFO`` (moderately verbose),
        ``WARNING`` (only log warnings and errors), and ``ERROR`` (only log
        errors). By default, ``"INFO"``.

"""
NODES_DOC = (
    "\n        :nodes: (int, optional)"
    "\n            Number of nodes to split the project points across. "
    "\n            Note that the total number of requested nodes for "
    "\n            a job may be larger than this value if the command"
    "\n            splits across other inputs. Default is ``1``."
)
EXTRA_EXEC_PARAM_DOC = "\n        :{name}: ({type})\n            {desc}"


class CommandDocumentation:
    """Generate documentation for a command.

    Commands are typically comprised of one or more functions. This
    definition includes class initializers and object methods.
    Documentation is compiled from all input functions and used to
    generate CLI help docs and template configuration files.
    """

    REQUIRED_TAG = "[REQUIRED]"

    def __init__(self, *functions, skip_params=None, is_split_spatially=False):
        """
        Parameters
        ----------
        *functions : callables
            Functions that comprise a single command for which to
            generate documentation. **IMPORTANT** The extended summary
            will be pulled form the first function only!
        skip_params : set, optional
            Set of parameter names (str) to exclude from documentation.
            Typically this is because the user would not explicitly have
            to specify these. By default, `None`.
        is_split_spatially : bool, optional
            Flag indicating whether or not this function is split
            spatially across nodes. If `True`, a "nodes" option is added
            to the execution control block of the generated
            documentation. By default, `False`.
        """
        self.signatures = [
            signature(func) for func in _as_functions(functions)
        ]
        self.docs = [
            NumpyDocString(func.__doc__ or "")
            for func in _as_functions(functions)
        ]
        self.param_docs = {
            p.name: p for doc in self.docs for p in doc["Parameters"]
        }
        self.skip_params = set() if skip_params is None else set(skip_params)
        self.skip_params |= {"cls", "self"} | set(EXTRA_EXEC_PARAMS)
        self.is_split_spatially = is_split_spatially

    @property
    def default_exec_values(self):
        """dict: Default "execution_control" config"""
        exec_vals = deepcopy(DEFAULT_EXEC_VALUES)
        if not self.is_split_spatially:
            exec_vals.pop("nodes", None)
        for param in EXTRA_EXEC_PARAMS:
            if self._param_in_func_signature(param):
                exec_vals[param] = (
                    self.REQUIRED_TAG
                    if self.param_required(param)
                    else self._param_value(param).default
                )
        return exec_vals

    @property
    def exec_control_doc(self):
        """str: Execution_control documentation"""
        nodes_doc = NODES_DOC if self.is_split_spatially else ""
        hardware_options = str([f"{opt}" for opt in HardwareOption])
        hardware_options = hardware_options.replace("[", "{").replace("]", "}")
        return EXEC_CONTROL_DOC.format(
            opts=hardware_options, n=nodes_doc, eep=self._extra_exec_param_doc
        )

    @property
    def _extra_exec_param_doc(self):
        """str: Docstring formatted with the info from the input func"""
        return "".join(
            [
                self._format_extra_exec_param_doc(param_name)
                for param_name in EXTRA_EXEC_PARAMS
            ]
        )

    def _format_extra_exec_param_doc(self, param_name):
        """Format extra exec control parameters"""
        param = self.param_docs.get(param_name)
        try:
            return EXTRA_EXEC_PARAM_DOC.format(
                name=param_name, type=param.type, desc=" ".join(param.desc)
            )
        except AttributeError:
            pass

        if self._param_in_func_signature(param_name):
            if self.param_required(param_name):
                param_type = "int"
                default_text = ""
            else:
                default_val = self._param_value(param_name).default
                if default_val is None:
                    param_type = "int, optional"
                else:
                    param_type = f"{type(default_val).__name__}, optional"
                default_text = f"By default, ``{default_val}``."
            return EXTRA_EXEC_PARAM_DOC.format(
                name=param_name,
                type=param_type,
                desc=" ".join([EXTRA_EXEC_PARAMS[param_name], default_text]),
            )
        return ""

    @property
    def required_args(self):
        """set: Required parameters of the input function"""
        return {
            name
            for sig in self.signatures
            for name, param in sig.parameters.items()
            if not name.startswith("_")
            and param.default is param.empty
            and name not in self.skip_params
        }

    @property
    def template_config(self):
        """dict: A template configuration file for this function"""
        config = {
            "execution_control": self.default_exec_values,
            "log_directory": "./logs",
            "log_level": "INFO",
        }
        config.update(
            {
                x: self.REQUIRED_TAG if v.default is v.empty else v.default
                for sig in self.signatures
                for x, v in sig.parameters.items()
                if not x.startswith("_") and x not in self.skip_params
            }
        )
        return config

    @property
    def _parameter_npd(self):
        """NumpyDocString: Parameter help `NumpyDocString` instance"""
        param_doc = NumpyDocString("")
        param_doc["Parameters"] = [
            p
            for p in self.param_docs.values()
            if p.name in self.template_config
        ]
        return param_doc

    @property
    def parameter_help(self):
        """str: Parameter help for the func"""
        return str(self._parameter_npd)

    @property
    def hpc_parameter_help(self):
        """str: Function parameter help, including execution control"""
        exec_dict_param = [
            p
            for p in NumpyDocString(self.exec_control_doc)["Parameters"]
            if p.name in {"execution_control", "log_directory", "log_level"}
        ]
        param_doc = deepcopy(self._parameter_npd)
        param_doc["Parameters"] = exec_dict_param + param_doc["Parameters"]
        return "\n".join(_format_lines(str(param_doc).split("\n")))

    @property
    def extended_summary(self):
        """str: Function extended summary, without extra whitespace"""
        return "\n".join(
            _uniform_space_strip(self.docs[0]["Extended Summary"])
        )

    def config_help(self, command_name):
        """Generate a config help string for a command.

        Parameters
        ----------
        command_name : str
            Name of command for which the config help is being
            generated.

        Returns
        -------
        str
            Help string for the config file.
        """
        if _is_sphinx_build():
            sample_config = SAMPLE_CONFIG_DOC.format(
                template_json_config=config_as_str_for_docstring(
                    self.template_config, config_type=ConfigType.JSON
                ),
                template_yaml_config=config_as_str_for_docstring(
                    self.template_config, config_type=ConfigType.YAML
                ),
                template_toml_config=config_as_str_for_docstring(
                    self.template_config, config_type=ConfigType.TOML
                ),
            )
        else:
            sample_config = ""

        doc = CONFIG_DOC.format(
            name=command_name,
            sample_config=sample_config,
            docstring=self.hpc_parameter_help,
        )
        return _cli_formatted(doc)

    def command_help(self, command_name):
        """Generate a help string for a command.

        Parameters
        ----------
        command_name : str
            Name of command for which the help string is being
            generated.

        Returns
        -------
        str
            Help string for the command.
        """
        doc = COMMAND_DOC.format(name=command_name, desc=self.extended_summary)
        return _cli_formatted(doc)

    @lru_cache(maxsize=16)  # noqa: B019
    def _param_value(self, param):
        """Extract parameter if it exists in signature"""
        for sig in self.signatures:
            for name in sig.parameters:
                if name == param:
                    return sig.parameters[param]
        return None

    @lru_cache(maxsize=16)  # noqa: B019
    def _param_in_func_signature(self, param):
        """`True` if `param` is a param of the input function"""
        return self._param_value(param) is not None

    @lru_cache(maxsize=16)  # noqa: B019
    def param_required(self, param):
        """Check whether a parameter is required for the run function

        Parameters
        ----------
        param : str
            Name of parameter to check.

        Returns
        -------
        bool
            ``True`` if `param` is a required parameter of `func`.
        """
        param = self._param_value(param)
        if param is None:
            return False
        return param.default is param.empty


def _main_command_help(prog_name, commands):
    """Generate main command help from commands input"""
    cli_help_str = "\n\n    ".join(
        [f"$ {prog_name} --help"]
        + [f"$ {prog_name} {command.name} --help" for command in commands]
    )
    return MAIN_DOC.format(name=prog_name, cli_help_str=cli_help_str)


def _pipeline_command_help(pipeline_config):  # pragma: no cover
    """Generate pipeline command help from a sample config"""
    if not _is_sphinx_build():
        return _cli_formatted(PIPELINE_CONFIG_DOC.format(sample_config=""))

    template_names = [
        "template_json_config",
        "template_yaml_config",
        "template_toml_config",
    ]
    sample_config = SAMPLE_CONFIG_DOC.format(
        **_format_dict(pipeline_config, template_names)
    )
    doc = PIPELINE_CONFIG_DOC.format(sample_config=sample_config)
    return _cli_formatted(doc)


def _batch_command_help():  # pragma: no cover
    """Generate batch command help from a sample config"""
    if not _is_sphinx_build():
        doc = BATCH_CONFIG_DOC.format(
            sample_config="", batch_args_dict="", batch_files=""
        )
        return _cli_formatted(doc)

    format_inputs = {}
    template_config = {
        "logging": {"log_file": None, "log_level": "INFO"},
        "pipeline_config": CommandDocumentation.REQUIRED_TAG,
        "sets": [
            {
                "args": CommandDocumentation.REQUIRED_TAG,
                "files": CommandDocumentation.REQUIRED_TAG,
                "set_tag": "set1",
            },
            {
                "args": CommandDocumentation.REQUIRED_TAG,
                "files": CommandDocumentation.REQUIRED_TAG,
                "set_tag": "set2",
            },
        ],
    }
    template_names = [
        "template_json_config",
        "template_yaml_config",
        "template_toml_config",
    ]

    format_inputs["sample_config"] = SAMPLE_CONFIG_DOC.format(
        **_format_dict(template_config, template_names)
    )

    sample_args_dict = {
        "args": {
            "input_constant_1": [18.02, 19.04],
            "path_to_a_file": [
                "/first/path.h5",
                "/second/path.h5",
                "/third/path.h5",
            ],
        }
    }
    sample_args_names = [
        "sample_json_args_dict",
        "sample_yaml_args_dict",
        "sample_toml_args_dict",
    ]
    format_inputs["batch_args_dict"] = _BATCH_ARGS_DICT.format(
        **_format_dict(sample_args_dict, sample_args_names, batch_docs=True)
    )

    sample_files = {"files": ["./config_run.yaml", "./config_analyze.json"]}
    sample_files_names = [
        "sample_json_files",
        "sample_yaml_files",
        "sample_toml_files",
    ]
    format_inputs["batch_files"] = _BATCH_FILES.format(
        **_format_dict(sample_files, sample_files_names, batch_docs=True)
    )

    doc = BATCH_CONFIG_DOC.format(**format_inputs)
    return _cli_formatted(doc)


def _format_dict(sample, names, batch_docs=False):  # pragma: no cover
    """Format a sample into a documentation config"""
    configs = {}
    for name, c_type in zip(names, CONFIG_TYPES):
        configs[name] = config_as_str_for_docstring(
            sample,
            config_type=c_type,
            num_spaces=(20 if "json" in name else 24) if batch_docs else 12,
        )
        if batch_docs and "json" in name:
            configs[name] = "\n".join(configs[name].split("\n")[1:-1]).lstrip()
    return configs


def _as_functions(functions):
    """Yield from input, converting all classes to their __init__"""
    for func in functions:
        if isclass(func):
            func = func.__init__  # noqa: PLW2901
        yield func


def _line_needs_newline(line):
    r"""Determine whether a \n should be added to the current line"""
    if any(
        f":{key}:" in line
        for key in chain(DEFAULT_EXEC_VALUES, EXTRA_EXEC_PARAMS)
    ):
        return True
    if not line.startswith("    "):
        return True
    if len(line) <= _TAB_LENGTH:
        return True
    return line[_TAB_LENGTH] == " "


def _format_lines(lines):
    """Format docs into longer lines for easier wrapping in CLI"""
    new_lines = [lines[0]]
    current_line = []
    for line in lines[1:]:
        if _line_needs_newline(line):
            current_line = " ".join(current_line)
            if current_line:
                line = f"{current_line}\n{line}"  # noqa: PLW2901
            new_lines.append(line)
            current_line = []
        else:
            if current_line:
                line = line.lstrip()  # noqa: PLW2901
            current_line.append(line)

    return new_lines


def _cli_formatted(doc):
    """Apply minor formatting changes when displaying to CLI"""
    if not _is_sphinx_build():
        doc = doc.replace("``", "`").replace("{{", "{").replace("}}", "}")
    return doc


def _uniform_space_strip(input_strs):
    """Uniformly strip left-hand whitespace from all strings in list"""
    if not input_strs:
        return input_strs

    input_strs = [x.rstrip() for x in input_strs]
    num_spaces_skip = min(
        len(x) - len(x.lstrip()) if x else float("inf") for x in input_strs
    )
    return [x[num_spaces_skip:] if x else x for x in input_strs]
