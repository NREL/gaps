# -*- coding: utf-8 -*-
"""
CLI documentation utilities.
"""
from copy import deepcopy
from inspect import signature

from numpydoc.docscrape import NumpyDocString

from gaps.config import config_as_str_for_docstring, ConfigType


DEFAULT_EXEC_VALUES = {
    "option": "local",
    "allocation": "[REQUIRED IF ON HPC]",
    "walltime": "[REQUIRED IF ON HPC]",
    "memory": None,
    "nodes": 1,
    "queue": None,
    "feature": None,
    "conda_env": None,
    "module": None,
    "sh_script": None,
}

CONFIG_TYPES = [
    ConfigType.JSON,
    ConfigType.YAML,
    ConfigType.TOML,
]

PIPELINE_CONFIG_DOC = """
Path to the "pipeline" configuration file. This argument can be left out,
but *one and only one file* with the name "pipeline" should exist in the
directory and contain the config information. Below is a sample template config

.. tabs::

    .. tab:: JSON
        ::

            {template_json_config}

    .. tab:: YAML
        ::

            {template_yaml_config}

    .. tab:: TOML
        ::

            {template_toml_config}


Parameters
----------
pipeline : list of dicts
    A list of dictionaries, where each dictionary represents one step
    in the pipeline. Each dictionary should have one key-value pair,
    where the key is the name of the CLI command to run, and the value
    is the path to a config file containing the configuration for that
    command.
logging : dict, optional
    Dictionary containing keyword-argument pairs to pass to
    :func:`gaps.log.init_logger`.

"""

BATCH_CONFIG_DOC = """
Path to the "batch" configuration file. Below is a sample template config

.. tabs::

    .. tab:: JSON
        ::

            {template_json_config}

    .. tab:: YAML
        ::

            {template_yaml_config}

    .. tab:: TOML
        ::

            {template_toml_config}


Parameters
----------
pipeline_config : str
    Path to the pipeline configuration defining the commands to run for
    every parametric set.
sets : list of dicts
    A list of dictionaries, where each dictionary defines a "set" of
    parametric runs. Each dictionary should have the following keys:

        args : dict
            A dictionary defining the arguments across all input
            configuration files to parameterize. Each argument to be
            parametrized should be a key in this dictionary, and the
            value should be a list of the parameter values to run for
            this argument.

            .. tabs::

                .. tab:: JSON
                    ::

                        {sample_json_args_dict}

                .. tab:: YAML
                    ::

                        {sample_yaml_args_dict}

                .. tab:: TOML
                    ::

                        {sample_toml_args_dict}


            This example would run a total of six pipelines, one with
            each of the following arg combinations:
            ::

                input_constant_1=18.20, path_to_a_file="/first/path.h5"
                input_constant_1=18.20, path_to_a_file="/second/path.h5"
                input_constant_1=18.20, path_to_a_file="/third/path.h5"
                input_constant_1=19.04, path_to_a_file="/first/path.h5"
                input_constant_1=19.04, path_to_a_file="/second/path.h5"
                input_constant_1=19.04, path_to_a_file="/third/path.h5"


            Remember that the keys in the ``args`` dictionary should be
            part of (at least) one of your other configuration files.
        files : list
            A list of paths to the configuration files that contain the
            arguments to be updated for every parametric run. Arguments
            can be spread out over multiple files. For example:

            .. tabs::

                .. tab:: JSON
                    ::

                        {sample_json_files}

                .. tab:: YAML
                    ::

                        {sample_yaml_files}

                .. tab:: TOML
                    ::

                        {sample_toml_files}


        set_tag : str, optional
            Optional string defining a set tag that will prefix each
            job tag for this set. This tag does not need to include an
            underscore, as that is provided during concatenation.


"""

CONFIG_DOC = """
Path to the ``{name}`` configuration file. Below is a sample template config

.. tabs::

    .. tab:: JSON
        ::

            {template_json_config}

    .. tab:: YAML
        ::

            {template_yaml_config}

    .. tab:: TOML
        ::

            {template_toml_config}


{docstring}

Note that you may remove any keys with a ``null`` value if you do not intend to update them yourself.
"""
COMMAND_DOC = """
Execute the ``{name}`` step from a config file.

{desc}

"""
EXEC_CONTROL_DOC = """
Parameters
----------
    execution_control : dict
        Dictionary containing execution control arguments. Allowed
        arguments are:

        :option: ({{'local', 'slurm', 'eagle', 'pbs', 'peregrine'}})
                 Hardware run option. 'eagle' and 'peregrine' are aliases for
                 'slurm and 'pbs', respectively.
        :allocation: (str) HPC project (allocation) handle.
        :walltime: (int) Node walltime request in hours.
        :memory: (int, optional) Node memory request in GB. Default is not to
                 specify.{n}{mw}
        :queue: (str, optional; PBS ONLY) HPC queue to submit job to.
                Examples include: 'debug', 'short', 'batch', 'batch-h',
                'long', etc. By default, `None`, which uses `test_queue`.
        :feature: (str, optional) Additional flags for SLURM job
                  (e.g. "--qos=high", "-p debug", etc). Default is not
                  to specify.
        :conda_env: (str, optional) Name of conda environment to activate.
                    Default is not to load any environments.
        :module: (str, optional) Module to load. Default is not to load any
                 modules.
        :sh_script: (str, optional) Extra shell script to run before
                    command call. Default is not to run any scripts.

        Only the "option" input is required for local execution. For
        execution on the HPC, the allocation and walltime are also
        required. All other options are populated with default values,
        as seen above.

"""
NODES_DOC = (
    "\n        :nodes: (int, optional) Number of nodes to split the project "
    "\n                points across. Note that the total number of requested "
    "\n                nodes for a job may be larger than this value if the "
    "\n                command splits across other inputs (e.g. analysis "
    "\n                years) Default is 1."
)
MW_DOC = "\n        :max_workers: ({type}) {desc}"


class FunctionDocumentation:
    """Generate documentation for a function."""

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
            Flag indicating wether or not this function is split
            spatially across nodes. If `True`, a "nodes" option is added
            to the execution control block of the generated
            documentation. By default, `False`.
        """
        self.signatures = [signature(func) for func in functions]
        self.docs = [NumpyDocString(func.__doc__ or "") for func in functions]
        self.param_docs = {
            p.name: p for doc in self.docs for p in doc["Parameters"]
        }
        self.skip_params = set() if skip_params is None else set(skip_params)
        self.skip_params |= {"cls", "self", "max_workers"}
        self.is_split_spatially = is_split_spatially

    @property
    def default_exec_values(self):
        """dict: Default "execution_control" config."""
        exec_vals = deepcopy(DEFAULT_EXEC_VALUES)
        if not self.is_split_spatially:
            exec_vals.pop("nodes", None)
        if self.max_workers_in_func_signature:
            exec_vals["max_workers"] = (
                self.REQUIRED_TAG
                if self.max_workers_required
                else self.max_workers_param.default
            )
        return exec_vals

    @property
    def exec_control_doc(self):
        """str: Execution_control documentation."""
        nodes_doc = NODES_DOC if self.is_split_spatially else ""
        return EXEC_CONTROL_DOC.format(n=nodes_doc, mw=self._max_workers_doc)

    @property
    def _max_workers_doc(self):
        """str: `MW_DOC` formatted with the info from the input func."""
        param = self.param_docs.get("max_workers")
        try:
            return MW_DOC.format(type=param.type, desc=" ".join(param.desc))
        except AttributeError:
            pass

        if self.max_workers_in_func_signature:
            return MW_DOC.format(
                type=(
                    "(int)" if self.max_workers_required else "(int, optional)"
                ),
                desc=(
                    "Maximum number of parallel workers run on each node."
                    "Default is `None`, which uses all available cores."
                ),
            )

        return ""

    @property
    def max_workers_in_func_signature(self):
        """bool: `True` if "max_workers" is a param of the input function."""
        return self.max_workers_param is not None

    @property
    def max_workers_param(self):
        """bool: `True` if "max_workers" is a required parameter of `func`."""
        for sig in self.signatures:
            for name in sig.parameters:
                if name == "max_workers":
                    return sig.parameters["max_workers"]
        return None

    @property
    def max_workers_required(self):
        """bool: `True` if "max_workers" is a required parameter of `func`."""
        param = self.max_workers_param
        if param is None:
            return False
        return param.default is param.empty

    @property
    def required_args(self):
        """set: Required parameters of the input function."""
        required_args = {
            name
            for sig in self.signatures
            for name, param in sig.parameters.items()
            if not name.startswith("_")
            and param.default is param.empty
            and name not in self.skip_params
        }
        return required_args

    @property
    def template_config(self):
        """dict: A template configuration file for this function."""
        config = {"execution_control": self.default_exec_values}
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
    def parameter_help(self):
        """str: Parameter help for the func, including execution control."""
        exec_dict_param = [
            p
            for p in NumpyDocString(self.exec_control_doc)["Parameters"]
            if p.name == "execution_control"
        ]
        param_only_doc = NumpyDocString("")
        param_only_doc["Parameters"] = exec_dict_param + [
            p
            for doc in self.docs
            for p in doc["Parameters"]
            if p.name in self.template_config
        ]
        return str(param_only_doc)

    @property
    def extended_summary(self):
        """str: Function extended summary, with extra whitespace stripped."""
        return "\n".join(
            [x.lstrip().rstrip() for x in self.docs[0]["Extended Summary"]]
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
        return CONFIG_DOC.format(
            name=command_name,
            template_json_config=config_as_str_for_docstring(
                self.template_config, config_type=ConfigType.JSON
            ),
            template_yaml_config=config_as_str_for_docstring(
                self.template_config, config_type=ConfigType.YAML
            ),
            template_toml_config=config_as_str_for_docstring(
                self.template_config, config_type=ConfigType.TOML
            ),
            docstring=self.parameter_help,
        )

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
        return COMMAND_DOC.format(
            name=command_name, desc=self.extended_summary
        )


def _pipeline_command_help(pipeline_config):
    """Generate pipeline command help from a sample config."""
    format_inputs = {}
    template_names = [
        "template_json_config",
        "template_yaml_config",
        "template_toml_config",
    ]
    for name, c_type in zip(template_names, CONFIG_TYPES):
        format_inputs[name] = config_as_str_for_docstring(
            pipeline_config, config_type=c_type
        )
    return PIPELINE_CONFIG_DOC.format(**format_inputs)


def _batch_command_help():
    """Generate batch command help from a sample config."""
    template_config = {
        "pipeline_config": FunctionDocumentation.REQUIRED_TAG,
        "sets": [
            {
                "args": FunctionDocumentation.REQUIRED_TAG,
                "files": FunctionDocumentation.REQUIRED_TAG,
                "set_tag": "set1",
            },
            {
                "args": FunctionDocumentation.REQUIRED_TAG,
                "files": FunctionDocumentation.REQUIRED_TAG,
                "set_tag": "set2",
            },
        ],
    }
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
    sample_files = {"files": ["./config_run.yaml", "./config_analyze.json"]}
    format_inputs = {}
    template_names = [
        "template_json_config",
        "template_yaml_config",
        "template_toml_config",
    ]
    for name, c_type in zip(template_names, CONFIG_TYPES):
        format_inputs[name] = config_as_str_for_docstring(
            template_config, config_type=c_type
        )

    sample_args_names = [
        "sample_json_args_dict",
        "sample_yaml_args_dict",
        "sample_toml_args_dict",
    ]
    for name, c_type in zip(sample_args_names, CONFIG_TYPES):
        format_inputs[name] = config_as_str_for_docstring(
            sample_args_dict,
            config_type=c_type,
            num_spaces=20 if "json" in name else 24,
        )
        if "json" in name:
            format_inputs[name] = "\n".join(
                format_inputs[name].split("\n")[1:-1]
            ).lstrip()

    sample_files_names = [
        "sample_json_files",
        "sample_yaml_files",
        "sample_toml_files",
    ]
    for name, c_type in zip(sample_files_names, CONFIG_TYPES):
        format_inputs[name] = config_as_str_for_docstring(
            sample_files,
            config_type=c_type,
            num_spaces=20 if "json" in name else 24,
        )
        if "json" in name:
            format_inputs[name] = "\n".join(
                format_inputs[name].split("\n")[1:-1]
            ).lstrip()

    return BATCH_CONFIG_DOC.format(**format_inputs)
