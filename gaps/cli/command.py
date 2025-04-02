"""GAPs command configuration preprocessing functions"""

from abc import ABC, abstractmethod
from functools import cached_property, wraps
from inspect import signature

import click

from gaps.cli.config import GAPS_SUPPLIED_ARGS
from gaps.cli.documentation import CommandDocumentation
from gaps.cli.preprocessing import split_project_points_into_ranges
from gaps.utilities import _is_sphinx_build


class AbstractBaseCLICommandConfiguration(ABC):
    """Abstract Base CLI Command representation.

    This base implementation provides helper methods to determine
    whether a given command is split spatially.

    Note that ``runner`` is a required part of the interface but is not
    listed as an abstract property to avoid unnecessary function
    wrapping.
    """

    def __init__(
        self,
        name,
        add_collect=False,
        split_keys=None,
        config_preprocessor=None,
        skip_doc_params=None,
    ):
        self.name = name
        self.add_collect = add_collect
        self.split_keys = set() if split_keys is None else set(split_keys)
        self.config_preprocessor = config_preprocessor or _passthrough
        self.skip_doc_params = (
            set() if skip_doc_params is None else set(skip_doc_params)
        )
        preprocessor_sig = signature(self.config_preprocessor)
        self.preprocessor_args = preprocessor_sig.parameters.keys()
        self.preprocessor_defaults = {
            name: param.default
            for name, param in preprocessor_sig.parameters.items()
            if param.default != param.empty
        }
        if self.is_split_spatially:
            self._add_split_on_points()

    def _add_split_on_points(self):
        """Add split points preprocessing"""
        self.config_preprocessor = _split_points(self.config_preprocessor)
        self.split_keys -= {"project_points"}
        self.split_keys |= {"project_points_split_range"}

    @property
    def is_split_spatially(self):
        """bool: ``True`` if execution is split across nodes"""
        return any(
            key in self.split_keys
            for key in ["project_points", "project_points_split_range"]
        )

    @property
    @abstractmethod
    def documentation(self):
        """CommandDocumentation: Documentation object"""
        raise NotImplementedError


class CLICommandFromFunction(AbstractBaseCLICommandConfiguration):
    """Configure a CLI command to execute a function on multiple nodes.

    This class configures a CLI command that runs a given function
    across multiple nodes on an HPC. The primary utility is to split the
    function execution spatially, meaning that individual nodes will run
    the function on a subset of the input points. However, this
    configuration also supports splitting the execution on other inputs
    (in lieu of or in addition to the geospatial partitioning).
    """

    def __init__(
        self,
        function,
        name=None,
        add_collect=False,
        split_keys=None,
        config_preprocessor=None,
        skip_doc_params=None,
    ):
        """

        Parameters
        ----------
        function : callable
            The function to run on individual nodes. This function will
            be used to generate all the documentation and template
            configuration files, so it should be thoroughly documented
            (using a `NumPy Style Python Docstring
            <https://numpydoc.readthedocs.io/en/latest/format.html>`_).
            In particular, the "Extended Summary" and the "Parameters"
            section will be pulled form the docstring.

            .. WARNING:: The "Extended Summary" section may not show up
                         properly if the short "Summary" section is
                         missing.

            This function must return the path to the output file it
            generates (or a list of paths if multiple output files are
            generated). If no output files are generated, the function
            must return ``None`` or an empty list. In order to avoid
            clashing output file names with jobs on other nodes, make
            sure to "request" the ``tag`` argument. This function can
            "request" the following arguments by including them in the
            function signature (``gaps`` will automatically pass them to
            the function without any additional used input):

                tag : str
                    Short string unique to this job run that can be used
                    to generate unique output filenames, thereby
                    avoiding clashing output files with jobs on other
                    nodes. This string  contains a leading underscore,
                    so the file name can easily be generated:
                    ``f"{out_file_name}{tag}.{extension}"``.
                command_name : str
                    Name of the command being run. This is equivalent to
                    the ``name`` input argument.
                pipeline_step : str
                    Name of the pipeline step being run. This is often
                    the same as `command_name`, but can be different if
                    a pipeline contains duplicate steps.
                config_file : str
                    Path to the configuration file specified by the
                    user.
                project_dir : str
                    Path to the project directory (parent directory of
                    the configuration file).
                job_name : str
                    Name of the job being run. This is typically a
                    combination of the project directory, the command
                    name, and a tag unique to a job. Note that the tag
                    will not be included if you request this argument
                    in a config preprocessing function, as the execution
                    has not been split into multiple jobs by that point.
                out_dir : str
                    Path to output directory - typically equivalent to
                    the project directory.
                out_fpath : str
                    Suggested path to output file. You are not required
                    to use this argument - it is provided purely for
                    convenience purposes. This argument combines the
                    ``out_dir`` with ``job_name`` to yield a unique
                    output filepath for a given node. Note that the
                    output filename will contain the tag. Also note that
                    this string *WILL NOT* contain a file-ending, so
                    that will have to be added by the node function.

            If your function is capable of multiprocessing, you should
            also include ``max_workers`` in the function signature.
            ``gaps`` will pass an integer equal to the number of
            processes the user wants to run on a single node for this
            value.

            .. WARNING:: The keywords ``{"max-workers",
                         "sites_per_worker", "memory_utilization_limit",
                         "timeout", "pool_size"}`` are assumed to
                         describe execution control. If you request any
                         of these as function arguments, users of your
                         CLI will specify them in the
                         `execution_control` block of the input config
                         file.

            Note that the ``config`` parameter is not allowed as
            a function signature item. Please request all the required
            keys/inputs directly instead. This function can also request
            "private" arguments by including a leading underscore in the
            argument name. These arguments are NOT exposed to users in
            the documentation or template configuration files. Instead,
            it is expected that the ``config_preprocessor`` function
            fills these arguments in programmatically before the
            function is distributed across nodes. See the implementation
            of :func:`gaps.cli.collect.collect` and
            :func:`gaps.cli.preprocessing.preprocess_collect_config`
            for an example of this pattern. You can use the
            ``skip_doc_params`` input below to achieve the same results
            without the underscore syntax (helpful for public-facing
            functions).
        name : str, optional
            Name of the command. This will be the name used to call the
            command on the terminal. This name does not have to match
            the function name. It is encouraged to use lowercase names
            with dashes ("-") instead of underscores ("_") to stay
            consistent with click's naming conventions. By default,
            ``None``, which uses the function name as the command name
            (with minor formatting to conform to ``click``-style
            commands).
        add_collect : bool, optional
            Option to add a "collect-{command_name}" command immediately
            following this command to collect the (multiple) output
            files generated across nodes into a single file. The collect
            command will only work if the output files the previous
            command generates are HDF5 files with ``meta`` and
            ``time_index`` datasets (standard ``rex`` HDF5 file
            structure). If you set this option to ``True``, your run
            function *must* return the path (as a string) to the output
            file it generates in order for users to be able to use
            ``"PIPELINE"`` as the input to the ``collect_pattern`` key
            in the collection config. The path returned by this function
            must also include the ``tag`` in the output file name in
            order for collection to function properly (an easy way to do
            this is to request ``tag`` in the function signature and
            name the output file generated by the function using the
            ``f"{out_file_name}{tag}.{extension}"`` format).
            By default, ``False``.
        split_keys : set | container, optional
            A set of strings identifying the names of the config keys
            that ``gaps`` should split the function execution on. To
            specify geospatial partitioning in particular, ensure that
            the main ``function`` has a "project_points" argument (which
            accepts a :class:`gaps.project_points.ProjectPoints`
            instance) and specify "project_points" as a split argument.
            Users of the CLI will only need to specify the path to the
            project points file and a "nodes" argument in the execution
            control. To split execution on additional/other inputs,
            include them by name in this input (and ensure the run
            function accepts them as input). You may include tuples of
            strings in this iterable as well. Tuples of strings will be
            interpreted as combinations of keys whose values should be
            iterated over simultaneously. For example, specifying
            ``split_keys=[("a", "b")]`` and invoking with a config file
            where ``a = [1, 2]`` and ``b = [3, 4]`` will run the main
            function two times (on two nodes), first with the inputs
            ``a=1, b=3`` and then with the inputs ``a=2, b=4``.
            It is the responsibility of the developer using this class
            to ensure that the user input for all ``split_keys`` is an
            iterable (typically a list), and that the lengths of all
            "paired" keys match. To allow non-iterable user input for
            split keys, use the ``config_preprocessor`` argument to
            specify a preprocessing function that converts the user
            input into a list of the expected inputs. If users specify
            an empty list or ``None`` for a key in ``split_keys``, then
            GAPs will pass ``None`` as the value for that key (i.e. if
            ``split_keys=["a"]`` and users specify ``"a": []`` in their
            config, then the ``function`` will be called with
            ``a=None``). If ``None``, execution is not split across
            nodes, and a single node is always used for the function
            call. By default, ``None``.
        config_preprocessor : callable, optional
            Optional function for configuration pre-processing. The
            preprocessing step occurs before jobs are split across HPC
            nodes, and can therefore be used to calculate the
            ``split_keys`` input and/or validate that it conforms to the
            requirements laid out above. At minimum, this function
            should have "config" as the first parameter (which will
            receive the user configuration input as a dictionary) and
            *must* return the updated config dictionary. This function
            can also "request" the following arguments by including them
            in the function signature:

                command_name : str
                    Name of the command being run. This is equivalent to
                    the ``name`` input above.
                pipeline_step : str
                    Name of the pipeline step being run. This is often
                    the same as `command_name`, but can be different if
                    a pipeline contains duplicate steps.
                config_file : Path
                    Path to the configuration file specified by the
                    user.
                project_dir : Path
                    Path to the project directory (parent directory of
                    the configuration file).
                job_name : str
                    Name of the job being run. This is typically a
                    combination of the project directory and the command
                    name.
                out_dir : Path
                    Path to output directory - typically equivalent to
                    the project directory.
                out_fpath : Path
                    Suggested path to output file. You are not required
                    to use this argument - it is provided purely for
                    convenience purposes. This argument combines the
                    ``out_dir`` with ``job_name`` to yield a unique
                    output filepath for a given node. Note that the
                    output filename *WILL NOT* contain the tag, since
                    the number of split nodes have not been determined
                    when the config pre-processing function is called.
                    Also note that this string *WILL NOT* contain a
                    file-ending, so that will have to be added by the
                    node function.
                log_directory : Path
                    Path to log output directory (defaults to
                    project_dir / "logs").
                verbose : bool
                    Flag indicating whether the user has selected a
                    DEBUG verbosity level for logs.

            These inputs will be provided by GAPs and *will not* be
            displayed to users in the template configuration files or
            documentation. See
            :func:`gaps.cli.preprocessing.preprocess_collect_config`
            for an example. Note that the ``tag`` parameter is not
            allowed as a pre-processing function signature item (the
            node jobs will not have been configured before this function
            executes). This function can also "request" new user inputs
            that are not present in the signature of the main run
            function. In this case, the documentation for these new
            arguments is pulled from the ``config_preprocessor``
            function. This feature can be used to request auxiliary
            information from the user to fill in "private" inputs to the
            main run function. See the implementation of
            :func:`gaps.cli.preprocessing.preprocess_collect_config` and
            :func:`gaps.cli.collect.collect` for an example of this
            pattern. Do not request parameters with the same names as
            any of your model function (i.e. if ``res_file`` is a model
            parameter, do not request it in the preprocessing function
            docstring - extract it from the config dictionary instead).
            By default, ``None``.
        skip_doc_params : iterable of str, optional
            Optional iterable of parameter names that should be excluded
            from the documentation/template configuration files. This
            can be useful if your pre-processing function automatically
            sets some parameters based on other user input. This option
            is an alternative to the "private" arguments discussed in
            the ``function`` parameter documentation above. By default,
            ``None``.
        """
        super().__init__(
            name or function.__name__.strip("_").replace("_", "-"),
            add_collect,
            split_keys,
            config_preprocessor,
            skip_doc_params,
        )
        self.runner = function

    @cached_property
    def documentation(self):
        """CommandDocumentation: Documentation object"""
        return CommandDocumentation(
            self.runner,
            self.config_preprocessor,
            skip_params=GAPS_SUPPLIED_ARGS | self.skip_doc_params,
            is_split_spatially=self.is_split_spatially,
        )


def CLICommandConfiguration(  # noqa: N802
    name, function, split_keys=None, config_preprocessor=None
):  # pragma: no cover
    """Do not use -  deprecated

    Please use :class:`CLICommandFromFunction`
    """
    from warnings import warn  # noqa: PLC0415
    from gaps.warn import gapsDeprecationWarning  # noqa: PLC0415

    warn(
        "The `CLICommandConfiguration` class is deprecated! Please use "
        "`CLICommandFromFunction` instead.",
        gapsDeprecationWarning,
    )
    return CLICommandFromFunction(
        function,
        name=name,
        add_collect=any(
            key in split_keys
            for key in ["project_points", "project_points_split_range"]
        ),
        split_keys=split_keys,
        config_preprocessor=config_preprocessor,
    )


class CLICommandFromClass(AbstractBaseCLICommandConfiguration):
    """Configure a CLI command to execute a method on multiple nodes

    This class configures a CLI command that initializes runs a given
    object and runs a particular method of that object across multiple
    nodes on an HPC. The primary utility is to split the method
    execution spatially, meaning that individual nodes will run the
    method on a subset of the input points. However, this configuration
    also supports splitting the execution on other inputs
    (in lieu of or in addition to the geospatial partitioning).
    """

    def __init__(
        self,
        init,
        method,
        name=None,
        add_collect=False,
        split_keys=None,
        config_preprocessor=None,
        skip_doc_params=None,
    ):
        """

        Parameters
        ----------
        init : class
            The class to be initialized and used to to run on individual
            nodes. The class must implement ``method``. The initializer,
            along with the corresponding method, will be used to
            generate all the documentation and template configuration
            files, so it should be thoroughly documented
            (using a `NumPy Style Python Docstring
            <https://numpydoc.readthedocs.io/en/latest/format.html>`_).
            In particular, the "Extended Summary" (from the ``__init__``
            method *only*) and the "Parameters" section (from *both* the
            ``__init__`` method and the run `method` given below) will
            be pulled form the docstring.

            .. WARNING:: The "Extended Summary" section may not display
                         properly if the short "Summary" section in the
                         ``__init__`` method is missing.

        method : str
            The name of a method of the ``init`` class to act as the
            model function to run across multiple nodes on the HPC. This
            method must return the path to the output file it generates
            (or a list of paths if multiple output files are generated).
            If no output files are generated, the method must return
            ``None`` or an empty list. In order to avoid clashing output
            file names with jobs on other nodes, make sure to "request"
            the ``tag`` argument. This method can "request" the
            following arguments by including them in the method
            signature (``gaps`` will automatically pass them to the
            method without any additional used input):

                tag : str
                    Short string unique to this job run that can be used
                    to generate unique output filenames, thereby
                    avoiding clashing output files with jobs on other
                    nodes. This string  contains a leading underscore,
                    so the file name can easily be generated:
                    ``f"{out_file_name}{tag}.{extension}"``.
                command_name : str
                    Name of the command being run. This is equivalent to
                    the ``name`` input argument.
                pipeline_step : str
                    Name of the pipeline step being run. This is often
                    the same as `command_name`, but can be different if
                    a pipeline contains duplicate steps.
                config_file : str
                    Path to the configuration file specified by the
                    user.
                project_dir : str
                    Path to the project directory (parent directory of
                    the configuration file).
                job_name : str
                    Name of the job being run. This is typically a
                    combination of the project directory, the command
                    name, and a tag unique to a job. Note that the tag
                    will not be included if you request this argument
                    in a config preprocessing function, as the execution
                    has not been split into multiple jobs by that point.
                out_dir : str
                    Path to output directory - typically equivalent to
                    the project directory.
                out_fpath : str
                    Suggested path to output file. You are not required
                    to use this argument - it is provided purely for
                    convenience purposes. This argument combines the
                    ``out_dir`` with ``job_name`` to yield a unique
                    output filepath for a given node. Note that the
                    output filename will contain the tag. Also note that
                    this string *WILL NOT* contain a file-ending, so
                    that will have to be added by the node function.

            If your function is capable of multiprocessing, you should
            also include ``max_workers`` in the function signature.
            ``gaps`` will pass an integer equal to the number of
            processes the user wants to run on a single node for this
            value.

            .. WARNING:: The keywords ``{"max-workers",
                         "sites_per_worker", "memory_utilization_limit",
                         "timeout", "pool_size"}`` are assumed to
                         describe execution control. If you request any
                         of these as function arguments, users of your
                         CLI will specify them in the
                         `execution_control` block of the input config
                         file.

            Note that the ``config`` parameter is not allowed as
            a function signature item. Please request all the required
            keys/inputs directly instead. This function can also request
            "private" arguments by including a leading underscore in the
            argument name. These arguments are NOT exposed to users in
            the documentation or template configuration files. Instead,
            it is expected that the ``config_preprocessor`` function
            fills these arguments in programmatically before the
            function is distributed across nodes. See the implementation
            of :func:`gaps.cli.collect.collect` and
            :func:`gaps.cli.preprocessing.preprocess_collect_config`
            for an example of this pattern. You can use the
            ``skip_doc_params`` input below to achieve the same results
            without the underscore syntax (helpful for public-facing
            functions).
        name : str, optional
            Name of the command. This will be the name used to call the
            command on the terminal. This name does not have to match
            the function name. It is encouraged to use lowercase names
            with dashes ("-") instead of underscores ("_") to stay
            consistent with click's naming conventions. By default,
            ``None``, which uses the ``method`` name (with minor
            formatting to conform to ``click``-style commands).
        add_collect : bool, optional
            Option to add a "collect-{command_name}" command immediately
            following this command to collect the (multiple) output
            files generated across nodes into a single file. The collect
            command will only work if the output files the previous
            command generates are HDF5 files with ``meta`` and
            ``time_index`` datasets (standard ``rex`` HDF5 file
            structure). If you set this option to ``True``, your run
            function *must* return the path (as a string) to the output
            file it generates in order for users to be able to use
            ``"PIPELINE"`` as the input to the ``collect_pattern`` key
            in the collection config. The path returned by this function
            must also include the ``tag`` in the output file name in
            order for collection to function properly (an easy way to do
            this is to request ``tag`` in the function signature and
            name the output file generated by the function using the
            ``f"{out_file_name}{tag}.{extension}"`` format).
            By default, ``False``.
        split_keys : set | container, optional
            A set of strings identifying the names of the config keys
            that ``gaps`` should split the function execution on. To
            specify geospatial partitioning in particular, ensure that
            the main ``function`` has a "project_points" argument (which
            accepts a :class:`gaps.project_points.ProjectPoints`
            instance) and specify "project_points" as a split argument.
            Users of the CLI will only need to specify the path to the
            project points file and a "nodes" argument in the execution
            control. To split execution on additional/other inputs,
            include them by name in this input (and ensure the run
            function accepts them as input). You may include tuples of
            strings in this iterable as well. Tuples of strings will be
            interpreted as combinations of keys whose values should be
            iterated over simultaneously. For example, specifying
            ``split_keys=[("a", "b")]`` and invoking with a config file
            where ``a = [1, 2]`` and ``b = [3, 4]`` will run the main
            function two times (on two nodes), first with the inputs
            ``a=1, b=3`` and then with the inputs ``a=2, b=4``.
            It is the responsibility of the developer using this class
            to ensure that the user input for all ``split_keys`` is an
            iterable (typically a list), and that the lengths of all
            "paired" keys match. To allow non-iterable user input for
            split keys, use the ``config_preprocessor`` argument to
            specify a preprocessing function that converts the user
            input into a list of the expected inputs. If users specify
            an empty list or ``None`` for a key in ``split_keys``, then
            GAPs will pass ``None`` as the value for that key (i.e. if
            ``split_keys=["a"]`` and users specify ``"a": []`` in their
            config, then the ``function`` will be called with
            ``a=None``). If ``None``, execution is not split across
            nodes, and a single node is always used for the function
            call. By default, ``None``.
        config_preprocessor : callable, optional
            Optional function for configuration pre-processing. The
            preprocessing step occurs before jobs are split across HPC
            nodes, and can therefore be used to calculate the
            ``split_keys`` input and/or validate that it conforms to the
            requirements laid out above. At minimum, this function
            should have "config" as the first parameter (which will
            receive the user configuration input as a dictionary) and
            *must* return the updated config dictionary. This function
            can also "request" the following arguments by including them
            in the function signature:

                command_name : str
                    Name of the command being run. This is equivalent to
                    the ``name`` input above.
                pipeline_step : str
                    Name of the pipeline step being run. This is often
                    the same as `command_name`, but can be different if
                    a pipeline contains duplicate steps.
                config_file : Path
                    Path to the configuration file specified by the
                    user.
                project_dir : Path
                    Path to the project directory (parent directory of
                    the configuration file).
                job_name : str
                    Name of the job being run. This is typically a
                    combination of the project directory and the command
                    name.
                out_dir : Path
                    Path to output directory - typically equivalent to
                    the project directory.
                out_fpath : Path
                    Suggested path to output file. You are not required
                    to use this argument - it is provided purely for
                    convenience purposes. This argument combines the
                    ``out_dir`` with ``job_name`` to yield a unique
                    output filepath for a given node. Note that the
                    output filename *WILL NOT* contain the tag, since
                    the number of split nodes have not been determined
                    when the config pre-processing function is called.
                    Also note that this string *WILL NOT* contain a
                    file-ending, so that will have to be added by the
                    node function.
                log_directory : Path
                    Path to log output directory (defaults to
                    project_dir / "logs").
                verbose : bool
                    Flag indicating whether the user has selected a
                    DEBUG verbosity level for logs.

            These inputs will be provided by GAPs and *will not* be
            displayed to users in the template configuration files or
            documentation. See
            :func:`gaps.cli.preprocessing.preprocess_collect_config`
            for an example. Note that the ``tag`` parameter is not
            allowed as a pre-processing function signature item (the
            node jobs will not have been configured before this function
            executes). This function can also "request" new user inputs
            that are not present in the signature of the main run
            function. In this case, the documentation for these new
            arguments is pulled from the ``config_preprocessor``
            function. This feature can be used to request auxiliary
            information from the user to fill in "private" inputs to the
            main run function. See the implementation of
            :func:`gaps.cli.preprocessing.preprocess_collect_config` and
            :func:`gaps.cli.collect.collect` for an example of this
            pattern. Do not request parameters with the same names as
            any of your model function (i.e. if ``res_file`` is a model
            parameter, do not request it in the preprocessing function
            docstring - extract it from the config dictionary instead).
            By default, ``None``.
        skip_doc_params : iterable of str, optional
            Optional iterable of parameter names that should be excluded
            from the documentation/template configuration files. This
            can be useful if your pre-processing function automatically
            sets some parameters based on other user input. This option
            is an alternative to the "private" arguments discussed in
            the ``function`` parameter documentation above. By default,
            ``None``.
        """
        super().__init__(
            name or method.strip("_").replace("_", "-"),
            add_collect,
            split_keys,
            config_preprocessor,
            skip_doc_params,
        )
        self.runner = init
        self.run_method = method
        self._validate_run_method_exists()

    def _validate_run_method_exists(self):
        """Validate that the ``run_method`` is implemented"""
        return getattr(self.runner, self.run_method)

    @cached_property
    def documentation(self):
        """CommandDocumentation: Documentation object"""
        return CommandDocumentation(
            self.runner,
            getattr(self.runner, self.run_method),
            self.config_preprocessor,
            skip_params=GAPS_SUPPLIED_ARGS | self.skip_doc_params,
            is_split_spatially=self.is_split_spatially,
        )


def _passthrough(config):
    """Pass the input config through with no modifications"""
    return config


def _split_points(config_preprocessor):
    """Add the `split_project_points_into_ranges` to preprocessing"""

    @wraps(config_preprocessor)
    def _config_preprocessor(config, *args, **kwargs):
        config = config_preprocessor(config, *args, **kwargs)
        return split_project_points_into_ranges(config)

    return _config_preprocessor


class _WrappedCommand(click.Command):
    """Click Command class with an updated `get_help` function

    References
    ----------
    https://stackoverflow.com/questions/55585564/python-click-formatting-help-text
    """

    _WRAP_TEXT_REPLACED = False

    def get_help(self, ctx):
        """Format the help into a string and return it"""
        if self._WRAP_TEXT_REPLACED:
            return super().get_help(ctx)

        orig_wrap_test = click.formatting.wrap_text

        def wrap_text(
            text,
            width=78,
            initial_indent="",
            subsequent_indent="",
            preserve_paragraphs=False,  # noqa: ARG001
        ):
            """Wrap text with gaps-style newline handling"""
            wrapped_text = orig_wrap_test(
                text.replace("\n", "\n\n"),
                width,
                initial_indent=initial_indent,
                subsequent_indent=subsequent_indent,
                preserve_paragraphs=True,
            )
            wrapped_text = (
                wrapped_text.replace("\n\n", "\n")
                .replace("::\n", ":\n\n")
                # .replace("}\nParameters", "}\n\nParameters")
                .replace("[required]", "\n[required]")
            )
            if "Parameters\n----------" not in wrapped_text.replace(" ", ""):
                wrapped_text = wrapped_text.replace(".\n", ".\n\n")
            elif not _is_sphinx_build():  # pragma: no cover
                wrapped_text = wrapped_text.replace(
                    "Parameters\n----------",
                    "\nConfig Parameters\n-----------------",
                )

            return wrapped_text

        click.formatting.wrap_text = wrap_text
        self._WRAP_TEXT_REPLACED = True
        return super().get_help(ctx)
