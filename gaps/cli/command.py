# -*- coding: utf-8 -*-
# pylint: disable=too-few-public-methods
"""
GAPs command configuration preprocessing functions.
"""
from inspect import signature

import click

from gaps.cli.documentation import FunctionDocumentation
from gaps.cli.preprocessing import split_project_points_into_ranges


GAPS_SUPPLIED_ARGS = {
    "tag",
    "command_name",
    "config_file",
    "project_dir",
    "job_name",
    "out_dir",
    "config",
}


class CLICommandConfiguration:
    """Configure a CLI command to execute a function on multiple nodes.

    This class configures a CLI command that runs a given function
    across multiple nodes on an HPC. The primary utility is to split the
    function execution spatially, meaning that individual nodes will run
    the function on a subset of the input points. However, this
    configuration also supports splitting the execution on other inputs
    (in lieu of or in addition to the geospatial partitioning).
    """

    def __init__(
        self, name, function, split_keys=None, config_preprocessor=None
    ):
        """

        Parameters
        ----------
        name : str
            Name of the command. This will be the name used to call the
            command on the terminal. This name does not have to match
            the function name. It is encouraged to use lowercase names
            with dashes ("-") instead of underscores ("_") to stay
            consistent with click's naming conventions.
        function : callable
            The function to run on individual nodes. This function will
            be used to generate all the documentation and template
            configuration files, so it should be thoroughly documented
            (using a `NumPy Style Python Docstring
            <https://numpydoc.readthedocs.io/en/latest/format.html>`_).
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
                    See :func:`gaps.cli.collect.collect` for an example.
                command_name : str
                    Name of the command being run. This is equivalent to
                    the ``name`` input above.
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

            If your function is capable of multiprocessing, you should
            also include ``max_workers`` in the function signature.
            ``gaps`` will pass an integer equal to the number of
            processes the user wants to run on a single node for this
            value. Note that the ``config`` parameter is not allowed as
            a function signature item. Please request all the required
            keys/inputs directly. This function can also request
            "private" arguments by including a leading underscore in the
            argument name. These arguments are NOT exposed to users in
            the documentation or template configuration files. Instead,
            it is expected that the ``config_preprocessor`` function
            fills these arguments in programmatically before the
            function is distributed across nodes. See the implementation
            of :func:`gaps.cli.preprocessing.preprocess_collect_config`
            and :func:`gaps.cli.collect.collect` for an example of this
            pattern.
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
            input into a list of the expected inputs. If ``None``,
            execution is not split across nodes, and a single node is
            always used for the function call. By default, ``None``.
        config_preprocessor : callable, optional
            Optional function for configuration pre-processing. At
            minimum, this function should have "config" as an argument,
            which will receive the user configuration input as a
            dictionary. This function can also "request" the following
            arguments by including them in the function signature:

                command_name : str
                    Name of the command being run. This is equivalent to
                    the ``name`` input above.
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

            See :func:`gaps.cli.preprocessing.preprocess_collect_config`
            for an example. Note that the ``tag`` parameter is not
            allowed as a pre-processing function signature item (the
            node jobs will not have been configured before this function
            executes). This function can also "request" new user inputs
            that are not present in the signature of the main run
            function. In this case, the documentation for these new
            arguments is pulled from the ``config_preprocessor``
            function. This feature can be used to request auxillary
            information from the user to fill in "private" inputs to the
            main run function. See the implementation of
            :func:`gaps.cli.preprocessing.preprocess_collect_config` and
            :func:`gaps.cli.collect.collect` for an example of this
            pattern. By default, ``None``.
        """
        self.name = name
        self.function = function
        self.split_keys = set() if split_keys is None else set(split_keys)
        self.config_preprocessor = config_preprocessor or _passthrough
        self.function_documentation = FunctionDocumentation(
            self.function,
            self.config_preprocessor,
            skip_params=GAPS_SUPPLIED_ARGS,
            is_split_spatially=self.is_split_spatially,
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
        """Add split points preprocessing."""
        self.config_preprocessor = _split_points(self.config_preprocessor)
        self.split_keys -= {"project_points"}
        self.split_keys |= {"project_points_split_range"}

    @property
    def is_split_spatially(self):
        """bool: ``True`` if execution is split spatially across nodes."""
        return any(
            key in self.split_keys
            for key in ["project_points", "project_points_split_range"]
        )


def _passthrough(config):
    """Pass the input config through with no modifications."""
    return config


def _split_points(config_preprocessor):
    """Add the `split_project_points_into_ranges` to preprocessing."""

    def _config_preprocessor(config, *args, **kwargs):
        config = config_preprocessor(config, *args, **kwargs)
        config = split_project_points_into_ranges(config)
        return config

    return _config_preprocessor


# pylint: disable=invalid-name,unused-argument
class _WrappedCommand(click.Command):
    """Click Command class with an updated `get_help` function.

    References
    ----------
    https://stackoverflow.com/questions/55585564/python-click-formatting-help-text
    """

    _WRAP_TEXT_REPLACED = False

    def get_help(self, ctx):
        """Format the help into a string and return it."""
        if self._WRAP_TEXT_REPLACED:
            return super().get_help(ctx)

        orig_wrap_test = click.formatting.wrap_text

        def wrap_text(
            text,
            width=78,
            initial_indent="",
            subsequent_indent="",
            preserve_paragraphs=False,
        ):
            """Wrap text with gaps-style newline handling."""
            wrapped_text = orig_wrap_test(
                text.replace("\n", "\n\n"),
                width,
                initial_indent=initial_indent,
                subsequent_indent=subsequent_indent,
                preserve_paragraphs=True,
            )
            wrapped_text = (
                wrapped_text.replace("\n\n", "\n").replace("::\n", ":\n\n")
                # .replace("}\nParameters", "}\n\nParameters")
                .replace("[required]", "\n[required]")
            )
            if "Parameters\n----------" not in wrapped_text.replace(" ", ""):
                wrapped_text = wrapped_text.replace(".\n", ".\n\n")
            return wrapped_text

        click.formatting.wrap_text = wrap_text
        self._WRAP_TEXT_REPLACED = True
        return super().get_help(ctx)
