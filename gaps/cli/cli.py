"""Main CLI entry points"""

from functools import partial

import click

from gaps import Pipeline
from gaps.cli.batch import batch_command
from gaps.cli.templates import template_command
from gaps.cli.reset import reset_command
from gaps.cli.pipeline import pipeline_command, template_pipeline_config
from gaps.cli.collect import collect
from gaps.cli.script import script
from gaps.cli.config import from_config
from gaps.cli.command import (
    CLICommandFromFunction,
    _WrappedCommand,
)
from gaps.cli.documentation import _main_command_help
from gaps.cli.preprocessing import (
    preprocess_collect_config,
    preprocess_script_config,
)
from gaps.cli.status import status_command


class _CLICommandGenerator:
    """Generate commands from a list of configurations"""

    def __init__(self, command_configs):
        """

        Parameters
        ----------
        command_configs : list of :class:`CLICommandFromFunction`
            List of command configs to convert to click commands.
        """
        self.command_configs = command_configs
        self.commands, self.template_configs = [], {}

    def add_collect_command_configs(self):
        """Add collect command if the function is split spatially"""
        all_commands = []
        for command_configuration in self.command_configs:
            all_commands.append(command_configuration)
            if command_configuration.add_collect:
                collect_configuration = CLICommandFromFunction(
                    name=f"collect-{command_configuration.name}",
                    function=collect,
                    split_keys=[("_out_path", "_pattern")],
                    config_preprocessor=preprocess_collect_config,
                )
                all_commands.append(collect_configuration)
        self.command_configs = all_commands
        return self

    def add_script_command(self):
        """Add script command as an option"""
        script_configuration = CLICommandFromFunction(
            name="script",
            function=script,
            split_keys=["_cmd"],
            config_preprocessor=preprocess_script_config,
        )
        self.command_configs.append(script_configuration)
        return self

    def convert_to_commands(self):
        """Convert all of the command configs into click commands"""
        for command_config in self.command_configs:
            command = as_click_command(command_config)
            self.commands.append(command)
            Pipeline.COMMANDS[command_config.name] = command
            template_config = command_config.documentation.template_config
            self.template_configs[command_config.name] = template_config
        return self

    def add_pipeline_command(self):
        """Add pipeline command, which includes the previous commands"""
        tpc = template_pipeline_config(self.command_configs)
        pipeline = pipeline_command(tpc)
        self.commands = [pipeline, *self.commands]
        self.template_configs["pipeline"] = tpc
        return self

    def add_batch_command(self):
        """Add the batch command"""
        self.commands.append(batch_command())
        return self

    def add_status_command(self):
        """Add the status command"""
        self.commands.append(status_command())
        return self

    def add_template_command(self):
        """Add the config template command"""
        self.commands.append(template_command(self.template_configs))
        return self

    def add_reset_command(self):
        """Add the status reset command"""
        self.commands.append(reset_command())
        return self

    def generate(self):
        """Generate a list of click commands from input configs"""
        return (
            self.add_collect_command_configs()
            .add_script_command()
            .convert_to_commands()
            .add_pipeline_command()
            .add_batch_command()
            .add_status_command()
            .add_template_command()
            .add_reset_command()
            .commands
        )


def as_click_command(command_config):
    """Convert a GAPs command configuration into a ``click`` command

    Parameters
    ----------
    command_config : command configuration object
        Instance of a class that inherits from
        :class:`~gaps.cli.command.AbstractBaseCLICommandConfiguration`
        (i.e. :class:`gaps.cli.command.CLICommandFromClass`,
        :class:`gaps.cli.command.CLICommandFromFunction`, etc.).

    Returns
    -------
    click.command
        A ``click`` command generated from the command configuration.
    """
    doc = command_config.documentation
    name = command_config.name
    params = [
        click.Option(
            param_decls=["--config_file", "-c"],
            required=True,
            type=click.Path(exists=True),
            help=doc.config_help(name),
        )
    ]

    return _WrappedCommand(
        name,
        context_settings=None,
        callback=partial(
            from_config,
            command_config=command_config,
        ),
        params=params,
        help=doc.command_help(name),
        epilog=None,
        short_help=None,
        options_metavar="[OPTIONS]",
        add_help_option=True,
        no_args_is_help=True,
        hidden=False,
        deprecated=False,
    )


def make_cli(commands, info=None):
    """Create a pipeline CLI to split execution across HPC nodes.

    This function generates a CLI for a package based on the input
    command configurations. Each command configuration is based around
    a function that is to be executed on one or more nodes.

    Parameters
    ----------
    commands : list of command configurations
        List of command configs to convert to click commands. See the
        :class:`~gaps.cli.command.CLICommandFromClass` or
        :class:`~gaps.cli.command.CLICommandFromFunction`
        documentation for a description of the input options. Each
        command configuration is converted into a subcommand. Any
        command configuration with ``project_points`` in the
        `split_keys` argument will get a corresponding
        ``collect-{command name}`` command that collects the outputs of
        the spatially-distributed command.
    info : dict, optional
        A dictionary that contains optional info about the calling
        program to include in the CLI. Allowed keys include:

            name : str
                Name of program to display at the top of the CLI help.
                This input is optional, but specifying it yields richer
                documentation for the main CLI command.
            version : str
                Include program version with a ``--version`` CLI option.

        By default, ``None``.

    Returns
    -------
    click.Group
        A group containing the requested subcommands. This can be used
        as the entrypoint to the package CLI.
    """
    info = info or {}
    command_generator = _CLICommandGenerator(commands)
    commands = command_generator.generate()

    options = [
        click.Option(
            param_decls=["-v", "--verbose"],
            is_flag=True,
            help="Flag to turn on debug logging. Default is not verbose.",
        ),
    ]

    prog_name = info.get("name")
    if prog_name:
        main_help = _main_command_help(
            prog_name, command_generator.command_configs
        )
    else:
        main_help = "Command Line Interface"

    main = click.Group(
        help=main_help, params=options, callback=_main_cb, commands=commands
    )
    version = info.get("version")
    if version is not None:
        main = click.version_option(version=version)(main)

    return main


@click.pass_context
def _main_cb(ctx, verbose):
    """Set the obj and verbose settings of the commands"""
    ctx.ensure_object(dict)
    ctx.obj["VERBOSE"] = verbose
    ctx.obj["PIPELINE_STEP"] = ctx.invoked_subcommand
    ctx.max_content_width = 92
