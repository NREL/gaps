# -*- coding: utf-8 -*-
"""
Generation CLI entry points.
"""
from functools import partial

import click

from gaps import Pipeline
from gaps.cli.batch import batch_command
from gaps.cli.templates import template_command
from gaps.cli.pipeline import pipeline_command, template_pipeline_config
from gaps.cli.collect import collect
from gaps.cli.config import from_config
from gaps.cli.command import CLICommandConfiguration, _WrappedCommand
from gaps.cli.preprocessing import preprocess_collect_config
from gaps.cli.status import status_command


class _CLICommandGenerator:
    """Generate commands from a list of configurations."""

    def __init__(self, command_configs):
        """

        Parameters
        ----------
        command_configs : list of :class:`CLICommandConfiguration`
            List of command configs to convert to click commands.
        """
        self.command_configs = command_configs
        self.commands, self.template_configs = [], {}

    def add_collect_command_configs(self):
        """Add collect command if the function is split spatially."""
        all_commands = []
        for command_configuration in self.command_configs:
            all_commands.append(command_configuration)
            if command_configuration.is_split_spatially:
                collect_configuration = CLICommandConfiguration(
                    name=f"collect-{command_configuration.name}",
                    function=collect,
                    split_keys=[("_out_path", "_pattern")],
                    config_preprocessor=preprocess_collect_config,
                )
                all_commands.append(collect_configuration)
        self.command_configs = all_commands
        return self

    def convert_to_commands(self):
        """Convert all of the command configs into click commands."""
        for command_config in self.command_configs:
            func_doc = command_config.function_documentation
            name = command_config.name
            params = [
                click.Option(
                    param_decls=["--config_file", "-c"],
                    required=True,
                    type=click.Path(exists=True),
                    help=func_doc.config_help(name),
                )
            ]

            command = _WrappedCommand(
                name,
                context_settings=None,
                callback=partial(
                    from_config,
                    command_config=command_config,
                ),
                params=params,
                help=func_doc.command_help(name),
                epilog=None,
                short_help=None,
                options_metavar="[OPTIONS]",
                add_help_option=True,
                no_args_is_help=True,
                hidden=False,
                deprecated=False,
            )
            self.commands.append(command)
            Pipeline.COMMANDS[name] = command
            self.template_configs[name] = func_doc.template_config
        return self

    def add_pipeline_command(self):
        """Add pipeline command, which includes the previous commands."""
        tpc = template_pipeline_config(self.command_configs)
        pipeline = pipeline_command(tpc)
        self.commands = [pipeline] + self.commands
        self.template_configs["pipeline"] = tpc
        return self

    def add_batch_command(self):
        """Add the batch command."""
        self.commands.append(batch_command())
        return self

    def add_status_command(self):
        """Add the status command."""
        self.commands.append(status_command())
        return self

    def add_template_command(self):
        """Add the config template command."""
        self.commands.append(template_command(self.template_configs))
        return self

    def generate(self):
        """Generate a list of click commands from input configurations."""
        return (
            self.add_collect_command_configs()
            .convert_to_commands()
            .add_pipeline_command()
            .add_batch_command()
            .add_status_command()
            .add_template_command()
            .commands
        )


def make_cli(commands, info=None):
    """Create a pipeline CLI to split execution across HPC nodes.

    This function generates a CLI for a package based on the input
    command configurations. Each command configuration is based around
    a function that is to be executed on one or more nodes.

    Parameters
    ----------
    commands : list of :class:`~gaps.cli.command.CLICommandConfiguration`
        List of command configs to convert to click commands. See the
        :class:`~gaps.cli.command.CLICommandConfiguration` documentation
        for a description of the input options. Each command
        configuration is converted into a subcommand. Any command
        configuration with ``project_points`` in the `split_keys`
        argument will get a corresponding ``collect-{command name}``
        command that collects the outputs of the spatially-distributed
        command.
    info : dict, optional
        A dictionary that contains optional info about the calling
        program to include in the CLI. Allowed keys include:

            name : str
                Name of program to display at the top of the CLI help.
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

    options = [
        click.Option(
            param_decls=["-v", "--verbose"],
            is_flag=True,
            help="Flag to turn on debug logging. Default is not verbose.",
        ),
    ]
    prog_name = [info["name"]] if "name" in info else []
    main = click.Group(
        help=" ".join(prog_name + ["Command Line Interface"]),
        params=options,
        callback=_main_cb,
        commands=command_generator.generate(),
    )
    version = info.get("version")
    if version is not None:
        main = click.version_option(version=version)(main)

    return main


@click.pass_context
def _main_cb(ctx, verbose):
    """Set the obj and verbose settings of the commands."""
    ctx.ensure_object(dict)
    ctx.obj["VERBOSE"] = verbose
