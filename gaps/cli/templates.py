"""GAPs CLI template config generation command"""

import logging
from pathlib import Path
from functools import partial
from warnings import warn

import click

from rex.utilities.loggers import init_logger
from gaps.config import ConfigType
from gaps.warn import gapsWarning
from gaps.cli.command import _WrappedCommand


logger = logging.getLogger(__name__)


@click.pass_context
def _make_template_config(ctx, commands, type, configs):  # noqa: A002
    """Filter configs and write to file based on type"""
    if ctx.obj.get("VERBOSE"):
        init_logger("gaps")

    configs_to_write = _filter_configs(commands, configs)
    _write_configs(configs_to_write, ConfigType(type))


def _filter_configs(commands, configs):
    """Filter down to only the configs to be written"""
    commands = commands or configs
    configs_to_write = {}
    missing_commands = []
    for command in commands:
        if command not in configs:
            missing_commands.append(command)
            continue
        configs_to_write[command] = configs[command]

    if missing_commands:
        msg = (
            f"The following commands were not found: {missing_commands}. "
            f" Skipping..."
        )
        warn(msg, gapsWarning)

    return configs_to_write


def _write_configs(configs_to_write, config_type):
    """Write out template configs"""
    for command_name, config in configs_to_write.items():
        sample_config_name = f"config_{command_name}.{config_type}"
        sample_config_name = sample_config_name.replace("-", "_")

        if Path(sample_config_name).exists():
            logger.info(
                "Template config already exists: %r", sample_config_name
            )
            continue

        logger.info(
            "Generating template config file for command %r: %r",
            command_name,
            sample_config_name,
        )
        if command_name == "pipeline" and "pipeline" in config:
            config["pipeline"] = [
                _update_file_types(pair, config_type)
                for pair in config["pipeline"]
            ]
        config_type.write(sample_config_name, config)


def _update_file_types(pairs, new_type):
    """Update the file endings for all items in the pairs dict"""
    old, new = f".{ConfigType.JSON}", f".{new_type}"
    return {command: path.replace(old, new) for command, path in pairs.items()}


def template_command(template_configs):
    """A template config generation CLI command"""
    allowed_types = " ".join(sorted(f"``{ct}``" for ct in ConfigType))
    params = [
        click.Argument(
            param_decls=["commands"],
            required=False,
            nargs=-1,
        ),
        click.Option(
            param_decls=["--type", "-t"],
            default=f"{ConfigType.JSON}",
            help=f"Configuration file type to generate. Allowed options "
            f"(case-insensitive): {allowed_types}.",
            show_default=True,
        ),
    ]
    return _WrappedCommand(
        "template-configs",
        context_settings=None,
        callback=partial(
            _make_template_config,
            configs=template_configs,
        ),
        params=params,
        help=(
            "Generate template config files for requested COMMANDS. If no "
            "COMMANDS are given, config files for the entire pipeline are "
            "generated.\n\nThe general structure for calling this CLI "
            "command is given below (add ``--help`` to print help info to "
            "the terminal)."
        ),
        epilog=None,
        short_help=None,
        options_metavar="",
        add_help_option=True,
        no_args_is_help=False,
        hidden=False,
        deprecated=False,
    )
