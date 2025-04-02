"""GAPs CLI entry points"""

import click

from gaps.cli.status import status_command
from gaps._version import __version__


@click.group()
@click.version_option(version=__version__)
@click.pass_context
def main(ctx):
    """GAPs command line interface"""
    ctx.ensure_object(dict)


main.add_command(status_command(), name="status")


if __name__ == "__main__":
    main(obj={})
