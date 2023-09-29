# cli.py
from model import run
from gaps.cli import CLICommandFromFunction, make_cli


commands = [
    CLICommandFromFunction(
        function=run,
        name="runner",
        add_collect=True,
        split_keys=["project_points"],
    )
]

cli = make_cli(commands)


if __name__ == "__main__":
    cli(obj={})
