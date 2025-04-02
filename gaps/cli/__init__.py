"""GAPs CLI functionality"""

from .cli import make_cli, as_click_command
from .command import (
    CLICommandConfiguration,
    CLICommandFromFunction,
    CLICommandFromClass,
)
