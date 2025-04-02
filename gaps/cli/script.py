"""GAPs script CLI function"""

import os
import logging

from gaps.hpc import submit


logger = logging.getLogger(__name__)


def script(_cmd, project_dir):
    """Run a command or script as part of a pipeline step

    This command runs one or more terminal commands/scripts as part of a
    pipeline step.

    Parameters
    ----------
    _cmd : str
        String representation of a command to execute on a node.

    Returns
    -------
    str
        Path to HDF5 file with the collected outputs.
    """
    original_directory = os.getcwd()  # noqa: PTH109
    try:
        os.chdir(project_dir)
        stdout, stderr = submit(_cmd)
        if stdout:
            logger.info("Subprocess received stdout: \n%s", stdout)
        if stderr:
            logger.warning("Subprocess received stderr: \n%s", stderr)
    finally:
        os.chdir(original_directory)
