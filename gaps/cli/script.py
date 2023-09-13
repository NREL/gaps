# -*- coding: utf-8 -*-
"""
GAPs collection CLI entry points.
"""
import logging

from gaps.hpc import submit


logger = logging.getLogger(__name__)


def script(cmd):
    """Run collection on local worker.

    Collect data generated across multiple nodes into a single HDF5
    file.

    Parameters
    ----------
    cmd : str
        String representation of a command to execute on a node.

    Returns
    -------
    str
        Path to HDF5 file with the collected outputs.
    """
    stdout, stderr = submit(cmd)
    if stdout:
        logger.info("Subprocess received stdout: \n%s", stdout)
    if stderr:
        logger.warning("Subprocess received stderr: \n%s", stderr)
