"""GAPs collection CLI entry points"""

import glob
import logging
from warnings import warn

from rex import Resource
from gaps import Collector
from gaps.warn import gapsWarning

logger = logging.getLogger(__name__)


def collect(
    _out_path,
    _pattern,
    project_points=None,
    datasets=None,
    purge_chunks=False,
    clobber=True,
):
    """Run collection on local worker.

    Collect data generated across multiple nodes into a single HDF5
    file.

    Parameters
    ----------
    project_points : str | list, optional
        This input should represent the project points that correspond
        to the *full collection* of points contained in the input HDF5
        files to be collected. You may simply point to a ProjectPoints
        csv file that contains the GID's that should be collected. You
        may also input the GID's as a list, though this may not be
        suitable for collections with a large number of points. You may
        also set this to input to ``None`` to generate a list of GID's
        automatically from the input files. By default, `None`.
    datasets : list of str, optional
        List of dataset names to collect into the output file. If
        collection is performed into multiple files (i.e. multiple input
        patterns), this list can contain all relevant datasets across
        all files (a warning wil be thrown, but it is safe to ignore
        it). If ``None``, all datasets from the input files are
        collected. By default, ``None``.
    purge_chunks : bool, optional
        Option to delete single-node input HDF5 files. Note that the
        input files will **not** be removed if any of the datasets they
        contain have not been collected, regardless of the value of this
        input. By default, ``False``.
    clobber : bool, optional
        Flag to purge all collection output HDF5 files prior to running
        the collection step if they exist on disk. This helps avoid any
        surprising data byproducts when re-running the collection step
        in a project directory. By default, ``True``.

    Returns
    -------
    str
        Path to HDF5 file with the collected outputs.
    """
    if "*" not in _pattern:
        logger.info("Collect pattern has no wildcard! No collection performed")
        return str(_out_path)

    logger.info(
        "Collection is being run with collection pattern: %s. Target output "
        "path is: %s",
        _pattern,
        _out_path,
    )

    datasets = _find_datasets(datasets, _pattern)
    collector = Collector(_out_path, _pattern, project_points, clobber=clobber)
    for dataset_name in datasets:
        logger.debug("Collecting %r...", dataset_name)
        collector.collect(dataset_name)

    if purge_chunks:
        collector.purge_chunks()
    else:
        collector.move_chunks()

    return str(_out_path)


def _find_datasets(datasets, pattern):
    """Find datasets from a sample file"""

    with Resource(glob.glob(pattern)[0]) as res:  # noqa: PTH207
        if datasets is None:
            return [
                d
                for d in res
                if not d.startswith("time_index") and d != "meta"
            ]
        missing = {d for d in datasets if d not in res}

    if any(missing):
        msg = (
            f"Could not find the following datasets in the output files: "
            f"{missing}. Skipping..."
        )
        warn(msg, gapsWarning)

    return list(set(datasets) - missing)
