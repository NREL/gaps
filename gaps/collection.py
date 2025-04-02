"""Base class to handle collection of datasets across multiple files"""

import sys
import time
import shutil
import logging
import contextlib
from pathlib import Path
from warnings import warn

import numpy as np
import psutil
import pandas as pd

from rex import Resource, Outputs
from gaps.log import log_versions
from gaps.warn import gapsCollectionWarning
from gaps.exceptions import gapsRuntimeError
from gaps.utilities import project_points_from_container_or_slice

logger = logging.getLogger(__name__)


class _OutputsWithAliases(Outputs):
    """Helper class that exposes and aliases some functions"""

    def create_dataset(self, *args, **kwargs):
        """Expose `_create_dset` call"""  # cspell:disable-line
        return self._create_dset(*args, **kwargs)  # cspell:disable-line

    def get_dataset_properties(self, *args, **kwargs):
        """Alias for `get_dset_properties`"""  # cspell:disable-line
        return self.get_dset_properties(*args, **kwargs)  # cspell:disable-line

    def get_time_index(self, *args, **kwargs):
        """Expose `_get_time_index` call"""
        return self._get_time_index(*args, **kwargs)

    def set_meta(self, *args, **kwargs):
        """Expose `_set_meta` call"""
        return self._set_meta(*args, **kwargs)

    def set_time_index(self, *args, **kwargs):
        """Expose `_set_time_index` call"""
        return self._set_time_index(*args, **kwargs)


class DatasetCollector:
    """Collector for a single dataset"""

    def __init__(
        self,
        h5_file,
        source_files,
        gids,
        dataset_in,
        dataset_out=None,
        memory_utilization_limit=0.7,
        pass_through=False,
    ):
        """
        Parameters
        ----------
        h5_file : path-like
            Path to h5_file into which dataset is to be collected.
        source_files : list
            List of source filepaths.
        gids : list
            List of gids to be collected.
        dataset_in : str
            Name of dataset to collect.
        dataset_out : str, optional
            Name of dataset into which collected data is to be written.
            If `None` the name of the output dataset is assumed to match
            the dataset input name. By default, `None`.
        memory_utilization_limit : float, optional
            Memory utilization limit (fractional). This sets how many
            sites will be collected at a time. By default, `0.7`.
        pass_through : bool, optional
            Flag to just pass through dataset from one of the source
            files, assuming all of the source files have identical
            copies of this dataset. By default, `False`.
        """
        self._h5_file = h5_file
        self._source_files = source_files
        self._gids = gids
        self._pass_through = pass_through
        self._dataset_in = dataset_in
        self._file_gid_map = {
            fp: parse_meta(fp)["gid"].to_numpy().tolist()
            for fp in self._source_files
        }
        if dataset_out is None:
            dataset_out = dataset_in
        self._dataset_out = dataset_out

        tot_mem = psutil.virtual_memory().total
        self._mem_avail = memory_utilization_limit * tot_mem
        self._axis, self._site_mem_req = self._pre_collect()

        logger.debug(
            "Available memory for collection is %.2f bytes", self._mem_avail
        )
        logger.debug(
            "Site memory requirement is: %.2f bytes", self._site_mem_req
        )

    @property
    def gids(self):
        """list: List of gids corresponding to all sites to combine"""
        return self._gids

    @property
    def duplicate_gids(self):
        """bool: `True` if there are duplicate gids being collected"""
        return len(self.gids) > len(set(self.gids))

    def _pre_collect(self):
        """Run a pre-collection check and get relevant dataset attrs

        Returns
        -------
        axis : int
            Axis size (1 is 1D array, 2 is 2D array).
        site_mem_req : float
            Memory requirement in bytes to collect a single site from
            one source file.
        """
        with _OutputsWithAliases(self._source_files[0], mode="r") as out:
            shape, dtype, chunks = out.get_dataset_properties(self._dataset_in)
            attrs = out.get_attrs(self._dataset_in)
            axis = len(out[self._dataset_in].shape)

        with _OutputsWithAliases(self._h5_file, mode="a") as out:
            if axis == 1:
                dataset_shape = (len(out),)
            elif axis == 2:  # noqa: PLR2004
                if "time_index" in out.datasets:
                    dataset_shape = out.shape
                else:
                    msg = (
                        "'time_index' must be combined before profiles can "
                        "be combined."
                    )
                    raise gapsRuntimeError(msg)
            else:
                msg = (
                    f"Cannot collect dataset {self._dataset_in!r} with "
                    f"axis {axis}"
                )
                raise gapsRuntimeError(msg)

            if self._dataset_out not in out.datasets:
                out.create_dataset(
                    self._dataset_out,
                    dataset_shape,
                    dtype,
                    chunks=chunks,
                    attrs=attrs,
                )

        site_mem_req = _get_site_mem_req(shape, dtype)

        return axis, site_mem_req

    def _get_source_gid_chunks(self, f_source):
        """Split gids from a source file into chunks based on memory req

        Parameters
        ----------
        f_source : :class:`rex.Outputs`
            Source file handler.

        Returns
        -------
        all_source_gids : list
            List of all source gids to be collected.
        source_gid_chunks : list
            List of source gid chunks to collect.
        """

        all_source_gids = f_source.get_meta_arr("gid")
        mem_req = len(all_source_gids) * self._site_mem_req

        if mem_req > self._mem_avail:
            num_chunks = 2
            while True:
                source_gid_chunks = np.array_split(all_source_gids, num_chunks)
                new_mem_req = len(source_gid_chunks[0]) * self._site_mem_req
                if new_mem_req > self._mem_avail:
                    num_chunks += 1
                else:
                    logger.debug(
                        "Collecting dataset %r in %d chunks with "
                        "an estimated %.2f bytes in each chunk "
                        "(mem avail limit is %.2f bytes).",
                        self._dataset_in,
                        num_chunks,
                        new_mem_req,
                        self._mem_avail,
                    )
                    break
        else:
            source_gid_chunks = [all_source_gids]

        return all_source_gids, source_gid_chunks

    def _collect_chunk(
        self, all_source_gids, source_gids, f_out, f_source, fp_source
    ):
        """Collect one set of source gids from f_source to f_out

        Parameters
        ----------
        all_source_gids : list
            List of all source gids to be collected.
        source_gids : np.ndarray | list
            Source gids to be collected.
        f_out : rex.Outputs
            Output file handler.
        f_source : rex.Outputs
            Source file handler.
        fp_source : str
            Source filepath.
        """

        out_slice, source_slice, source_indexer = self._get_chunk_indices(
            all_source_gids, source_gids, fp_source
        )

        try:
            if self._axis == 1:
                data = f_source[self._dataset_in, source_slice]
                if not all(source_indexer):
                    data = data[source_indexer]
                f_out[self._dataset_out, out_slice] = data

            elif self._axis == 2:  # noqa: PLR2004
                data = f_source[self._dataset_in, :, source_slice]
                if not all(source_indexer):
                    data = data[:, source_indexer]
                f_out[self._dataset_out, :, out_slice] = data

        except Exception as exc:
            msg = (
                f"Failed to collect {self._dataset_in!r} from source file "
                f"{fp_source.name!r}."
            )
            raise gapsRuntimeError(msg) from exc

    def _get_chunk_indices(self, all_source_gids, source_gids, fp_source):
        """Slices and indices used for selecting source gids

        Parameters
        ----------
        all_source_gids : list
            List of all source gids to be collected.
        source_gids : np.ndarray | list
            Source gids to be collected. This is the same as
            `all_source_gids` if collection is not being done in chunks.
        f_out : :class:`rex.Outputs`
            Output file handler.
        f_source : :class:`rex.Outputs`
            Source file handler.
        fp_source : str
            Source filepath.

        Returns
        -------
        out_slice : slice | ndarray
            Slice specifying location of source data in output file.
            This can also be a boolean array if source gids are not
            sequential in the output file.
        source_slice : slice
            Slice specifying index range of source data in input file.
            If collection is not being done in chunks this is just
            slice(None).
        source_indexer : ndarray
            boolean array specifying which source gids (not just a
            range) should be stored in output.
        """
        source_indexer = np.isin(source_gids, self._gids)
        out_slice = _get_gid_slice(
            self._gids, source_gids, Path(fp_source).name
        )

        if self.duplicate_gids:
            msg = "Cannot collect duplicate gids in multiple chunks"
            assert len(all_source_gids) == len(source_gids), msg
            out_i0 = 0
            for source_file in self._source_files:
                if source_file == fp_source:
                    break
                out_i0 += len(self._file_gid_map[source_file])
            out_i1 = out_i0 + len(self._file_gid_map[fp_source])
            out_slice = slice(out_i0, out_i1)
            source_slice = slice(None)

        elif all(sorted(source_gids) == source_gids):
            source_i0 = np.where(all_source_gids == np.min(source_gids))[0][0]
            source_i1 = np.where(all_source_gids == np.max(source_gids))[0][0]
            source_slice = slice(source_i0, source_i1 + 1)

        elif all(source_gids == all_source_gids):
            source_slice = slice(None)

        else:
            source_slice = np.isin(all_source_gids, source_gids)
            msg = (
                "source_gids is not in ascending order or equal to "
                "all_source_gids. This can cause issues with the "
                "collection ordering. Please check your data carefully."
            )
            warn(msg, gapsCollectionWarning)

        return out_slice, source_slice, source_indexer

    def _collect(self):
        """Simple & robust serial collection optimized for low mem"""
        logger.info("Collecting %s...", self._dataset_in)
        with _OutputsWithAliases(self._h5_file, mode="a") as f_out:
            if self._pass_through:
                with Resource(self._source_files[0]) as f_source:
                    f_out[self._dataset_out] = f_source[self._dataset_in]

            else:
                for f_ind, file_path in enumerate(self._source_files, start=1):
                    with Resource(file_path) as f_source:
                        chunks = self._get_source_gid_chunks(f_source)
                        all_source_gids, source_gid_chunks = chunks

                        for source_gids in source_gid_chunks:
                            self._collect_chunk(
                                all_source_gids,
                                source_gids,
                                f_out,
                                f_source,
                                file_path,
                            )

                    mem = psutil.virtual_memory()
                    logger.debug(
                        "Finished collecting %r from %d out of %d "
                        "files. Memory utilization is %.3f GB out "
                        "of %.3f GB total (%.1f%% used)",
                        self._dataset_in,
                        f_ind,
                        len(self._source_files),
                        mem.used / (1024.0**3),
                        mem.total / (1024.0**3),
                        100 * mem.used / mem.total,
                    )

    @classmethod
    def collect_dataset(
        cls,
        h5_file,
        source_files,
        gids,
        dataset_in,
        dataset_out=None,
        memory_utilization_limit=0.7,
        pass_through=False,
    ):
        """Collect a dataset from multiple source files into one file

        Parameters
        ----------
        h5_file : path-like
            Path to h5_file into which dataset is to be collected.
        source_files : list
            List of source filepaths.
        gids : list
            List of gids to be collected.
        dataset_in : str
            Name of dataset to collect.
        dataset_out : str, optional
            Name of dataset into which collected data is to be written.
            If `None` the name of the output dataset is assumed to match
            the dataset input name. By default, `None`.
        memory_utilization_limit : float, optional
            Memory utilization limit (fractional). This sets how many
            sites will be collected at a time. By default, `0.7`.
        pass_through : bool, optional
            Flag to just pass through dataset from one of the source
            files, assuming all of the source files have identical
            copies of this dataset. By default, `False`.
        """
        collector = cls(
            h5_file,
            source_files,
            gids,
            dataset_in,
            dataset_out=dataset_out,
            memory_utilization_limit=memory_utilization_limit,
            pass_through=pass_through,
        )
        collector._collect()  # noqa: SLF001


class Collector:
    """Collector of multiple source files into a single output file"""

    def __init__(
        self, h5_file, collect_pattern, project_points, clobber=False
    ):
        """
        Parameters
        ----------
        h5_file : path-like
            Path to output HDF5 file into which data will be collected.
        collect_pattern : str
            Unix-style /filepath/pattern*.h5 representing a list of
            input files to be collected into a single HDF5 file.
        project_points : str | slice | list | pandas.DataFrame | None
            Project points that correspond to the full collection of
            points contained in the HDF5 files to be collected. `None`
            if points list is to be ignored (i.e. collect all data in
            the input HDF5 files without checking that all gids are
            there).
        clobber : bool, optional
            Flag to purge output HDF5 file if it already exists.
            By default, `False`.
        """
        log_versions()
        self.h5_out = Path(h5_file)
        self.collect_pattern = collect_pattern
        if clobber and self.h5_out.exists():
            warn(
                f"{h5_file} already exists and is being replaced",
                gapsCollectionWarning,
            )
            self.h5_out.unlink()

        self._h5_files = find_h5_files(
            self.collect_pattern, ignore=self.h5_out.name
        )
        if project_points is not None:
            logger.debug("Parsing project points...")
            self._gids = parse_project_points(project_points)
        else:
            self._gids = parse_gids_from_files(self._h5_files)

        self.combine_meta()

    def get_dataset_shape(self, dataset_name):
        """Extract dataset shape from the first file in the collection

        Parameters
        ----------
        dataset_name : str
            Dataset to be collected whose shape is in question.

        Returns
        -------
        shape : tuple
            Dataset shape tuple.
        """
        with Resource(self.h5_files[0]) as file_:
            return file_.shapes[dataset_name]

    @property
    def h5_files(self):
        """list: List of paths to HDF5 files to be combined"""
        return self._h5_files

    @property
    def gids(self):
        """list: List of gids corresponding to all sites to combine"""
        return self._gids

    def combine_meta(self):
        """Combine meta data from input files and write to out file"""

        logger.info(
            "Combining meta data from list of %d source files: %s",
            len(self.h5_files),
            self.h5_files,
        )

        with _OutputsWithAliases(self.h5_out, mode="a") as f_out:
            if "meta" in f_out.datasets:
                self._check_meta(f_out.meta)
            else:
                with Resource(self.h5_files[0]) as f_in:
                    global_attrs = f_in.get_attrs()
                    meta_attrs = f_in.get_attrs("meta")

                for key, value in global_attrs.items():
                    f_out.h5.attrs[key] = value

                meta = [parse_meta(h5_file) for h5_file in self.h5_files]

                meta = pd.concat(meta, axis=0)
                meta = self._check_meta(meta)
                logger.info("Writing meta data with shape %s", meta.shape)
                f_out.set_meta("meta", meta, attrs=meta_attrs)

        logger.debug("\t- 'meta' collected")

    def combine_time_index(self):
        """Combine `time_index` from input files and write to out file

        If `time_index` is not given in the input HDF5 files, the
        `time_index` in the output file is set to `None`.
        """
        # If we ever become Python 3.10+ exclusive, we can use
        # parentheses here
        # fmt: off
        with _OutputsWithAliases(self.h5_files[0], mode="r") as f_source, \
             _OutputsWithAliases(self.h5_out, mode="a") as f_out:
            time_index_datasets = [
                d for d in list(f_source) if d.startswith("time_index")
            ]
            time_index = None
            for name in time_index_datasets:
                time_index = f_source.get_time_index(name, slice(None))
                attrs = f_source.get_attrs(name)
                f_out.set_time_index(name, time_index, attrs=attrs)

    def _check_meta(self, meta):
        """Validate meta

        In particular, this method checks the combined meta against
        `self._gids` to make sure all sites are present in
        `self.h5_files`.

        Parameters
        ----------
        meta : :class:`pd.DataFrame`
            DataFrame of combined meta from all files in
            `self.h5_files`. Duplicate GIDs are dropped and a warning is
            raised.
        """
        meta_gids = meta["gid"].to_numpy()
        gids = np.array(self.gids)
        missing = gids[~np.isin(gids, meta_gids)]
        if any(missing):
            # TODO: Write missing gids to disk to allow for automated
            # re-run
            msg = f"gids: {missing} are missing"
            raise gapsRuntimeError(msg)

        if len(set(meta_gids)) != len(meta):
            msg = (
                f"Meta of length {len(meta)} has {len(set(meta_gids))} "
                f"unique gids! There are duplicate gids in the source "
                f"file list: {self.h5_files!r}"
            )
            warn(msg, gapsCollectionWarning)

        if not all(sorted(meta["gid"].values) == meta["gid"].to_numpy()):
            msg = (
                "Collection was run with non-ordered meta data GIDs! "
                "This can cause issues with the collection ordering. "
                "Please check your data carefully."
            )
            warn(msg, gapsCollectionWarning)

        return meta.reset_index(drop=True)

    def purge_chunks(self):
        """Remove chunked files from a directory

        Warns
        -----
        gapsCollectionWarning
            If some datasets have not been collected.

        Warnings
        --------
        This function WILL NOT delete files if any datasets were not
        collected.
        """

        with Resource(self.h5_out) as out:
            collected_datasets = out.datasets

        with Resource(self.h5_files[0]) as out:
            source_datasets = out.datasets

        missing = [d for d in source_datasets if d not in collected_datasets]

        if any(missing):
            msg = (
                f"Not purging chunked output files. These datasets "
                f"have not been collected: {missing}"
            )
            warn(msg, gapsCollectionWarning)
            return

        for fpath in self.h5_files:
            fpath.unlink()

        logger.info("Purged chunk files from %s", self.collect_pattern)

    def move_chunks(self, sub_dir="chunk_files"):
        """Move chunked files from a directory to a sub-directory

        Parameters
        ----------
        sub_dir : path-like, optional
            Sub directory name to move chunks to. By default,
            `"chunk_files"`.
        """
        for fpath in self.h5_files:
            new_dir = fpath.parent / sub_dir
            new_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(fpath, new_dir / fpath.name)

        logger.info(
            "Moved chunk files from %s to sub_dir: %s",
            self.collect_pattern,
            sub_dir,
        )

    def collect(
        self,
        dataset_in,
        dataset_out=None,
        memory_utilization_limit=0.7,
        pass_through=False,
    ):
        """Collect a dataset from h5_dir to h5_file

        Parameters
        ----------
        dataset_in : str
            Name of dataset to collect. If source shape is 2D,
            time index will be collected as well.
        dataset_out : str
            Name of dataset into which collected data is to be written.
            If `None` the name of the output dataset is assumed to match
            the dataset input name. By default, `None`.
        memory_utilization_limit : float
            Memory utilization limit (fractional). This sets how many
            sites will be collected at a time. By default, `0.7`.
        pass_through : bool
            Flag to just pass through dataset from one of the source
            files, assuming all of the source files have identical
            copies of this dataset. By default, `False`.

        See Also
        --------
        Collector.add_dataset
            Collect a dataset into an existing HDF5 file.
        """

        logger.info(
            "Collecting dataset %r from files based on pattern %r to output: "
            "%s",
            dataset_in,
            self.collect_pattern,
            self.h5_out,
        )
        start_time = time.time()
        dataset_shape = self.get_dataset_shape(dataset_in)
        if len(dataset_shape) > 1:
            self.combine_time_index()
            logger.debug("\t- 'time_index' collected")

        DatasetCollector.collect_dataset(
            self.h5_out,
            self.h5_files,
            self.gids,
            dataset_in,
            dataset_out=dataset_out,
            memory_utilization_limit=memory_utilization_limit,
            pass_through=pass_through,
        )

        logger.debug("\t- Collection of %r complete", dataset_in)

        elapsed_time = (time.time() - start_time) / 60
        logger.info("Collection complete")
        logger.debug("\t- Collection took %.4f minutes", elapsed_time)

    @classmethod
    def add_dataset(
        cls,
        h5_file,
        collect_pattern,
        dataset_in,
        dataset_out=None,
        memory_utilization_limit=0.7,
        pass_through=False,
    ):
        """Collect and add a dataset to a single HDF5 file

        Parameters
        ----------
        h5_file : path-like
            Path to output HDF5 file into which data will be collected.
            Note that this file must already exist and have a valid
            `meta`.
        collect_pattern : str
            Unix-style /filepath/pattern*.h5 representing a list of
            input files to be collected into a single HDF5 file.
        dataset_in : str
            Name of dataset to collect. If source shape is 2D,
            time index will be collected as well.
        dataset_out : str
            Name of dataset into which collected data is to be written.
            If `None` the name of the output dataset is assumed to match
            the dataset input name. By default, `None`.
        memory_utilization_limit : float
            Memory utilization limit (fractional). This sets how many
            sites will be collected at a time. By default, `0.7`.
        pass_through : bool
            Flag to just pass through dataset from one of the source
            files, assuming all of the source files have identical
            copies of this dataset. By default, `False`.

        See Also
        --------
        Collector.collect
            Collect a dataset into a file that does not yet exist.
        """
        logger.info(
            "Collecting dataset %r from files based on pattern %r and "
            "adding to: %s",
            dataset_in,
            collect_pattern,
            h5_file,
        )
        with Resource(h5_file) as res:
            points = res.meta

        collector = cls(h5_file, collect_pattern, points, clobber=False)
        collector.collect(
            dataset_in,
            dataset_out=dataset_out,
            memory_utilization_limit=memory_utilization_limit,
            pass_through=pass_through,
        )


def parse_project_points(project_points):
    """Extract resource gids from project points

    Parameters
    ----------
    project_points : str | slice | list | pandas.DataFrame
        Reference to resource points that were processed and need
        collecting.

    Returns
    -------
    gids : list
        List of resource gids that are to be collected.
    """
    with contextlib.suppress((TypeError, ValueError)):
        project_points = pd.read_csv(project_points)

    points = project_points_from_container_or_slice(project_points)
    if sorted(points) != points:
        msg = (
            "Project points contain non-ordered meta data GIDs! This "
            "can cause issues with the collection ordering. Please "
            "check your data carefully."
        )
        warn(msg, gapsCollectionWarning)
    return points


def find_h5_files(collect_pattern, ignore=None):
    """Search pattern for existing HDF5 files

    Parameters
    ----------
    collect_pattern : str
        Unix-style /filepath/pattern*.h5 representing a list of
        input files to be collected into a single HDF5 file.
    ignore : str | container | None, optional
        File name(s) to ignore. By default, `None`.

    Returns
    -------
    list
        List of sorted filepaths.

    Raises
    ------
    gapsRuntimeError
        If not all source files end in ".h5" (i.e. are not of type
        HDF5).
    """
    ignore = ignore or []
    if isinstance(ignore, str):
        ignore = [ignore]

    h5_files = []
    logger.debug("Looking for source files based on %s", collect_pattern)

    collect_pattern = Path(collect_pattern)
    h5_files = [
        fp
        for fp in collect_pattern.parent.glob(collect_pattern.name)
        if fp.name not in ignore
    ]
    h5_files = sorted(h5_files, key=lambda fp: fp.name)

    if not all(fp.name.endswith(".h5") for fp in h5_files):
        msg = (
            f"Source pattern resulted in non-h5 files that cannot "
            f"be collected: {h5_files}, pattern: {collect_pattern}"
        )
        raise gapsRuntimeError(msg)

    return h5_files


def parse_gids_from_files(h5_files):
    """Extract a gid list from a list of h5_files

    Parameters
    ----------
    h5_files : list
        List of h5 files to be collected.

    Returns
    -------
    gids : list
        List of resource gids to be collected.
    """
    logger.debug("Parsing gid list from files...")
    meta = [parse_meta(h5_file) for h5_file in h5_files]
    meta = pd.concat(meta, axis=0)
    gids = list(meta["gid"].to_numpy().astype(int).tolist())

    if len(gids) > len(set(gids)):
        msg = "Duplicate GIDs were found in source files!"
        warn(msg, gapsCollectionWarning)

    if sorted(gids) != gids:
        msg = (
            "Collection was run without project points file and with "
            "non-ordered meta data GIDs! This can cause issues with "
            "the collection ordering. Please check your data "
            "carefully."
        )
        warn(msg, gapsCollectionWarning)

    return gids


def parse_meta(h5_file):
    """Extract and convert meta data from a rec.array to a DataFrame

    Parameters
    ----------
    h5_file : path-like
        Path to HDF5 file from which meta is to be parsed.

    Returns
    -------
    meta : :class:`pd.DataFrame`
        Portion of meta data corresponding to sites in `h5_file`.
    """
    with Resource(h5_file) as res:
        return res.meta


def _get_site_mem_req(shape, dtype, num_prototype_sites=100):
    """Calculate memory requirement to collect a dataset

    Parameters
    ----------
    shape : tuple
        Shape of dataset to be collected (n_time, n_sites).
    dtype : np.dtype
        Numpy dtype of dataset (disk dtype).
    num_prototype_sites : int, optional
        Number of sites to prototype the memory req with. By default,
        `100`.

    Returns
    -------
    site_mem : float
        Memory requirement in bytes to collect a dataset with shape and
        dtype.
    """
    num_prototype_time_steps = shape[0] if len(shape) > 1 else 1
    shape = (num_prototype_time_steps, num_prototype_sites)
    site_mem = sys.getsizeof(np.ones(shape, dtype=dtype))
    return site_mem / num_prototype_sites


def _get_gid_slice(gids_out, source_gids, fn_source):
    """Return site slice that the chunked set of source gids belongs to

    Parameters
    ----------
    gids_out : list
        List of resource GIDS in the final output meta data f_out.
    source_gids : list
        List of resource GIDS in one chunk of source data.
    fn_source : str
        Source filename for warning printout.

    Returns
    -------
    site_slice : slice | np.ndarray
        Slice in the final output file to write data to from source
        gids. If gids in destination file are non-sequential, a boolean
        array of indexes is returned and a warning is thrown.
    """
    gid_in_source = np.isin(gids_out, source_gids)
    locs = np.where(gid_in_source)[0]
    if len(locs) == 0:
        msg = (
            f"DatasetCollector could not locate source gids in output "
            f"gids.\n\tSource gids: {source_gids}"
            f"\n\tOutput gids: {gids_out}"
        )
        raise gapsRuntimeError(msg)

    sequential_locs = np.arange(locs.min(), locs.max() + 1)

    if (locs != sequential_locs).any():
        msg = (
            f"GID indices for source file {fn_source!r} are not "
            f"sequential in destination file!"
        )
        warn(msg, gapsCollectionWarning)
        return gid_in_source

    return slice(locs.min(), locs.max() + 1)
