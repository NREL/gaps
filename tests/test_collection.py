"""GAPs Collection tests"""

import shutil
from itertools import product
from pathlib import Path

import h5py
import pandas as pd
import numpy as np
import pytest
from packaging import version

from rex import Resource, Outputs
from gaps import logger
from gaps.log import init_logger
from gaps.collection import (
    Collector,
    DatasetCollector,
    find_h5_files,
    parse_gids_from_files,
    parse_project_points,
    _get_gid_slice,
)
from gaps.warn import gapsCollectionWarning
from gaps.exceptions import gapsRuntimeError, gapsValueError


@pytest.fixture
def enable_logging():
    """Enable logging for a single test."""
    init_logger("gaps.collection")
    yield
    for handler in logger.handlers:
        logger.removeHandler(handler)


def pd_date_range(*args, **kwargs):
    """A simple wrapper on the pd.date_range() method that handles the closed
    vs. inclusive kwarg change in pd 1.4.0"""
    incl = version.parse(pd.__version__) >= version.parse("1.4.0")

    if incl and "closed" in kwargs:
        kwargs["inclusive"] = kwargs.pop("closed")
    elif not incl and "inclusive" in kwargs:
        kwargs["closed"] = kwargs.pop("inclusive")
        if kwargs["closed"] == "both":
            kwargs["closed"] = None

    return pd.date_range(*args, **kwargs)


def write_chunk(meta, times, data, features, out_file):
    """Write data chunk to an h5 file

    Parameters
    ----------
    meta : dict
        Dictionary of meta data for this chunk. Includes flattened lat and lon
        arrays
    times : pd.DatetimeIndex
        times in this chunk
    features : list
        List of feature names in this chunk
    out_file : str
        Name of output file
    """
    with Outputs(out_file, "w") as out:
        out.meta = meta
        out.time_index = times
        for feature in features:
            flat_data = data.reshape((-1, len(times)))
            flat_data = np.transpose(flat_data, (1, 0))
            out.add_dataset(out_file, feature, flat_data, dtype=np.float32)


def make_fake_h5_chunks(temp_dir, features, shuffle=False):
    """Make fake h5 chunks to test collection

    Parameters
    ----------
    temp_dir : pathlib.Path
        Path to temporary directory for test.
    features : list
        List of datasets to write to chunks
    shuffle : bool
        Whether to shuffle gids

    Returns
    -------
    out_pattern : path-like
        Pattern for output file names
    data : ndarray
        Full non-chunked data array
    features : list
        List of feature names in output
    s_slices : list
        List of spatial slices used to chunk full data array
    times : pd.DatetimeIndex
        Times in output
    """
    data = np.random.uniform(0, 20, (50, 50, 48))
    lon, lat = np.meshgrid(np.linspace(-180, 0, 50), np.linspace(90, 0, 50))
    gids = np.arange(np.prod(lat.shape))
    if shuffle:
        np.random.shuffle(gids)

    gids = gids.reshape((50, 50))
    times = pd_date_range(
        "20220101", "20220103", freq="3600s", inclusive="left"
    )
    s_slices = [slice(0, 25), slice(25, 50)]
    out_fn_pattern = "chunks_{}_{}.h5"

    for ind, (sl1, sl2) in enumerate(product(s_slices, s_slices)):
        out_fn = out_fn_pattern.format(*divmod(ind, len(s_slices)))
        meta = pd.DataFrame(
            {
                "latitude": lat[sl1, sl2].flatten(),
                "longitude": lon[sl1, sl2].flatten(),
                "gid": gids[sl1, sl2].flatten(),
            }
        )
        write_chunk(
            meta=meta,
            times=times,
            data=data[sl1, sl2],
            features=features,
            out_file=temp_dir / out_fn,
        )

    return temp_dir / out_fn_pattern.format("*", "*")


def test_parse_project_points(points_path):
    """Test the parse_project_points function."""
    base_pp = pd.read_csv(points_path)
    expected_gids = sorted(base_pp.gid.values)

    gids = parse_project_points(points_path)
    assert gids == expected_gids

    gids = parse_project_points(points_path.as_posix())
    assert gids == expected_gids

    gids = parse_project_points(base_pp)
    assert gids == expected_gids

    gids = parse_project_points(list(range(100)))
    assert gids == expected_gids

    gids = parse_project_points(slice(0, 100))
    assert gids == expected_gids

    gids = parse_project_points(slice(None, 100, 1))
    assert gids == expected_gids

    gids = parse_project_points(set(range(100)))
    assert gids == expected_gids

    gids = parse_project_points({"gid": list(range(100))})
    assert gids == expected_gids

    with pytest.raises(gapsValueError):
        parse_project_points(slice(0, None))

    with pytest.warns(gapsCollectionWarning):
        parse_project_points([3, 2, 1])


def test_find_h5_files(collect_pattern, caplog):
    """Test the find_h5_files func."""

    collect_dir, pattern = collect_pattern
    pattern = collect_dir / pattern
    for pattern_type in [pattern, pattern.as_posix()]:
        h5_files = find_h5_files(pattern_type)

        expected_names = [
            "peregrine_2012_node00_x000.h5",
            "peregrine_2012_node00_x001.h5",
            "peregrine_2012_node01_x000.h5",
            "peregrine_2012_node01_x001.h5",
        ]

        assert [fp.name for fp in h5_files] == expected_names

        assert any(
            "Looking for source files based on" in record.message
            for record in caplog.records
        )


def test_find_h5_files_bad_files(tmp_path, caplog):
    """Test the find_h5_files func with bad files to collect."""
    h5_files = find_h5_files(tmp_path / "file_dne.h5")
    assert not h5_files

    h5_files = find_h5_files(tmp_path / "file_dne.csv")
    assert not h5_files

    (tmp_path / "file_dne.csv").touch()
    assert "file_dne.csv" in {fp.name for fp in tmp_path.glob("*")}
    with pytest.raises(gapsRuntimeError):
        find_h5_files(tmp_path / "file_dne.csv")

    assert any(
        "Source pattern resulted in non-h5 files that cannot be collected"
        in record.message
        for record in caplog.records
    )

    h5_files = find_h5_files(tmp_path / "file_dne.csv", ignore="file_dne.csv")
    assert not h5_files

    assert (
        sum(
            "Source pattern resulted in non-h5 files that cannot be collected"
            in record.message
            for record in caplog.records
        )
        == 1
    )


# pylint: disable=undefined-loop-variable
def test_parse_gids_from_files_duplicate_gids(
    tmp_path, caplog, collect_pattern
):
    """Test the parse_gids_from_files func with duplicate GIDS."""
    collect_dir, pattern = collect_pattern

    assert not list(tmp_path.glob("*"))
    h5_files = sorted(collect_dir.glob(pattern), key=lambda fp: fp.name)
    for h5_file in h5_files:
        shutil.copy(h5_file, tmp_path / h5_file.name)

    with Outputs(tmp_path / h5_file.name, "a") as out:
        meta = out.meta
        meta.gid = 100
        out.meta = meta

    with pytest.warns(gapsCollectionWarning):
        h5_files = sorted(tmp_path.glob("*"), key=lambda fp: fp.name)
        gids = parse_gids_from_files(h5_files)

    assert any(
        "Duplicate GIDs were found in source files!" in record.message
        for record in caplog.records
    )

    expected_gids = [100] * 100
    expected_gids[:82] = list(range(82))
    assert gids == expected_gids


# pylint: disable=undefined-loop-variable
def test_parse_gids_from_files_not_sorted_gids(
    tmp_path, caplog, collect_pattern
):
    """Test the parse_gids_from_files func with non-sorted GIDS."""
    collect_dir, pattern = collect_pattern

    assert not list(tmp_path.glob("*"))
    h5_files = sorted(collect_dir.glob(pattern), key=lambda fp: fp.name)
    for h5_file in h5_files:
        shutil.copy(h5_file, tmp_path / h5_file.name)

    with Outputs(tmp_path / h5_file.name, "a") as out:
        meta = out.meta
        meta.gid = meta.gid.values[::-1]
        out.meta = meta

    with pytest.warns(gapsCollectionWarning):
        h5_files = sorted(tmp_path.glob("*"), key=lambda fp: fp.name)
        gids = parse_gids_from_files(h5_files)

    expected_msg = (
        "Collection was run without project points file and with "
        "non-ordered meta data GIDs! This can cause issues with "
        "the collection ordering. Please check your data "
        "carefully."
    )

    assert any(expected_msg in record.message for record in caplog.records)
    expected_gids = list(range(100))
    expected_gids[82:] = expected_gids[82:][::-1]
    assert gids == expected_gids


def test_collection(
    tmp_path,
    caplog,
    enable_logging,
    collect_pattern,
    points_path,
    manual_collect,
):
    """Test collection on 'cf_profile' ensuring output array is correct."""
    collect_dir, pattern = collect_pattern

    profiles = manual_collect(collect_dir / pattern, "cf_profile")
    h5_file = tmp_path / "collection.h5"
    h5_file.touch()
    assert h5_file.exists()

    ctc = Collector(h5_file, collect_dir / pattern, points_path, clobber=True)
    ctc.collect("cf_profile", dataset_out=None)

    logged_messages = {record.message for record in caplog.records}
    expected_message_subset = {
        "\t- 'meta' collected",
        "\t- 'time_index' collected",
        "\t- Collection of 'cf_profile' complete",
        "Collection complete",
    }
    assert all(m in logged_messages for m in expected_message_subset)

    with h5py.File(h5_file, "r") as collected_outputs:
        cf_profiles = collected_outputs["cf_profile"][...]

    diff = np.mean(np.abs(profiles - cf_profiles))
    assert np.allclose(profiles, cf_profiles), f"Arrays differ by {diff:.4f}"

    source_file = collect_dir / "peregrine_2012_node00_x000.h5"
    with h5py.File(source_file, "r") as f_s:

        def check_attrs(name, obj):
            object_s = f_s[name]
            for key, val in obj.attrs.items():
                val_s = object_s.attrs[key]
                assert val == val_s

        with h5py.File(h5_file, "r") as file_:
            file_.visititems(check_attrs)


def test_profiles_means(tmp_path, collect_pattern, points_path):
    """Test adding means to pre-collected profiles."""
    h5_file = tmp_path / "cf.h5"
    collect_dir, pattern = collect_pattern

    # block below forces test for multiple pre-collection calls later
    files = find_h5_files(collect_dir / pattern, ignore="cf.h5")
    gids = parse_gids_from_files(files)
    DatasetCollector(h5_file, files, gids, "cf_mean")

    ctc = Collector(h5_file, collect_dir / pattern, points_path)
    ctc.collect("cf_profile", dataset_out=None)
    Collector.add_dataset(
        h5_file, collect_dir / pattern, "cf_mean", dataset_out=None
    )

    with h5py.File(h5_file, "r") as collected_outputs:
        assert "cf_profile" in collected_outputs
        assert "cf_mean" in collected_outputs
        data = collected_outputs["cf_profile"][...]

    node_file = collect_dir / "peregrine_2012_node01_x001.h5"
    with h5py.File(node_file, "r") as collected_outputs:
        source_data = collected_outputs["cf_profile"][...]

    # pylint: disable=no-member
    assert np.allclose(source_data, data[:, -1 * source_data.shape[1] :])


def test_low_mem_collect(tmp_path, collect_pattern, points_path):
    """Test memory limited multi chunk collection"""
    h5_file = tmp_path / "cf.h5"
    collect_dir, pattern = collect_pattern

    ctc = Collector(h5_file, collect_dir / pattern, points_path)
    ctc.collect(
        "cf_profile", dataset_out=None, memory_utilization_limit=0.00002
    )

    with h5py.File(h5_file, "r") as collected_outputs:
        assert "cf_profile" in collected_outputs
        data = collected_outputs["cf_profile"][...]

    node_file = collect_dir / "peregrine_2012_node01_x001.h5"
    with h5py.File(node_file, "r") as collected_outputs:
        source_data = collected_outputs["cf_profile"][...]

    # pylint: disable=no-member
    assert np.allclose(source_data, data[:, -1 * source_data.shape[1] :])


def test_means_lcoe(tmp_path, collect_pattern, points_path):
    """Test adding means to pre-collected profiles."""
    h5_file = tmp_path / "cf_lcoe.h5"
    collect_dir, pattern = collect_pattern

    ctc = Collector(h5_file, collect_dir / pattern, points_path)
    ctc.collect("cf_mean", dataset_out=None)

    Collector.add_dataset(
        h5_file, collect_dir / pattern, "lcoe_fcr", dataset_out=None
    )

    with h5py.File(h5_file, "r") as collected_outputs:
        assert "cf_mean" in collected_outputs
        assert "lcoe_fcr" in collected_outputs


def test_means_multiple_collect_calls(tmp_path, collect_pattern, points_path):
    """Test calling `collect` multiple times."""
    h5_file = tmp_path / "cf_lcoe.h5"
    collect_dir, pattern = collect_pattern

    ctc = Collector(h5_file, collect_dir / pattern, points_path)
    ctc.collect("cf_mean", dataset_out=None)
    ctc.collect("lcoe_fcr", dataset_out=None)

    with h5py.File(h5_file, "r") as collected_outputs:
        assert "cf_mean" in collected_outputs
        assert "lcoe_fcr" in collected_outputs


@pytest.mark.parametrize("set_to_zero", [False, True])
def test_collect_duplicates(tmp_path, collect_pattern, set_to_zero):
    """Test the collection of duplicate gids."""

    collect_dir, pattern = collect_pattern
    pattern = "pv_gen_2018*.h5"

    source_fps = sorted(collect_dir.glob(pattern))
    assert len(source_fps) > 1

    if set_to_zero:
        assert not list(tmp_path.glob("*"))
        for source_h5_file in source_fps:
            shutil.copy(source_h5_file, tmp_path / source_h5_file.name)

        with Outputs(tmp_path / source_h5_file.name, "a") as out:
            meta = out.meta
            meta.gid = 0
            out.meta = meta

        pat = tmp_path / pattern
    else:
        pat = collect_dir / pattern

    h5_file = tmp_path / "collection.h5"
    ctc = Collector(h5_file, pat, None)
    ctc.collect("cf_profile", dataset_out=None)

    with Resource(h5_file) as res:
        test_cf = res["cf_profile"]
        test_meta = res.meta

    assert len(test_meta) == 250

    index = 0
    for file_path in source_fps:
        with Resource(file_path) as res:
            truth_cf = res["cf_profile"]
            truth_meta = res.meta
            if set_to_zero and source_h5_file.name == file_path.name:
                truth_meta.gid = 0

        collect_slice = slice(index, index + len(truth_meta))

        assert np.allclose(test_cf[:, collect_slice], truth_cf)
        for col in ("latitude", "longitude", "gid"):
            test_meta_col = test_meta[col].values[collect_slice]
            assert np.allclose(test_meta_col, truth_meta[col].values)

        index += len(truth_meta)


def test_move_purge_chunks(
    tmp_path, caplog, enable_logging, collect_pattern, points_path
):
    """Test moving chunk files to separate folder"""

    collect_dir, pattern = collect_pattern
    chunk_folder = tmp_path / "chunk_files"

    assert not list(tmp_path.glob("*"))
    for h5_file in collect_dir.glob(pattern):
        shutil.copy(h5_file, tmp_path / h5_file.name)

    files = list(tmp_path.glob("*"))
    assert len(files) == 4
    assert chunk_folder not in files

    h5_file = tmp_path / "cf.h5"
    pattern_str = (tmp_path / pattern).as_posix()
    Collector(h5_file, pattern_str, points_path).move_chunks()

    assert any(
        f"Moved chunk files from {pattern_str}" in record.message
        for record in caplog.records
    )
    assert any("to sub_dir" in record.message for record in caplog.records)

    files = list(tmp_path.glob("*"))
    assert len(files) == 2
    assert chunk_folder in files

    files = list(chunk_folder.glob("*"))
    assert len(files) == 4
    assert all(f.name.startswith("peregrine_2012") for f in files)

    h5_file = chunk_folder / "cf.h5"
    pattern_str = (chunk_folder / pattern).as_posix()
    with pytest.warns(gapsCollectionWarning):
        Collector(h5_file, pattern_str, points_path).purge_chunks()

    expected_partial_warning = (
        "Not purging chunked output files. These datasets have not been "
        "collected:"
    )

    assert (
        sum(
            expected_partial_warning in record.message
            for record in caplog.records
        )
        == 1
    )

    files = list(chunk_folder.glob("*"))
    assert len(files) == 5

    ctc = Collector(h5_file, pattern_str, points_path)
    ctc.collect("cf_profile")
    ctc.collect("cf_mean", dataset_out="cf_mean")
    Collector.add_dataset(
        h5_file, collect_dir / pattern, "lcoe_fcr", dataset_out="lcoe"
    )
    Collector.add_dataset(
        h5_file, collect_dir / pattern, "lcoe_fcr", dataset_out=None
    )

    expected_datasets = {"cf_profile", "cf_mean", "lcoe", "lcoe_fcr"}
    with Resource(h5_file) as res:
        assert all(dataset in res.datasets for dataset in expected_datasets)

    ctc.purge_chunks()

    assert any(
        f"Purged chunk files from {pattern_str}" in record.message
        for record in caplog.records
    )

    files = list(chunk_folder.glob("*"))
    assert len(files) == 1
    assert files[0].name == "cf.h5"


def test_unordered_collection(tmp_path, manual_collect):
    """Test collection of multiple datasets from chunks with unordered gids"""

    features = ["cf_profile", "ac"]
    out_files = make_fake_h5_chunks(tmp_path, features, shuffle=True)

    for feature in features:
        profiles = manual_collect(out_files, feature)
        h5_file = tmp_path / "collection.h5"
        ctc = Collector(h5_file, out_files, None)
        ctc.collect(feature, dataset_out=None)
        with h5py.File(h5_file, "r") as collected_outputs:
            cf_profiles = collected_outputs[feature][...]

        diff = np.mean(np.abs(profiles - cf_profiles))
        msg = f"Arrays differ by {diff:.4f}"
        assert np.allclose(profiles, cf_profiles), msg


# pylint: disable=invalid-name
def test_invalid_collection_of_2D_dataset_with_no_time_index(
    tmp_path, collect_pattern, points_path
):
    """Test that collection raises error if `time_index` not collected."""

    h5_file = tmp_path / "collection.h5"
    collect_dir, pattern = collect_pattern

    files = list(collect_dir.glob(pattern))
    gids = list(pd.read_csv(points_path).gid.values)
    with pytest.raises(gapsRuntimeError) as exc_info:
        DatasetCollector(h5_file, files, gids, "cf_profile")

    expected_msg = (
        "'time_index' must be combined before profiles can be combined"
    )
    assert expected_msg in str(exc_info.value)


# pylint: disable=invalid-name
def test_invalid_collection_of_3D_dataset(
    tmp_path, collect_pattern, points_path
):
    """Test that collection raises error if dataset is 3D."""

    collect_dir, pattern = collect_pattern

    for h5_file in collect_dir.glob(pattern):
        new_file = tmp_path / h5_file.name
        shutil.copy(h5_file, new_file)
        with h5py.File(new_file, "a") as chunk:
            profile = chunk["cf_profile"][...]
            del chunk["cf_profile"]
            chunk["cf_profile"] = profile[..., None]

    h5_file = tmp_path / "collection.h5"
    files = list(tmp_path.glob(pattern))
    gids = list(pd.read_csv(points_path).gid.values)
    with pytest.raises(gapsRuntimeError) as exc_info:
        DatasetCollector(h5_file, files, gids, "cf_profile")

    assert "Cannot collect dataset" in str(exc_info.value)
    assert "with axis 3" in str(exc_info.value)


def test_get_gid_slice():
    """Test the _get_gid_slice method."""
    with pytest.raises(gapsRuntimeError) as exc_info:
        _get_gid_slice([0], [1, 2], "test")

    assert "could not locate source gids in output" in str(exc_info.value)


def test_get_first_gid_slice():
    """Test the _get_gid_slice method."""
    assert _get_gid_slice([1], [1, 2], "test") == slice(0, 1)


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
