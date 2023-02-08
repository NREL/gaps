# -*- coding: utf-8 -*-
"""
Tests for GAPs ProjectPoints
"""
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

from gaps.project_points import ProjectPoints
from gaps.exceptions import gapsKeyError, gapsRuntimeError


def test_project_points_general():
    """Test initializing Projects points and the len method"""
    pp = ProjectPoints(1)
    assert len(pp) == 1
    assert pp.split_range == (0, 1)

    pp = ProjectPoints(slice(0, 100), test=2)
    assert len(pp) == 100
    assert pp.split_range == (0, 100)
    assert "test" in pp.df
    assert np.allclose(pp.df.test, 2)

    pp = ProjectPoints(slice(0, 100, 2))
    assert len(pp) == 50
    assert pp.split_range == (0, 50)

    pp = ProjectPoints([1, 3, 5], test=2, another=["a", "b", "d"])
    assert len(pp) == 3
    assert pp.split_range == (0, 3)
    assert "test" in pp.df
    assert np.allclose(pp.df.test, 2)
    assert "another" in pp.df
    assert list(pp.df.another) == ["a", "b", "d"]

    pp = ProjectPoints((1, 3, 5))
    assert len(pp) == 3
    assert pp.split_range == (0, 3)

    pp = ProjectPoints(np.array((1, 3, 5)))
    assert len(pp) == 3
    assert pp.split_range == (0, 3)

    pp = ProjectPoints(pp.df)
    assert len(pp) == 3
    assert pp.split_range == (0, 3)

    pp = ProjectPoints(pp.df.to_dict())
    assert len(pp) == 3
    assert pp.split_range == (0, 3)

    assert pp.df.equals(pd.DataFrame({"gid": {0: 1, 1: 3, 2: 5}}))
    assert str(pp) == "ProjectPoints with 3 sites from gid 1 through 5"
    assert pp.gids == [1, 3, 5]

    with pytest.raises(gapsKeyError):
        ProjectPoints(pd.DataFrame())

    with pytest.raises(gapsRuntimeError):
        ProjectPoints(lambda x: x)


def test_project_points_ordering():
    """Test that GID's are sorted if input out of order."""
    with pytest.warns(UserWarning):
        pp = ProjectPoints([5, 1, 2])
    assert len(pp) == 3
    assert "points_order" in pp.df
    assert np.allclose(pp.df.gid, [1, 2, 5])


def test_project_points_iter():
    """Test ProjectPoints iteration."""
    pp = ProjectPoints([1, 3, 5])

    for ind, site in enumerate(pp):
        assert np.allclose(site, pp.df.iloc[ind])


def test_project_points_get():
    """Test ProjectPoints get item."""
    pp = ProjectPoints([1, 3, 5])

    assert np.allclose(pp[1], pp.df.iloc[0])
    assert np.allclose(pp[3], pp.df.iloc[1])
    assert np.allclose(pp[5], pp.df.iloc[2])

    with pytest.raises(KeyError):
        __ = pp[0]


def test_project_points_sites_as_slice():
    """Test ProjectPoints sites_as_slice."""
    pp = ProjectPoints(1)
    assert pp.sites_as_slice == slice(1, 2, 1)

    pp = ProjectPoints([1, 2])
    assert pp.sites_as_slice == slice(1, 3, 1)

    pp = ProjectPoints([1, 4, 7])
    assert pp.sites_as_slice == slice(1, 8, 3)

    pp = ProjectPoints([1, 3, 7])
    assert pp.sites_as_slice == [1, 3, 7]


def test_project_points_index():
    """Test ProjectPoints index."""
    pp = ProjectPoints([1, 3, 5])

    assert pp.index(1) == 0
    assert pp.index(3) == 1
    assert pp.index(5) == 2

    with pytest.raises(IndexError):
        __ = pp.index(0)


def test_project_points_join_df():
    """Test ProjectPoints join_df."""
    pp = ProjectPoints(np.array((1, 3, 5)), test=2)
    df2 = pd.DataFrame(
        {
            "gid": {0: 1, 1: 3, 2: 5},
            "other": {0: 3, 1: 4, 2: 5},
            "test": {0: 5, 1: 5, 2: 5},
        }
    )
    pp.join_df(df2)

    assert "other" in pp.df
    assert np.allclose(pp.df.other, [3, 4, 5])
    assert "test" in pp.df
    assert np.allclose(pp.df.test, 2)

    pp = ProjectPoints(np.array((1, 3, 5)))
    df2 = pd.DataFrame(
        {"gid2": {0: 1, 1: 3, 2: 5}, "other": {0: 3, 1: 4, 2: 5}}
    )
    pp.join_df(df2, key="gid2")

    assert "other" in pp.df
    assert np.allclose(pp.df.other, [3, 4, 5])
    assert "gid" in pp.df
    assert np.allclose(pp.df.gid, [1, 3, 5])


def test_project_points_get_sites_from_key():
    """Test ProjectPoints get_sites_from_key."""
    pp = ProjectPoints(np.array((1, 3, 5)), test=2)

    assert not pp.get_sites_from_key("dne", 0)
    assert not pp.get_sites_from_key("gid", 0)
    assert pp.get_sites_from_key("gid", 1) == [1]
    assert not pp.get_sites_from_key("test", 0)
    assert pp.get_sites_from_key("test", 2) == [1, 3, 5]


@pytest.mark.parametrize(
    ("start", "interval"), [[0, 1], [13, 1], [10, 2], [13, 3]]
)
def test_project_points_split(start, interval):
    """Test the split operation of project points."""
    num_per_split = 3
    pp = ProjectPoints(slice(start, 100, interval))

    for ind, pp_split in enumerate(pp.split(sites_per_split=num_per_split)):
        assert isinstance(pp_split, ProjectPoints)
        assert len(pp_split) <= num_per_split
        i0_nom = ind * num_per_split
        i1_nom = ind * num_per_split + num_per_split
        split = pp_split.df
        target = pp.df.iloc[i0_nom:i1_nom, :]
        assert np.allclose(split, target)


def test_project_points_split_range():
    """Test that split range is correct after splitting."""
    pp = ProjectPoints(slice(0, 4))

    for pp_split in pp.split(sites_per_split=2):
        assert isinstance(pp_split, ProjectPoints)
        assert len(pp_split) == 2
        i0_nom, i1_nom = pp_split.split_range
        split = pp_split.df
        target = pp.df.iloc[i0_nom:i1_nom, :]
        assert np.allclose(split, target)


@pytest.mark.parametrize(
    ("start", "interval"), [[0, 1], [13, 1], [10, 2], [13, 3]]
)
def test_project_points_from_range(start, interval):
    """Test the from_range operation of project points."""
    pp = ProjectPoints(slice(start, 100, interval), test=2)

    iter_interval = 5
    for start_ind in range(0, len(pp), iter_interval):
        end_ind = start_ind + iter_interval
        if end_ind > len(pp):
            break

        pp_0 = ProjectPoints.from_range((start_ind, end_ind), pp.df, test=2)
        assert np.allclose(pp_0.df, pp.df.iloc[start_ind:end_ind])
        assert "test" in pp_0.df
        assert np.allclose(pp_0.df.test, 2)

        pp_0 = ProjectPoints.from_range(
            (start_ind, end_ind), slice(start, 100, interval), test=2
        )
        assert np.allclose(pp_0.df, pp.df.iloc[start_ind:end_ind])
        assert "test" in pp_0.df
        assert np.allclose(pp_0.df.test, 2)


def test_split_iter():
    """Test ProjectPoints on two slices."""
    pp = ProjectPoints(slice(0, 500, 5))

    num_per_split = 3
    for start, end in [(0, 50), (50, 100)]:
        points = ProjectPoints.from_range((start, end), pp.df)

        for ind, pp_split in enumerate(
            points.split(sites_per_split=num_per_split)
        ):
            i0_nom = start + ind * num_per_split
            i1_nom = min(start + ind * num_per_split + num_per_split, end)

            split = pp_split.df
            target = pp.df.iloc[i0_nom:i1_nom]

            assert np.allclose(split, target)


def test_nested_sites():
    """
    Test check for nested points list
    """
    with pytest.raises(RuntimeError):
        points = [[1, 2, 3, 5]]
        ProjectPoints(points)


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
