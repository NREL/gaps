# -*- coding: utf-8 -*-
"""
GAPs Status tests.
"""
from pathlib import Path

import pytest
import pandas as pd

from gaps import TEST_DATA_DIR
from gaps.utilities import (
    CaseInsensitiveEnum,
    recursively_update_dict,
    resolve_path,
    project_points_from_container_or_slice,
    _slice_to_list,
)
from gaps.exceptions import gapsValueError

TEST_1_ATTRS_1 = {"job_name": "test1", "job_status": "R", "run_id": 1234}
TEST_1_ATTRS_2 = {"job_name": "test1", "job_status": "successful"}


def test_case_insensitive_enum():
    """Tets subclassing the case insensitive enum"""

    # pylint: disable=too-few-public-methods
    class TestEnum(CaseInsensitiveEnum):
        """A test enum."""

        HELLO = "hello"
        THERE = "THERE"
        THIS = "ThIS"

        @classmethod
        def _new_post_hook(cls, obj, value):
            obj.my_test_len = len(value)
            return obj

    assert f"{TestEnum.HELLO}" == "hello"
    assert f"{TestEnum.THERE}" == "there"
    assert f"{TestEnum.THIS}" == "this"

    for text in ["hello", " HELLO", " HeLlo  "]:
        assert TestEnum(text) == TestEnum.HELLO

    for text in ["there", " THERE", " ThEre  "]:
        assert TestEnum(text) == TestEnum.THERE

    for text in ["this", " THIS", " ThIs  "]:
        assert TestEnum(text) == TestEnum.THIS

    with pytest.raises(ValueError):
        TestEnum("dne")

    with pytest.raises(ValueError):
        TestEnum("DNE")

    with pytest.raises(ValueError):
        TestEnum("DnE")

    with pytest.raises(ValueError):
        TestEnum(None)

    # pylint: disable=no-member
    assert TestEnum.HELLO.my_test_len == 5
    assert TestEnum.THERE.my_test_len == 5
    assert TestEnum.THIS.my_test_len == 4

    assert TestEnum.members_as_str() == {"hello", "there", "this"}


def test_project_points_from_container_or_slice():
    """Test the parse_project_points function."""

    base_pp = pd.read_csv(TEST_DATA_DIR / "project_points_100.csv")
    expected_gids = sorted(base_pp.gid.values)

    gids = project_points_from_container_or_slice(base_pp)
    assert gids == expected_gids

    gids = project_points_from_container_or_slice(list(range(100)))
    assert gids == expected_gids

    gids = project_points_from_container_or_slice(slice(0, 100))
    assert gids == expected_gids

    gids = project_points_from_container_or_slice(slice(None, 100, 1))
    assert gids == expected_gids

    gids = project_points_from_container_or_slice(set(range(100)))
    assert gids == expected_gids

    gids = project_points_from_container_or_slice({"gid": list(range(100))})
    assert gids == expected_gids

    with pytest.raises(gapsValueError):
        project_points_from_container_or_slice(slice(0, None))


def test_slice_to_list():
    """Test the _slice_to_list function."""
    expected_list = list(range(10))

    out_list = _slice_to_list(slice(None, 10))
    assert out_list == expected_list

    out_list = _slice_to_list(slice(None, 10, None))
    assert out_list == expected_list

    out_list = _slice_to_list(slice(None, 10, 1))
    assert out_list == expected_list

    out_list = _slice_to_list(slice(0, 10))
    assert out_list == expected_list

    out_list = _slice_to_list(slice(0, 10, 1))
    assert out_list == expected_list

    with pytest.raises(gapsValueError):
        _slice_to_list(slice(0, None))

    with pytest.raises(gapsValueError):
        _slice_to_list(slice(None, None))


def test_recursively_update_dict():
    """Test a recursive merge of two dictionaries"""

    test = recursively_update_dict(
        {"generation": TEST_1_ATTRS_1}, {"generation": TEST_1_ATTRS_2}
    )

    assert test["generation"]["job_name"] == TEST_1_ATTRS_1["job_name"]
    assert test["generation"]["run_id"] == TEST_1_ATTRS_1["run_id"]
    assert test["generation"]["job_status"] == TEST_1_ATTRS_2["job_status"]


def test_resolve_path():
    """Test resolving path."""

    base_dir = Path.home()

    assert resolve_path("test", base_dir) == "test"
    assert resolve_path("~test", base_dir) == "~test"
    assert (
        resolve_path("/test/f.csv", base_dir) == Path("/test/f.csv").as_posix()
    )
    assert resolve_path("./test", base_dir) == (base_dir / "test").as_posix()
    assert resolve_path("../", base_dir) == base_dir.parent.as_posix()
    assert resolve_path(".././", base_dir) == base_dir.parent.as_posix()
    assert (
        resolve_path("../test_file.json", base_dir)
        == (base_dir.parent / "test_file.json").as_posix()
    )
    assert (
        resolve_path("../test_dir/./../", base_dir)
        == base_dir.parent.as_posix()
    )
    assert (
        resolve_path("test_dir/./", base_dir)
        == Path("test_dir").resolve().as_posix()
    )
    assert (
        resolve_path("test_dir/../", base_dir)
        == Path("test_dir").resolve().parent.as_posix()
    )
    assert resolve_path("~/test_dir/../", base_dir) == Path.home().as_posix()


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
