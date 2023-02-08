# -*- coding: utf-8 -*-
"""
GAPs Config tests.
"""
from pathlib import Path

import pytest

from gaps.config import (
    load_config,
    ConfigType,
    config_as_str_for_docstring,
    resolve_all_paths,
)


def test_resolve_all_paths():
    """Test resolving all paths."""

    base_dir = Path.home()

    assert resolve_all_paths("test", base_dir) == "test"
    assert resolve_all_paths("~test", base_dir) == "~test"
    assert (
        resolve_all_paths("/test/f.csv", base_dir)
        == Path("/test/f.csv").as_posix()
    )
    assert (
        resolve_all_paths("./test", base_dir) == (base_dir / "test").as_posix()
    )
    assert resolve_all_paths("../", base_dir) == base_dir.parent.as_posix()
    assert resolve_all_paths(".././", base_dir) == base_dir.parent.as_posix()
    assert (
        resolve_all_paths("../test_file.json", base_dir)
        == (base_dir.parent / "test_file.json").as_posix()
    )
    assert (
        resolve_all_paths("../test_dir/./../", base_dir)
        == base_dir.parent.as_posix()
    )
    assert (
        resolve_all_paths("test_dir/./", base_dir)
        == Path("test_dir").resolve().as_posix()
    )
    assert (
        resolve_all_paths("test_dir/../", base_dir)
        == Path("test_dir").resolve().parent.as_posix()
    )
    assert (
        resolve_all_paths("~/test_dir/../", base_dir) == Path.home().as_posix()
    )


def test_resolve_all_paths_list():
    """Test resolving all paths in a list."""
    base_dir = Path.home()
    input_ = [
        "test",
        "./test",
        "../",
        ".././",
        "../test_file.json",
        "../test_dir/./../",
        ["test", "../test_dir/./../"],
    ]
    expected_output = [
        "test",
        (base_dir / "test").as_posix(),
        base_dir.parent.as_posix(),
        base_dir.parent.as_posix(),
        (base_dir.parent / "test_file.json").as_posix(),
        base_dir.parent.as_posix(),
        ["test", base_dir.parent.as_posix()],
    ]

    assert resolve_all_paths(input_, base_dir) == expected_output


def test_resolve_all_paths_dict():
    """Test resolving all paths in a dict."""
    base_dir = Path.home()
    input_ = {
        "a": "test",
        "b": "./test",
        "c": "../",
        "d": ".././",
        "e": "../test_file.json",
        "f": "../test_dir/./../",
        "g": ["test", "../test_dir/./../"],
        "h": {
            "a": "test",
            "b": ["test", "../test_dir/./../"],
        },
    }
    expected_output = {
        "a": "test",
        "b": (base_dir / "test").as_posix(),
        "c": base_dir.parent.as_posix(),
        "d": base_dir.parent.as_posix(),
        "e": (base_dir.parent / "test_file.json").as_posix(),
        "f": base_dir.parent.as_posix(),
        "g": [
            "test",
            base_dir.parent.as_posix(),
        ],
        "h": {
            "a": "test",
            "b": [
                "test",
                base_dir.parent.as_posix(),
            ],
        },
    }

    assert resolve_all_paths(input_, base_dir) == expected_output


@pytest.mark.parametrize("config_type", list(ConfigType))
def test_write_load_config(tmp_path, config_type):
    """Test loading a configuration file."""

    base_fn = f"test.{config_type}"

    test_dictionary = {"a": 1, "b": 2}
    with open(tmp_path / base_fn, "w") as config_file:
        config_type.dump(test_dictionary, config_file)

    assert load_config(tmp_path / "." / base_fn) == test_dictionary

    test_dictionary = {
        "a": 1,
        "b": "A string",
        "path_a": "./config.json",
        "path_b": "./../another.json",
        "path_c": "./something/.././../another.json",
    }
    config_type.write(tmp_path / base_fn, test_dictionary)

    expected_dict = {
        "a": 1,
        "b": "A string",
        "path_a": (tmp_path / "config.json").as_posix(),
        "path_b": (tmp_path.parent / "another.json").as_posix(),
        "path_c": (tmp_path.parent / "another.json").as_posix(),
    }
    assert load_config(tmp_path / "." / base_fn) == expected_dict

    assert (
        load_config(tmp_path / "." / base_fn, resolve_paths=False)
        == test_dictionary
    )


@pytest.mark.parametrize("config_type", list(ConfigType))
def test_config_dumps_loads(config_type):
    """Test dumping and loading a configuration file to and from a str."""

    test_dictionary = {
        "a": 1,
        "b": "A string",
        "path_a": "./config.json",
        "path_b": "./../another.json",
        "path_c": "./something/.././../another.json",
    }
    assert (
        config_type.loads(config_type.dumps(test_dictionary))
        == test_dictionary
    )


@pytest.mark.parametrize("config_type", list(ConfigType))
def test_config_as_str_for_docstring(config_type):
    """Test the test_config_as_str_for_docstring function."""

    test_dictionary = {
        "a": 1,
        "b": "A string",
        "path_a": "./config.json",
        "path_b": "./../another.json",
        "path_c": "./something/.././../another.json",
    }
    as_str = config_as_str_for_docstring(test_dictionary, config_type)
    split_str = as_str.split("\n")
    assert len(split_str) >= 6
    for str_part in as_str.split("\n")[1:]:
        assert str_part.startswith("        ")


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
