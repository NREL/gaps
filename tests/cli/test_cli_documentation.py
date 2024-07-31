# -*- coding: utf-8 -*-
# pylint: disable=unused-argument,function-redefined,protected-access,
# pylint: disable=invalid-name,too-few-public-methods
"""
GAPs CLI documentation tests.
"""
from copy import deepcopy
from pathlib import Path

import pytest

import gaps.cli.documentation
from gaps.cli.documentation import (
    DEFAULT_EXEC_VALUES,
    EXTRA_EXEC_PARAMS,
    CommandDocumentation,
)


def func_no_args():
    """A short description."""


def test_command_documentation_copies_skip_params():
    """Test that the `CommandDocumentation` copies the skip params input."""

    skip_params_set = {"a"}
    CommandDocumentation(func_no_args, skip_params=skip_params_set)
    assert skip_params_set == {"a"}

    skip_params_set = ["a"]
    doc = CommandDocumentation(func_no_args, skip_params=skip_params_set)
    assert skip_params_set == ["a"]
    assert doc.skip_params == {"a", "cls", "self"} | set(EXTRA_EXEC_PARAMS)


def test_command_documentation_extra_exec_params():
    """Test the `CommandDocumentation` with extra exec params."""

    def func(
        max_workers,
        sites_per_worker,
        memory_utilization_limit,
        timeout,
        pool_size,
    ):
        """A short description.

        Parameters
        ----------
        max_workers : int
            Number of workers to run.
        sites_per_worker : float
            Number of sites to run.
        memory_utilization_limit : str
            A test documentation.
        timeout : dict
            A timeout value.
        pool_size : list
            A worker pool size.
        """

    expected_parameters = [
        "max_workers",
        "sites_per_worker",
        "memory_utilization_limit",
        "timeout",
        "pool_size",
    ]
    expected_types = ["(int)", "(float)", "(str)", "(dict)", "(list)"]
    expected_decs = [
        "Number of workers to run.",
        "Number of sites to run.",
        "A test documentation.",
        "A timeout value.",
        "A worker pool size.",
    ]
    expected_iter = zip(expected_parameters, expected_types, expected_decs)

    doc = CommandDocumentation(func)
    for param, p_type, p_doc in expected_iter:
        assert doc._param_in_func_signature(param)
        assert doc.param_required(param)
        assert p_doc in doc._format_extra_exec_param_doc(param)
        assert p_type in doc._format_extra_exec_param_doc(param)
        assert param in doc.exec_control_doc

        execution_control = doc.template_config["execution_control"]
        assert execution_control[param] == doc.REQUIRED_TAG
        assert doc.default_exec_values[param] == doc.REQUIRED_TAG


def test_command_documentation_extra_exec_params_no_user_doc():
    """Test the `CommandDocumentation` with extra exec params no user doc."""

    def func(
        max_workers,
        sites_per_worker,
        memory_utilization_limit,
        timeout,
        pool_size,
    ):
        """A short description."""

    expected_parameters = [
        "max_workers",
        "sites_per_worker",
        "memory_utilization_limit",
        "timeout",
        "pool_size",
    ]
    doc = CommandDocumentation(func)
    for param in expected_parameters:
        assert doc._param_in_func_signature(param)
        assert doc.param_required(param)
        assert doc._format_extra_exec_param_doc(param)
        assert "(int)" in doc._format_extra_exec_param_doc(param)
        assert param in doc.exec_control_doc

        execution_control = doc.template_config["execution_control"]
        assert execution_control["max_workers"] == doc.REQUIRED_TAG
        assert doc.default_exec_values["max_workers"] == doc.REQUIRED_TAG


def test_command_documentation_extra_exec_params_user_defaults():
    """Test the `CommandDocumentation` with extra exec params and defaults."""

    def func(
        max_workers=2,
        sites_per_worker=0.4,
        memory_utilization_limit="test",
        timeout=None,
        pool_size=None,
    ):
        """A short description.

        Parameters
        ----------
        max_workers : int, optional
            Number of workers to run. By default, ``2``.
        sites_per_worker : float, optional
            Number of sites to run. By default, ``0.4``.
        memory_utilization_limit : str, optional
            A test documentation. By default, ``"test"``.
        timeout : dict, optional
            A timeout value. By default, ``None``.
        pool_size : list, optional
            A worker pool size. By default, ``None``.
        """

    expected_parameters = [
        "max_workers",
        "sites_per_worker",
        "memory_utilization_limit",
        "timeout",
        "pool_size",
    ]
    expected_types = [
        "(int, optional)",
        "(float, optional)",
        "(str, optional)",
        "(dict, optional)",
        "(list, optional)",
    ]
    expected_decs = [
        "Number of workers to run.",
        "Number of sites to run.",
        "A test documentation.",
        "A timeout value.",
        "A worker pool size.",
    ]
    expected_value = [2, 0.4, "test", None, None]
    expected_iter = zip(
        expected_parameters, expected_types, expected_decs, expected_value
    )

    doc = CommandDocumentation(func)
    for param, p_type, p_doc, p_val in expected_iter:
        assert doc._param_in_func_signature(param)
        assert not doc.param_required(param)
        assert p_doc in doc._format_extra_exec_param_doc(param)
        assert p_type in doc._format_extra_exec_param_doc(param)
        assert param in doc.exec_control_doc

        execution_control = doc.template_config["execution_control"]
        assert execution_control[param] == p_val
        assert doc.default_exec_values[param] == p_val


def test_command_documentation_extra_exec_params_defaults_no_docs():
    """Test documentation with extra exec params, defaults, no doc."""

    def func(
        max_workers=2,
        sites_per_worker=0.4,
        memory_utilization_limit="test",
        timeout=None,
        pool_size=None,
    ):
        """A short description."""

    expected_parameters = [
        "max_workers",
        "sites_per_worker",
        "memory_utilization_limit",
        "timeout",
        "pool_size",
    ]
    expected_types = [
        "(int, optional)",
        "(float, optional)",
        "(str, optional)",
        "(int, optional)",
        "(int, optional)",
    ]

    expected_value = [2, 0.4, "test", None, None]
    expected_iter = zip(expected_parameters, expected_types, expected_value)

    doc = CommandDocumentation(func)
    for param, p_type, p_val in expected_iter:
        assert doc._param_in_func_signature(param)
        assert not doc.param_required(param)
        assert doc._extra_exec_param_doc
        assert p_type in doc._format_extra_exec_param_doc(param)
        assert f"``{p_val}``" in doc._format_extra_exec_param_doc(param)
        assert param in doc.exec_control_doc

        execution_control = doc.template_config["execution_control"]
        assert execution_control[param] == p_val
        assert doc.default_exec_values[param] == p_val


def test_command_documentation_no_extra_exec_params():
    """Test documentation with no extra exec params"""

    doc = CommandDocumentation(func_no_args)
    for param in EXTRA_EXEC_PARAMS:
        assert not doc._param_in_func_signature(param)
        assert not doc.param_required(param)
        assert not doc._format_extra_exec_param_doc(param)
        assert param not in doc.template_config["execution_control"]
        assert param not in doc.default_exec_values
        assert param not in doc.exec_control_doc

    assert not doc._extra_exec_param_doc


def test_command_documentation_default_exec_values_and_doc():
    """Test `CommandDocumentation.default_exec_values` and docs."""

    doc = CommandDocumentation(func_no_args)
    assert ":nodes:" not in doc.default_exec_values
    assert ":max_workers:" not in doc.default_exec_values
    assert ":nodes:" not in doc.exec_control_doc
    assert ":max_workers:" not in doc.exec_control_doc

    doc = CommandDocumentation(func_no_args, is_split_spatially=True)
    assert doc.default_exec_values == DEFAULT_EXEC_VALUES
    assert ":max_workers:" not in doc.default_exec_values
    assert ":nodes:" in doc.exec_control_doc
    assert ":max_workers:" not in doc.exec_control_doc


def test_command_documentation_required_args():
    """Test `CommandDocumentation.required_args`."""

    doc = CommandDocumentation(func_no_args)
    assert not doc.required_args

    def func(a=1):
        """Test func."""

    doc = CommandDocumentation(func)
    assert not doc.required_args

    def func(a, b=None):
        """Test func."""

    doc = CommandDocumentation(func)
    assert doc.required_args == {"a"}

    def func(cls, self, max_workers):
        """Test func."""

    doc = CommandDocumentation(func)
    assert not doc.required_args

    def func(a, b=None):
        """Test func."""

    doc = CommandDocumentation(func, skip_params={"a"})
    assert not doc.required_args


def test_command_documentation_template_config():
    """Test `CommandDocumentation.template_config`."""

    def func(project_points, a, b=1, c=None, max_workers=None):
        """Test func."""

    doc = CommandDocumentation(
        func, skip_params={"a"}, is_split_spatially=True
    )

    exec_vals = deepcopy(DEFAULT_EXEC_VALUES)
    exec_vals["max_workers"] = None
    expected_config = {
        "execution_control": exec_vals,
        "log_directory": "./logs",
        "log_level": "INFO",
        "project_points": doc.REQUIRED_TAG,
        "b": 1,
        "c": None,
    }
    assert doc.template_config == expected_config

    def func(project_points, a, max_workers, b=1, c=None):
        """Test func."""

    doc = CommandDocumentation(func, skip_params={"a"})
    exec_vals.pop("nodes")
    exec_vals["max_workers"] = doc.REQUIRED_TAG
    expected_config = {
        "execution_control": exec_vals,
        "log_directory": "./logs",
        "log_level": "INFO",
        "project_points": doc.REQUIRED_TAG,
        "b": 1,
        "c": None,
    }
    assert doc.template_config == expected_config


def test_command_documentation_hpc_parameter_help():
    """Test `CommandDocumentation.hpc_parameter_help`."""

    def func(project_points):
        """Test func.

        Parameters
        ----------
        project_points : str
            Path to project points file.
        """

    doc = CommandDocumentation(func, is_split_spatially=True)
    param_help = doc.hpc_parameter_help

    section_dividers = [
        any(line) and all(c == "-" for c in line)
        for line in param_help.split("\n")
    ]
    assert sum(section_dividers) == 1
    assert "Parameters" in param_help
    for key in DEFAULT_EXEC_VALUES:
        assert str(key) in param_help

    assert "project_points" in param_help
    assert "Path to project points file." in param_help
    assert "log_directory :" in param_help
    assert "log_level :" in param_help


def test_command_documentation_extended_summary():
    """Test `CommandDocumentation.extended_summary`."""

    def func():
        """Test func.

            An extended summary.

        Another line of extended summary.

        Returns
        -------
        None
        """

    doc = CommandDocumentation(func, is_split_spatially=True)

    expected_str = (
        "    An extended summary.\n\nAnother line of extended summary."
    )
    assert doc.extended_summary == expected_str


def test_command_documentation_config_help(monkeypatch):
    """Test `CommandDocumentation.config_help`."""

    monkeypatch.setattr(
        gaps.cli.documentation, "_is_sphinx_build", lambda: True, raising=True
    )

    doc = CommandDocumentation(func_no_args, is_split_spatially=True)
    config_help = doc.config_help(command_name="my_command_name")

    assert "my_command_name" in config_help
    assert (
        gaps.cli.documentation._cli_formatted(doc.hpc_parameter_help)
        in config_help
    )
    assert ".. tabs::" in config_help
    assert ".. group-tab::" in config_help


def test_command_documentation_command_help():
    """Test `CommandDocumentation.command_help`."""

    doc = CommandDocumentation(func_no_args, is_split_spatially=True)
    command_help = doc.command_help(command_name="my_command_name")

    assert "my_command_name" in command_help
    assert doc.extended_summary in command_help


def test_command_documentation_multiple_functions():
    """Test `CommandDocumentation` with multiple functions as input."""

    def func(project_points, a, b=1, _c=None, max_workers=None):
        """Test func.

        Parameters
        ----------
        project_points : str
            Path to project points.
        a : int
            Some input.
        b : int, optional
            More input. By default, ``1``.
        _c : float, optional
            A private input. By default, ``None``.
        max_workers : int, optional
            Max num workers. By default, ``None``.
        """

    def _func2(another_param, d=42, e=None):
        pass

    doc = CommandDocumentation(
        func, _func2, skip_params={"a"}, is_split_spatially=True
    )

    assert len(doc.signatures) == 2
    assert doc.required_args == {"project_points", "another_param"}

    exec_vals = deepcopy(DEFAULT_EXEC_VALUES)
    exec_vals["max_workers"] = None
    expected_config = {
        "execution_control": exec_vals,
        "log_directory": "./logs",
        "log_level": "INFO",
        "project_points": doc.REQUIRED_TAG,
        "b": 1,
        "another_param": doc.REQUIRED_TAG,
        "d": 42,
        "e": None,
    }
    assert doc.template_config == expected_config

    docstring = doc.hpc_parameter_help
    assert "Max num workers" in docstring
    assert "Path to project points." in docstring
    assert "More input" in docstring
    assert "_c :" not in docstring
    assert "A private input" not in docstring
    assert "log_directory :" in docstring
    assert "log_level :" in docstring

    assert not doc.extended_summary


def test_command_documentation_no_docstring():
    """Test `CommandDocumentation` with func missing docstring."""

    def _func(another_param, d=42, e=None):
        pass

    doc = CommandDocumentation(
        _func, skip_params={"a"}, is_split_spatially=True
    )
    assert len(doc.signatures) == 1

    docstring = doc.hpc_parameter_help
    assert "Max num workers" not in docstring
    assert "another_param :" not in docstring
    assert "d :" not in docstring
    assert "e :" not in docstring

    assert not doc.extended_summary


def test_command_documentation_for_class():
    """Test `CommandDocumentation` for a mix of classes and functions."""

    class TestCommand:
        """A test command as a class."""

        def __init__(
            self, _arg0, arg1, arg2=None, arg3="hello", max_workers=None
        ):
            """Initialize Model.

            Extended from init.

            Parameters
            ----------
            _arg0 : int
                A private input.
            arg1 : int
                Arg1 for model.
            arg2 : float, optional
                Arg2 for model. By default, ``None``.
            arg3 : str, optional
                Arg3 for model. By default, ``"hello"``.
            max_workers : int, optional
                Max num workers. By default, ``None``.
            """

        def func(self, project_points, a, _private_arg1, b=1, c=None):
            """A test function.

            Extended from func.

            Parameters
            ----------
            project_points : str
                Path to project points file.
            a : float
                An arg.
            b : int, optional
                Another arg. By default, ``1``.
            c : str, optional
                A str arg. By default, ``None``.
            """

    def preprocessor(another_input, _a_private_input, another_input2=None):
        """A sample pre-processor function

        Extended from preprocessor.

        Parameters
        ----------
        another_input : int
            Another model input.
        another_input2 : float, optional
            Another model input. with default. By default, ``None``.
        """

    doc = CommandDocumentation(
        TestCommand,
        getattr(TestCommand, "func"),
        preprocessor,
        skip_params={"a"},
        is_split_spatially=True,
    )
    assert len(doc.signatures) == 3

    docstring = doc.hpc_parameter_help
    assert ":max_workers:" in docstring
    assert "\narg1 :" in docstring
    assert "\narg2 :" in docstring
    assert "\narg3 :" in docstring
    assert "project_points :" in docstring
    assert "\nb :" in docstring
    assert "\nc :" in docstring
    assert "\nanother_input :" in docstring
    assert "\nanother_input2 :" in docstring
    assert "log_directory :" in docstring
    assert "log_level :" in docstring

    assert "\na :" not in docstring
    assert "\nself :" not in docstring
    assert "_private_arg1" not in docstring
    assert "_a_private_input" not in docstring

    assert "Extended from init" in doc.extended_summary
    assert "Extended from func" not in doc.extended_summary
    assert "Extended from preprocessor" not in doc.extended_summary

    exec_vals = deepcopy(DEFAULT_EXEC_VALUES)
    exec_vals["max_workers"] = None
    expected_config = {
        "execution_control": exec_vals,
        "log_directory": "./logs",
        "log_level": "INFO",
        "arg1": doc.REQUIRED_TAG,
        "arg2": None,
        "arg3": "hello",
        "project_points": doc.REQUIRED_TAG,
        "b": 1,
        "c": None,
        "another_input": doc.REQUIRED_TAG,
        "another_input2": None,
    }
    assert doc.template_config == expected_config


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
