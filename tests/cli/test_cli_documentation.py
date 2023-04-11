# -*- coding: utf-8 -*-
# pylint: disable=unused-argument,function-redefined,protected-access,
# pylint: disable=invalid-name
"""
GAPs CLI documentation tests.
"""
from copy import deepcopy
from pathlib import Path

import pytest

from gaps.cli.documentation import DEFAULT_EXEC_VALUES, FunctionDocumentation


def func_no_args():
    """A short description."""


def test_function_documentation_copies_skip_params():
    """Test that the `FunctionDocumentation` copies the skip params input."""

    skip_params_set = {"a"}
    FunctionDocumentation(func_no_args, skip_params=skip_params_set)
    assert skip_params_set == {"a"}

    skip_params_set = ["a"]
    func_doc = FunctionDocumentation(func_no_args, skip_params=skip_params_set)
    assert skip_params_set == ["a"]
    assert func_doc.skip_params == {"a", "cls", "self", "max_workers"}


def test_function_documentation_max_workers():
    """Test the `FunctionDocumentation` with and without `max_workers`."""

    def func(max_workers):
        """A short description.

        Parameters
        ----------
        max_workers : int
            Number of workers to run.
        """

    func_doc = FunctionDocumentation(func)
    assert func_doc.max_workers_in_func_signature
    assert func_doc.max_workers_required
    assert "Number of workers to run." in func_doc._max_workers_doc
    assert "(int)" in func_doc._max_workers_doc
    assert "max_workers" in func_doc.exec_control_doc

    execution_control = func_doc.template_config["execution_control"]
    assert execution_control["max_workers"] == func_doc.REQUIRED_TAG
    assert func_doc.default_exec_values["max_workers"] == func_doc.REQUIRED_TAG

    def func(max_workers):
        """A short description."""

    func_doc = FunctionDocumentation(func)
    assert func_doc.max_workers_in_func_signature
    assert func_doc.max_workers_required
    assert func_doc._max_workers_doc
    assert "(int)" in func_doc._max_workers_doc
    assert "max_workers" in func_doc.exec_control_doc

    execution_control = func_doc.template_config["execution_control"]
    assert execution_control["max_workers"] == func_doc.REQUIRED_TAG
    assert func_doc.default_exec_values["max_workers"] == func_doc.REQUIRED_TAG

    def func(max_workers=2):
        """A short description.

        Parameters
        ----------
        max_workers : int, optional
            Number of workers to run. By default, `2`.
        """

    func_doc = FunctionDocumentation(func)
    assert func_doc.max_workers_in_func_signature
    assert not func_doc.max_workers_required
    assert "Number of workers to run." in func_doc._max_workers_doc
    assert "(int, optional)" in func_doc._max_workers_doc
    assert "max_workers" in func_doc.exec_control_doc

    execution_control = func_doc.template_config["execution_control"]
    assert execution_control["max_workers"] == 2
    assert func_doc.default_exec_values["max_workers"] == 2

    def func(max_workers=None):
        """A short description."""

    func_doc = FunctionDocumentation(func)
    assert func_doc.max_workers_in_func_signature
    assert not func_doc.max_workers_required
    assert func_doc._max_workers_doc
    assert "(int, optional)" in func_doc._max_workers_doc
    assert "max_workers" in func_doc.exec_control_doc

    execution_control = func_doc.template_config["execution_control"]
    assert execution_control["max_workers"] is None
    assert func_doc.default_exec_values["max_workers"] is None

    func_doc = FunctionDocumentation(func_no_args)
    assert not func_doc.max_workers_in_func_signature
    assert not func_doc.max_workers_required
    assert not func_doc._max_workers_doc
    assert "max_workers" not in func_doc.template_config["execution_control"]
    assert "max_workers" not in func_doc.default_exec_values
    assert "max_workers" not in func_doc.exec_control_doc


def test_function_documentation_default_exec_values_and_doc():
    """Test `FunctionDocumentation.default_exec_values` and docs."""

    func_doc = FunctionDocumentation(func_no_args)
    assert "nodes" not in func_doc.default_exec_values
    assert "max_workers" not in func_doc.default_exec_values
    assert "nodes" not in func_doc.exec_control_doc
    assert "max_workers" not in func_doc.exec_control_doc

    func_doc = FunctionDocumentation(func_no_args, is_split_spatially=True)
    assert func_doc.default_exec_values == DEFAULT_EXEC_VALUES
    assert "max_workers" not in func_doc.default_exec_values
    assert "nodes" in func_doc.exec_control_doc
    assert "max_workers" not in func_doc.exec_control_doc


def test_function_documentation_required_args():
    """Test `FunctionDocumentation.required_args`."""

    func_doc = FunctionDocumentation(func_no_args)
    assert not func_doc.required_args

    def func(a=1):
        """Test func."""

    func_doc = FunctionDocumentation(func)
    assert not func_doc.required_args

    def func(a, b=None):
        """Test func."""

    func_doc = FunctionDocumentation(func)
    assert func_doc.required_args == {"a"}

    def func(cls, self, max_workers):
        """Test func."""

    func_doc = FunctionDocumentation(func)
    assert not func_doc.required_args

    def func(a, b=None):
        """Test func."""

    func_doc = FunctionDocumentation(func, skip_params={"a"})
    assert not func_doc.required_args


def test_function_documentation_template_config():
    """Test `FunctionDocumentation.template_config`."""

    def func(project_points, a, b=1, c=None, max_workers=None):
        """Test func."""

    func_doc = FunctionDocumentation(
        func, skip_params={"a"}, is_split_spatially=True
    )

    exec_vals = deepcopy(DEFAULT_EXEC_VALUES)
    exec_vals["max_workers"] = None
    expected_config = {
        "execution_control": exec_vals,
        "project_points": func_doc.REQUIRED_TAG,
        "b": 1,
        "c": None,
    }
    assert func_doc.template_config == expected_config

    def func(project_points, a, max_workers, b=1, c=None):
        """Test func."""

    func_doc = FunctionDocumentation(func, skip_params={"a"})
    exec_vals.pop("nodes")
    exec_vals["max_workers"] = func_doc.REQUIRED_TAG
    expected_config = {
        "execution_control": exec_vals,
        "project_points": func_doc.REQUIRED_TAG,
        "b": 1,
        "c": None,
    }
    assert func_doc.template_config == expected_config


def test_function_documentation_parameter_help():
    """Test `FunctionDocumentation.parameter_help`."""

    def func(project_points):
        """Test func.

        Parameters
        ----------
        project_points : str
            Path to project points file.
        """

    func_doc = FunctionDocumentation(func, is_split_spatially=True)
    param_help = func_doc.parameter_help

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


def test_function_documentation_extended_summary():
    """Test `FunctionDocumentation.extended_summary`."""

    def func():
        """Test func.

            An extended summary.

        Another line of extended summary.

        Returns
        -------
        None
        """

    func_doc = FunctionDocumentation(func, is_split_spatially=True)

    expected_str = "An extended summary.\n\nAnother line of extended summary."
    assert func_doc.extended_summary == expected_str


def test_function_documentation_config_help():
    """Test `FunctionDocumentation.config_help`."""

    func_doc = FunctionDocumentation(func_no_args, is_split_spatially=True)
    config_help = func_doc.config_help(command_name="my_command_name")

    assert "my_command_name" in config_help
    assert func_doc.parameter_help in config_help
    assert ".. tabs::" in config_help
    assert ".. tab::" in config_help


def test_function_documentation_command_help():
    """Test `FunctionDocumentation.command_help`."""

    func_doc = FunctionDocumentation(func_no_args, is_split_spatially=True)
    command_help = func_doc.command_help(command_name="my_command_name")

    assert "my_command_name" in command_help
    assert func_doc.extended_summary in command_help


def test_function_documentation_multiple_functions():
    """Test `FunctionDocumentation` with multiple functions as input."""

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

    func_doc = FunctionDocumentation(
        func, _func2, skip_params={"a"}, is_split_spatially=True
    )

    assert len(func_doc.signatures) == 2
    assert func_doc.required_args == {"project_points", "another_param"}

    exec_vals = deepcopy(DEFAULT_EXEC_VALUES)
    exec_vals["max_workers"] = None
    expected_config = {
        "execution_control": exec_vals,
        "project_points": func_doc.REQUIRED_TAG,
        "b": 1,
        "another_param": func_doc.REQUIRED_TAG,
        "d": 42,
        "e": None,
    }
    assert func_doc.template_config == expected_config

    docstring = func_doc.parameter_help
    assert "Max num workers" in docstring
    assert "Path to project points." in docstring
    assert "More input" in docstring
    assert "_c :" not in docstring
    assert "A private input" not in docstring

    assert not func_doc.extended_summary


def test_function_documentation_no_docstring():
    """Test `FunctionDocumentation` with func missing docstring."""

    def _func(another_param, d=42, e=None):
        pass

    func_doc = FunctionDocumentation(
        _func, skip_params={"a"}, is_split_spatially=True
    )
    assert len(func_doc.signatures) == 1

    docstring = func_doc.parameter_help
    assert "Max num workers" not in docstring
    assert "another_param :" not in docstring
    assert "d :" not in docstring
    assert "e :" not in docstring

    assert not func_doc.extended_summary


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
