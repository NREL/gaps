# -*- coding: utf-8 -*-
"""
GAPs Logging tests.
"""
import logging
from pathlib import Path

import pytest

from gaps.log import init_logger, log_versions, log_mem


def clear_handlers(logger):
    """Clear logger handlers"""

    while logger.handlers:
        logger.removeHandler(logger.handlers[0])


def test_log_versions(caplog):
    """Test that logging versions gives some of the expected output."""
    log_versions()
    assert any(
        "Running with gaps version" in record.message
        for record in caplog.records
    )

    assert any("rex version" in record.message for record in caplog.records)


def test_log_mem(caplog):
    """Test that logging memory gives some of the expected output."""
    log_mem()
    assert any(
        "Memory utilization is" in record.message for record in caplog.records
    )


@pytest.mark.parametrize("logger_name", ["gaps", "gaps.cli"])
def test_basic_logging_to_sys(
    logger_name, caplog, capsys, assert_message_was_logged
):
    """Test that logger correctly writes to sys.out"""
    assert not caplog.records

    logger = logging.getLogger(logger_name)
    clear_handlers(logger)

    logger.info("Test")
    captured = capsys.readouterr()
    assert not captured.out
    assert_message_was_logged("Test", log_level="INFO", clear_records=True)
    assert not caplog.records

    init_logger()

    logger.info("Test")
    captured = capsys.readouterr()
    assert "Test" in captured.out
    assert_message_was_logged("Test", log_level="INFO", clear_records=True)

    clear_handlers(logger)


@pytest.mark.parametrize("logger_name", ["gaps", "gaps.cli"])
def test_basic_logging_to_file(
    logger_name, caplog, capsys, tmp_path, assert_message_was_logged
):
    """Test that logger correctly writes to file"""
    assert not caplog.records
    assert not list(tmp_path.glob("*"))

    logger = logging.getLogger(logger_name)
    clear_handlers(logger)

    logger.info("Test")
    captured = capsys.readouterr()
    assert not captured.out
    assert not list(tmp_path.glob("*"))
    assert_message_was_logged("Test", log_level="INFO", clear_records=True)

    test_log_file = tmp_path / "test.log"
    init_logger(stream=False, file=test_log_file.as_posix())

    logger.info("Test")
    captured = capsys.readouterr()
    assert not captured.out
    assert [f.name for f in tmp_path.glob("*")] == [test_log_file.name]
    assert_message_was_logged("Test", log_level="INFO", clear_records=True)

    with open(test_log_file, "r") as log_file:
        contents = log_file.readlines()

    assert len(contents) == 1
    assert "Test" in contents[0]

    clear_handlers(logger)


@pytest.mark.parametrize("logger_name", ["gaps", "gaps.cli"])
def test_adding_more_sever_handler(
    logger_name, caplog, capsys, assert_message_was_logged
):
    """Test that logger correctly writes to sys.out"""
    assert not caplog.records

    logger = logging.getLogger(logger_name)
    clear_handlers(logger)

    logger.info("Test")
    captured = capsys.readouterr()
    assert not captured.out
    assert_message_was_logged("Test", log_level="INFO", clear_records=True)

    init_logger(level="WARNING")

    logger.info("Test")
    captured = capsys.readouterr()
    assert not captured.out
    assert_message_was_logged("Test", log_level="INFO", clear_records=True)

    init_logger(level="INFO")

    logger.info("Test")
    captured = capsys.readouterr()
    assert "Test" in captured.out
    assert_message_was_logged("Test", log_level="INFO", clear_records=True)

    clear_handlers(logger)


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
