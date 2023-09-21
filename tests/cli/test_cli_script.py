# -*- coding: utf-8 -*-
# pylint: disable=unused-argument
"""
GAPs script command tests.
"""
import json
from pathlib import Path

import pytest
import pandas as pd

from gaps.cli import CLICommandFromFunction, make_cli


SAMPLE_SCRIPT = """
import pandas as pd
from pathlib import Path

dir_name = f"{Path(__file__).parent.name}_test"
pd.DataFrame({"s": [dir_name]}).to_csv("test_out.csv", index=False)
"""


def run_func():
    """Test run function"""


def test_script_cli(tmp_path, cli_runner, runnable_script):
    """Test the script command basic execution."""

    main = make_cli([CLICommandFromFunction(run_func, add_collect=False)])

    pipe_config_fp = tmp_path / "config_pipeline.json"
    script_config_fp = tmp_path / "config_script.json"
    script_fp = tmp_path / "test.py"

    pipe_config = {
        "pipeline": [{"script": "./config_script.json"}],
        "logging": {"log_file": None, "log_level": "INFO"},
    }

    script_config = {"cmd": "python test.py"}

    with open(pipe_config_fp, "w") as config_file:
        json.dump(pipe_config, config_file)

    with open(script_config_fp, "w") as config_file:
        json.dump(script_config, config_file)

    with open(script_fp, "w") as script_file:
        script_file.write(SAMPLE_SCRIPT)

    assert "test_out.csv" not in {f.name for f in tmp_path.glob("*")}
    assert tmp_path / "logs" not in set(tmp_path.glob("*"))
    cli_runner.invoke(main, ["pipeline", "-c", pipe_config_fp.as_posix()])
    assert len(set((tmp_path / "logs").glob("*script*"))) == 1
    assert tmp_path / "logs" in set(tmp_path.glob("*"))
    assert "test_out.csv" in {f.name for f in tmp_path.glob("*")}

    test_df = pd.read_csv(tmp_path / "test_out.csv")
    pd.testing.assert_frame_equal(
        test_df, pd.DataFrame({"s": [f"{tmp_path.name}_test"]})
    )


if __name__ == "__main__":
    pytest.main(["-q", "--show-capture=all", Path(__file__), "-rapP"])
