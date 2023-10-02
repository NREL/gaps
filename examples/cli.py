# cli.py
from model import run, model_preprocessor, another_model, MyFinalModel
from gaps.cli import CLICommandFromFunction, CLICommandFromClass, make_cli


commands = [
    CLICommandFromFunction(
        function=run,
        name="runner",
        add_collect=True,
        split_keys=["project_points", ("a", "b")],
        config_preprocessor=model_preprocessor,
        skip_doc_params=["a", "b"],
    ),
    CLICommandFromFunction(
        function=another_model,
        name="analysis",
        add_collect=False,
        split_keys=["x"],
    ),
    CLICommandFromClass(
        MyFinalModel,
        method="execute",
        name="finalize",
        add_collect=False,
        split_keys=["m", "o"],
    ),
]

cli = make_cli(commands)


if __name__ == "__main__":
    cli(obj={})
