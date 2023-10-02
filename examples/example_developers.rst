How to use GAPs
===============

Intro
-----

If you are a model developer looking to leverage GAPs to scale your model to the HPC,
you are in the right place. The following examples start with a basic example and
progressively get more advanced to demonstrate the full power of this framework.

If you would like to dive into some real-world examples instead, feel free to check
out `reV <https://github.com/NREL/reV>`_, `reVX <https://github.com/NREL/reVX>`_,
or `PVDegradationTools <https://github.com/NREL/PVDegradationTools>`_.

Don't forget to check out the `API documentation <https://nrel.github.io/gaps/_autosummary/gaps.html>`_
for details on inputs to the GAPs function calls described below.

Basic Setup
***********
Let's suppose you have a function designed to execute a model at a particular location:

.. code-block::
    python

    # model.py

    def run_model(lat, lon, a, b, c):
        """Example model that runs computation for a single site."""

        # simple computation for example purposes
        x = lat + lon
        return a * x**2 + b * x + c


To get ready to couple to GAPs, we need to implement a function to iterate over some
locations (the latter will be provided by GAPs) and execute the model above.


.. code-block::
    python

    # model.py
    import numpy as np
    from rex import Outputs

    ...


    def run(project_points, a, b, c, tag):
        """Run model on a single mode."""

        data = []
        for site in project_points:
            data.append(run_model(site.lat, site.lon, a, b, c))

        out_fp = f"results{tag}.h5"
        with Outputs(out_fp, "w") as fh:
            fh.meta = project_points.df
            fh.write_dataset("outputs", data=np.array(data), dtype="float32")

        return out_fp


Let's break this function down. The first input, ``project_points``, is a parameter
that will be provided by GAPs based on user input. In particular, the user will
provide a ``project_points`` CSV file, where each row will represent a single location.
By iterating over the GAPs ``ProjectPoints`` object, you can access the ``pandas.Series``
representation of the locations to process, which includes all of the user input
for each location. In this case, we expect the user to include "lat" and "lon", columns,
so we access those directly using ``site.lat`` and ``site.lon`` as inputs to our model
function.

Note that we also request the other model parameters, ``a``, ``b``, and ``c``, as
function inputs. This means users will provide values for those parameters, and GAPs
will supply those to our function during the call.

Finally, we "request" the ``tag`` input from GAPs. This is a special input that GAPs
can pass to our function call (you can "request" it by including it in the function
signature). To see a full list of the parameters you can request from GAPs, check
out the documentation for `CLICommandFromFunction <https://nrel.github.io/gaps/_autosummary/gaps.cli.command.CLICommandFromFunction.html#gaps.cli.command.CLICommandFromFunction>`_.
The ``tag`` value is a unique string that you can append onto your output file to
make it unique compared to other nodes running the same function. This way, there is
no race condition for writing data when the user executes the model on multiple HPC
nodes in parallel (each node has its own unique file it can write to at will). Adding
a tag like this is also required for the GAPs-provided collect command to function
properly.

Once the data is processed, we use the ``rex.Outputs`` class to write the results to
an HDF5 file. You can write outputs to other data formats as well, but you will have to
write your own collection methods (to collect data from multiple nodes), as HDF5 files
are the only ones GAPs support out-of-the-box. You could also use ``h5py`` to write to
an HDF5 output file, but we find that ``rex`` makes it simple to save the output to a
GAPs-compatible format.

To write the output data, we just need to specify a meta ``DataFrame`` (we can use the user
``project_points`` input) and the output data as a dataset in ``numpy`` array format
(we can also specify a ``time_index`` if our output data has a temporal component; see
the ``rex.Outputs`` documentation for more details). Make sure that there is a 1:1
correspondence between meta rows and output data (i.e. the first meta row corresponds to the
data at index 0 of the output array, the second meta row corresponds to the data at
index 1 of the output array, and so on).

Finally, we return the path to the output HDF5 file so that GAPs can record that as our
results output. This is also required for the GAPs-provided collect command to
function properly.

Now that we have our run function set up, let's use GAPs to build out a CLI:

.. code-block::
    python

    # cli.py
    from model import run
    from gaps.cli import CLICommandFromFunction, make_cli


    commands = [
        CLICommandFromFunction(
            function=run,
            name="runner",
            add_collect=True,
            split_keys=["project_points"],
        )
    ]

    cli = make_cli(commands)


    if __name__ == "__main__":
        cli(obj={})


To build our CLI, we first put together a CLI Command Configuration for our run function.
Specifically, we specify our run function as the one to execute on each node and choose
"runner" as the name of the CLI command attached to this function. We also ask GAPs to
add a "collect" command, since our function writes the output data to an HDF5 file. Finally,
we specify that the ``project_points`` input be used as the input to split execution across
nodes. This means that the user can specify how many nodes they want to split execution across,
and GAPs will take care of distributing the project point locations across the requested
number of nodes.

If we wanted to add more commands, we would build a similar Command Configuration for each
run function, and then compile them all into a ``commands`` list. This list is then passed
to the ``make_cli`` function, the output of which can be used as the entry point for our
brand-new command line interface.

Congratulations you have set up your first GAPs-powered model that can be scaled up to
run on the HPC. Let's take a closer look at everything we get from these few lines of code:

.. code-block::
    shell

    $ python cli.py
    Usage: cli.py [OPTIONS] COMMAND [ARGS]...

    Command Line Interface

    Options:
        -v, --verbose  Flag to turn on debug logging. Default is not verbose.
        --help         Show this message and exit.

    Commands:
        batch             Execute an analysis pipeline over a parametric set of...
        collect-runner    Execute the `collect-runner` step from a config file.
        pipeline          Execute multiple steps in an analysis pipeline.
        reset-status      Reset the pipeline/job status (progress) for a given...
        runner            Execute the `runner` step from a config file.
        script            Execute the `script` step from a config file.
        status            Display the status of a project FOLDER.
        template-configs  Generate template config files for requested COMMANDS.


We can immediately see that ``runner`` is one of the command options, along with
``collect-runner``, which will collect the one-per-node result files into a single
output file. The rest of the commands contain useful functionality for the user
of your command-line interface.

For details on executing your model using this CLI, see How to Run GAPs-powered models.


Multiprocessing
***************

In most cases, it is inefficient (and inconsiderate to other HPC users) to only utilize
a single CPU core on an HPC node dedicated to running your model (the only rare exceptions
to this rule are processes that require a very large amount of memory and therefore
can only afford to run one at a time without running up against memory limits).
Therefore, it is important to parallelize your model execution once you are running on the
node itself. GAPs cannot do this for you, since that would require too much knowledge about
your model and/or place several limitation about the kinds of computations you can run with GAPs.

Luckily, the Python standard library provides excellent tools to help you parallelize model
execution on each node. In particular, we will demonstrate how to utilize all available CPU
cores on a node by modifying the ``run`` function from above to make use of
`concurrent.futures <https://docs.python.org/3/library/concurrent.futures.html>`_:


.. code-block::
    python

    # model.py
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from rex import Outputs

    ...

    def run(project_points, a, b, c, tag, max_workers=None):
        """Run model on a single node with multiprocessing."""

        out_fp = f"results{tag}.h5"
        Outputs.init_h5(
            out_fp,
            ["outputs"],
            shapes={"outputs": (project_points.df.shape[0],)},
            attrs={"outputs": None},
            chunks={"outputs": None},
            dtypes={"outputs": "float32"},
            meta=project_points.df,
        )

        futures = {}
        with ProcessPoolExecutor(max_workers=max_workers) as exe:
            for site in project_points:
                future = exe.submit(run_model, site.lat, site.lon, a, b, c)
                futures[future] = site.gid

        with Outputs(out_fp, "a") as out:
            for future in as_completed(futures):
                gid = futures.pop(future)
                ind = project_points.index(gid)
                out["outputs", ind] = future.result()

        return out_fp


The first thing to note is that we are now requesting an extra GAPs-provided input:
``max_workers``. This is an input the user will be able to control, and it dictates
exactly how many process each node should run concurrently. Note that this input can
be set to ``None``, which uses the max number of cores available on the node.

Next, we initialize the output file for the node. This simplifies our block of code
that collects all the futures running on this node. Alternatively, we could have
initialized an empty ``numpy`` array, collected all the future outputs there, and written
the array to the output file directly like we did in the first function. These approaches
are equivalent - feel free to sue whatever suits your style most.

The next block of code initialized a ``ProcessPoolExecutor`` with the number of ``max_workers``
requested by the user. We then submit ``run_model`` function executions for all sites in the
``project_points`` input. Note that each submission makes a copy of the inputs to the
run function. This means that model inputs that take up large amounts of memory may be
copied many times, depending on how many points the user wants to execute on each node.
For example, if the input ``a`` to the model is a 100 MB array, and the user submits 1000
points to run on the node, this submission process will create 1000 copies of the input
array, requiring at least 100 GB of RAM for the processing. For this reason, you should
minimize the memory footprints of your model inputs as much as possible (i.e. by loading
the data in the ``run_model`` function itself, whenever possible). For alternative
workarounds to this issue, see the chunking approach employed by `reVX exclusions
calculators: <https://github.com/NREL/reVX/blob/2dd05402c9c05ca0bf7f0e5bc2849ede0d0bc3cb/reVX/utilities/exclusions.py#L323-L367>`_.

Note that when we submit the futures, we store them in a dictionary to collect later with
the ``as_completed`` function. This allows us to store some metadata along with each future
object. In particular, we store the site GID (note that this requires users to specify a
``gid`` column in their project points CSV, which is typical for models that rely on
WTK/NSRDB/Sup3rCC data) corresponding to each future, allowing us to place the data
in the appropriate location in the output array. We obtain the index into the output array
using the `ProjectPoints.index <https://nrel.github.io/gaps/_autosummary/gaps.project_points.ProjectPoints.html#gaps.project_points.ProjectPoints.index>`_
function, and store the result immediately in the output HDF5 file. An alternative approach
to obtain this index is to use the iteration ``for ind, site in project_points.df.iterrows()``,
and then store the ``ind`` values in the future metadata. The latter approach may be preferable
if you do not wish to assign a GID value to each location submitted to your model.

Once all processing is complete, we return the path to the output file as normal.
With only a few extra lines of code, our model execution is parallelized on each node!

Advanced Topics
---------------
Split Keys
**********
In the example above, we specified ``split_keys=["project_points"]`` in the ``make_cli`` call,.
This informed GAPs that the function we are running on each node is geospatial in nature and
should be split by input sites. However, sometimes we may wish to split execution across nodes
based on other/additional model inputs. GAPs supports this kind of configuration as well. The only
requirement is that the keys given in ``split_keys`` are provided as lists.

For example, let's suppose we would like to allow our users to specify multiple values for the ``a``
model input. To do so, we can call ``make_cli`` with the argument ``split_keys=["a"]``. Then, if a
user specifies ``a=[1, 2, 3]`` in their config file, GAPs will submit execution of our model to three
separate nodes, where each node will process one of the three values for ``a`` given in that list.
Importantly, your ``run_model`` function **will not** receive the full ``[1, 2, 3]`` list as the
input for ``a``, but rather a single integer value that should be executed for that model run.

In the example above, execution is no longer split across points, but only across the ``a`` input.
This can be counterproductive to our efforts of scaling geospatial execution across HPC nodes.
To get around this, we can specify both ``project_points`` and ``a`` as split inputs:
``split_keys=["project_points", "a"]``. This means that *for each value of a that the user
specifies*, GAPs will split model execution across the inputs sites on multiple nodes (the exact
configuration will be configurable by the user's ``nodes`` input in the ``execution_control``
block of the run config).

GAPs allows you to specify as many keys as you want in the ``split_keys`` list. GAPs will take these
inputs and perform a parameter permutation of them before submitting to the HPC nodes. For example,
let's suppose we specify ``split_keys=["a", "b"]`` and the user provides ``a=[1, 2, 3], b=[4, 5]``
in their config file. GAPs will submit the processing to a total of six HPC nodes, each node getting
one of the following combinations as input:

    - a=1, b=4
    - a=2, b=4
    - a=3, b=4
    - a=1, b=5
    - a=2, b=5
    - a=3, b=5

However, sometimes this permutation of inputs does not make sense (i.e. if you want to run specific
combinations of turbine rotor diameter and hub height, instead of all possible permutations). In this
case, you can specify inputs as *combined* split keys, like so: ``split_keys=[("a", "b")]``. This means
that the keys ``a`` and ``b`` will be processed in tandem before submitting to nodes for execution.
For example, if the user specifies ``a=[1, 2, 3], b=[4, 5, 6]`` in their config file, then GAPs will
submit the processing to a total of three HPC nodes, each node getting one of the following combinations
as input:

    - a=1, b=4
    - a=2, b=5
    - a=3, b=6

Note that this requires that the ``a`` and ``b`` inputs are **lists of the same length**. We can,
of course, recombine this with the geospatial processing above: ``split_keys=["project_points", ("a", "b")]``.
This configuration tells GAPs to split processing across the ``project_points`` input *for each combination
listed above*.

Preprocessors
*************

In the section above, we noted several times that the split key inputs must be lists (sometimes of the
same length as other inputs). GAPs will not perform this verification for you, so the onus is you to
verify the inputs provided by their users. However, you cannot perform this check in your run function,
since GAPs requires that the input be a list *before* the values are passed to your function (your function
never sees the list input anyways). Instead, GAPs allows you to to specify "pre processing functions", which
allow you to read and modify the user inputs before GAPs performs the parallelization to nodes. Here is an
example of such a function:

.. code-block::
    python

    # model.py

    ...

    def model_preprocessor(config):
        """Preprocess user input."""
        if not isinstance(config["a"], list):
            config["a"] = [config["a"]]

        if not isinstance(config["b"], list):
            config["b"] = config["a"]

        if len(config["a"]) != len(config["b"]):
            raise ValueError("Inputs 'a' and 'b' must be of the same length!")

        return config


We request yet another GAPs-provided input in this function: ``config``. This will be the dictionary
representation of the user's input configuration file. We are free to modify this file at will before
returning the dictionary and allowing GAps to continue processing. Note that we can raise errors at
this point, which is useful since the user's execution will be terminated before any nodes are requested
from the HPC. Therefore, it is often good practice to perform minor and.or critical data validation at
this stage.

To tell GAPs that we want to use this function as the pre-processing for our model execution, we specify it
in the command configuration like so:

.. code-block::
    python

    # cli.py
    from model import run, model_preprocessor
    from gaps.cli import CLICommandFromFunction, make_cli


    commands = [
        CLICommandFromFunction(
            function=run,
            name="runner",
            add_collect=True,
            split_keys=[("a", "b")],
            config_preprocessor=model_preprocessor
        )
    ]

    cli = make_cli(commands)


    if __name__ == "__main__":
        cli(obj={})


Hidden parameters
*****************

Suppose you wanted to use the ``split_keys=[("a", "b")]`` configuration, but wanted the user to provide these
two inputs from a separate CSV file. In the section above, we learned that we can use a preprocessing function
to do this:

.. code-block::
    python

    # model.py
    import pandas as pd

    ...

    def model_preprocessor(config):
        """Preprocess user input - not final version."""
        df = pd.read_csv(config["param_csv_fp"])
        config["a"] = list(df["a"])
        config["b"] = list(df["b"])

        return config


While this would technically work, there are a couple of problems with this approach. First, your users
would not have any information about the required ``"param_csv_fp"`` input. Since it is not used as a
function input anywhere, it will not show up in their template configs nor anywhere is the documentation.
On the flip side, the parameters ``a`` and ``b`` *would* show up in the template configs and documentation,
yet they would have no impact on execution, since the pre-processing function always overwrites these inputs
before they are used. Therefore, we need to find a way to expose the ``"param_csv_fp"`` as a model input and
"hide" the ``a`` and ``b`` inputs from the user.

GAPs provides solutions to both of these problems. To expose the ``"param_csv_fp"`` input, simply *include it
as a function parameter in your pre-processing function*. GAPs will detect this as a required input and
request it from the user. Then, to hide the ``a`` and ``b`` parameters, we can specify them as
``skip_doc_params`` in the ``CLICommandFromFunction`` initialization:


.. code-block::
    python

    # model.py
    import pandas as pd

    ...

    def model_preprocessor(config, param_csv_fp):
        """Preprocess user input. """
        df = pd.read_csv(param_csv_fp)
        config["a"] = list(df["a"])
        config["b"] = list(df["b"])

        return config


    # cli.py
    from model import run, model_preprocessor
    from gaps.cli import CLICommandFromFunction, make_cli


    commands = [
        CLICommandFromFunction(
            function=run,
            name="runner",
            add_collect=True,
            split_keys=["project_points", ("a", "b")],
            config_preprocessor=model_preprocessor,
            skip_doc_params=["a", "b"]
        )
    ]

    cli = make_cli(commands)


    if __name__ == "__main__":
        cli(obj={})


This configuration gives us the desired behavior.


Multiple commands
*****************

So far, we have seen how to set up a function to be executed on multiple HPC nodes. As your model grows, it is
likely that more functions will be written that require HPC scaling. Sometimes, these functions will not
require geospatial scaling at all, and therefore never include ``project_points`` at all. No worries, GAPs can
still support that. Let's suppose you write another function to execute on the HPC:

.. code-block::
    python

    # model.py

    ...

    def another_model(x, y, z):
        """Execute another model"""
        ...


To add this function to your CLI, simply set up another configuration as before and add it to the commands
list:

.. code-block::
    python

    # cli.py
    from model import run, model_preprocessor, another_model
    from gaps.cli import CLICommandFromFunction, make_cli


    commands = [
        CLICommandFromFunction(
            function=run,
            name="runner",
            add_collect=True,
            split_keys=["project_points", ("a", "b")],
            config_preprocessor=model_preprocessor,
            skip_doc_params=["a", "b"]
        ),
        CLICommandFromFunction(
            function=another_model,
            name="analysis",
            add_collect=False,
            split_keys=["x"]
        ),
    ]

    cli = make_cli(commands)


    if __name__ == "__main__":
        cli(obj={})


Now, if you run your cli files, you can see the new function was added as another command:

.. code-block::
    shell

    $ python cli.py
    Usage: cli.py [OPTIONS] COMMAND [ARGS]...

    Command Line Interface

    Options:
        -v, --verbose  Flag to turn on debug logging. Default is not verbose.
        --help         Show this message and exit.

    Commands:
        analysis          Execute the `analysis` step from a config file.
        batch             Execute an analysis pipeline over a parametric set of...
        collect-runner    Execute the `collect-runner` step from a config file.
        pipeline          Execute multiple steps in an analysis pipeline.
        reset-status      Reset the pipeline/job status (progress) for a given...
        runner            Execute the `runner` step from a config file.
        script            Execute the `script` step from a config file.
        status            Display the status of a project FOLDER.
        template-configs  Generate template config files for requested COMMANDS.


Sometimes, your model logic is contained within an object that has some sort of run method. Instead of
having to write a new function to initialize that object and call the run method, GAPs allows you to
create a Command configuration directly from a class:

.. code-block::
    python

    # model.py

    ...

    class MyFinalModel:
        """Execute the last model"""

        def __init__(self, m, n):
            self.m = m
            self.n = n
            ...

        def execute(self, o, p):
            """Execute the model"""
            ...
            return f"out_path{self.m}_{self.n}_{o}_{p}.out"


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
            skip_doc_params=["a", "b"]
        ),
        CLICommandFromFunction(
            function=another_model,
            name="analysis",
            add_collect=False,
            split_keys=["x"]
        ),
        CLICommandFromClass(
            MyFinalModel,
            method="execute",
            name="finalize",
            add_collect=False,
            split_keys=["m", "o"]
        ),
    ]

    cli = make_cli(commands)


    if __name__ == "__main__":
        cli(obj={})


This will add a "finalize" command that requests the parameters ``m``, ``n``, ``o``, and ``p`` from
users, splits the execution across all permutations of ``m`` and ``o``, initializes the ``MyFinalModel``
object with the ``m`` and ``n`` inputs, and calls the ``execute`` object method with the ``o``, and ``p``
user inputs on each node. Nifty!



Integrating GAPs with your python package
-----------------------------------------

As your model matures, you may wish to convert it to a proper python package. This process typically involves
several steps, one of which is creating a ``setup.py`` file. When you do this, you will have the option
to set your GAPs-provided CLI as a package entry point. To do this, first place the ``cli.py`` file somewhere
in your package folder. Let's suppose you place it under ``src/cli.py``. When you call the ``setup`` function
in ``setup.py``, simply include:

.. code-block::
    python

    # setup.py

    ...

    setup(
        ...
        entry_points={
            "console_scripts": ["model=src.cli:cli", ...],
            ...
        }
        ...
    )


When users install you package using ``pip install``, they will get ``model`` set as an entry point to your
CLI. Then, they can execute your commands like:

.. code-block::
    shell

    $ model --help
    Usage: model [OPTIONS] COMMAND [ARGS]...

    Command Line Interface

    Options:
        -v, --verbose  Flag to turn on debug logging. Default is not verbose.
        --help         Show this message and exit.

    Commands:
        analysis          Execute the `analysis` step from a config file.
        batch             Execute an analysis pipeline over a parametric set of...
        collect-runner    Execute the `collect-runner` step from a config file.
        finalize          Execute the `finalize` step from a config file.
        pipeline          Execute multiple steps in an analysis pipeline.
        reset-status      Reset the pipeline/job status (progress) for a given...
        runner            Execute the `runner` step from a config file.
        script            Execute the `script` step from a config file.
        status            Display the status of a project FOLDER.
        template-configs  Generate template config files for requested COMMANDS.

    $ model runner -c config_runner.json
    ...


For a real-world example of this, check out the `reV setup.py file <https://github.com/NREL/reV/blob/main/setup.py>`_.

Another important part of finalizing your package is creating documentation for your users. Luckily, GAPs
greatly simplifies this process for your CLI. All you need to do is document all of your model parameters
in the run function (e.g. ``run_model`` above) using the `Numpy Docstring format <https://numpydoc.readthedocs.io/en/latest/format.html>`_.
GAPs will collect your documentation and use it for the ``--help`` invocation for each command.

If you are using Sphinx to generate your documentation, you can use `sphinx-click <https://sphinx-click.readthedocs.io/en/latest/>`_
to render the CLI documentation for you into a nice format. For an example on how to do this, see the reV docs
`setup <https://github.com/NREL/reV/tree/main/docs>`_ and `final result <https://nrel.github.io/reV/_cli/cli.html>`_.
