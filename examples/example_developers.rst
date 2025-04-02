How to use GAPs
===============

Intro
-----
If you are a model developer looking to leverage GAPs for scaling your model to the HPC,
you are in the right place. The following examples start with a basic example and
progressively become more advanced to demonstrate the full power of this framework.

If you prefer to explore real-world examples instead, feel free to check
out `reV <https://github.com/NREL/reV>`_, `reVX <https://github.com/NREL/reVX>`_,
or `PVDegradationTools <https://github.com/NREL/PVDegradationTools>`_.

Don't forget to refer to the `API documentation <https://nrel.github.io/gaps/_autosummary/gaps.html>`_
for details regarding inputs to the GAPs function calls described below.

Basic Setup
^^^^^^^^^^^
Let's suppose you have a function designed to execute a model at a particular location:

.. code-block::
    python

    # model.py

    def run_model(lat, lon, a, b, c):
        """Example model that runs computation for a single site."""

        # simple computation for example purposes
        x = lat + lon
        return a * x**2 + b * x + c


To prepare for coupling with GAPs, we need to implement another function that iterates over specific
locations (which will be provided by GAPs) and runs the model function above.


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


Let's break down this function. The first input, ``project_points``, is a parameter
provided by GAPs based on user input. Specifically, the user will provide a CSV file
named ``project_points``, where each row represents a single location. By iterating over
the GAPs ``ProjectPoints`` object, you can access the ``pandas.Series`` representation of
the locations to process, which includes all user input for each location. In this case,
we expect the user to include ``"lat"`` and ``"lon"`` columns, so we access those directly using
``site.lat`` and ``site.lon`` as inputs to our model function.

Additionally, we request the other model parameters, ``a``, ``b``, and ``c``, as function inputs.
This means users will provide values for those parameters, and GAPs will supply them to
our function during the call.

Finally, we "request" the ``tag`` input from GAPs. This is a special input that GAPs can
pass to our function call (you can "request" it by including it in the function signature).
To see a full list of the parameters you can request from GAPs, check out the documentation
for `CLICommandFromFunction <https://nrel.github.io/gaps/_autosummary/gaps.cli.command.CLICommandFromFunction.html>`_.
The ``tag`` value is a unique string that you can append to your output file to make it unique
compared to other nodes running the same function. This way, there is no race condition for
writing data when the user executes the model on multiple HPC nodes in parallel (each node has
its own unique file it can write to at will). Adding a tag like this is also required for
the GAPs-provided collect command to function properly.

Once the data is processed, we use the ``rex.Outputs`` class to write the results to an HDF5 file.
You can write outputs to other data formats as well, but you will have to write your own
collection methods (to collect data from multiple nodes), as HDF5 files are the only ones
GAPs supports out-of-the-box. You could also use ``h5py`` to write to an HDF5 output file, but
we find that ``rex`` makes it simple to save the output to a GAPs-compatible format.

To write the output data, we just need to specify a meta ``DataFrame`` (we can use the user
``project_points`` input) and the output data as a dataset in ``numpy`` array format (we can also
specify a ``time_index`` if our output data has a temporal component; see the ``rex.Outputs``
documentation for more details). Ensure that there is a 1:1 correspondence between meta rows
and output data (i.e., the first meta row corresponds to the data at index 0 of the output
array, the second meta row corresponds to the data at index 1 of the output array, and so on).

Finally, we return the path to the output HDF5 file so that GAPs can record it as our results
output. This is also required for the GAPs-provided collect command to function properly.

Now that we have our run function set up, let's use GAPs to build a Command Line Interface (CLI):

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


To construct our CLI, we start by creating a CLI Command Configuration for our run function.
Specifically, we designate our run function as the one to execute on each node and assign
"runner" as the name of the CLI command associated with this function. Additionally, we request
GAPs to include a "collect" command, as our function generates output data saved to an HDF5 file.
Lastly, we specify that the ``project_points`` input should be utilized to distribute execution
across nodes. This enables users to define how many nodes they want for parallel execution, with GAPs
taking care of distributing project point locations across the designated nodes.

If we wish to incorporate additional commands, we follow a similar process to create a Command
Configuration for each run function. We then consolidate these configurations into a ``commands``
list. This list is subsequently passed to the ``make_cli`` function, generating the entry point
for our brand-new command line interface.

Congratulations! You have successfully built your first GAPs-powered model, ready for scalable
execution on the HPC. Let's take a closer look at everything we get from these few lines of code:

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


In the available CLI commands, we can immediately identify ``"runner"`` as one of the options, along
with ``"collect-runner"``. The latter is responsible for consolidating the one-per-node result files
into a single output file. The remaining commands offer valuable functionality for users of your
command-line interface.

For detailed instructions on executing your model using this CLI, please refer to the
`How to Run a Model Powered by GAPs <https://nrel.github.io/gaps/misc/examples.users.html>`_ example.


Multiprocessing
^^^^^^^^^^^^^^^
In most cases, relying on a single CPU core on an HPC node dedicated to running your model is
inefficient and inconsiderate to other HPC users. The only rare exceptions to this rule involve
processes that demand a very large amount of memory and can only run one at a time to avoid exceeding
memory limits. Therefore, it is crucial to parallelize your model execution when operating on the node
itself. However, GAPs cannot perform this parallelization for you, as it would necessitate extensive
knowledge about your model and potentially impose limitations on the types of computations
you can conduct with GAPs.

Fortunately, the Python standard library offers excellent tools to assist you in parallelizing model
execution on each node. Specifically, we will demonstrate how to make use of all available CPU cores
on a node by modifying the ``run`` function shown above to leverage
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


Let's explore the implementation of parallelized model execution using the ``concurrent.futures`` module.
We'll begin by introducing an additional GAPs-provided input, ``max_workers``. This input enables users
to specify the number of processes to run concurrently on each node. Notably, users can set this input
to ``None``, allowing it to utilize the maximum number of available cores on the node.

Following that, we initialize the output file for the node, simplifying the last code block responsible for
collecting all running futures on this node. Alternatively, we could have initialized an empty ``numpy`` array,
gathered all future outputs there, and directly written the array to the output file, similar to the approach
used in the initial function. These methods are equivalent, so you can choose the one that aligns with your
preferred coding style.

The subsequent code block initializes a ``ProcessPoolExecutor`` with the number of ``max_workers`` as requested by
the user. We then submit executions of the ``run_model`` function for all sites provided in the ``project_points``
input. It's important to note that each submission creates a copy of the inputs for the ``run_model`` function.
As a result, model inputs consuming significant memory may be copied multiple times, depending on the number of
points the user intends to execute on each node. For instance, if the input ``a`` to the model is a 100 MB array,
and the user submits 1000 points for execution on the node, this submission process generates 1000 copies of
the input array, necessitating at least 100 GB of RAM for processing. Therefore, it's advisable to minimize
the memory footprint of your model inputs as much as possible, such as by loading the data within the ``run_model``
function itself whenever feasible. For alternative strategies to address this issue, consider exploring the chunking
approach employed by `reVX exclusions calculators <https://github.com/NREL/reVX/blob/2dd05402c9c05ca0bf7f0e5bc2849ede0d0bc3cb/reVX/utilities/exclusions.py#L323-L367>`_.

When submitting the futures, we store them in a dictionary for later collection using the ``as_completed`` function.
This approach enables us to retain some metadata alongside each future object. Specifically, we store the site GID
(please note that GAPs requires users to specify a ``gid`` column in their project points CSV, which is typical for
models relying on WTK/NSRDB/Sup3rCC data) corresponding to each future, allowing us to place the data in the appropriate
location in the output array. We obtain the index into the output array using the
`ProjectPoints.index <https://nrel.github.io/gaps/_autosummary/gaps.project_points.ProjectPoints.html#gaps.project_points.ProjectPoints.index>`_
function and immediately store the result in the output HDF5 file.

Upon completing all processing, we return the path to the output file as usual. With just a few additional lines of code,
our model execution is effectively parallelized on each node!

Advanced Topics
---------------
Logging
^^^^^^^
GAPs automatically initializes a logger instance for the module containing the ``run`` function based on the user configuration
inputs. This means that setting up logging for your code is extremely simple:

.. code-block::
    python

    # model.py
    import logging
    ...

    logger = logging.getLogger(__name__)


    def run(project_points, a, b, c, tag, max_workers=None):
        """Run model on a single node with multiprocessing."""

        logger.info("Running with inputs a=%s b=%s c=%s", a, b, c)

        ...


Initializing logging for your `Pre-processors`_ takes extra care (see section for details).

Split Keys
^^^^^^^^^^
In the example above, we utilized the ``split_keys=["project_points"]`` parameter in the ``make_cli`` call to inform GAPs
that our node-level function operates in a geospatial context and should be split based on input sites. However, there
are scenarios where we may want to distribute execution across nodes using other or additional model inputs. GAPs offers
support for this type of configuration, with the sole requirement being that the keys provided within ``split_keys`` are
specified as lists.

For instance, consider a situation where we wish to enable users to specify multiple values for the ``a`` model input.
To achieve this, we can call ``make_cli`` with the argument ``split_keys=["a"]``. Subsequently, if a user specifies
``a=[1, 2, 3]`` in their configuration file, GAPs will dispatch the execution of our model to three nodes.
Each node will process one of the three ``a`` values from the provided list. Notably, the ``run_model`` function will
not receive the entire ``[1, 2, 3]`` list as the input for ``a``. Instead, it will receive a single integer value,
which should be executed for that specific model run.

In the above example, execution is no longer split across points but solely across the ``a`` input values. This approach
may not align with our original goal of scaling geospatial execution across HPC nodes. To address this, we can specify both
``project_points`` and ``a`` as split inputs using ``split_keys=["project_points", "a"]``. This signifies that
*for each value of `a` specified by the user*, GAPs will distribute model execution across the input sites on multiple nodes.
The exact configuration will be determined by the user's ``nodes`` input within the ``execution_control`` block of the run
configuration.

Notably, GAPs allows you to specify as many keys as needed within the ``split_keys`` list. GAPs will then generate parameter
permutations of these inputs before submitting them to the HPC nodes. For instance, assume we specify ``split_keys=["a", "b"]``,
and the user provides ``a=[1, 2, 3], b=[4, 5]`` in their configuration file. In this scenario, GAPs will delegate processing
to a total of six HPC nodes. Each node will receive one of the following input combinations:

    - a=1, b=4
    - a=2, b=4
    - a=3, b=4
    - a=1, b=5
    - a=2, b=5
    - a=3, b=5

However, there are situations where generating permutations of inputs may not be appropriate, especially if you intend to
execute specific combinations of input parameters (e.g., turbine rotor diameter and hub height) instead of all possible
permutations. In such cases, you can specify inputs as *combined* split keys by using the format: ``split_keys=[("a", "b")]``.
This signifies that the keys ``a`` and ``b`` will be jointly processed before being dispatched to nodes for execution.
For instance, if a user specifies ``a=[1, 2, 3]`` and ``b=[4, 5, 6]`` in their configuration file, GAPs will distribute
the processing to a total of three HPC nodes. Each node will receive one of the following input combinations:

    - a=1, b=4
    - a=2, b=5
    - a=3, b=6

Please note that this approach assumes that both the ``a`` and ``b`` inputs are **lists of the same length**. You can also
combine this with geospatial processing as follows: ``split_keys=["project_points", ("a", "b")]``. This configuration
instructs GAPs to divide the processing based on the ``project_points`` input *for each combination listed above*.


Pre-processors
^^^^^^^^^^^^^^
In the section above, we emphasized that the split key inputs must be lists, sometimes of the
same length as other split key inputs. GAPs does not perform this verification for you, so the
responsibility lies with you to validate the inputs provided by your users. However, you cannot
perform this check in your run function since GAPs requires that the input be a list *before* the
values are passed to your function (your function never sees the list input anyway). Instead,
GAPs allows you to specify "pre-processing functions", which enable you to inspect and modify user
inputs before GAPs proceeds with parallelization to nodes. Here is an example of such a function:

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


In this function, we request yet another GAPs-provided input: ``config``, which will be the dictionary
representation of the user's input configuration file. We have the freedom to modify this dictionary as
needed before returning it and allowing GAPs to proceed with processing. It's worth noting that we can
also raise errors at this stage. This is beneficial because the user's execution will be terminated
before any nodes are requested from the HPC. Therefore, it's a good practice to perform both minor and
critical data validation at this stage.

Note that GAPs has not yet had a chance to initialize a logger for your CLI, so you must configure a
logger manually if you would like to display/capture the log outputs during job kickoff. GAPs does provide
some useful parameters to assist you with logger initialization: ``job_name``, ``log_directory``,
and ``verbose``. See the documentation for
`CLICommandFromFunction <https://nrel.github.io/gaps/_autosummary/gaps.cli.command.CLICommandFromFunction.html>`_.
for more details on these parameters. Here is what a typical pre-processing functions with logging setup
might look like:

.. code-block::
    python

    # model.py

    ...

    def model_preprocessor(config, job_name, log_directory, verbose):
        """Preprocess user input."""
        # Custom logger initialization logic
        my_custom_logger_init_func(job_name, log_directory, verbose)

        logger.info("Running pre-processor!")

        ...


Once your pre-processing function is ready, you can inform GAPs to use it prior to model execution like so:

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


Hidden Parameters
^^^^^^^^^^^^^^^^^

Suppose you wish to utilize the ``split_keys=[("a", "b")]`` configuration, but you want the user to provide
these two inputs from a CSV file. As discussed above, you can use a pre-processing function to achieve this:

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


While this approach would technically work, it presents a couple of issues. First, your users wouldn't have
information about the required input ``param_csv_fp`` since it's not used as a function input anywhere.
As a result, it wouldn't appear in their template configurations or in the model documentation. On the other
hand, the parameters ``a`` and ``b`` would appear in template configurations and documentation, but they wouldn't
affect execution: the pre-processing function overwrites these inputs before they are used. Therefore, we need a
way to expose ``param_csv_fp`` as a model input while hiding the ``a`` and ``b`` inputs from the user.

GAPs offers solutions to both of these problems. To expose ``param_csv_fp``, include it as a function parameter
in your pre-processing function. GAPs will recognize it as a required input and prompt the user for it.
To hide the ``a`` and ``b`` parameters, specify them as ``skip_doc_params`` in the ``CLICommandFromFunction``
initialization:


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


Multiple Commands
^^^^^^^^^^^^^^^^^
Up to this point, we've covered setting up a single function for execution on multiple HPC nodes. As your model
expands, it's likely that additional functions will be added that require HPC scaling. Some of these may not
require geospatial scaling at all and therefore exclude the ``project_points`` parameter completely.
GAPs can still accommodate such scenarios! Let's assume you've developed another function for HPC execution:


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


Sometimes, your model logic may live within an object that with a "run method". Rather than
writing a new function to instantiate that object and invoke the method, GAPs allows you to
generate a command configuration directly from a class:

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


This will introduce a "finalize" command, which solicits user-defined parameters ``m``, ``n``, ``o``, and ``p``.
It then distributes the execution across all permutations of ``m`` and ``o``, instantiates the ``MyFinalModel``
object with the provided ``m`` and ``n`` inputs, and invokes the ``execute`` method with user inputs ``o`` and ``p``
on each node. Nifty!


Integrating GAPs with your python package
-----------------------------------------
As your model matures, you may want to transform it into a proper Python package. This process typically involves
several steps, including creating a ``setup.py`` file. During this process, you'll have the opportunity
to designate your GAPs-provided CLI as a package entry point. To achieve this, move the ``cli.py`` file
somewhere within your package directory. For example, you could place it under ``src/cli.py``. Then,
when you call the ``setup`` function within your ``setup.py`` file, simply include:

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


When users install your package using ``pip install``, they will have ``model`` added as an entry
point to your CLI. Consequently, they can execute your commands like this:

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

Another essential aspect of finalizing your package is creating documentation for your users. Fortunately, GAPs
simplifies this process for your CLI significantly. All you need to do is document all of your model parameters
in the run function (e.g., ``run_model`` above) using the `Numpy Docstring format <https://numpydoc.readthedocs.io/en/latest/format.html>`_.
GAPs will collect your documentation and use it for the ``--help`` invocation for each command.

If you are using Sphinx to generate your documentation, you can utilize `sphinx-click <https://sphinx-click.readthedocs.io/en/latest/>`_
to render the CLI documentation for you in a visually appealing format. For an example of how to do this, refer to the reV documentation
`setup <https://github.com/NREL/reV/tree/main/docs>`_ and `final result <https://nrel.github.io/reV/_cli/cli.html>`_.


Questions?
----------
If you run into any issues or questions while coupling GAPs with your model, please reach out to
Paul Pinchuk (ppinchuk@nrel.gov).
