How to use GAPs
---------------

If you are a model developer looking to leverage GAPs to scale your model to the HPC,
you are in the right place. The following examples start with a basic example and
progressively get more advanced to demonstrate the full power of this framework.

If you would like to dive into some real-world examples instead, feel free to check
out reV, reVX, or PVDegredationTools.

Basic Setup
-----------
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
locations (which will be provided by GAPs) and execute the model above.


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
out the documentation
for https://nrel.github.io/gaps/_autosummary/gaps.cli.command.CLICommandFromFunction.html#gaps.cli.command.CLICommandFromFunction.
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
the ``rex.Outputs`` documentation for more details).

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


