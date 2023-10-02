How to run a model powered by GAPs
==================================

If you are an analyst interested in executing a GAPs-powered model, you are in the
right place. This guide will introduce the basic concepts of GAPs models and show you
how to set up your first model run. Note, however, that you will have to consult your
model's documentation for details on model-specific inputs.

The General Idea
----------------
Every GAPs model is a collection of one or more processing steps that can be executed
sequentially as part of a *pipeline*. When you go to set up a model execution, you will
define your pipeline - which model steps you would like to execute and in what order.
Once your pipeline is defined, you will fill in configuration files for each step,
and then you can kick off the pipeline run. You will have full control over the
execution parameters (i.e. nodes to run, walltime of each node, number of workers per
node if your model supports that, etc) for each step, and you will be able to monitor
the pipeline progress as it runs.

One important thing to keep in mind is that the directory where you set up your model
configs is exclusive to that pipeline execution - no other pipelines are allowed in that
directory. To set up a new/separate pipeline, you will have to create a new directory
with it's own set of configuration files.


Getting Started
***************

For the examples shown here, we will use the `reV <https://github.com/NREL/reV>`_ model
CLI in particular, but the general concepts that will be presented can be applied to any
GAPs-powered model. If you would like to follow along with these examples using your
own model, simply replace ``reV`` with your model CLI name in the command line calls.

Let's begin by examining the commands available for us to run:


.. code-block::
    shell

    reV --help
    Usage: reV [OPTIONS] COMMAND [ARGS]...

    reV Command Line Interface.

    ...

    The general structure of the reV CLI is given below.

    Options:
        -v, --verbose  Flag to turn on debug logging. Default is not verbose.
        --version      Show the version and exit.
        --help         Show this message and exit.

    Commands:
        batch                     Execute an analysis pipeline over a...
        bespoke                   Execute the `bespoke` step from a config file.
        collect                   Execute the `collect` step from a config file.
        econ                      Execute the `econ` step from a config file.
        generation                Execute the `generation` step from a config...
        hybrids                   Execute the `hybrids` step from a config file.
        multi-year                Execute the `multi-year` step from a config...
        nrwal                     Execute the `nrwal` step from a config file.
        pipeline                  Execute multiple steps in an analysis pipeline.
        project-points            reV ProjectPoints generator
        qa-qc                     Execute the `qa-qc` step from a config file.
        qa-qc-extra               Execute extra QA/QC utility
        rep-profiles              Execute the `rep-profiles` step from a config...
        reset-status              Reset the pipeline/job status (progress) for...
        script                    Execute the `script` step from a config file.
        status                    Display the status of a project FOLDER.
        supply-curve              Execute the `supply-curve` step from a config...
        supply-curve-aggregation  Execute the `supply-curve-aggregation` step...
        template-configs          Generate template config files for requested...


The ``reV`` CLI gives a longer message with some useful tips on how to get started, but
for the purposes of this example, we will focus on the commands that are available to us.
In particular, we can separate the commands into two categories. The first are
``reV``-specific commands:

.. code-block::
    shell

    Commands:
        bespoke                   Execute the `bespoke` step from a config file.
        collect                   Execute the `collect` step from a config file.
        econ                      Execute the `econ` step from a config file.
        generation                Execute the `generation` step from a config...
        hybrids                   Execute the `hybrids` step from a config file.
        multi-year                Execute the `multi-year` step from a config...
        nrwal                     Execute the `nrwal` step from a config file.
        project-points            reV ProjectPoints generator
        qa-qc                     Execute the `qa-qc` step from a config file.
        qa-qc-extra               Execute extra QA/QC utility
        rep-profiles              Execute the `rep-profiles` step from a config...
        supply-curve              Execute the `supply-curve` step from a config...
        supply-curve-aggregation  Execute the `supply-curve-aggregation` step...

These are model steps that we can configure into a pipeline. The rest of the commands are
ones that come with every GAPs model:

.. code-block::
    shell

    Commands:
        batch                     Execute an analysis pipeline over a...
        pipeline                  Execute multiple steps in an analysis pipeline.
        reset-status              Reset the pipeline/job status (progress) for...
        script                    Execute the `script` step from a config file.
        status                    Display the status of a project FOLDER.
        template-configs          Generate template config files for requested...

A good place to start when setting up a pipeline is with the ``template-configs`` command.
This command will generate a set of template configuration files for you, including all
required and optional model input parameters for every step. Let's create a new directory
and try it out:

.. code-block::
    shell

    $ mkdir my_model_run
    $ cd my_model_run/
    $ reV template-configs

By default the ``template-configs`` command generates JSON template files, but you can
select a different config file type by using the ``-t`` flag (see
``reV template-configs --help`` for all available options). If we now list out the
directory contents, we will see template config files generated for all reV steps
listed above:

.. code-block::
    shell

    $ ls
    config_bespoke.json                   config_econ.json        config_hybrids.json
    config_nrwal.json                     config_qa_qc.json       config_script.json
    config_supply_curve_aggregation.json  config_collect.json     config_generation.json
    config_multi_year.json                config_pipeline.json    config_rep_profiles.json
    config_supply_curve.json

For this example, we will only execute the ``generation``, ``collect``, and ``multi-year``
steps. We will delete the config files for all other steps, and are left with:

.. code-block::
    shell

    $ ls
    config_collect.json  config_generation.json  config_multi_year.json  config_pipeline.json

Note that we left the ``config_pipeline.json`` file. This is where we will define the model steps
we wish to execute and in what order. If we read this file, we will see it was pre-populated
with all possible pipeline steps:

.. code-block::
    shell

    $ cat config_pipeline.json
    {
        "pipeline": [
            {
                "bespoke": "./config_bespoke.json"
            },
            {
                "generation": "./config_generation.json"
            },
            {
                "econ": "./config_econ.json"
            },
            {
                "collect": "./config_collect.json"
            },
            {
                "multi-year": "./config_multi_year.json"
            },
            {
                "supply-curve-aggregation": "./config_supply_curve_aggregation.json"
            },
            {
                "supply-curve": "./config_supply_curve.json"
            },
            {
                "rep-profiles": "./config_rep_profiles.json"
            },
            {
                "hybrids": "./config_hybrids.json"
            },
            {
                "nrwal": "./config_nrwal.json"
            },
            {
                "qa-qc": "./config_qa_qc.json"
            },
            {
                "script": "./config_script.json"
            }
        ],
        "logging": {
            "log_file": null,
            "log_level": "INFO"
        }
    }

Let's remove all steps except ``generation``, ``collect``, and ``multi-year``, which we
will run in that order. Our pipeline file shoudl now look like this:

.. code-block::
    shell

    $ cat config_pipeline.json
    {
        "pipeline": [
            {
                "generation": "./config_generation.json"
            },
            {
                "collect": "./config_collect.json"
            },
            {
                "multi-year": "./config_multi_year.json"
            }
        ],
        "logging": {
            "log_file": null,
            "log_level": "INFO"
        }
    }

Note that the "pipeline" key is required, and it must be a list of dictionaries. The
order of the list is important, as it defined the sequence of your pipeline. The key of
each dictionary within this list is the name of the model step you want to run, and the
value is the path to the config file for that command. The paths can be relative to the
"project directory" (i.e. the directory containing the pipeline config file).

Now that our pipeline is set up, we need to populate the config files for each step. If
we take a look at the generation config, we can see that a lot of the inputs have default
values that have been filled out for us:

.. code-block::
    shell

    $ cat config_generation.json
    {
        "execution_control": {
            "option": "local",
            "allocation": "[REQUIRED IF ON HPC]",
            "walltime": "[REQUIRED IF ON HPC]",
            "qos": "normal",
            "memory": null,
            "nodes": 1,
            "queue": null,
            "feature": null,
            "conda_env": null,
            "module": null,
            "sh_script": null,
            "max_workers": 1,
            "sites_per_worker": null,
            "memory_utilization_limit": 0.4,
            "timeout": 1800,
            "pool_size": 16
        },
        "log_directory": "./logs",
        "log_level": "INFO",
        "technology": "[REQUIRED]",
        "project_points": "[REQUIRED]",
        "sam_files": "[REQUIRED]",
        "resource_file": "[REQUIRED]",
        "low_res_resource_file": null,
        "output_request": [
            "cf_mean"
        ],
        "site_data": null,
        "curtailment": null,
        "gid_map": null,
        "drop_leap": false,
        "scale_outputs": true,
        "write_mapped_gids": false,
        "bias_correct": null,
        "analysis_years": null
    }


The first important import we notice is the ``"execution_control"`` block. This block
is common to every GAPs-powered pipeline step, and it allows you to control how you would
like to execute this step on the HPC. For a detailed description of each of the execution
control options, see ``reV generation --help`` (or the help on any pipeline step in your
model). Here, we will focus on only the important few inputs.

First, we will switch the ``"option"`` to "kestrel". This will allow us to execute the
pipeline on NREl's Kestrel HPC instead of on our local machine (though if you **do** want
to run a pipeline step locally, just leave the ``"option"`` set to "local" and remove
all inputs up to "max_workers"). We will also set the allocation and the walltime (as an
integer in hours). If your model supports it, you can also specify ``"max_workers"``, which
controls how many cores are used for execution on each node. Typically it is good to set
this input to ``null``, which will use all available cores. Finally, we can set the
``"nodes"`` input to however many nodes we want to split our execution across. This input is
included in this execution control because ``"project_points"`` is a required input key for
this step.

The ``"project_points"`` is a GAPs-specific key that allows you to define the geospatial
locations at which you want to execute the model. Typically you would provide this input
as a CSV file, where each row is a location:

.. code-block::
    shell

    $ cat points.csv
    gid,lat,lon
    0,5,10
    1,6,11
    2,7,12
    3,8,13


Note that a "gid" column ins required as part of this input (typically, this will correspond
to the GID of the resource data at that point). You can also include other columns in this CSV,
but they will be ignores unless your model explicitly allows you to pass through site-specific
inputs via the project points (check your model documentation). The ``"nodes"`` input in the
execution control block then dictates how many HPC nodes these points will be split across to
execute the model. For example, if we selected ``"nodes": 1``, then all four points above would
be executed on a single node. Alternatively, if we specified ``"nodes": 2``, then the first two
points would be run on one HPC node and the last two points would run on another node, and so on.

The rest of the inputs are reV-specific, and we fill them out with the help of the CLI
documentation (``$ reV generation --help``). If we do not wish to change the default values of
parameters in the template configs, we can remove them completely (or leave them in if you would
like to be explicit). This is what a "bare minimum" ``reV`` generation config might look like:

.. code-block::
    shell

    $ cat config_generation.json
    {
        "execution_control": {
            "option": "kestrel",
            "allocation": "rev",
            "walltime": 4,
            "qos": "normal",
            "nodes": 20,
            "max_workers": 36
        },
        "technology": "pvwattsv8",
        "project_points": "./points.csv",
        "sam_files": "./sam.json",
        "resource_file": "/path/to/NSRDB.h5"
    }

Note that this call will split execution across 20 nodes, where each node will generate data
into it's own HDF5 output file. Therefore, after all jobs are completed, we must collect the
outputs into a single file for further processing/analysis. This is the point of the ``collect``
step that often comes with GAPs-powered model steps that split execution across nodes. Therefore,
we fill out the ``config_collect.json`` file next.

.. code-block::
    shell

    $ cat config_collect.json
    {
        "execution_control": {
            "option": "local",
            "allocation": "[REQUIRED IF ON HPC]",
            "walltime": "[REQUIRED IF ON HPC]",
            "qos": "normal",
            "memory": null,
            "queue": null,
            "feature": null,
            "conda_env": null,
            "module": null,
            "sh_script": null
        },
        "log_directory": "./logs",
        "log_level": "INFO",
        "project_points": null,
        "datasets": null,
        "purge_chunks": false,
        "clobber": true,
        "collect_pattern": "PIPELINE"
    }

We can see a similar ``"execution_control"`` block as before, but this time without a
``"nodes"`` input. This is because collection will be performed on a single node (20 files
will be read and compiled into a single output file). After filling out the ``"allocation"``
and ``"walltime"`` inputs, we can move onto the multi-year step config, where we repeat
this process once more.


Execution
*********
Once all config files are filled out, we can kick off pipeline execution! The standard procedure
for this is to kick off a single pipeline step, monitor it's execution, validate outputs, and
then kick off the next pipeline step. You can do this by submitting each step individually, like so:

.. code-block::
    shell

    $ reV generation -c config_generation.json


After waiting for generation to complete  you can then kick off the next step:

.. code-block::
    shell

    $ reV collect -c config_collect.json


However, an easier way to do this is to use the ``pipeline`` command:

.. code-block::
    shell

    $ reV pipeline -c config_pipeline.json

This command will check the status of the current step, and if it is completed, it will
kick off the next step. Alternatively, if the step has failed, it will re-submit the failed
jobs. After each step, you can call ``$ reV pipeline -c config_pipeline.json`` again and not
have to keep track of which step the pipeline is currently on.

To make it even easier, if you have **exactly one** config file with the word ``"pipeline"``
in the name, then you can simply call

.. code-block::
    shell

    $ reV pipeline

and GAPs will interpret that file to be the pipeline config file.

Finally, if you have several sub-directories set up, each with their own unique pipeline,
you can submit

.. code-block::
    shell

    $ reV pipeline -r

As before, this assumes that you have **exactly one** config file with the word ``"pipeline"``
ion the name per directory. If you have multiple files that match this criteria, the directory
is skipped entirely.

Although we recommend submitting the pipeline one step at a time in order to validate model outputs
between steps, we realize that this is not a desirable workflow in all cases. Therefore, the
``pipeline`` command includes a ``--monitor`` option that will continuously monitor the pipeline
and submit the next step once the current one finishes. Note that this option takes over your
terminal and prints logging messages, so it is best run in a `linux screen <https://www.gnu.org/software/screen/manual/screen.html>`_.

If you would prefer not to set up a screen, you can kick off the monitor in a detached process
using the ``--background`` option for the ``pipeline`` command.

.. warning:: When running ``pipeline --background``, the spawned monitor process is detached,
    so you may safely disconnect from your ssh session without stopping pipeline execution. However,
    if the process is killed in any other way, the pipeline will one finish executing the current step.
    This can happen if you kick off your monitor job on an interactive node, which you then relinquish
    before the pipeline can finish executing. For best results, run the background pipeline from a node
    that outlives the pipeline execution.


Monitoring
**********
Once your pipeline is running, you can check the status using the ``status`` command:

.. code-block::
    shell

    $ reV status

    my_model_run:
                job_status       pipeline_index  job_id    time_submitted    time_start    time_end    total_runtime    hardware    qos
    ----------  -------------  ----------------  --------  ----------------  ------------  ----------  ---------------  ----------  -----
    generation  not submitted                 0  --        --                --            --          --               --          --
    collect     not submitted                 1  --        --                --            --          --               --          --
    multi-year  not submitted                 2  --        --                --            --          --               --          --
    -------------------------------------------------------------------------------------------------------------------------------------
    Total number of jobs: 3
    3  not submitted
    Total node runtime: 0:00:00
    **Statistics only include shown jobs (excluding any previous runs or other steps)**


The status command gives several different options to filter this output based on your needs, so
take a look at ``$ rev status --help`` to customize the outputs you want to see for your pipeline.


Scripts
*******
GAPs also allows analysis to run their own scripts as part of a model analysis pipeline. To do this,
simply generate a script config file:

.. code-block::
    shell

    $ reV template-configs script
    $ cat config_script.json
    {
        "execution_control": {
            "option": "local",
            "allocation": "[REQUIRED IF ON HPC]",
            "walltime": "[REQUIRED IF ON HPC]",
            "qos": "normal",
            "memory": null,
            "queue": null,
            "feature": null,
            "conda_env": null,
            "module": null,
            "sh_script": null
        },
        "log_directory": "./logs",
        "log_level": "INFO",
        "cmd": "[REQUIRED]"
    }

The familiar ``"execution_control"`` block allows the user to customize the HPC options for this
script execution. The script itself can be run using the ``cmd`` input. In particular, this input
should be a string (or list of strings) that represent a command to be executed on the terminal.
Each command will be executed on it's own node. For example, we can modify this config to be

.. code-block::
    shell

    $ cat config_script.json
    {
        "execution_control": {
            "option": "kestrel",
            "allocation": "rev",
            "walltime": 0.5
        },
        "log_directory": "./logs",
        "log_level": "INFO",
        "cmd": ["python my_script.py", "./my_bash_script.sh"]
    }

This config will kick off two script jobs, each on it's own node. The first node will execute
the python script, while the second node will execute the bash script. Note that this execution
may happen in any order, including in parallel, so make sure there are no dependencies between
the different script executions. If you need one script to execute strictly after another, submit
them as separate sequential pipeline steps (see `Duplicate Pipeline Steps`_ for info on submitting
multiple duplicate steps within a single pipeline).

Don't forget to add the script step to your pipeline config:

.. code-block::
    shell

    $ cat config_pipeline.json
    {
        "pipeline": [
            {
                "generation": "./config_generation.json"
            },
            {
                "collect": "./config_collect.json"
            },
            {
                "multi-year": "./config_multi_year.json"
            },
            {
                "script": "./config_script.json"
            }
        ],
        "logging": {
            "log_file": null,
            "log_level": "INFO"
        }
    }


Status-reset
************
Sometimes you may with to partially or completely reset the status of a model pipeline. You can do this
with the ``reset-status`` command:

.. code-block::
    shell

    $ reV reset-status

Note that this will reset the pipeline status back to the beginning, but it **will not delete any of the
model output files**. You will have to remove any model outputs yourself before restarting the pipeline
from scratch.

You can also reset the status of a pipeline to a particular step using:

.. code-block::
    shell

    $ reV reset-status --after-step generation


This will reset the status of all steps after "generation", leaving "generation" itself untouched.N
Note that this still does not remove model outputs, so you will still have to remove those manually.


Duplicate Pipeline Steps
************************
As mentions in the `Scripts`_ section, sometimes you may wish to run the same model steps multiple
times within a single pipeline. You can do this by adding an extra key to the step dictionary
in the pipeline config:

.. code-block::
    shell

    $ cat config_pipeline.json
    {
        "pipeline": [
            {
                "setup": "./config_setup.json",
                "command": "script"
            },
            {
                "generation": "./config_generation.json"
            },
            {
                "collect": "./config_collect.json"
            },
            {
                "multi-year": "./config_multi_year.json"
            },
            {
                "analyze": "./config_analyze.json",
                "command": "script"
            },
            {
                "second_gen": "./config_generation_again.json",
                "command": "generation"
            },
        ],
        "logging": {
            "log_file": null,
            "log_level": "INFO"
        }
    }

The ``"command"`` key should point to the actual model step you wish to execute, while the key
pointing to the config file should be a **unique** name for that pipeline step. Here, we run
the script command twice, first as a ``"setup"`` step, and then as an ``"analyze"`` step.
We also run generation twice, first as a standard ``"generation"`` invocation, and then again at
the end as a ``"second_gen"`` step. Note that ``config_setup.json`` and ``config_analyze.json``
should both be config files for the ``script`` step, while ``config_generation.json`` and
``config_generation_again.json`` should both contain ``reV`` generation parameters.


Batch execution
***************
Future versions of this document may describe batched execution in more detail. In the meantime,
please refer to the
`reVX setbacks batched execution example <https://github.com/NREL/reVX/tree/main/reVX/setbacks#batched-execution>`_,
which is powered by GAPs.
