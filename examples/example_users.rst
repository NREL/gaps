How to Run a Model Powered by GAPs
==================================

If you are an analyst interested in executing a GAPs-powered model, you are in the
right place. This guide will introduce the basic concepts of GAPs models and demonstrate
how to set up your first model run. Please note, however, that you will need to consult
your model's documentation for details on model-specific inputs.

The General Idea
----------------
Every GAPs model is a collection of one or more processing steps that can be executed
sequentially as part of a *pipeline*. When you set up a model execution, you first
define your pipeline - specifying which model steps you would like to execute and
in what order. Once your pipeline is defined, you fill in configuration files for
each step and then kick off the pipeline run. You have complete control over the
execution parameters (i.e., nodes to run, walltime of each node, the number of
workers per node if your model supports that, etc.) for each step, and you can
monitor the pipeline's progress as it runs.

One important point to keep in mind is that the directory where you set up your model
configs is exclusive to that pipeline execution - no other pipelines are allowed in
that directory. To set up a new or separate pipeline, you will have to create a new
directory with its own set of configuration files.


The Basics
----------
For the examples shown here, we will use the `reV <https://github.com/NREL/reV>`_ model
CLI in particular, but the general concepts that will be presented can be applied to any
GAPs-powered model. If you wish to follow along with these examples using your
own model, simply replace ``reV`` with your model's CLI name in the command line calls.


Getting Started
***************
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


The ``reV`` CLI prints a more extensive message with tips on how to set up a pipeline, but
for the sake of this example, we will concentrate on the available commands. Specifically,
we can categorize the commands into two groups. The first consists of ``reV``-specific commands:

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

These are model steps that we can configure within a pipeline. The remaining commands are
those that come standard with every GAPs model:

.. code-block::
    shell

    Commands:
        batch                     Execute an analysis pipeline over a...
        pipeline                  Execute multiple steps in an analysis pipeline.
        reset-status              Reset the pipeline/job status (progress) for...
        script                    Execute the `script` step from a config file.
        status                    Display the status of a project FOLDER.
        template-configs          Generate template config files for requested...

A good starting point for setting up a pipeline is the ``template-configs`` command.
This command generates a set of template configuration files, including all
required and optional model input parameters for each step. Let's create a new directory
and run this command:

.. code-block::
    shell

    $ mkdir my_model_run
    $ cd my_model_run/
    $ reV template-configs

By default, the ``template-configs`` command generates JSON template files, but you have the option to
choose a different configuration file type by using the ``-t`` flag (refer to
``reV template-configs --help`` for all available options). If we now list the
contents of the directory, we will find template configuration files generated for all reV steps
mentioned above:

.. code-block::
    shell

    $ ls
    config_bespoke.json                   config_econ.json        config_hybrids.json
    config_nrwal.json                     config_qa_qc.json       config_script.json
    config_supply_curve_aggregation.json  config_collect.json     config_generation.json
    config_multi_year.json                config_pipeline.json    config_rep_profiles.json
    config_supply_curve.json

In this example, we will only execute the ``generation``, ``collect``, and ``multi-year``
steps. We will remove the configuration files for all other steps, leaving us with:

.. code-block::
    shell

    $ ls
    config_collect.json  config_generation.json  config_multi_year.json  config_pipeline.json

Note that we saved the ``config_pipeline.json`` file. This file is where we will specify the model steps
we want to execute and their execution order. If we examine this file, we see that it has been pre-populated
with all available pipeline steps:

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
will run in that order. Our pipeline file should now look like this:

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

Note that the ``pipeline`` key is mandatory, and it must point to a list of dictionaries. The
order of the list is significant as it defines the sequence of your pipeline. The key within each
dictionary in this list is the name of the model step you want to execute, and the
value is the path to the configuration file for that command. The paths can be specified relative to the
"project directory" (i.e., the directory containing the pipeline configuration file).

Now that our pipeline is defined, we need to populate the configuration files for each step. If
we examine the generation configuration file, we see that many of the inputs already have default
values pre-filled for us:

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


The first important section we see is the ``execution_control`` block. This block
is a common feature in every GAPs-powered pipeline step, and it allows you to define how you want
to execute this step on the HPC. For a detailed description of each execution
control option, please refer to ``reV generation --help`` (or the help section of any pipeline step
in your model). Here, we will focus on only the essential inputs.

First, let's change the ``option`` to ``"kestrel"``. This will enable us to run the
pipeline on NREL's Kestrel HPC instead of our local machine (although if you **do** want
to execute a pipeline step locally, simply leave the ``option`` set to "local" and remove
all inputs up to ``max_workers``). We will also configure the allocation and the walltime (specified as
an integer or float in hours). If your model supports it, you can also define ``max_workers``, which
controls the number of cores used for execution on each node. Typically, it is a good practice to set
this input to ``null`` - this will utilize all available cores on the node. Finally, we can specify the
``nodes`` input to determine how many nodes we want to distribute our execution across. This input is
included in this execution control because ``project_points`` is a required input key for
this step.

The ``project_points`` is a GAPs-specific key that allows you to specify the geospatial
locations where you want to execute the model. Typically, you would provide this input
as a CSV file, with each row representing a location:

.. code-block::
    shell

    $ cat points.csv
    gid,lat,lon
    0,5,10
    1,6,11
    2,7,12
    3,8,13


Note that a ``"gid"`` column is required as part of this input (typically, this corresponds
to the GID of the resource data at that point). You can also include other columns in this CSV,
but they will be ignored unless your model explicitly allows you to pass through site-specific
inputs via the project points (refer to your model documentation). The ``nodes`` input in the
execution control block then determines how many HPC nodes these points will be distributed across to
execute the model. For instance, if we select ``nodes: 1``, then all four points mentioned above would
be executed on a single node. Conversely, if we specify ``nodes: 2``, then the first two
points would run on one HPC node, and the last two points would run on another node, and so on.

The remaining inputs are reV-specific, and we fill them out with the assistance of the CLI
documentation (``$ reV generation --help``). If we do not wish to modify the default values of
parameters in the template configs, we can remove them entirely (we can also leave them in to be explicit).
This is an example of what a "bare minimum" ``reV`` generation config might look like:

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

This command will distribute the execution across 20 nodes, with each node generating data
into its own HDF5 output file. Consequently, after all jobs are finished, we need to gather the
outputs into a single file for further processing and analysis. This is the purpose of the ``collect``
step, which is commonly included with GAPs-powered model steps that distribute execution across nodes.
Therefore, we need to fill out the ``config_collect.json`` file:

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

We see a similar ``execution_control`` block as before, but this time without a ``nodes`` input.
This is because collection will be conducted on a single node (where 20 files
will be read and consolidated into a single output file). After filling out the ``allocation``
and ``walltime`` inputs, we can proceed to configure the multi-year step, repeating this process
once more.


Execution
*********
Once all configuration files are set up, we can initiate pipeline execution! The typical process
for this involves starting one pipeline step, monitoring its execution, validating outputs, and
then initiating the next pipeline step. You can achieve this by submitting each step individually,
as follows:

.. code-block::
    shell

    $ reV generation -c config_generation.json


After waiting for generation to complete you can then kick off the next step:

.. code-block::
    shell

    $ reV collect -c config_collect.json


However, an easier way to execute this process is to use the ``pipeline`` command:

.. code-block::
    shell

    $ reV pipeline -c config_pipeline.json

This command will check the status of the current step, and if it is completed, it will
trigger the next step. Alternatively, if the step has failed, it will re-submit the failed
jobs. After each step, you can once again run ``$ reV pipeline -c config_pipeline.json`` without
having to keep track of the current step in the pipeline.

To make it even more convenient, if you have **exactly one** config file with the word ``"pipeline"``
in the name, you can simply call

.. code-block::
    shell

    $ reV pipeline

and GAPs will interpret that file to be the pipeline config file.

Finally, if you have several sub-directories set up, each with their own unique pipeline configuration,
you can submit

.. code-block::
    shell

    $ reV pipeline -r

As mentioned earlier, this assumes that you have **exactly one** configuration file with the word
``"pipeline"`` in the filename per directory. If you have multiple files that meet this criteria,
the entire directory will be skipped.

.. NOTE:: While the ``pipeline`` command does support recursive submissions, we recommend using the
    ``batch`` command in these cases because it can manage both the setup and execution of a large number
    of model runs. For more details, refer to `Batched Execution`_.

While we recommend submitting the pipeline one step at a time to validate model outputs
between steps, we understand that this workflow may not be ideal in all cases. Therefore, the
``pipeline`` command includes a ``--monitor`` option that continuously checks the pipeline status
and submits the next step as soon as the current one finishes. Please note that this option takes
control of your terminal and prints logging messages, so it is best to run it within a
`Linux screen <https://www.gnu.org/software/screen/manual/screen.html>`_. Alternatively,
you can send the whole process into the background and then
`disown <https://en.wikipedia.org/wiki/Disown_(Unix)>`_ it or use `nohup <https://en.wikipedia.org/wiki/Nohup>`_
to keep the monitor running after you log off. A ``nohup`` invocation might look something like
this:

.. code-block::
    shell

    $ nohup reV pipeline --monitor > my_model_run.out 2> my_model_run.err < /dev/null &


If you prefer not to deal with background processes and would rather use a more integrated approach,
you can start the monitor as a detached process by using the ``--background`` option of the ``pipeline``
command:

.. code-block::
    shell

    $ reV pipeline --background

This will achieve the same effect as the `nohup` invocation described above, except without
``stdout`` capture.

.. WARNING:: When running ``pipeline --background``, the spawned monitor process is detached,
    so you can safely disconnect from your SSH session without stopping pipeline execution. However,
    if the process is terminated in any other manner, the pipeline will only complete the current step.
    This can occur if you start the monitor job on an interactive node and then disconnect
    before the pipeline finishes executing. For optimal results, run the background pipeline from a node
    that remains active throughout the pipeline execution.


Monitoring
----------
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
take a look at ``$ rev status --help`` to customize the outputs you want displayed.


Scripts
-------
GAPs also enables analysts to execute their own scripts as part of a model analysis pipeline.
To start, simply create a script configuration file:

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

The familiar ``execution_control`` block enables the user to customize the HPC options for this
script execution. The script itself can be executed using the ``cmd`` input. Specifically, this input
should be a string (or a list of strings) that represents a command to be executed in the terminal.
Each command will run on its own node. For instance, we can modify this configuration to be:

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

This configuration will initiate two script jobs, each on its own node. The first node will execute
the Python script, while the second node will execute the Bash script. Please note that this execution
may occur in any order, potentially in parallel. Therefore, ensure that there are no dependencies between
the various script executions. If you require one script to run strictly after another, submit
them as separate sequential pipeline steps (refer to `Duplicate Pipeline Steps`_ for information on
submitting duplicate steps within a single pipeline).

.. IMPORTANT:: It is inefficient to run scripts that only use a single processor on HPC nodes for extended
    periods of time. Always make sure your long-running scripts use Python's multiprocessing library
    wherever possible to make the most use of shared HPC resources.

Don't forget to include the script step in your pipeline configuration:

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


Status Reset
------------
Sometimes you may wish to partially or completely reset the status of a model pipeline. You can achieve this
using the ``reset-status`` command:

.. code-block::
    shell

    $ reV reset-status

Note that this action will reset the pipeline status back to the beginning, but it **will not delete any of
the model output files**. You will need to remove any model outputs manually before restarting the pipeline
from scratch.

You can also reset the status of a pipeline to a specific step using:

.. code-block::
    shell

    $ reV reset-status --after-step generation

This will reset the status of all steps after "generation," leaving "generation" itself untouched.
Note that this action still does not remove model outputs, so you will need to delete them manually.


Duplicate Pipeline Steps
------------------------
As mentioned in the `Scripts`_ section, there are times when you may want to execute the same model steps
multiple times within a single pipeline. You can achieve this by adding an additional key to the step
dictionary in the pipeline configuration:

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

The ``command`` key should point to the actual model step you intend to execute, while the key
referring to the config file should be a **unique** name for that pipeline step. In this example,
we execute the script command twice, first as a ``setup`` step, and then as an ``analyze`` step.
We also execute the generation step twice, first as a standard ``generation`` step, and then again at
the end as a ``second_gen`` step. Please note that ``config_setup.json`` and ``config_analyze.json``
should both contain configurations for the ``script`` step, while ``config_generation.json`` and
``config_generation_again.json`` should both include ``reV`` generation parameters.


Batched Execution
-----------------
It is often desirable to conduct multiple end-to-end executions of a model and compare the results
across scenarios. While manual execution is feasible for small parameter spaces, the task becomes
increasingly challenging as the parameter space expands. Managing the setup of hundreds or thousands
of run directories manually not only becomes impractical but also introduces a heightened risk of errors.

GAPs provides a streamlined solution for parameterizing model executions by allowing users to specify the
parameters to be modified in their configurations. GAPs then automates the process of creating separate
run directories for each parameter combination and orchestrating all model executions.

Let's examine the most basic execution of ``batch``, the GAPs command that performs this process.

Standard Parametric
^^^^^^^^^^^^^^^^^^^
Let's suppose you wanted to run ``reV`` for three different turbine hub-heights with five different FCR
values for each turbine height (for a total of 15 scenarios). Begin by setting up a model run directory as
normal. We will refer to this as the top-level directory since it will ultimately contain the 15
sub-directories for the parametric runs. After configuring the directory to the ``reV`` run you want
to execute for each of the 15 parameter combinations, create a batch config like so:

.. code-block::
    shell

    $ cat config_batch.json
    {
        "pipeline_config": "./config_pipeline.json",
        "sets": [
            {
                "args": {
                    "wind_turbine_hub_ht": [100, 110, 120],
                    "fixed_charge_rate": [0.05, 0.06, 0.08, 0.1, 0.2]
                },
                "files": ["./turbine.json"],
                "set_tag": "set1"
            }
        ]
    }


As you can see, the batch config has only two required keys: ``"pipeline_config"`` and ``"sets"``.
The ``"pipeline_config"`` key should point to the pipeline configuration file that can be used
to execute the model once the parametric runs have been set up. The ``"sets"`` key is a list that
defines our parametrizations. Each "set" (defined in `Custom Parametric`_) is a dictionary with
three keys. The first key is ``"args"``, which we use to define the parameters we want to change
across scenarios and the values they should take. Specifically, ``"args"`` should point to a dictionary
where the keys are parameter names from other config files that point to a list containing the values
we want to model. In our case, the values we are changing across scenarios are all floats, but they
can also be strings or other JSON objects (list, dict, etc.). The second key in the set dictionary is
``"files"``, which should be a list of all the files in the top-level directory that should be modified
int the sub-directory with the key-value pairs from ``"args"``. Note that in our case, both
``"wind_turbine_hub_ht"`` and ``"fixed_charge_rate"`` are keys in the ``turbine.json`` config file, so
that is the only file we list. If we wanted to, for example, parametrize the resource input in addition
to the hub-height and FCR, we would add ``"resource_file": [...]`` to the ``args`` dictionary and
modify the ``"files"`` list to include the generation config:
``"files": ["./turbine.json", "./config_gen.json"]``. Finally, the ``"set_tag"`` key allows us to add
a custom tag to the sub-directory names that belong to this set. We will see the effect of this key
in a minute.

At this point, your directory should look something like:

.. code-block::
    shell

    $ ls
    config_batch.json  config_gen.json  config_pipeline.json  turbine.json  ...


To test out batch configuration setup, run the following command:

.. code-block::
    shell

    $ reV batch -c config_batch.json --dry


The ``--dry`` argument creates all the run sub-directories without actually kicking off any runs.
This allows us to double-check the batch setup and make any final tweaks before kicking off the
parametrized model runs.

If you examine the top-level directory now, it should look something like this:

.. code-block::
    shell

    $ ls
    batch_jobs.csv     config_gen.json       set1_wthh100_fcr005  set1_wthh100_fcr008  set1_wthh100_fcr02   set1_wthh110_fcr006  set1_wthh110_fcr01  set1_wthh120_fcr005  set1_wthh120_fcr008  set1_wthh120_fcr02
    config_batch.json  config_pipeline.json  set1_wthh100_fcr006  set1_wthh100_fcr01   set1_wthh110_fcr005  set1_wthh110_fcr008  set1_wthh110_fcr02  set1_wthh120_fcr006  set1_wthh120_fcr01   turbine.json


Firstly, we see that ``batch`` created a ``batch_jobs.csv`` file that is used internally to keep
track of the parametrized sub-directories. More importantly, we see that the command also created
fifteen sub-directories, each prefixed with our ``"set_tag"`` from above, and each containing a
copy of the run configuration.

.. WARNING:: ``batch`` copies *ALL* files in your top-level directory to each of the sub-directories.
   This means large files in your top-level directory may be (unnecessarily) copied many times. Always
   keep "static" files somewhere other than your top-level directory and generally try to limit your run
   directory to only contain configuration files.

We can also verify that batch correctly updated the parameters in each sub-directory:

.. code-block::
    shell

    $ cat set1_wthh100_fcr005/turbine.json
    {
        ...
        "fixed_charge_rate": 0.05,
        ...
        "wind_turbine_hub_ht": 100,
        ...
    }

    $ cat set1_wthh110_fcr008/turbine.json
    {
        ...
        "fixed_charge_rate": 0.08,
        ...
        "wind_turbine_hub_ht": 110,
        ...
    }

    ...


If we wanted to continue tweaking the batch configuration, we can get a clean top-level directory
by running the command

.. code-block::
    shell

    $ reV batch -c config_batch.json --delete

This removes the CSV file created by batch as well as all of the parametric sub-directories.
When we are happy with the configuration and ready to kick off model executions, we can simply run

.. code-block::
    shell

    $ reV batch -c config_batch.json

This command will set up the directories as before, but will then execute the pipeline in each
sub-directory so that you don't have to!

.. Note:: Like the standard ``pipeline`` command, ``batch`` will ony execute one step at a time.
   To kick off the next step, you will have to execute the ``batch`` command once again as before.
   If you prefer to live dangerously and kick off the the full pipeline execution at once, you can
   use the ``--monitor-background`` flag for batch, which will kick off the full pipeline run for
   each sub-directory in the background.


Custom Parametric
^^^^^^^^^^^^^^^^^
While the standard ``batch`` workflow is great for model sensitivity analyses and general parametric
sweeps, often you will want finer control over the parameter combinations that you want to run. The
``"sets"`` input of the batch config allows you to do just that. In particular, the values of all
parameters in each "set" will be permuted with each other, but *not* across sets. Therefore, you can
set up multiple sets without having to model permutations of all the inputs.

For example, let's suppose you want to model three different turbines:

    - 110m HH 145m RD
    - 110m HH 170m RD
    - 120m HH 160m RD

It would not make much sense to set up batch as we did before, since we don't want to model non-existent
turbines (i.e. 110m HH 160m RD, 120m HH 154m RD, etc.). Instead, we will separate these parameter
combinations into multiple sets in our batch config:

.. code-block::
    shell

    $ cat config_batch.json
    {
        "pipeline_config": "./config_pipeline.json",
        "sets": [
            {
                "args": {
                    "wind_turbine_hub_ht": [110],
                    "wind_turbine_rotor_diameter": [145, 170]
                },
                "files": ["./turbine.json"],
                "set_tag": "110hh"
            },
            {
                "args": {
                    "wind_turbine_hub_ht": [120],
                    "wind_turbine_rotor_diameter": [160]
                },
                "files": ["./turbine.json"],
                "set_tag": "120hh_wtrd160"
            }
        ]
    }

Now if we run batch (``--dry``), we will only get three sub-directories, which is exactly what we wanted:

.. code-block::
    shell

    $ ls
    110hh_wtrd145  110hh_wtrd170  120hh_wtrd160  batch_jobs.csv  config_batch.json  config_gen.json  config_pipeline.json  turbine.json

Note how we used the ``"set_tag"`` key to get consistent names across the newly-created runs. Once again,
we can verify that batch correctly updated the parameters in each sub-directory:


.. code-block::
    shell

    $ cat 110hh_wtrd145/turbine.json
    {
        ...
        "wind_turbine_rotor_diameter": 145,
        ...
        "wind_turbine_hub_ht": 110,
        ...
    }

    $ cat 110hh_wtrd170/turbine.json
    {
        ...
        "wind_turbine_rotor_diameter": 170,
        ...
        "wind_turbine_hub_ht": 110,
        ...
    }

    $ cat 120hh_wtrd160/turbine.json
    {
        ...
        "wind_turbine_rotor_diameter": 160,
        ...
        "wind_turbine_hub_ht": 120,
        ...
    }

Once we are happy with the setup, we can use the ``batch`` command to kickoff pipeline execution in
each sub-directory as before.


CSV Batch Config
^^^^^^^^^^^^^^^^
If we want to model many unique combinations of parameters with ``batch``, the setup of individual sets
can become cumbersome (and barely more efficient than writing a script to perform the setup by hand).
Luckily, ``batch`` allows you to intuitively and efficiently setup many parameter combinations with
a simple CSV input.

Let's take the example from the previous section, but add a few more turbine combinations to the mix:

    - 110m HH 145m RD
    - 115m HH 150m RD
    - 120m HH 155m RD
    - 125m HH 160m RD
    - 130m HH 170m RD
    - 140m HH 175m RD
    - 150m HH 190m RD
    - 170m HH 200m RD

To avoid having to setup a unique set for each of these combinations, we can instead put them in a
CSV file like so:

+---------+----------------------+-----------------------------+------------------------+-----------------------+
| set_tag | wind_turbine_hub_ht  | wind_turbine_rotor_diameter | pipeline_config        | files                 |
+=========+======================+=============================+========================+=======================+
| T1      | 110                  | 145                         | ./config_pipeline.json | "['./turbine.json']"  |
+---------+----------------------+-----------------------------+------------------------+-----------------------+
| T2      | 115                  | 150                         | ./config_pipeline.json | "['./turbine.json']"  |
+---------+----------------------+-----------------------------+------------------------+-----------------------+
| T3      | 120                  | 155                         | ./config_pipeline.json | "['./turbine.json']"  |
+---------+----------------------+-----------------------------+------------------------+-----------------------+
| T4      | 125                  | 160                         | ./config_pipeline.json | "['./turbine.json']"  |
+---------+----------------------+-----------------------------+------------------------+-----------------------+
| T5      | 130                  | 170                         | ./config_pipeline.json | "['./turbine.json']"  |
+---------+----------------------+-----------------------------+------------------------+-----------------------+
| T6      | 140                  | 175                         | ./config_pipeline.json | "['./turbine.json']"  |
+---------+----------------------+-----------------------------+------------------------+-----------------------+
| T7      | 150                  | 190                         | ./config_pipeline.json | "['./turbine.json']"  |
+---------+----------------------+-----------------------------+------------------------+-----------------------+
| T8      | 170                  | 200                         | ./config_pipeline.json | "['./turbine.json']"  |
+---------+----------------------+-----------------------------+------------------------+-----------------------+


Notice how we have included the ``set_tag``, ``pipeline_config``, and ``files`` columns. This is because this
CSV file doubles as the batch config file! In other words, once you set up the CSV file with the parameter
combination you want to model, you can pass this file directly to ``batch`` and let it do all the work for you!
Let's try running the command to see what we get:

.. code-block::
    shell

    $ reV batch -c parameters.csv --dry
    $ ls
    batch_jobs.csv  config_gen.json  config_pipeline.json  parameters.csv  T1  T2  T3  T4  T5  T6  T7  T8  turbine.json


Note that the sub-directory names are now uniquely defined by the ``set_tag`` column.
As before, we can validate that the setup worked as intended and kickoff the model runs by leaving off the ``--dry``
flag.

One important caveat for the CSV batch input is that any JSON-like objects (e.g. lists, dicts, etc), *must* be
enclosed in double quotes (``"``). This means that any strings within those objects *must* be enclosed in
single quotes. You can see this use pattern in the ``files`` column in the table above. Although this can be
tricky to get used to at first, this does allow you to use ``batch`` to parametrize more complicated inputs
like dictionaries (e.g. ``"{'dset': 'big_brown_bat', 'method': 'sum', 'value': 0}"``).


.. Note:: For more about ``batch``, see the `reVX setbacks batched execution example <https://github.com/NREL/reVX/tree/main/reVX/setbacks#batched-execution>`_, which is powered by GAPs.


Known Limitations
^^^^^^^^^^^^^^^^^
There are several known limitations/common pitfalls of ``batch`` that may be good to be aware of. These are
listed below and may or may not be addressed in a future update to ``batch`` functionality:

    1) ``batch`` copies ALL files in your top-level directory into the sub-directories it creates.
       This means any large files in that directory may be copied many times (often unnecessarily).
       Take care to store such files somewhere outside of your top-level directory to avoid this problem.
    2) When using a CSV batch config, there is no shortcut for specifying a default value of a
       parameter for "most" sets and changing it for a select few sets. You must specify a parametric
       value for every set (row), even if that means duplicating a default value across many sets.
       Note that this limitation goes away if you set up your batch config as shown in `Custom Parametric`_.
    3) Comments in YAML files do not currently transfer correctly (this is a limitation of the
       underlying PyYAML library), so leave comments out of parametric values for best results.


Questions?
----------
If you run into any issues or questions while executing a GAPs-powered model, please reach out to
Paul Pinchuk (ppinchuk@nrel.gov).
