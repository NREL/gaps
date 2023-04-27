================================================
Welcome to Geospatial Analysis Pipelines (GAPs)!
================================================

.. image:: https://github.com/NREL/gaps/workflows/Documentation/badge.svg
    :target: https://nrel.github.io/gaps/

.. image:: https://github.com/NREL/gaps/workflows/Pytests/badge.svg
    :target: https://github.com/NREL/gaps/actions?query=workflow%3A%22Pytests%22

.. image:: https://github.com/NREL/gaps/workflows/Lint%20Code%20Base/badge.svg
    :target: https://github.com/NREL/gaps/actions?query=workflow%3A%22Lint+Code+Base%22

.. image:: https://img.shields.io/pypi/pyversions/NREL-gaps.svg
    :target: https://pypi.org/project/NREL-gaps/

.. image:: https://badge.fury.io/py/NREL-gaps.svg
    :target: https://badge.fury.io/py/NREL-gaps

.. image:: https://codecov.io/gh/NREL/gaps/branch/main/graph/badge.svg?token=6VZK0Q2QNQ
    :target: https://codecov.io/gh/NREL/gaps


.. inclusion-intro

Geospatial Analysis Pipelines (GAPs) is a framework designed
to assist users in scaling their geospatial models to a
High-Performance Computing (HPC) environment. In particular,
GAPs automatically distributes the execution of a
single-location model (such as the `System Advisor Model <https://sam.nrel.gov>`_)
over a large geospatial extent (e.g. CONUS) across many parallel
HPC nodes. Born from the open-source `reV <https://github.com/NREL/reV>`_ model, GAPs is a
robust and easy-to-use engine that provides a rich set of features
such as configuration file generation, job status monitoring,
CLI Documentation, and more.


To get started, take a look at the `documentation <https://nrel.github.io/gaps/>`_ (examples coming soon!)


Installing gaps
===============

NOTE: The installation instruction below assume that you have python installed
on your machine and are using `conda <https://docs.conda.io/en/latest/index.html>`_
as your package/environment manager.


1. Clone the `gaps` repository.
    - Using ssh: :code:`git clone git@github.com:NREL/gaps.git`
    - Using https: :code:`git clone https://github.com/NREL/gaps.git`


2. Create and activate  the ``gaps`` environment and install the package:
    1) Create a conda env: ``conda create -n gaps python=3.10``
    2) Activate the newly-created conda env: ``conda activate gaps``
    3) Change directories into the repository: ``cd gaps``
    4) Prior to running ``pip`` below, make sure the branch is correct (install from main!): ``git branch -vv``
    5) Install ``gaps`` and its dependencies by running:
       ``pip install -e .`` (or ``pip install -e .[dev]`` if running a dev branch or working on the source code)



Development
===========

This repository uses `pylint <https://pylint.pycqa.org/en/latest/>`_ to lint the code and
`black <https://black.readthedocs.io/en/stable/index.html>`_ to format it (check out the
`black formatting style <https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html>`_).
If you wish to contribute to this repository, your code will have to adhere to both of these guidelines and pass all existing tests.


Acknowledgments
===============
.. inclusion-ack

Paul Pinchuk and Grant Buster. Geospatial Analysis Pipelines. 2023. https://doi.org/10.11578/dc.20230426.7

The authors of this code would like to thank ExxonMobil Corporation for their contributions to this effort.