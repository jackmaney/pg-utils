pg-utils: PostgreSQL Utils
==========================

.. image:: https://badge.fury.io/py/pg-utils.svg
    :target: https://badge.fury.io/py/pg-utils

.. image:: https://readthedocs.org/projects/pg-utils/badge/?version=latest
    :target: http://pg-utils.readthedocs.org/en/latest/?badge=latest
    :alt: Documentation Status

When analyzing large datasets, it can often be useful to let the database do as much of the analysis as possible. While `Pandas is great at manipulating datasets that are large enough to fit on one machine, but possibly not large enough to fit into memory <http://stackoverflow.com/a/14268804/554546>`_, concerns over performance and data security can sometimes make analysis in the database more convenient.

This package is built for use with PostgreSQL. Support for other databases *might* follow (but don't hold your breath).

Note
----

The ``Table`` API has significantly changed from ``0.3.x`` to ``0.4.0``. In particular, schema and connection parameters are now optional (replaced with your username and a fresh connection, respectively). This leaves the table name as the only required parameter for many of these methods. If ``table_name`` is already qualified with a schema (eg ``"foo.bar"``), then pg-utils will Do The Right Thing and just set ``schema="foo"`` and ``table_name="bar"``.

Goals
-----

The goals for this package are:

* Providing a simple ``Connection`` object that builds easy connections to the database based off of environment variables (overridden with parameters, if specified).

* Mocking Pandas Series and DataFrame objects with metadata constructs of ``Column`` and ``Table`` (respectively). Columns and Tables will implement some parts of the Pandas API that do calculations in the database.

* Possibly other tools for automation of simple tasks and conveniently displaying metadata (if needed).

Non-Goals
---------

This package will never be:

* An object-relational mapper (ORM). The only SQL-ish operations will be those that are implemented and that mock SQL-ish bits of the Pandas API for Series and/or DataFrames.

Installation
------------

It's up on PyPI. So, just do

::

    pip install pg-utils

for the base package, or

::

    pip install pg-utils[graphics]

to install `Seaborn <https://stanford.edu/~mwaskom/software/seaborn/>`_ for graphical visualizations.


