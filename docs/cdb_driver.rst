.. _cdb-driver-description:

Driver description
==================

The ``cdb_driver`` command includes many options. Its help command lists:

.. command-output:: cdb_driver --help

The ``cdb_driver`` takes as an argument a header file that was pre-processed by ``cdb_query_archive optimset_months`` and returns 
``bash`` scripts for each model, rip, experiment combination.

There are two groups of optional arguments: Setup and Processing. 

Setup
-----
In the Setup options, one can specify a ``debug`` mode or one can request that the created scripts be run by the driver by using the option ``run``.

Processing 
----------
Processing options are offering possibilities for asynchronous computing and sumitting jobs through a PBS scheduler. These options
are still in alpha phase and can be treacherous to use. A full documentation will become available when the implementation will be more
stable.
