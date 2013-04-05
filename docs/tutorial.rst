Tutorial
========

Once installed, this package should make the command line tool ``cdb_query_archive`` visible
to the user's path. This is typically the case for common python installations.

The command ``cdb_query_archive`` contains several commands. The first one, ``cdb_query_archive optimset``,
searches the archive and produces a file with pointers to the data. This file can be inspected for a list of
models matching a diagnostic's experiments and variables criteria using ``cdb_query_archive simulations``.
Then the command ``cdb_query_archive optimset_months`` can be used to find all the experiments that have all of
the avialalble years. This command outputs a file that points to data for every month of the requested period.
This is for easy compatibility with ``cdb_driver``. The final command, ``cdb_query_archive retrieve``,
reads the output from ``cdb_query_archive optimset_months``, takes (center, model, experiment, frequency,
realm, mip, rip, year, month) as an input and returns a **single** path to file. This makes it
easy to retrieve data from simple scripts.

Months were chosen as the atomic time period the CDB because for some variables and/or some models years are not
atomic. To maximize flexibility it is therefore easier to see months as the atomic time period for all diagnostics.

Find the optimal set of models that satisfy a set of criteria
-------------------------------------------------------------

optimset
^^^^^^^^

The first step is to create a diagnostic header file in JSON format. We will call this file ``test_diags.hdr``::

    {
    "header":{
    "diagnostic_name":"test_diags",
    "experiment_list":
        {
        "amip":"1979,2004"
        },
    "variable_list":
        {
        "ta":["day","atmos","day"],
        "orog":["fx","atmos","fx"]
        },
    "search_list":
        [
        "$HOME/data",
        "http://esgf-index1.ceda.ac.uk/esg-search/search"
        ],
    "file_type_list":
        [
        "local_file",
        "OPeNDAP",
        "HTTPServer"
        ]
    }
    }

.. note::
    This file structure is likely to evolve over time.

The five first entries are not necessary for ``cdb_query_archive optimset``. They are used for other tools being
developed as part of this project.

* The ``experiment_list`` entry lists the resquested experiments and the requested years range. 
  More than one experiment can be specified.
* The ``variable_list`` entry lists the requested variables with their [frequency,realm,mip].
* The ``search_list`` entry lists the different places where the query should look for data. 
  The first entry is a local archive, on the file system. The script walks through subdirectories and tries to locate data 
  that match ``experiment_list`` and ``variable_list``. This path should point to the top directory
  of the CMIP5 archive that lists the different centers. The second entry queries the ESGF BADC node for data matching
  the ``experiment_list`` and ``variable_list``.

.. important::
    ``search_list`` is ordered. Its order will affect which path is retrieved.

* The ``file_type_list`` specifies what kind of files should be included. In this case, we consider data only if it is in a
  ``local_file``, if it is accessible by ``OPeNDAP``, or if it can be retrieved by wget on an ``HTTPserver``. This list is ordered
  and the final path will be chosen depending on its ``file_type``. In this case, when a variable has more than one paths,
  the ``local_file`` will be privileged over the ``OPeNDAP`` file.

.. important::
    ``file_type_list`` is ordered. Its order will affect which path is retrieved.

The script is run using::

    $ cdb_query_archive optimset test_diags.hdr test_diags_pointers.hdr

and returns a self-descriptive JSON file with pointers to the data.

simulations
^^^^^^^^^^^

By using the ``simulations`` command on the output it possible to find how many models are available::

    $ cdb_query_archive simulations test_diags_pointers.hdr

.. important::
    Before running ``optimset_time`` and ``list_paths`` the user must be logged onto the ESGF using a
    ``myproxy`` client.

optimset_time
^^^^^^^^^^^^^^^

If ``optimset`` returned enough models, it is important to ensure that all the requested years and months are available. To do so,
one runs ``optimset_time``::

    $ cdb_query_archive optimset_time test_diags_ponters.hdr test_diags_pointers_time.hdr

The returned file is easy to reuse but is extremely redundant and therefore results in large files.
It is suggested that it be output in gzip format::

    $ cdb_query_archive optimset_time -z test_diags_ponters.hdr test_diags_pointers_time.hdr

This command will create the file ``test_diags_pointers_time.hdr.gz``, which can be ``gunzipped`` to  
obtain the same output that would be obtained without the ``-z`` option.

.. note::
    ``optimset_time`` must be run before ``cdb_driver`` described in section :ref:`cdb_driver_tutorial`.

list_paths
^^^^^^^^^^

The ``list_paths`` command simply reads ``test_diags_pointers_time.hdr`` and returns a path to the file::

    $ cdb_query_archive list_paths --center=MOHC --model=HadGEM2-A --experiment=amip --rip=r1i1p1
        --var=ta --frequency=day --realm=atmos --mip=day --year=2000 --month=10 test_diags_pointers.hdr
    http://cmip-dn1.badc.rl.ac.uk/thredds/dodsC/cmip5.output1.MOHC.HadGEM2-A.amip.day.atmos.day.r1i1p1.ta.20110513.aggregation.1

In this case, it returns an ``OPeNDAP`` aggregation pointer. If ``-z`` was used in the ``optimset`` query, the
``list_paths`` steps works seamlessly if th ``.gz`` is kept in the filename::

    $ cdb_query_archive list_paths --center=MOHC --model=HadGEM2-A --experiment=amip --rip=r1i1p1
        --var=ta --frequency=day --realm=atmos --mip=day --year=2000 --month=10 test_diags_pointers.hdr
    http://cmip-dn1.badc.rl.ac.uk/thredds/dodsC/cmip5.output1.MOHC.HadGEM2-A.amip.day.atmos.day.r1i1p1.ta.20110513.aggregation.1|9527|9557

The string ``|9527|9557`` is not part of the ``OPeNDAP`` url but it gives the indices corresponding to October 2000.
To use this string efficiently, there flag ``-f`` passed to ``list_paths`` returns only the file type::

    $ cdb_query_archive list_paths MOHC HadGEM2-A amip r1i1p1 ta day atmos day 2000 10 test_diags_pointers.hdr.gz -f
    OPeNDAP

.. note::
    The ``OPeNDAP`` file type is only available is a myproxy session is loaded on the machine runnning ``cdb_query_archive``

.. _cdb_driver_tutorial:

Run the diagnostic
------------------

To run the diagnostic, one must have put informations about the run directories
in the diagnostic header file `test_diags.hdr`::

    {
    "header":{
    "diagnostic_name":"test_diags",
    "experiment_list":
        {
        "amip":"1979,2004"
        },
    "variable_list":
        {
        "ta":["day","atmos","day"],
        "orog":["fx","atmos","fx"]
        },
    "search_list":
        [
        "$HOME/data",
        "http://esgf-index1.ceda.ac.uk/esg-search/search"
        ],
    "file_type_list":
        [
        "local_file",
        "OPeNDAP",
        "HTTPServer"
        ],
    "diagnostic_dir":"../scripts/",
    "runscripts_dir":"./runscripts/",
    "output_dir":"./outputs",
    "temp_dir":"./temp",
    "months_list":[1,2,12]
    }
    }

One must first run ``optimset`` and ``optimset_time`` sequentially::

    $ cdb_query_archive optimset test_diags.hdr test_diags_pointers.hdr
    $ cdb_query_archive optimset_time -z test_diags_ponters.hdr test_diags_pointers_time.hdr

and then run ``cdb_driver`` on the result::

    $ cdb_driver test_diags_pointers.hdr

See section :ref:`cdb-driver-description` for a complete description of the options available in the driver script.

Advanced Options
----------------

slice
^^^^^

Let say that the result is too large or that it includes undesired features. For example, you like to subset your results
to a single year in order to debug your scripts. This would be accomplished with the ``slice`` command::

    $ cdb_query_archive slice --year=1979 test_diags_pointers_time.hdr.gz test_diags_pointers_time.hdr.1979

The result will the same file as ``test_diags_pointers_time.hdr.gz`` but with only the year 1979 left.
Other options (they can be combined) are::

    $ cdb_query_archive slice --help
      --file_type FILE_TYPE
                            File type: OPEnDAP, local_file, HTTPServer, GridFTP
      --rip RIP             RIP identifier, e.g. r1i1p1
      --month MONTH         Month as an integer ranging from 1 to 12
      --frequency FREQUENCY
                            Frequency, e.g. day
      --year YEAR           Year
      --realm REALM         Realm, e.g. atmos
      --center CENTER       Modelling center name
      --experiment EXPERIMENT
                            Experiment name
      --var VAR             Variable name, e.g. tas
      --mip MIP             MIP table name, e.g. day
      --model MODEL         Model name

find_local
^^^^^^^^^^

Let say that in the file ``test_diags_pointers_time.hdr.gz`` some of the remote links have ``file_type``=``HTTPServer``.
Then these links will have to be retrieved before an analysis can be carried.
By performing the command::

    $ cdb_query_archive list_paths --wget --file_type=HTTPServer test_diags_pointers_time.hdr.gz

A list of ``wget`` filenames with checksums is printed and these can be used to retrieve the files.
The files will be put in the ``output_dir/in`` and will preserve the CMIP5 DRS.

The command ``find_local`` can then be used to convert the pointers 
file that contain only local links::

    $ cdb_query_archive find_local test_diags_pointers_time.hdr.gz test_diags_pointers_time.hdr.local

.. warning::
    DO NOT DELETE the file ``test_diags_pointers_time.hdr.gz``. This file contains a snapshot of the archive 
    that you could need to reuse in the future. Moreover, this file could be passed to collaborators or submitted
    as supplementary material when publishing results based on CMIP5 data. That will ensure exact reproducibility
    or your results (unless the remote files are changed without having their version number changed. In an ideal
    world this should not happen).

