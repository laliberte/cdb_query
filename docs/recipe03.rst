Retrieving precipitation for JJAS over France (CORDEX)
------------------------------------------------------

Header file
^^^^^^^^^^^
The first step is to create a diagnostic header file in JSON format. We will call this file ``pr_JJAS_France.hdr``::

    {
    "header":{
    "experiment_list":
        {
        "historical":"1979,2004"
        },
    "months_list": [6,7,8,9],
    "variable_list":
        {
        "pr":["mon"],
        "orog":["fx"]
        },
    "search_list":
        [
        "./in/CORDEX",
        "http://esgf-node.ipsl.fr/esg-search/"
        ],
    "file_type_list":
        [
        "local_file",
        "HTTPServer"
        ]
    }
    }

* The ``experiment_list`` entry lists the resquested experiments and the requested years range. 
  More than one experiment can be specified.
  Here, we are requesting one experiment: historical for years 1979 to 2004, inclusive.
* The ``months_list`` entry lists the requested months.
  Here, we are requesting June, July, August and September
* The ``variable_list`` entry lists the requested variables with their [time_frequency].
  Here, we are requesting monthly precipitation (pr) and fixed orography (orog).
* The ``search_list`` entry lists the different places where the query should look for data. 
  The first entry is a local archive, on the file system. The script walks through subdirectories and tries to locate data 
  that match ``experiment_list`` and ``variable_list``. This path should point to the top directory
  of the CORDEX archive that lists the different institutes.
  The second entry queries the ESGF IPSL node for data matching the ``experiment_list`` and ``variable_list``.

.. important::
    ``search_list`` is ordered. Its order will affect which path is retrieved.

* The ``file_type_list`` specifies what kind of files should be included. In this case, we consider data only if it is in a
  ``local_file``, or if it can be retrieved by wget on an ``HTTPserver``. This list is ordered
  and the final path will be chosen depending on its ``file_type``. In this case, when a variable has more than one paths,
  the ``local_file`` will be privileged over the ``HTTPServer`` file.

.. important::
    ``file_type_list`` is ordered. Its order will affect which path is retrieved.

Discovering the data
^^^^^^^^^^^^^^^^^^^^
First, make sure that the directory ``./in/CORDEX`` exists by creating it ::

    $ mkdir -p ./in/CORDEX

The script is run using::

    $ cdb_query_CORDEX discover pr_JJAS_France.hdr pr_JJAS_France_pointers.nc

This should take a few minutes, depending on your connection to the ESGF BADC node. It returns a self-descriptive netCDF file 
with pointers to the data. Try looking at the resulting netCDF file using ``ncdump``: ::

    $ ncdump -h pr_JJAS_France_pointers.nc

As you can see, it generates a hierarchical netCDF4 file. ``cdb_query_CORDEX list_fields`` offer a tool to query this file. 

Querying the discovered data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
For example, if we want to know how many different simulations were made available, we would run ::
    
    $ cdb_query_CORDEX list_fields -f domain -f institute -f model -f ensemble pr_JJAS_France_pointers.nc
    EUR-11,IPSL-INERIS,IPSL-INERIS-WRF331F,r0i0p0
    EUR-11,IPSL-INERIS,IPSL-INERIS-WRF331F,r1i1p1
    EUR-44,IPSL-INERIS,IPSL-INERIS-WRF331F,r0i0p0
    EUR-44,IPSL-INERIS,IPSL-INERIS-WRF331F,r1i1p1

This test was run on Feburary 4rd, 2014 and these results represent the data presented by the ESGF IPSL node on that day. These
results indicate that the IPSL-INERIS has two simulations, one from domain EUR-11 and one for domain EUR-44.
The r0i0p0 ensemble name is the ensemble associated with fixed (time_frequency=fx) variables and its presence suggests that these
simulations have provided the requested orog variable.

This is a small subset of what is available in the CORDEX archive. To have access to the whole archive, you can either include more ESGF 
nodes in the search path or use ``cdb_query_CORDEX discover --distrib`` for a distributed search. This last method would is the preferred
method. It however tends to generate harmless warning messages that can be safely ignored. These warning messages come about because some nodes
in the ESGF are unresponsive. This is likely to be fixed as the ESGF infrastructure improves.

.. attention::
    The command ``cdb_query_CORDEX discover --distrib`` can take a very long time to complete, espcially is many files are found. This means
    that querying for daily or 6hr data is generally slow.

If this list of models in satisfying, we next check the paths  ::
    
    $ cdb_query_CORDEX list_fields -f path pr_JJAS_France_pointers.nc
    http://esgf-node.ipsl.fr/thredds/fileServer/cordex/EUR-11/IPSL-INERIS/IPSL-IPSL-CM5A-MR/historical/r0i0p0/IPSL-INERIS-WRF331F/v1/fx/orog/v20131223/orog_EUR-11_IPSL-IPSL-CM5A-MR_historical_r0i0p0_IPSL-INERIS-WRF331F_v1_fx.nc|1cd0e1ef163ab7b047ad90a781ac5494
    ...

We consider the first path. It is constituted of two parts. The first part begins with ``http://esgf-node.ipsl.fr/...`` and 
ends a the vertical line. This is a `wget` link. The second part, at the right of the vertical line, ``1cd0e1ef163ab7b047ad90a781ac5494``
is the hexadecimal checksum. This is as published on the EGSF website. The file found at the other end of the `wget` link should be
expected to have the same checksum.

.. hint::
    The command ``cdb_query_CORDEX discover`` does not guarantee that the simulations found satisfy ALL the requested criteria.

Finding the optimal set of simulations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. warning::
    From now on it is assumed that the user has installed appropriate certificates to retrieve data from the ESGF CORDEX archive. Failure to do
    so will result in either an incomplete query or an error ``RuntimeError: NetCDF: I/O failure``.
    
To narrow down our results to the simulations that satisfy ALL the requested criteria, we can use  ::

        $ cdb_query_CORDEX optimset pr_JJAS_France_pointers.nc pr_JJAS_France_pointers.optimset.nc

To output now has a time axis for each variable (except fx). It links every time index to a time index in a UNIQUE file (remote or local).
Try looking at the resulting netCDF file using ``ncdump``: ::

    $ ncdump -h pr_JJAS_France_pointers.optimset.nc

Again, this file can be queried for simulations::

    $ cdb_query_CORDEX list_fields -f domain -f institute -f model -f ensemble pr_JJAS_France_pointers.optimset.nc
    EUR-11,IPSL-INERIS,IPSL-INERIS-WRF331F,r0i0p0
    EUR-11,IPSL-INERIS,IPSL-INERIS-WRF331F,r1i1p1
    EUR-44,IPSL-INERIS,IPSL-INERIS-WRF331F,r0i0p0
    EUR-44,IPSL-INERIS,IPSL-INERIS-WRF331F,r1i1p1

We can see that no simulations were excluded. This means that they had ALL the variables for ALL the months of ALL the years for the historical
experiment.

Retrieving the data: `wget`
^^^^^^^^^^^^^^^^^^^^^^^^^^^

`cdb_query_CORDEX` includes built-in functionality for retrieving the paths. It is used as follows ::

    $ cdb_query_CORDEX download pr_JJAS_France_pointers.optimset.nc ./in/CORDEX/

It downloads the paths listed in ``pr_JJAS_France_pointers.optimset.nc``.

.. hint:: It is good practice to run this command at least twice. It will not retrieve already retrieved files that match the MD5 checksum
          and will redownload partially downloaded files. It is only when this command only returns ``File found.MD5 OK! Not retrieving.`` output for
          every file that we can be sure that all the files are properly retrieved.

.. warning:: The retrieved files are structure with the CORDEX DRS. It is good practice not to change this directory structure.
             If the structure is kept then ``cdb_query_CORDEX discover`` will recognized the retrieved files as local if they were
             retrieved to a directory listed in the ``search_list`` of the header file.

The downloaded paths are now discoverable by ``cdb_query_CORDEX discover``.

Retrieving the data: `OPeNDAP`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We retrieve the first month::

    $ cdb_query_CORDEX remote_retrieve --year=1979 --month=6 \
                                    pr_JJAS_France_pointers.optimset.197906.nc \
                                   pr_JJAS_France_pointers.optimset.197906.retrieved.nc 

This step took about 4 minutes from the University of Toronto. Next, we extract precipitation for the simulation with the EUR-11 domain::

    $ ncks -G : -g /EUR-11/IPSL-INERIS/IPSL-IPSL-CM5A-MR/historical/r1i1p1/IPSL-INERIS-WRF331F/v1/mon/pr \
                    pr_JJAS_France_pointers.optimset.197906.retrieved.nc \
                    pr_JJAS_France_pointers.optimset.197906.retrieved.EUR-11.nc
    $ ncview pr_JJAS_France_pointers.optimset.197906.retrieved.EUR-11.nc

By looking at the map, we see that France lies between indices 120 and 210 along the rotated longitude (rlon) and between indices
130 and 225 alon the rotated latitude (rlat). We thus subset this region and look at the result::

    $ ncks -d rlon,-15.5,-5.5 -d rlat,-9.1,1.5 pr_JJAS_France_pointers.optimset.197906.retrieved.EUR-11.nc \
                                           pr_JJAS_France_pointers.optimset.197906.retrieved.EUR-11_France.nc

.. note:: We added one the the indices to conform with ``ncks`` conventions.

We can make sure that our subsetting was ok::
    
    $ ncview pr_JJAS_France_pointers.optimset.197906.retrieved.EUR-11_France.nc

Next subset the data. We can use ``ncks`` or  `apply` to do this. ``ncks`` tends to be
slow because it has not been optimized::

    $ ncks -d rlon,-15.5,-5.5 -d rlat,-9.1,1.5 -v pr \
                                pr_JJAS_France_pointers.optimset.nc \
                                pr_JJAS_France_pointers.optimset.France.nc
                            
or::

    $ cdb_query_CORDEX apply --var=pr 'ncks -d rlon,-15.5,-5.5 -d rlat,-9.1,1.5' \
                                pr_JJAS_France_pointers.optimset.nc \
                                pr_JJAS_France_pointers.optimset.France.nc

Finally, we retrieve the data::
    
    $ cdb_query_CORDEX remote_retrieve pr_JJAS_France_pointers.optimset.France.nc \
                                        pr_JJAS_France_pointers.optimset.France.retrieved.nc 

This step took about 40s from the University of Toronto. It retrieves the whole time series for France.
We can then check the variables::

    $ ncks -G : -g /EUR-44/IPSL-INERIS/IPSL-IPSL-CM5A-MR/historical/r1i1p1/IPSL-INERIS-WRF331F/mon/pr \
                    pr_JJAS_France_pointers.optimset.France.retrieved.nc \
                    pr_JJAS_France_pointers.optimset.France.retrieved.EUR-44.nc
    $ ncks -G : -g /EUR-11/IPSL-INERIS/IPSL-IPSL-CM5A-MR/historical/r1i1p1/IPSL-INERIS-WRF331F/mon/pr \
                    pr_JJAS_France_pointers.optimset.France.retrieved.nc \
                    pr_JJAS_France_pointers.optimset.France.retrieved.EUR-11.nc

BASH script
^^^^^^^^^^^
This recipe is summarized in the following BASH script::

    #!/bin/bash
    cat > pr_JJAS_France.hdr <<EndOfHDR
    {
    "header":{
    "experiment_list":
        {
        "historical":"1979,2004"
        },
    "months_list": [6,7,8,9],
    "variable_list":
        {
        "pr":["mon"],
        "orog":["fx"]
        },
    "search_list":
        [
        "http://esgf-node.ipsl.fr/esg-search/"
        ],
    "file_type_list":
        [
        "local_file",
        "HTTPServer"
        ]
    }
    }
    EndOfHDR
        #"./in/CORDEX",
    #Make search dir otherwise result in error:
    mkdir -p ./in/CORDEX

    #Discover data:
    cdb_query_CORDEX discover pr_JJAS_France.hdr pr_JJAS_France_pointers.nc

    #List simulations:
    cdb_query_CORDEX list_fields -f institute \
                                -f model \
                                -f ensemble \
                                pr_JJAS_France_pointers.nc

    #Find optimal set of simulations:
    cdb_query_CORDEX optimset pr_JJAS_France_pointers.nc \
                             pr_JJAS_France_pointers.optimset.nc
    #CHOOSE:
        # *1* Retrieve files:
            #cdb_query_CORDEX download \
            #                    pr_JJAS_France_pointers.optimset.nc \
            #                    ./in/CORDEX/

        # *2* Retrieve to netCDF:
            #Retrieve first month:
            cdb_query_CORDEX remote_retrieve  --year=1979 --month=6 \
                                    pr_JJAS_France_pointers.optimset.nc \
                                    pr_JJAS_France_pointers.optimset.197906.retrieved.nc
            #Extract first domain:
            ncks -G : -g /EUR-11/IPSL-INERIS/IPSL-IPSL-CM5A-MR/historical/r1i1p1/IPSL-INERIS-WRF331F/v1/mon/pr \
                            pr_JJAS_France_pointers.optimset.197906.retrieved.nc \
                            pr_JJAS_France_pointers.optimset.197906.retrieved.EUR-11.nc

            #We can then use any program, eg ncview to find where France lies in the rotated coordinates:
            # ncview  pr_JJAS_France_pointers.optimset.197906.retrieved.EUR-11.nc
            #
            #We then find that France lies between -15.5 and -5.5 of rlon and between -9.1 and 1.5 of rlat.
            #We can verify that this is right:
            ncks -d rlon,-15.5,-5.5 -d rlat,-9.1,1.5 pr_JJAS_France_pointers.optimset.197906.retrieved.EUR-11.nc \
                                                 pr_JJAS_France_pointers.optimset.197906.retrieved.EUR-11_France.nc
            #We can verify that this was right:
            # ncview  pr_JJAS_France_pointers.optimset.197906.retrieved.EUR-11_France.nc

            #When we are satisfied, we can subset the pointers file. Any program would do but
            #we suggest using ncks. We can use cdb_query_CORDEX apply to do this:
            cdb_query_CORDEX apply --var=pr 'ncks -d rlon,-15.5,-5.5 -d rlat,-9.1,1.5' \
                                    pr_JJAS_France_pointers.optimset.nc \
                                    pr_JJAS_France_pointers.optimset.France.nc

            #We then retrieve the whole time series over France:
            cdb_query_CORDEX remote_retrieve pr_JJAS_France_pointers.optimset.France.nc \
                                             pr_JJAS_France_pointers.optimset.France.retrieved.nc

            #Finally, convert back to a CMIP5 file tree:
            cdb_query_CORDEX convert pr_JJAS_France_pointers.optimset.France.retrieved.nc \
                                     out/CORDEX/
