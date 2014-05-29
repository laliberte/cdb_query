Retrieving surface temperature for ONDJF (CMIP5)
------------------------------------------------

Header file
^^^^^^^^^^^
The first step is to create a diagnostic header file in JSON format. We will call this file ``tas_ONDJF.hdr``::

    {
    "header":{
    "experiment_list":
        {
        "amip":"1979,2004"
        },
    "months_list": [1,2,10,11,12],
    "variable_list":
        {
        "tas":["day","atmos","day"],
        "orog":["fx","atmos","fx"]
        },
    "search_list":
        [
        "./in/CMIP5",
        "http://esgf-index1.ceda.ac.uk/esg-search/"
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
  Here, we are requesting one experiment: amip for years 1979 to 2004, inclusive.
* The ``months_list`` entry lists the requested months.
  Here, we are requesting October, November, December, January and February.
* The ``variable_list`` entry lists the requested variables with their [time_frequency,realm,cmor_table].
  Here, we are requesting daily surface temperature (tas) and fixed orography (orog).
* The ``search_list`` entry lists the different places where the query should look for data. 
  The first entry is a local archive, on the file system. The script walks through subdirectories and tries to locate data 
  that match ``experiment_list`` and ``variable_list``. This path should point to the top directory
  of the CMIP5 archive that lists the different institutes.
  The second entry queries the ESGF BADC node for data matching the ``experiment_list`` and ``variable_list``.

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
First, make sure that the directory ``./in/CMIP5`` exists by creating it ::

    $ mkdir -p ./in/CMIP5

The script is run using::

    $ cdb_query_CMIP5 ask tas_ONDJF.hdr tas_ONDJF_pointers.nc

This should take a few minutes, depending on your connection to the ESGF BADC node. It returns a self-descriptive netCDF file 
with pointers to the data. Try looking at the resulting netCDF file using ``ncdump``: ::

    $ ncdump -h tas_ONDJF_pointers.nc

As you can see, it generates a hierarchical netCDF4 file. ``cdb_query_CMIP5 list_fields`` offer a tool to query this file. 

Querying the discovered data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
For example, if we want to know how many different simulations were made available, we would run ::

    $ cdb_query_CMIP5 list_fields -f institute -f model -f ensemble tas_ONDJF_pointers.nc
    CNRM-CERFACS,CNRM-CM5,r0i0p0
    CNRM-CERFACS,CNRM-CM5,r1i1p1
    INM,INM-CM4,r0i0p0
    INM,INM-CM4,r1i1p1
    IPSL,IPSL-CM5A-LR,r0i0p0
    IPSL,IPSL-CM5A-LR,r1i1p1
    IPSL,IPSL-CM5A-LR,r2i1p1
    IPSL,IPSL-CM5A-LR,r3i1p1
    IPSL,IPSL-CM5A-LR,r4i1p1
    IPSL,IPSL-CM5A-LR,r5i1p1
    IPSL,IPSL-CM5A-LR,r6i1p1
    MOHC,HadGEM2-A,r0i0p0
    MOHC,HadGEM2-A,r1i1p1
    MOHC,HadGEM2-A,r1i2p1
    MOHC,HadGEM2-A,r2i3p1
    MOHC,HadGEM2-A,r3i2p1
    MOHC,HadGEM2-A,r4i3p1
    MOHC,HadGEM2-A,r5i3p1

This test was run on Feburary 3rd, 2014 and these results represent the data presented by the ESGF BADC node on that day. These
results indicate that CNRM-CERFACS and INM each have one simulation. The r0i0p0 ensemble name is the ensemble associated
with fixed (time_frequency=fx) variables and its presence suggests that these three institutes have provided the requested orog variable.
These results also indicate that IPSL and MOHC have both provided six simulations. In the case of the IPSL, it is six simulations that
were initialized and parametrized using the same method (all six are i1p1). In the case of the MOHC, all six have the same parametrizations
(all p1) but they differ in their initializations: one uses i1, two use i2 and three use i3.

This is a small subset of what is available in the CMIP5 archive. To have access to the whole archive, you can either include more ESGF 
nodes in the search path or use ``cdb_query_CMIP5 ask --distrib`` for a distributed search. This last method is the preferred
method. It however tends to generate harmless warning messages that can be safely ignored. These warning messages come about because some nodes
in the ESGF are unresponsive. This is likely to be fixed as the ESGF infrastructure improves.

.. attention::
    The command ``cdb_query_CMIP5 ask --distrib`` can take a very long time to complete, espcially is many files are found. This means
    that querying for daily or 6hr data is generally slow.

If this list of models in satisfying, we next check the paths  ::
    
    $ cdb_query_CMIP5 list_fields -f path tas_ONDJF_pointers.nc
    http://cmip-dn1.badc.rl.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/MOHC/HadGEM2-A/amip/day/atmos/day/r1i1p1/v20110513/tas/tas_day_HadGEM2-A_amip_r1i1p1_19780901-19781230.nc|00691bac1d889e071e0e105271df8f2e
    http://cmip-dn1.badc.rl.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/MOHC/HadGEM2-A/amip/day/atmos/day/r1i1p1/v20110513/tas/tas_day_HadGEM2-A_amip_r1i1p1_19790101-19881230.nc|553bea8fb25ab01abc8a003653e9146e
    http://cmip-dn1.badc.rl.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/MOHC/HadGEM2-A/amip/day/atmos/day/r1i1p1/v20110513/tas/tas_day_HadGEM2-A_amip_r1i1p1_19890101-19981230.nc|0e51f3e591d4338eaaff1f28bbcf6b7c
    http://cmip-dn1.badc.rl.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/MOHC/HadGEM2-A/amip/day/atmos/day/r1i1p1/v20110513/tas/tas_day_HadGEM2-A_amip_r1i1p1_19990101-20081230.nc|08b85358d1811dab90e0b649f25f5be8
    http://cmip-dn1.badc.rl.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/MOHC/HadGEM2-A/amip/day/atmos/day/r1i2p1/v20110629/tas/tas_day_HadGEM2-A_amip_r1i2p1_19780901-20081130.nc|f466343056fd8ceb2e9d4c3a36a5bc96
    http://cmip-dn1.badc.rl.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/MOHC/HadGEM2-A/amip/day/atmos/day/r2i3p1/v20110629/tas/tas_day_HadGEM2-A_amip_r2i3p1_19780901-20081130.nc|8c1e2511dfc67c8c452972a129422118
    http://cmip-dn1.badc.rl.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/MOHC/HadGEM2-A/amip/day/atmos/day/r3i2p1/v20110630/tas/tas_day_HadGEM2-A_amip_r3i2p1_19780901-20081130.nc|1f2ab30bd4e3332c21739f791ffbfdb0
    http://cmip-dn1.badc.rl.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/MOHC/HadGEM2-A/amip/day/atmos/day/r4i3p1/v20110630/tas/tas_day_HadGEM2-A_amip_r4i3p1_19780901-20081130.nc|9b906e6a07c2f236aedb83d5fb773b89
    http://cmip-dn1.badc.rl.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/MOHC/HadGEM2-A/amip/day/atmos/day/r5i3p1/v20110630/tas/tas_day_HadGEM2-A_amip_r5i3p1_19780901-20081130.nc|9048325c740192dc325fe28f0df23cdd
    http://cmip-dn1.badc.rl.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/MOHC/HadGEM2-A/amip/fx/atmos/fx/r0i0p0/v20120215/orog/orog_fx_HadGEM2-A_amip_r0i0p0.nc|3813abee6a5e12d1d675760b59caacd5
    http://esg2.e-inis.ie/thredds/fileServer/esg_dataroot/CMIP5/output/ICHEC/EC-EARTH/amip/day/atmos/tas/r1i1p1/tas_day_EC-EARTH_amip_r1i1p1_19780401-19781231.nc|d127253c13dde3c2ee3b34b063297432
    ...

We consider the first path. It is constituted of two parts. The first part begins with ``http://cmip-dn1.badc.rl.ac.uk/...`` and 
ends a the vertical line. This is a `wget` link. The second part, at the right of the vertical line, ``00691bac1d889e071e0e105271df8f2e``
is the hexadecimal checksum. This is as published on the EGSF website. The file found at the other end of the `wget` link should be
expected to have the same checksum.

The string that precedes ``thredds`` in the `wget` link is what we call the data node. Here, we have two data nodes: ``http://cmip-dn1.badc.rl.ac.uk``
and ``http://esg2.e-inis.ie``. Those are the adresses of the data node. Retrieving two files from two different data nodes at the same time should
therefore not hinder the transfer of one another.

.. hint::
    The command ``cdb_query_CMIP5 ask`` does not guarantee that the simulations found satisfy ALL the requested criteria.

Finding the optimal set of simulations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. warning::
    From now on it is assumed that the user has installed appropriate certificates to retrieve data from the ESGF CMIP5 archive. Failure to do
    so will result in either an incomplete query or an error ``RuntimeError: NetCDF: I/O failure``.
    
To narrow down our results to the simulations that satisfy ALL the requested criteria, we can use  ::

    $ cdb_query_CMIP5 validate tas_ONDJF_pointers.nc tas_ONDJF_pointers.validate.nc

To output now has a time axis for each variable (except fx). It links every time index to a time index in a UNIQUE file (remote or local).
Try looking at the resulting netCDF file using ``ncdump``: ::

    $ ncdump -h tas_ONDJF_pointers.validate.nc

Again, this file can be queried for simulations::

    $ cdb_query_CMIP5 list_fields -f institute -f model -f ensemble tas_ONDJF_pointers.validate.nc
    CNRM-CERFACS,CNRM-CM5,r0i0p0
    CNRM-CERFACS,CNRM-CM5,r1i1p1
    INM,INM-CM4,r0i0p0
    INM,INM-CM4,r1i1p1
    IPSL,IPSL-CM5A-LR,r0i0p0
    IPSL,IPSL-CM5A-LR,r1i1p1
    IPSL,IPSL-CM5A-LR,r2i1p1
    IPSL,IPSL-CM5A-LR,r3i1p1
    IPSL,IPSL-CM5A-LR,r4i1p1
    IPSL,IPSL-CM5A-LR,r5i1p1
    IPSL,IPSL-CM5A-LR,r6i1p1
    MOHC,HadGEM2-A,r0i0p0
    MOHC,HadGEM2-A,r1i1p1
    MOHC,HadGEM2-A,r1i2p1
    MOHC,HadGEM2-A,r2i3p1
    MOHC,HadGEM2-A,r3i2p1
    MOHC,HadGEM2-A,r4i3p1
    MOHC,HadGEM2-A,r5i3p1

We can see that no simulations were excluded. This means that they had ALL the variables for ALL the months of ALL the years for the amip
experiment.

Retrieving the data: `wget`
^^^^^^^^^^^^^^^^^^^^^^^^^^^

`cdb_query_CMIP5` includes built-in functionality for retrieving the paths. It is used as follows ::

    $ cdb_query_CMIP5 download_raw tas_ONDJF_pointers.validate.nc ./in/CMIP5/

It downloads the paths listed in ``tas_ONDJF_pointers.validate.nc``.

.. hint:: It is good practice to run this command at least twice. It will not retrieve already retrieved files that match the MD5 checksum
          and will redownload partially downloaded files. It is only when this command only returns ``File found.MD5 OK! Not retrieving.`` output for
          every file that we can be sure that all the files are properly retrieved.

.. warning:: The retrieved files are structure with the CMIP5 DRS. It is good practice not to change this directory structure.
             If the structure is kept then ``cdb_query_CMIP5 ask`` will recognized the retrieved files as local if they were
             retrieved to a directory listed in the ``search_list`` of the header file.

The downloaded paths are now discoverable by ``cdb_query_CMIP5 ask``.

Retrieving the data: `OPeNDAP`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`cdb_query_CMIP5` includes built-in functionality for retrieving a subset of the data.

To retrieve the first month of daily data::
    
    $ cdb_query_CMIP5 download --year=1979 \
                                      --month=1 \
                            tas_ONDJF_pointers.validate.197901.nc \
                            tas_ONDJF_pointers.validate.197901.retrieved.nc 

The file ``tas_ONDJF_pointers.validate.197901.retrieved.nc`` should now contain the first thirty days for all experiments! To check the daily
surface temperature in the amip experiment from simulation CNRM-CERFACS,CNRM-CM5,r1i1p1 `ncview` (if installed)::

    $ ncks -G : -g /CNRM-CERFACS/CNRM-CM5/amip/day/atmos/day/r1i1p1/tas \
                    tas_ONDJF_pointers.validate.197901.retrieved.nc \
                    tas_ONDJF_pointers.validate.197901.retrieved.CNRM-CERFACS_CNRM-CM5_r1i1p1.nc
    $ ncview tas_ONDJF_pointers.validate.197901.retrieved.CNRM-CERFACS_CNRM-CM5_r1i1p1.nc

BASH script
^^^^^^^^^^^
This recipe is summarized in the following BASH script::

    #!/bin/bash
    cat > tas_ONDJF.hdr <<EndOfHDR
    {
    "header":{
    "experiment_list":
        {
        "amip":"1979,2004"
        },
    "months_list": [1,2,10,11,12],
    "variable_list":
        {
        "tas":["day","atmos","day"],
        "orog":["fx","atmos","fx"]
        },
    "search_list":
        [
        "./in/CMIP5",
        "http://esgf-index1.ceda.ac.uk/esg-search/"
        ],
    "file_type_list":
        [
        "local_file",
        "HTTPServer"
        ]
    }
    }
    EndOfHDR
    #Make search dir otherwise result in error:
    mkdir -p ./in/CMIP5

    #Discover data:
    cdb_query_CMIP5 ask tas_ONDJF.hdr tas_ONDJF_pointers.nc

    #List simulations:
    cdb_query_CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                tas_ONDJF_pointers.nc

    #Find optimal set of simulations:
    cdb_query_CMIP5 validate tas_ONDJF_pointers.nc \
                             tas_ONDJF_pointers.validate.nc

    #List simulations:
    cdb_query_CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                tas_ONDJF_pointers.validate.nc

    #CHOOSE:
        # *1* Retrieve files:
            #cdb_query_CMIP5 download_raw \
            #                    tas_ONDJF_pointers.validate.nc \
            #                    ./in/CMIP5/

        # *2* Retrieve to netCDF:
            #Retrieve the first month:
            cdb_query_CMIP5 download --year=1979 --month=1 \
                                tas_ONDJF_pointers.validate.nc \
                                tas_ONDJF_pointers.validate.197901.retrieved.nc

            #Pick one simulation:
            ncks -G : -g /CNRM-CERFACS/CNRM-CM5/amip/day/atmos/day/r1i1p1/tas \
               tas_ONDJF_pointers.validate.197901.retrieved.nc \
               tas_ONDJF_pointers.validate.197901.retrieved.CNRM-CERFACS_CNRM-CM5_r1i1p1.nc
            
            #Look at it:
            #When done, look at it. A good tool for that is ncview:
            #   ncview tas_ONDJF_pointers.validate.197901.retrieved.CNRM-CERFACS_CNRM-CM5_r1i1p1.nc
