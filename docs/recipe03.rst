3. Retrieving precipitation for JJAS over France (CORDEX)
---------------------------------------------------------

Specifying the discovery
^^^^^^^^^^^^^^^^^^^^^^^^

This relies on the idea that all queries are for a experiment list and a variable list. The CORDEX project
has however another important component that one might want to query: its domain. The first step is thus
to find what domains are available ::

    $ cdb_query CORDEX ask --ask_experiment=historical:1979-2004 \
                           --ask_var=pr:day \
                           --ask_month=6,7,8,9 \
                           --list_only_field=domain \
                           pr_JJAS_France_pointers.nc
    MNA-44
    EAS-44
    SAM-44
    MNA-22
    WAS-44i
    ANT-44
    EUR-44
    CAM-44
    EUR-11
    ARC-44
    AFR-44
    WAS-44
    NAM-44

Here the ``--list_only_field=domain`` option lists all available domains. The result is an (unsorted) list of domain
identifiers. The European domains (``EUR-11`` and ``EUR-44``) are what we want. For the sake of this recipe,
we are going to limit our discovery to the higher resolution data ``EUR-11``

Discovering the data
^^^^^^^^^^^^^^^^^^^^
The script is run using::

    $ cdb_query CORDEX ask --ask_experiment=historical:1979-2004 \
                           --ask_var=pr:day \
                           --month=6,7,8,9 \
                           --domain=EUR-11 \
                           --driving_model=ICHEC-EC-EARTH \
                           --num_procs=10 \
                           pr_JJAS_France_pointers.nc
    This is a list of simulations that COULD satisfy the query:
    EUR-11,DMI,ICHEC-EC-EARTH,HIRHAM5,v1,r3i1p1,historical
    EUR-11,CLMcom,ICHEC-EC-EARTH,CCLM4-8-17,v1,r12i1p1,historical
    EUR-11,KNMI,ICHEC-EC-EARTH,RACMO22E,v1,r1i1p1,historical
    EUR-11,SMHI,ICHEC-EC-EARTH,RCA4,v1,r12i1p1,historical
    cdb_query will now attempt to confirm that these simulations have all the requested variables.
    This can take some time. Please abort if there are not enough simulations for your needs.

Obtaining the tentative list of simulations can take a few minutes but confirming that these simulations have all the requested
variables should take a few minutes, depending on your connection to the ESGF IPSL node. It returns a self-descriptive netCDF file 
with pointers to the data. Try looking at the resulting netCDF file using ``ncdump``: ::

    $ ncdump -h pr_JJAS_France_pointers.nc

As you can see, it generates a hierarchical netCDF4 file. ``cdb_query CORDEX list_fields`` offer a tool to query this file. 

Querying the discovered data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
For example, if we want to know how many different simulations were made available, we would run ::
    
    $ cdb_query CORDEX list_fields -f domain -f driving_model -f institute \
                                   -f rcm_model -f rcm_version -f ensemble pr_JJAS_France_pointers.nc
    EUR-11,ICHEC-EC-EARTH,CLMcom,CCLM4-8-17,v1,r12i1p1
    EUR-11,ICHEC-EC-EARTH,DMI,HIRHAM5,v1,r3i1p1
    EUR-11,ICHEC-EC-EARTH,KNMI,RACMO22E,v1,r1i1p1
    EUR-11,ICHEC-EC-EARTH,SMHI,RCA4,v1,r12i1p1

This test was run on June 23, 2016 and these results represent the data presented by the ESGF on that day.

If this list of models in satisfying, we next check the paths  ::
    
    $ cdb_query CORDEX list_fields -f path pr_JJAS_France_pointers.nc
    http://cordexesg.dmi.dk/thredds/dodsC/cordex_general/cordex/output/EUR-11/DMI/ICHEC-EC-EARTH/historical/r3i1p1/DMI-HIRHAM5/v1/day/pr/v20131119/pr_EUR-11_ICHEC-EC-EARTH_historical_r3i1p1_DMI-HIRHAM5_v1_day_19510101-19551231.nc|SHA256|d172a848bfaa24db89c5f550046c8dfc789e61f5b81c6d9ea21709c70b17eff7|d2d75739-4023-446a-a834-c111daf6d970
    ...

We consider the first path. It is constituted of two parts. The first part begins with ``http://esgf-node.ipsl.fr/...`` and 
ends a the vertical line. This is an `OPENDAP` link. The second part, at the right of the vertical line, is the checksum type, the checksum and the tracking id.

.. hint::
    The command ``cdb_query CORDEX ask`` does not guarantee that the simulations found satisfy ALL the requested criteria.

Validating the simulations
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. warning::
    From now on it is assumed that the user has installed properly resgistered with the ESGF.
    Using the ``--openid`` option combined with an ESGF account takes care of this.
    
To narrow down our results to the simulations that satisfy ALL the requested criteria, we can use  ::

        $ cdb_query CORDEX validate \
                        --openid=$OPENID \
                        --num_procs=10 \
                        pr_JJAS_France_pointers.nc \
                        pr_JJAS_France_pointers.validate.nc

To output now has a time axis for each variable (except fx). It links every time index to a time index in a UNIQUE file (remote or local).
Try looking at the resulting netCDF file using ``ncdump``: ::

    $ ncdump -h pr_JJAS_France_pointers.validate.nc

Again, this file can be queried for simulations::

    $ cdb_query CORDEX list_fields -f domain -f driving_model -f institute \
                                   -f rcm_model -f rcm_version -f ensemble pr_JJAS_France_pointers.validate.nc
    EUR-11,ICHEC-EC-EARTH,CLMcom,CCLM4-8-17,v1,r12i1p1
    EUR-11,ICHEC-EC-EARTH,DMI,HIRHAM5,v1,r3i1p1
    EUR-11,ICHEC-EC-EARTH,KNMI,RACMO22E,v1,r1i1p1
    EUR-11,ICHEC-EC-EARTH,SMHI,RCA4,v1,r12i1p1

We can see that no simulations were excluded. This means that they had ALL the variables for ALL the months of ALL the years for the historical
experiment.

Retrieving the data: `wget`
^^^^^^^^^^^^^^^^^^^^^^^^^^^

`cdb_query CORDEX` includes built-in functionality for retrieving the paths. It is used as follows ::

    $ cdb_query CORDEX download_files --out_download_dir=./in/CMIP5/ \
                                    --openid=$OPENID \
                                    --download_all_files \
                                    pr_JJAS_France_pointers.validate.nc \
                                    pr_JJAS_France_pointers.validate.files.nc

It downloads the paths listed in ``pr_JJAS_France_pointers.validate.nc`` and create a new
soft links file ``pr_JJAS_France_pointers.validate.files.nc`` with the downloaded path registered.

.. warning:: The retrieved files are structured with the CORDEX DRS. It is good practice not to change this directory structure.
             If the structure is kept then ``cdb_query CORDEX ask`` will recognized the retrieved files as local if they were
             retrieved to a directory listed in the ``--Search_path``.

The downloaded paths are now discoverable by ``cdb_query CORDEX ask``.

Retrieving the data: `OPeNDAP`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We retrieve the first month::

    $ cdb_query CORDEX download_opendap --year=1979 --month=6 \
                                   --openid=$OPENID \
                                   pr_JJAS_France_pointers.validate.nc \
                                   pr_JJAS_France_pointers.validate.197906.retrieved.nc 

This step took about 4 minutes from the University of Toronto on June 23, 2016. Next, we extract precipitation for the simulation with the EUR-11 domain::

    $ ncks -G :9 -g /EUR-11/IPSL-INERIS/IPSL-IPSL-CM5A-MR/historical/r1i1p1/WRF331F/v1/day/pr \
                    pr_JJAS_France_pointers.validate.197906.retrieved.nc \
                    pr_JJAS_France_pointers.validate.197906.retrieved.EUR-11.nc
    $ ncview pr_JJAS_France_pointers.validate.197906.retrieved.EUR-11.nc

.. hint:: This file contains a ``soft_links`` subgroup that contains full traceability informations for the accompyning data.

This data is projected onto a rotated pole grid, making it difficult to zoom in onto France by using slices along dimensions.
Sever tools can be used to zoom in even with a rotated pole grid. With `CDO`, one would do::
    
    $ cdo -f nc -sellonlatbox,-5.0,10.0,40.0,53.0 -selgrid,curvilinear,gaussian,lonlat \
                            pr_JJAS_France_pointers.validate.197906.retrieved.EUR-11.nc \
                            pr_JJAS_France_pointers.validate.197906.retrieved.EUR-11_France.nc

Alternatively, bundled with ``cdb_query`` there is a simple tool that can accomplish this::

    $ nc4sl subset --lonlatbox -5.0 10.0 40.0 53.0 \
                            pr_JJAS_France_pointers.validate.197906.retrieved.EUR-11.nc \
                            pr_JJAS_France_pointers.validate.197906.retrieved.EUR-11_France.nc

We can make sure that our subsetting was ok::
    
    $ ncview pr_JJAS_France_pointers.validate.197906.retrieved.EUR-11_France.nc

Subsetting the data BEFORE the `OPENDAP` retrieval
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We can subset the soft link file before using ``download_opendap`` and ``cdb_query`` will only download
the requested data::

    $ nc4sl subset --lonlatbox -5.0 10.0 40.0 53.0 \
                            pr_JJAS_France_pointers.validate.nc \
                            pr_JJAS_France_pointers.validate.France.nc
                            
or, using ``reduce_soft_links``::

    $ cdb_query CORDEX reduce_soft_links \
                                --num_procs=10 \
                                'nc4sl subset --lonlatbox -5.0 10.0 40.0 53.0' \
                                pr_JJAS_France_pointers.validate.nc \
                                pr_JJAS_France_pointers.validate.France.nc

In the second method, the subsetting can be performed asynchronously (``--num_procs=10``).
Finally, we retrieve the subsetted data::
    
    $ cdb_query CORDEX download_opendap --year=1979 --month=6 \
                                   --openid=$OPENID \
                                   pr_JJAS_France_pointers.validate.France.nc \
                                   pr_JJAS_France_pointers.validate.France.197906.retrieved.nc 

This step took about 3m40s from the University of Toronto. It retrieves all models but only over France.
We can then check the variables::

    $ ncks -G :9 -g /EUR-11/IPSL-INERIS/IPSL-IPSL-CM5A-MR/historical/r1i1p1/WRF331F/v1/day/pr \
                    pr_JJAS_France_pointers.validate.France.197906.retrieved.nc \
                    pr_JJAS_France_pointers.validate.France.197906.retrieved.EUR-11.nc
    $ ncview pr_JJAS_France_pointers.validate.France.197906.retrieved.EUR-11.nc

Should show precipitation over France in June 1979. 

The amount of time required for the download is not substantially improved for single month but they are for longer retrievals::

    $ time cdb_query CORDEX download_opendap --month=6  \
                                             --openid=$OPENID\
                                             pr_JJAS_France_pointers.validate.France.nc \
                                             pr_JJAS_France_pointers.validate.France.June.retrieved.nc
    real    25m28.268s
    user    14m25.368s
    sys 3m18.299s
    $ time cdb_query CORDEX download_opendap --month=6  \
                                             --openid=$OPENID\
                                             pr_JJAS_France_pointers.validate.nc \
                                             pr_JJAS_France_pointers.validate.June.retrieved.nc
    real    43m45.656s
    user    21m59.345s
    sys 8m53.251s

BASH script
^^^^^^^^^^^
This recipe is summarized in the following BASH script::

    #!/bin/bash
    #Change to set number of processes to use:
    NUM_PROCS=10
    #Specify your OPENID
    OPENID="your openid"
    PASSWORD="your ESGF password"

    #Discover data:
    cdb_query CORDEX ask --ask_experiment=historical:1979,2004 \
                         --ask_var=pr:day \
                         --domain=EUR-11 \
                         --num_procs=${NUM_PROCS} \
                         pr_JJAS_France_pointers.nc 

    #List simulations:
    cdb_query CORDEX list_fields -f domain -f driving_model -f institute \
                               -f rcm_model -f rcm_version -f ensemble pr_JJAS_France_pointers.nc

    #Validate simulations:
    #Exclude data_node http://esgf2.dkrz.de because it is on a tape archive (slow)
    #If you do not exclude it, it will likely be excluded because of its slow
    echo $PASSWORD | cdb_query CORDEX validate \
                --openid=$OPENID \
                --password_from_pipe \
                --num_procs=${NUM_PROCS} \
                --Xdata_node=http://esgf2.dkrz.de \
                pr_JJAS_France_pointers.nc \
                pr_JJAS_France_pointers.validate.nc
    #CHOOSE:
        # *1* Retrieve files:
            #echo $PASSWORD | cdb_query CORDEX download_files \ 
            #                    --out_download_dir=./in/CMIP5/ \
            #                    --openid=$OPENID \
            #                    --download_all_files \
            #                    --password_from_pipe \
            #                    pr_JJAS_France_pointers.validate.nc \
            #                    pr_JJAS_France_pointers.validate.files.nc

        # *2* Retrieve to netCDF:
            #Retrieve one month:
            echo $PASSWORD | cdb_query CORDEX download_opendap --year=1979 --month=6 \
                               --openid=$OPENID \
                               --password_from_pipe \
                               pr_JJAS_France_pointers.validate.nc \
                               pr_JJAS_France_pointers.validate.197906.retrieved.nc
            
            #Convert to filesystem:
            cdb_query CORDEX reduce --out_destination=./out/CORDEX/ '' \
                                    pr_JJAS_France_pointers.validate.197906.retrieved.nc \
                                    pr_JJAS_France_pointers.validate.197906.retrieved.converted.nc 

            #Subset France on soft_links:
            cdb_query CORDEX reduce_soft_links \
                            --num_procs=${NUM_PROCS} \
                            'nc4sl subset --lonlatbox -5.0 10.0 40.0 53.0' \
                            pr_JJAS_France_pointers.validate.nc \
                            pr_JJAS_France_pointers.validate.France.nc

            #We then retrieve the whole time series over France:
            echo $PASSWORD | cdb_query CORDEX download_opendap \
                                 --openid=$OPENID \
                                 --password_from_pipe \
                                 pr_JJAS_France_pointers.validate.France.nc \
                                 pr_JJAS_France_pointers.validate.France.retrieved.nc

            #Convert to filesystem:
            cdb_query CORDEX reduce --out_destination=./out_France/CORDEX/ \
                                    --num_procs=${NUM_PROCS} \
                                     '' \
                                     pr_JJAS_France_pointers.validate.France.retrieved.nc
                                     pr_JJAS_France_pointers.validate.France.retrieved.converted.nc

