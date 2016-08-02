1. Retrieving surface temperature for ONDJF (CMIP5)
---------------------------------------------------

.. hint:: Don't forget to use the extensive command-line helps ``cdb_query -h``, ``cdb_query CMIP5 -h``, etc.

Discovering the data
^^^^^^^^^^^^^^^^^^^^
The script is run using::

    $ cdb_query CMIP5 ask \
                          --ask_month=1,2,10,11,12 \
                          --ask_var=tas:day-atmos-day,orog:fx-atmos-fx \
                          --ask_experiment=amip:1979-2004 \
                          --model=CanAM4 --model=CCSM4 --model=GISS-E2-R --model=MRI-CGCM3 \
                          --num_procs=10 \
                          tas_ONDJF_pointers.nc 
    This is a list of simulations that COULD satisfy the query:
    NCAR,CCSM4,r7i1p1,amip
    NCAR,CCSM4,r2i1p1,amip
    NCAR,CCSM4,r1i1p1,amip
    NCAR,CCSM4,r4i1p1,amip
    NCAR,CCSM4,r3i1p1,amip
    NCAR,CCSM4,r5i1p1,amip
    CCCMA,CanAM4,r2i1p1,amip
    CCCMA,CanAM4,r1i1p1,amip
    CCCMA,CanAM4,r4i1p1,amip
    CCCMA,CanAM4,r3i1p1,amip
    MRI,MRI-CGCM3,r4i1p2,amip
    MRI,MRI-CGCM3,r2i1p1,amip
    MRI,MRI-CGCM3,r1i1p1,amip
    MRI,MRI-CGCM3,r3i1p1,amip
    NASA-GISS,GISS-E2-R,r6i1p1,amip
    NASA-GISS,GISS-E2-R,r6i1p3,amip
    cdb_query will now attempt to confirm that these simulations have all the requested variables.
    This can take some time. Please abort if there are not enough simulations for your needs.

Obtaining the tentative list of simulations should be very quick (a few seconds to a minute) but confirming that these simulations have all the requested
variables should take a few minutes, depending on your connection to the ESGF node. It returns a self-descriptive netCDF4 file 
with pointers to the data. The ``num_procs`` flag substantially speeds up the discovery, and comes with a very small price to the user.

.. hint:: Try looking at the resulting netCDF file using ``ncdump``: ::

    $ ncdump -h tas_ONDJF_pointers.nc

As you can see, it generates a hierarchical netCDF4 file. ``cdb_query CMIP5 list_fields`` offer a tool to query this file. 

Querying the discovered data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
For example, if we want to know how many different simulations were made available, we would run ::

    $ cdb_query CMIP5 list_fields -f institute -f model -f ensemble tas_ONDJF_pointers.nc
    CCCMA,CanAM4,r0i0p0
    CCCMA,CanAM4,r1i1p1
    CCCMA,CanAM4,r2i1p1
    CCCMA,CanAM4,r3i1p1
    CCCMA,CanAM4,r4i1p1
    MRI,MRI-CGCM3,r0i0p0
    MRI,MRI-CGCM3,r1i1p1
    MRI,MRI-CGCM3,r2i1p1
    MRI,MRI-CGCM3,r3i1p1
    NCAR,CCSM4,r0i0p0
    NCAR,CCSM4,r1i1p1
    NCAR,CCSM4,r2i1p1
    NCAR,CCSM4,r3i1p1
    NCAR,CCSM4,r4i1p1
    NCAR,CCSM4,r5i1p1
    NCAR,CCSM4,r7i1p1

This test was run on July 2nd, 2016 and these results represent the data presented by the ESGF node on that day.
The r0i0p0 ensemble name is the ensemble associated with fixed (time_frequency=fx) variables and its presence suggests that these three institutes have provided the requested orog variable.
These results also indicate that models CanAM4, MRI-CGCM3 and CCSM4 provided 4, 3 and 6 simulations,respectively.

If this list of models is satisfying, we next check the paths  ::
    
    $ cdb_query CMIP5 list_fields -f path tas_ONDJF_pointers.nc
    ...
    http://esgf-data1.ceda.ac.uk/thredds/fileServer/esg_dataroot/cmip5/output1/NCAR/CCSM4/amip/fx/atmos/fx/r0i0p0/v20130312/orog/orog_fx_CCSM4_amip_
    r0i0p0.nc|SHA256|87b29a7d2731e6b028d81b07edbe84c3f06e1321986401482f8c5d76d5361516|2b43ce02-7124-40bd-8ae4-0961e399e9ec
    http://esgf-data1.diasjp.net/thredds/fileServer/esg_dataroot/cmip5/output1/MRI/MRI-CGCM3/amip/day/atmos/day/r1i1p1/v20120701/tas/tas_day_MRI-CGCM3_am
    ip_r1i1p1_19790101-19881231.nc|SHA256|804f3325a2b0e29bad14e5773a7216c2893a5200fba62ffa83db992b9765b283|e95dd229-1e36-4e8a-9e50-8797e2a136a2
    ...

We consider the first path. It is constituted of two parts. The first part begins with ``http://esgf-data1.ceda.ac.uk/...`` and 
ends a the vertical line. This is a `wget` link. The second part, separated by vertical lines, are the checksum typw, checksum and tracking id, respectively.
The checksum is as published on the EGSF website. The file found at the other end of the `wget` link can be
expected to have the same checksum.

The string that precedes ``/thredds/...`` in the `wget` link is the `data node`. Here, we have two data nodes: 
``http://esgf-data1.ceda.ac.uk`` and ``http://esgf-data1.diasjp.net``. Retrieving two files from two different data nodes at the same time should
not hinder the transfer of one another.

.. hint::
    The command ``cdb_query CMIP5 ask`` does not guarantee that the simulations found satisfy ALL the requested criteria.

Validating the set of simulations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. warning::
    From now on it is assumed that the user has installed properly resgistered with the ESGF.
    Using the ``--openid`` option combined with an ESGF account takes care of this.
    The first time this function is used, it might fail and ask you to register your kind of user.
    This has to be done only once.
    
To narrow down our results to the simulations that satisfy ALL the requested criteria, we can use  ::

    $ cdb_query CMIP5 validate \
                --openid=$OPENID \
                --Xdata_node=http://esgf2.dkrz.de \
                --num_procs=10 \
                tas_ONDJF_pointers.nc \
                tas_ONDJF_pointers.validate.nc

Here, we are removing data node ``http://esgf2.dkrz.de`` from the validation because on this data node, data sits on a tape archive and
it can be very slow to recover it.

To output now has a time axis for each variable (except fx). It links every time index to a time index in a UNIQUE file (remote or local).
Try looking at the resulting netCDF file using ``ncdump``: ::

    $ ncdump -h tas_ONDJF_pointers.validate.nc

Again, this file can be queried for simulations::

    $ cdb_query CMIP5 list_fields -f institute -f model -f ensemble tas_ONDJF_pointers.validate.nc
    CCCMA,CanAM4,r0i0p0
    CCCMA,CanAM4,r1i1p1
    CCCMA,CanAM4,r2i1p1
    CCCMA,CanAM4,r3i1p1
    CCCMA,CanAM4,r4i1p1
    MRI,MRI-CGCM3,r0i0p0
    MRI,MRI-CGCM3,r1i1p1
    MRI,MRI-CGCM3,r2i1p1
    MRI,MRI-CGCM3,r3i1p1
    NCAR,CCSM4,r0i0p0
    NCAR,CCSM4,r1i1p1
    NCAR,CCSM4,r2i1p1
    NCAR,CCSM4,r3i1p1
    NCAR,CCSM4,r4i1p1
    NCAR,CCSM4,r5i1p1
    NCAR,CCSM4,r7i1p1

Retrieving the data: `wget`
^^^^^^^^^^^^^^^^^^^^^^^^^^^

`cdb_query CMIP5` includes built-in functionality for retrieving the paths. It is used as follows ::

    $ cdb_query CMIP5 download_files \
                    --download_all_files \
                    --openid=$OPENID \
                    --out_download_dir=./in/CMIP5/ \
                    tas_ONDJF_pointers.validate.nc \
                    tas_ONDJF_pointers.validate.downloaded.nc

It downloads the paths listed in ``tas_ONDJF_pointers.validate.nc`` to ``./in/CMIP5/`` and records the soft links to the local data in ``tas_ONDJF_pointers.validate.downloaded.nc``.

.. warning:: The retrieved files are structured with the CMIP5 DRS. It is good practice not to change this directory structure.
             If the structure is kept then ``cdb_query CMIP5 ask`` will recognize the retrieved files as local if they were
             retrieved to a directory listed in the ``Search_path``.

The downloaded paths are now discoverable by ``cdb_query CMIP5 ask``.

Retrieving the data: `OPeNDAP`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`cdb_query CMIP5` includes built-in functionality for retrieving a subset of the data.

To retrieve the first month of daily data::
    
    $ cdb_query CMIP5 download_opendap \
                            --year=1979 \
                            --month=1 \
                            --openid=$OPENID \
                            tas_ONDJF_pointers.validate.nc \
                            tas_ONDJF_pointers.validate.197901.retrieved.nc 

The file ``tas_ONDJF_pointers.validate.197901.retrieved.nc`` should now contain the first thirty days for all experiments! To check the daily
surface temperature in the amip experiment from simulation NCAR,CCSM4,r1i1p1 `ncview` (if installed)::

    $ ncks -G : -g /NCAR/CCSM4/amip/day/atmos/day/r1i1p1/tas \
                    tas_ONDJF_pointers.validate.197901.retrieved.nc \
                    tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc
    $ ncview tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc

.. note::
    The ``ncks`` command can be slow. For some unknown reasons NCO version 4.5.3 and earlier with netCDF version 4.3.3.1 and earlier
    does not seem optimized for highly hierarchical files. At the moment, there are no indications that more recent versions have fixed
    this issue.

BASH script
^^^^^^^^^^^
This recipe is summarized in the following BASH script. The ``--password_from_pipe`` option is introduced to fully automatize the script::

    #!/bin/bash

    OPENID="your openid"
    PASSWORD="your ESGF password"
    #Discover data:
    cdb_query CMIP5 ask --ask_month=1,2,10,11,12 \
                        --ask_var=tas:day-atmos-day,orog:fx-atmos-fx \
                        --ask_experiment=amip:1979-2004 \
                        --model=CanAM4 --model=CCSM4 --model=GISS-E2-R --model=MRI-CGCM3 \
                        --num_procs=10 \
                        tas_ONDJF_pointers.nc

    #List simulations:
    cdb_query CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                tas_ONDJF_pointers.nc

    #Validate simulations:
    #Exclude data_node http://esgf2.dkrz.de because it is on a tape archive (slow)
    #If you do not exclude it, it will likely be excluded because of its slow
    #response time.
    #
    #The first time this function is used, it might fail and ask you to register your kind of user.
    #This has to be done only once.
    echo $PASSWORD | cdb_query CMIP5 validate \
                                --openid=$OPENID \
                                --password_from_pipe \
                                --num_procs=10 \
                                --Xdata_node=http://esgf2.dkrz.de \
                                tas_ONDJF_pointers.nc \
                                tas_ONDJF_pointers.validate.nc

    #List simulations:
    cdb_query CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                tas_ONDJF_pointers.validate.nc

    #CHOOSE:
        # *1* Retrieve files:
            #echo $PASSWORD | cdb_query CMIP5 download_files \
            #                    --download_all_files \
            #                    --openid=$OPENID \
            #                    --password_from_pipe \
            #                    --out_download_dir=./in/CMIP5/ \
            #                    tas_ONDJF_pointers.validate.nc \
            #                    tas_ONDJF_pointers.validate.downloaded.nc 

        # *2* Retrieve to netCDF:
            #Retrieve the first month:
            echo $PASSWORD | cdb_query CMIP5 download_opendap --year=1979 --month=1 \
                                --openid=$OPENID \
                                --password_from_pipe \
                                tas_ONDJF_pointers.validate.nc \
                                tas_ONDJF_pointers.validate.197901.retrieved.nc

            #Pick one simulation:
            #Note: this can be VERY slow!
            ncks -G :8 -g /NCAR/CCSM4/amip/day/atmos/day/r1i1p1/tas \
               tas_ONDJF_pointers.validate.197901.retrieved.nc \
               tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc
            #Remove soft_links informations:
            ncks -L 0 -O -x -g soft_links \
               tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc \
               tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc
            
            #Look at it:
            #When done, look at it. A good tool for that is ncview:
            #   ncview tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc

            #Convert hierarchical file to files on filesystem (much faster than ncks):
            #Identity reduction simply copies the data to disk
            cdb_query CMIP5 reduce \
                                '' \
                                --out_destination=./out/CMIP5/ \
                                tas_ONDJF_pointers.validate.197901.retrieved.nc \
                                tas_ONDJF_pointers.validate.197901.retrieved.converted.nc

            #The files can be found in ./out/CMIP5/:
            #find ./out/CMIP5/ -name '*.nc'
