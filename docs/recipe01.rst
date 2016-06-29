1. Retrieving surface temperature for ONDJF (CMIP5)
---------------------------------------------------

.. hint:: Don't forget to use the extensive command-line helps ``cdb_query -h``, ``cdb_query CMIP5 -h``, etc.

Discovering the data
^^^^^^^^^^^^^^^^^^^^
The script is run using::

    $ cdb_query CMIP5 ask \
                          --Month 1 2 10 11 12 \
                          --Var tas:day,atmos,day orog:fx,atmos,fx \
                          --Experiment amip:1979,2004 \
                          --num_procs=10 \
                          tas_ONDJF_pointers.nc 

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
    CNRM-CERFACS,CNRM-CM5,r0i0p0
    CNRM-CERFACS,CNRM-CM5,r1i1p1
    CSIRO-BOM,ACCESS1.0,r0i0p0
    CSIRO-BOM,ACCESS1.0,r1i1p1
    CSIRO-BOM,ACCESS1.3,r0i0p0
    CSIRO-BOM,ACCESS1.3,r1i1p1
    CSIRO-BOM,ACCESS1.3,r1i1p2
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r0i0p0
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r10i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r1i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r2i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r3i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r4i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r5i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r6i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r7i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r8i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r9i1p1
    INM,INM-CM4,r0i0p0
    INM,INM-CM4,r1i1p1
    IPSL,IPSL-CM5A-LR,r0i0p0
    IPSL,IPSL-CM5A-LR,r1i1p1
    IPSL,IPSL-CM5A-LR,r2i1p1
    IPSL,IPSL-CM5A-LR,r3i1p1
    IPSL,IPSL-CM5A-LR,r4i1p1
    IPSL,IPSL-CM5A-LR,r5i1p1
    IPSL,IPSL-CM5A-LR,r6i1p1
    IPSL,IPSL-CM5A-MR,r0i0p0
    IPSL,IPSL-CM5A-MR,r1i1p1
    IPSL,IPSL-CM5A-MR,r2i1p1
    IPSL,IPSL-CM5A-MR,r3i1p1
    LASG-CESS,FGOALS-g2,r0i0p0
    LASG-CESS,FGOALS-g2,r1i1p1
    MIROC,MIROC5,r0i0p0
    MIROC,MIROC5,r1i1p1
    MIROC,MIROC5,r2i1p1
    MOHC,HadGEM2-A,r0i0p0
    MOHC,HadGEM2-A,r1i1p1
    MOHC,HadGEM2-A,r1i2p1
    MOHC,HadGEM2-A,r2i3p1
    MOHC,HadGEM2-A,r3i2p1
    MOHC,HadGEM2-A,r4i3p1
    MOHC,HadGEM2-A,r5i3p1
    MPI-M,MPI-ESM-LR,r0i0p0
    MPI-M,MPI-ESM-LR,r1i1p1
    MPI-M,MPI-ESM-LR,r2i1p1
    MPI-M,MPI-ESM-LR,r3i1p1
    MPI-M,MPI-ESM-MR,r0i0p0
    MPI-M,MPI-ESM-MR,r1i1p1
    MPI-M,MPI-ESM-MR,r2i1p1
    MPI-M,MPI-ESM-MR,r3i1p1
    MRI,MRI-AGCM3.2H,r0i0p0
    MRI,MRI-AGCM3.2H,r1i1p1
    MRI,MRI-AGCM3.2H,r1i1p2
    MRI,MRI-AGCM3.2H,r1i1p3
    MRI,MRI-AGCM3.2S,r0i0p0
    MRI,MRI-AGCM3.2S,r1i1p1
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
    NCC,NorESM1-M,r0i0p0
    NCC,NorESM1-M,r1i1p1
    NCC,NorESM1-M,r2i1p1
    NCC,NorESM1-M,r3i1p1
    NOAA-GFDL,GFDL-HIRAM-C180,r0i0p0
    NOAA-GFDL,GFDL-HIRAM-C180,r1i1p1
    NOAA-GFDL,GFDL-HIRAM-C180,r2i1p1
    NOAA-GFDL,GFDL-HIRAM-C180,r3i1p1
    NOAA-GFDL,GFDL-HIRAM-C360,r0i0p0
    NOAA-GFDL,GFDL-HIRAM-C360,r1i1p1
    NOAA-GFDL,GFDL-HIRAM-C360,r2i1p1

This test was run on June 20, 2016 and these results represent the data presented by the ESGF node on that day.
The r0i0p0 ensemble name is the ensemble associated with fixed (time_frequency=fx) variables and its presence suggests that these three institutes have provided the requested orog variable.
These results also indicate that IPSL and MOHC have both provided six simulations. 

Note that EC-EARTH was identified as a potential model for our query but was not confirmed by ``ask``. This is most likely because
the r0i0p0 ensemble name was not identified as a potential ensemble member for EC-EARTH.

If this list of models is satisfying, we next check the paths  ::
    
    $ cdb_query CMIP5 list_fields -f path tas_ONDJF_pointers.nc
    http://esg.cnrm-game-meteo.fr/thredds/fileServer/CMIP5_CNRM/output1/CNRM-CERFACS/CNRM-CM5/amip/day/atmos/day/r1i1p1/v20111006/tas/tas_day_CNRM-CM5_amip_r1i1p1_19790101-19831231.nc|SHA256|d3568f8cbdfc6568c197b478a
    25bf2f89b3e8b3c764723977e342afd01238325|cc8d9375-f69f-486d-99b6-68ba16d5fe3d
    http://esg.cnrm-game-meteo.fr/thredds/fileServer/CMIP5_CNRM/output1/CNRM-CERFACS/CNRM-CM5/amip/day/atmos/day/r1i1p1/v20111006/tas/tas_day_CNRM-CM5_amip_r1i1p1_19840101-19881231.nc|SHA256|6e47187528d06adb2787d6d14
    b0959c5d07cb44cd6bf48681bbfed5b41ef32f1|973b547f-a290-4e10-811d-b56151fc1f07
    http://esg.cnrm-game-meteo.fr/thredds/fileServer/CMIP5_CNRM/output1/CNRM-CERFACS/CNRM-CM5/amip/day/atmos/day/r1i1p1/v20111006/tas/tas_day_CNRM-CM5_amip_r1i1p1_19890101-19931231.nc|SHA256|40d66394854b25c0fedd150e4
    7b655169278c492723b47aa18ce927973f286e0|b982096b-cf02-4f64-8181-5b12b65c6889
    http://esg.cnrm-game-meteo.fr/thredds/fileServer/CMIP5_CNRM/output1/CNRM-CERFACS/CNRM-CM5/amip/day/atmos/day/r1i1p1/v20111006/tas/tas_day_CNRM-CM5_amip_r1i1p1_19940101-19981231.nc|SHA256|c63d72d3869fbf4850229227f
    1f952241f75baf1b338c153b67c0569877115a2|507b2fa5-9d7b-4765-b755-68a7d4c96673
    http://esg.cnrm-game-meteo.fr/thredds/fileServer/CMIP5_CNRM/output1/CNRM-CERFACS/CNRM-CM5/amip/day/atmos/day/r1i1p1/v20111006/tas/tas_day_CNRM-CM5_amip_r1i1p1_19990101-20031231.nc|SHA256|08c35d8a7d218799b86dede28
    530f6b3435e8b37539fdea310a6b3b04d47ee41|efeae564-86ab-40c3-859d-044f78b8c777
    http://esg.cnrm-game-meteo.fr/thredds/fileServer/CMIP5_CNRM/output1/CNRM-CERFACS/CNRM-CM5/amip/day/atmos/day/r1i1p1/v20111006/tas/tas_day_CNRM-CM5_amip_r1i1p1_20040101-20081231.nc|SHA256|78da372f95d3a0372232b0044
    37c062715cec002dc09b9a45d64f8bdb882d844|0a4930ea-0d98-4913-bd83-a50fdf39695d
    http://esg.cnrm-game-meteo.fr/thredds/fileServer/CMIP5_CNRM/output1/CNRM-CERFACS/CNRM-CM5/amip/fx/atmos/fx/r0i0p0/v20111018/orog/orog_fx_CNRM-CM5_amip_r0i0p0.nc|SHA256|18b5fac1743e7ee7171412a53013d585d52071fdbf75
    79689ac8c37cbf244706|3da7988e-bb5a-428f-ae0c-c2b0585e767e
    http://esgf-data1.ceda.ac.uk/thredds/dodsC/esg_dataroot/cmip5/output1/CCCma/CanAM4/amip/day/atmos/day/r1i1p1/v2/tas/tas_day_CanAM4_amip_r1i1p1_19790101-20091231.nc|SHA256|2c6e198cdeb24ecf64368d29235e2a1bde0a4bfa2
    faa968581d9ec0ea6a89591|015de642-b49a-4463-bbf6-549a2d47cf69
    http://esgf-data1.ceda.ac.uk/thredds/dodsC/esg_dataroot/cmip5/output1/CCCma/CanAM4/amip/day/atmos/day/r2i1p1/v1/tas/tas_day_CanAM4_amip_r2i1p1_19790101-20091231.nc|SHA256|ad5d5f04aa213ad02e7bd72f204c5e535878eabd8
    981f35960a42bdb22e1264c|7f7dbf72-4ed9-4b0b-85b9-4dc30de7f805
    ...

We consider the first path. It is constituted of two parts. The first part begins with ``http://esg.cnrm-game-meteo.fr/...`` and 
ends a the vertical line. This is a `wget` link. The second part, separated by vertical lines, are the checksum typw, checksum and tracking id, respectively.
The checksum is as published on the EGSF website. The file found at the other end of the `wget` link can be
expected to have the same checksum.

The string that precedes ``/thredds/...`` in the `wget` link is the `data node`. Here, we have two data nodes: ``http://esg.cnrm-game-meteo.fr``
and ``http://esgf-data1.ceda.ac.uk``. Those are the adresses of the data node. Retrieving two files from two different data nodes at the same time should
therefore not hinder the transfer of one another.

.. hint::
    The command ``cdb_query CMIP5 ask`` does not guarantee that the simulations found satisfy ALL the requested criteria.

Validating the set of simulations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. warning::
    From now on it is assumed that the user has installed appropriate certificates to retrieve data from the ESGF CMIP5 archive.
    Simply passing ``--username=BADC_USERNAME`` will take care of this.
    
To narrow down our results to the simulations that satisfy ALL the requested criteria, we can use  ::

    $ cdb_query CMIP5 validate \
                --username=BADC_USERNAME \
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
    CNRM-CERFACS,CNRM-CM5,r0i0p0
    CNRM-CERFACS,CNRM-CM5,r1i1p1
    CSIRO-BOM,ACCESS1.0,r0i0p0
    CSIRO-BOM,ACCESS1.0,r1i1p1
    CSIRO-BOM,ACCESS1.3,r0i0p0
    CSIRO-BOM,ACCESS1.3,r1i1p1
    CSIRO-BOM,ACCESS1.3,r1i1p2
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r0i0p0
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r10i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r1i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r2i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r3i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r4i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r5i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r6i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r7i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r8i1p1
    CSIRO-QCCCE,CSIRO-Mk3.6.0,r9i1p1
    INM,INM-CM4,r0i0p0
    INM,INM-CM4,r1i1p1
    IPSL,IPSL-CM5A-LR,r0i0p0
    IPSL,IPSL-CM5A-LR,r1i1p1
    IPSL,IPSL-CM5A-LR,r2i1p1
    IPSL,IPSL-CM5A-LR,r3i1p1
    IPSL,IPSL-CM5A-LR,r4i1p1
    IPSL,IPSL-CM5A-LR,r5i1p1
    IPSL,IPSL-CM5A-LR,r6i1p1
    IPSL,IPSL-CM5A-MR,r0i0p0
    IPSL,IPSL-CM5A-MR,r1i1p1
    IPSL,IPSL-CM5A-MR,r2i1p1
    IPSL,IPSL-CM5A-MR,r3i1p1
    LASG-CESS,FGOALS-g2,r0i0p0
    LASG-CESS,FGOALS-g2,r1i1p1
    MIROC,MIROC5,r0i0p0
    MIROC,MIROC5,r1i1p1
    MIROC,MIROC5,r2i1p1
    MOHC,HadGEM2-A,r0i0p0
    MOHC,HadGEM2-A,r1i1p1
    MOHC,HadGEM2-A,r1i2p1
    MOHC,HadGEM2-A,r2i3p1
    MOHC,HadGEM2-A,r3i2p1
    MOHC,HadGEM2-A,r4i3p1
    MOHC,HadGEM2-A,r5i3p1
    MPI-M,MPI-ESM-LR,r0i0p0
    MPI-M,MPI-ESM-LR,r1i1p1
    MPI-M,MPI-ESM-LR,r2i1p1
    MPI-M,MPI-ESM-LR,r3i1p1
    MPI-M,MPI-ESM-MR,r0i0p0
    MPI-M,MPI-ESM-MR,r1i1p1
    MPI-M,MPI-ESM-MR,r2i1p1
    MPI-M,MPI-ESM-MR,r3i1p1
    MRI,MRI-AGCM3.2H,r0i0p0
    MRI,MRI-AGCM3.2H,r1i1p1
    MRI,MRI-AGCM3.2S,r0i0p0
    MRI,MRI-AGCM3.2S,r1i1p1
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
    NCC,NorESM1-M,r0i0p0
    NCC,NorESM1-M,r1i1p1
    NCC,NorESM1-M,r2i1p1
    NCC,NorESM1-M,r3i1p1
    NOAA-GFDL,GFDL-HIRAM-C180,r0i0p0
    NOAA-GFDL,GFDL-HIRAM-C180,r1i1p1
    NOAA-GFDL,GFDL-HIRAM-C360,r0i0p0
    NOAA-GFDL,GFDL-HIRAM-C360,r1i1p1

We can see that only some of the NOAA-GFDL simulations were excluded. 
This means that the other simulations they had ALL the variables for ALL the months of ALL the years for the amip
experiment.

Retrieving the data: `wget`
^^^^^^^^^^^^^^^^^^^^^^^^^^^

`cdb_query CMIP5` includes built-in functionality for retrieving the paths. It is used as follows ::

    $ cdb_query CMIP5 download_files \
                    --download_all_files \
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
                            --username=BADC_USERNAME \
                            tas_ONDJF_pointers.validate.nc \
                            tas_ONDJF_pointers.validate.197901.retrieved.nc 

The file ``tas_ONDJF_pointers.validate.197901.retrieved.nc`` should now contain the first thirty days for all experiments! To check the daily
surface temperature in the amip experiment from simulation CNRM-CERFACS,CNRM-CM5,r1i1p1 `ncview` (if installed)::

    $ ncks -G : -g /CNRM-CERFACS/CNRM-CM5/amip/day/atmos/day/r1i1p1/tas \
                    tas_ONDJF_pointers.validate.197901.retrieved.nc \
                    tas_ONDJF_pointers.validate.197901.retrieved.CNRM-CERFACS_CNRM-CM5_r1i1p1.nc
    $ ncview tas_ONDJF_pointers.validate.197901.retrieved.CNRM-CERFACS_CNRM-CM5_r1i1p1.nc

.. note::
    The ``ncks`` command can be slow. For some unknown reasons NCO version 4.5.3 and earlier with netCDF version 4.3.3.1 and earlier
    does not seem optimized for highly hierarchical files. At the moment, there are no indications that more recent versions have fixed
    this issue.

BASH script
^^^^^^^^^^^
This recipe is summarized in the following BASH script. The ``--password_from_pipe`` option is introduced to fully automatize the script::

    #!/bin/bash

    BADC_USERNAME="your badc username"
    BADC_PASSWORD="your badc password"
    #Discover data:
    cdb_query CMIP5 ask --Month 1 2 10 11 12 \
                          --Var tas:day,atmos,day orog:fx,atmos,fx \
                          --Experiment amip:1979,2004 \
                          --num_procs=10 \
                          tas_ONDJF_pointers.nc

    #List simulations:
    cdb_query CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                tas_ONDJF_pointers.nc

    #Find optimal set of simulations:
    echo $BADC_PASSWORD | cdb_query CMIP5 validate \
                             --username=$BADC_USERNAME \
                             --password_from_pipe \
                              --num_procs=10 \
                             tas_ONDJF_pointers.nc \
                             tas_ONDJF_pointers.validate.nc

    #List simulations:
    cdb_query CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                tas_ONDJF_pointers.validate.nc

    #CHOOSE:
        # *1* Retrieve files:
            #cdb_query CMIP5 download_files \
            #                    --download_all_files \
            #                    --username=BADC_USERNAME \
            #                    --out_download_dir=./in/CMIP5/ \
            #                    tas_ONDJF_pointers.validate.nc \
            #                    tas_ONDJF_pointers.validate.downloaded.nc 

        # *2* Retrieve to netCDF:
            #Retrieve the first month:
            echo $BADC_PASSWORD | cdb_query CMIP5 download_opendap --year=1979 --month=1 \
                                --username=$BADC_USERNAME \
                                --password_from_pipe \
                                tas_ONDJF_pointers.validate.nc \
                                tas_ONDJF_pointers.validate.197901.retrieved.nc

            #Pick one simulation:
            ncks -G : -g /CNRM-CERFACS/CNRM-CM5/amip/day/atmos/day/r1i1p1/tas \
               tas_ONDJF_pointers.validate.197901.retrieved.nc \
               tas_ONDJF_pointers.validate.197901.retrieved.CNRM-CERFACS_CNRM-CM5_r1i1p1.nc
            
            #Look at it:
            #When done, look at it. A good tool for that is ncview:
            #   ncview tas_ONDJF_pointers.validate.197901.retrieved.CNRM-CERFACS_CNRM-CM5_r1i1p1.nc

            #Convert hierarchical file to files on filesystem:
            #Identity reduction simply copies the data to disk
            cdb_query CMIP5 reduce \
                                '' \
                                --out_destination=./out/CMIP5/ \
                                tas_ONDJF_pointers.validate.197901.retrieved.nc \
                                tas_ONDJF_pointers.validate.197901.retrieved.converted.nc
