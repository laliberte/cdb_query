6. Retrieving DJF monthly atmospheric data over a latitude band (CMIP5)
-----------------------------------------------------------------------

The following BASH script recovers several variables over a latitude band::

    #!/bin/bash

    #This script discovers and retrieves the geopotential height (zg), meridional wind (va) and
    #atmospheric temperature (ta) at the monthly frequency (mon) from the atmospheric realm (atmos)
    #and from monthly atmospheric mean CMOR table (Amon) for years 1979 to 2005 of experiment
    #historical and years 2006 to 2015 for experiment rcp85.
    #
    #A ramdisk (/dev/shm/) swap directory is used (--swap_dir option)
    #Data node http://esgf2.dkrz.de is excluded because it is a tape archive
    #(and therefore too slow for the type of multiple concurrent requests that are required)
    #
    #The data is reduced to a latitude band (55.0 to 60.0) using the 
    #--reduce_soft_links_script='ncrcat -d lat 55.0 60.0' option and the reduce_soft_links command.

    #The results are stored in:
    #   1) a validate file (${OUT_FILE}.validate), 
    #   2) a directory tree under ${OUT_DIR} and
    #   3) a pointer file ${OUT_FILE} that can be used in a further reduce step.

    #Use 5 processors:
    NUM_PROCS=5
    OPENID='your openid'
    # Single quotes are necessary here:
    PASSWORD='your ESGF password'

    SWAP_DIR="/dev/shm/lat_band/"
    OUT_FILE="DJF_lat_band.nc"
    OUT_DIR="out_lat_band/"

    #Create swap directory:
    mkdir ${SWAP_DIR}
    echo $PASSWORD | cdb_query CMIP5 ask validate record_validate reduce_soft_links download_opendap reduce \
          --openid=$OPENID \
          --password_from_pipe \
          --swap_dir=${SWAP_DIR} \
          --num_procs=$NUM_PROCS \
          --ask_experiment=historical:1979-2005,rcp85:2006-2015 \
          --ask_var=zg:mon-atmos-Amon,va:mon-atmos-Amon,ta:mon-atmos-Amon \
          --ask_month=1,2,12 \
          --related_experiments \
          --Xdata_node=http://esgf2.dkrz.de \
          --reduce_soft_links_script='ncrcat -d lat,55.0,65.0' \
          '' \
           --out_destination=${OUT_DIR} \
           ${OUT_FILE}
    #Remove swap directory:
    rm -r ${SWAP_DIR}
