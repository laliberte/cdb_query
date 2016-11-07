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
NUM_PROCS=10

OUT_FILE="DJF_lat_band.nc"
OUT_DIR="out_lat_band/"

echo $PASSWORD_ESGF | cdb_query CMIP5 ask validate reduce_soft_links record_reduce_soft_links download_opendap reduce \
      --openid=$OPENID_ESGF \
      --password_from_pipe \
      --num_procs=$NUM_PROCS \
      --ask_experiment=historical:1979-2005,rcp85:2006-2015 \
      --ask_var=zg:mon-atmos-Amon,va:mon-atmos-Amon,ta:mon-atmos-Amon \
      --ask_month=1,2,12 \
      --related_experiments \
      --model=CCSM4 \
      --Xdata_node=http://esgf2.dkrz.de \
      --reduce_soft_links_script='ncrcat -d lat,55.0,65.0' \
      '' \
       --out_destination=${OUT_DIR} \
       ${OUT_FILE}

#Testing check: 
if [ $(cat ${OUT_FILE}.log | grep ERROR | wc -l) -gt 0 ]; then
    exit 1
fi
