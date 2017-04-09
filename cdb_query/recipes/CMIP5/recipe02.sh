#!/bin/bash
TIMEOUT=15

function inspectlogs {
    if [ ! -f $1 ]; then
        exit 1
    fi
    if [ ! -f $1.log ]; then
        exit 1
    fi
    if [ $(cat $1.log | grep ERROR | wc -l) -gt 0 ]; then
        cat $1.log | grep ERROR
        exit 1
    fi
}


echo $PASSWORD_ESGF | cdb_query CMIP5 ask validate record_validate download_opendap reduce \
                  --log_files \
                  --debug \
                  --timeout=$TIMEOUT \
                  --ask_month=1,2,10,11,12 \
                  --ask_var=tas:day-atmos-day,orog:fx-atmos-fx \
                  --ask_experiment=amip:1979-2004 \
                  --Xdata_node=http://esgf2.dkrz.de \
                  --openid=$OPENID_ESGF \
                  --password_from_pipe \
                  --year=1979 --month=1 \
                  --model=CanAM4 --model=CCSM4 --model=GISS-E2-R --model=MRI-CGCM3 \
                  --ensemble=r1i1p1 \
                  --Xdata_node=http://esgf-data1.ceda.ac.uk \
                  --out_destination=./out/CMIP5/ \
                  --num_procs=3 \
                  '' \
                  tas_ONDJF_pointers.validate.197901.retrieved.converted.nc
#Testing check: 
inspectlogs tas_ONDJF_pointers.validate.197901.retrieved.converted.nc
rm tas_ONDJF_pointers.validate.197901.retrieved.converted.nc
rm -r ./out
