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

echo $PASSWORD_ESGF | cdb_query CORDEX ask validate record_validate \
                                  reduce_soft_links record_reduce_soft_links \
                                  download_opendap reduce \
                  --log_files \
                  --debug \
                  --timeout=$TIMEOUT \
                  --ask_experiment=historical:1979-2004 \
                  --ask_var=pr:day \
                  --ask_month=6,7,8,9 \
                  --openid=$OPENID_ESGF \
                  --password_from_pipe \
                  --year=1979 --month=6 \
                  --domain=EUR-11 \
                  --driving_model=ICHEC-EC-EARTH \
                  --institute=KNMI \
                  --ensemble=r1i1p1 \
                  --out_destination=./out_France/CORDEX/ \
                  --Xdata_node=http://esgf2.dkrz.de \
                  --num_procs=3 \
                  --reduce_soft_links_script='nc4sl subset --lonlatbox -5.0 10.0 40.0 53.0' \
                  '' \
                  pr_JJAS_France_pointers.validate.France.retrieved.converted.nc
#Testing check: 
inspectlogs pr_JJAS_France_pointers.validate.France.retrieved.converted.nc
rm pr_JJAS_France_pointers.validate.France.retrieved.converted.nc
rm -r ./out_France

#Test only CORDEX for efficiency (i.e. can skip recip03.sh):

if [ $1 != "test" ]; then
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
fi
rm -r ./out
