#!/bin/bash


#Change to set number of processes to use:
NUM_PROCS=10

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

if [ $1 != "test" ]; then
    #Discover data:
    cdb_query CORDEX ask \
                         --debug \
                         --log_files \
                         --ask_experiment=historical:1979-2004 \
                         --ask_var=pr:day \
                         --domain=EUR-11 \
                         --driving_model=ICHEC-EC-EARTH \
                         --institute=KNMI \
                         --rcm_model=RACMO22E \
                         --rcm_version=v1 \
                         --ensemble=r1i1p1 \
                         --num_procs=${NUM_PROCS} \
                         pr_JJAS_France_pointers.nc
    #Testing check: 
    inspectlogs pr_JJAS_France_pointers.nc

    #List simulations:
    cdb_query CORDEX list_fields -f domain -f driving_model -f institute \
                               -f rcm_model -f rcm_version -f ensemble pr_JJAS_France_pointers.nc

    #Validate simulations:
    #Exclude data_node http://esgf2.dkrz.de because it is on a tape archive (slow)
    #If you do not exclude it, it will likely be excluded because of its slow
    echo $PASSWORD_ESGF | cdb_query CORDEX validate \
                --debug \
                --log_files \
                --openid=$OPENID_ESGF \
                --password_from_pipe \
                --num_procs=${NUM_PROCS} \
                --Xdata_node=http://esgf2.dkrz.de \
                pr_JJAS_France_pointers.nc \
                pr_JJAS_France_pointers.validate.nc

    #Testing check: 
    inspectlogs pr_JJAS_France_pointers.validate.nc

    #CHOOSE:
    # *1* Retrieve files:
        #echo $PASSWORD_ESGF | cdb_query CORDEX download_files \
        #                    --out_download_dir=./in/CMIP5/ \
        #                    --openid=$OPENID_ESGF \
        #                    --download_all_files \
        #                    --password_from_pipe \
        #                    pr_JJAS_France_pointers.validate.nc \
        #                    pr_JJAS_France_pointers.validate.files.nc

    # *2* Retrieve to netCDF:
        #Retrieve one month:
        echo $PASSWORD_ESGF | cdb_query CORDEX download_opendap --year=1979 --month=6 \
                           --log_files \
                           --debug \
                           --openid=$OPENID_ESGF \
                           --password_from_pipe \
                           pr_JJAS_France_pointers.validate.nc \
                           pr_JJAS_France_pointers.validate.197906.retrieved.nc
        #Testing check: 
        inspectlogs pr_JJAS_France_pointers.validate.197906.retrieved.nc

        #Convert to filesystem:
        cdb_query CORDEX reduce --out_destination=./out/CORDEX/ '' \
                                --log_files \
                                --debug \
                                pr_JJAS_France_pointers.validate.197906.retrieved.nc \
                                pr_JJAS_France_pointers.validate.197906.retrieved.converted.nc
        #Testing check: 
        inspectlogs pr_JJAS_France_pointers.validate.197906.retrieved.converted.nc


        #Subset France on soft_links:
        cdb_query CORDEX reduce_soft_links \
                        --log_files \
                        --debug \
                        --num_procs=${NUM_PROCS} \
                        'nc4sl subset --lonlatbox -5.0 10.0 40.0 53.0' \
                        pr_JJAS_France_pointers.validate.nc \
                        pr_JJAS_France_pointers.validate.France.nc
        #Testing check: 
        inspectlogs pr_JJAS_France_pointers.validate.197906.retrieved.converted.nc

        #We then retrieve the whole time series over France:
        echo $PASSWORD_ESGF | cdb_query CORDEX download_opendap \
                             --log_files \
                             --debug \
                             --openid=$OPENID_ESGF \
                             --password_from_pipe \
                             --year=1979,1980,1990,2000 \
                             --month=6 \
                             pr_JJAS_France_pointers.validate.France.nc \
                             pr_JJAS_France_pointers.validate.France.retrieved.nc
        #Testing check: 
        inspectlogs pr_JJAS_France_pointers.validate.France.retrieved.nc

        #Convert to filesystem:
        cdb_query CORDEX reduce --out_destination=./out_France/CORDEX/ \
                                --log_files \
                                --debug \
                                 '' \
                                 pr_JJAS_France_pointers.validate.France.retrieved.nc \
                                 pr_JJAS_France_pointers.validate.France.retrieved.converted.nc
        #Testing check: 
        inspectlogs pr_JJAS_France_pointers.validate.France.retrieved.converted.nc
        rm -r out/ out_France/
        rm pr_JJAS_France_pointers.*
fi 
