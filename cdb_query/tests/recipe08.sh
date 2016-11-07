#!/bin/bash

NUM_PROCS=$2

if [ "$1" == "compute" ]; then
    ########################################
    #######   RECIPE 01 
    ########################################
    # http://pythonhosted.org/cdb_query/recipes.html#bash-script

    #Discover data:
    echo "Asking scientific question"
    cdb_query CMIP5 ask --ask_var=msftmyz:mon-ocean-Omon \
                        --ask_experiment=abrupt4xCO2:1-100,piControl:1-100 \
                        --related_experiments \
                        --log_files \
                        --debug \
                        --model=CCSM4 --model=NorESM1-M \
                        --num_procs=${NUM_PROCS} \
                        coupled_ocean_pointers.nc

    #Testing check: 
    if [ $(cat coupled_ocean_pointers.nc.log | grep ERROR | wc -l) -gt 0 ]; then
        exit 1
    fi
    #List simulations:
    echo "Discovered simulations:"
    cdb_query CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                coupled_ocean_pointers.nc

    #Validate simulations:
    #Exclude data_node http://esgf2.dkrz.de because it is on a tape archive (slow)
    #If you do not exclude it, it will likely be excluded because of its slow
    #response time.
    #
    #The first time this function is used, it might fail and ask you to register your kind of user.
    #This has to be done only once.
    #List data nodes:
    echo "Data nodes where the data resides:"
    cdb_query CMIP5 list_fields -f data_node coupled_ocean_pointers.nc

    echo "Validating simulations:"
    echo $PASSWORD_ESGF | cdb_query CMIP5 validate \
                                --openid=$OPENID_ESGF \
                                --password_from_pipe \
                                --num_procs=${NUM_PROCS} \
                                --log_files \
                                --debug \
                                --related_experiments \
                                --Xdata_node=http://esgf2.dkrz.de \
                                coupled_ocean_pointers.nc \
                                coupled_ocean_pointers.validate.nc

    #List simulations:
    echo "Validated simulations:"
    cdb_query CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                coupled_ocean_pointers.validate.nc
    #Testing check: 
    if [ $(cat coupled_ocean_pointers.validate.nc.log | grep ERROR | wc -l) -gt 0 ]; then
        exit 1
    fi

    #CHOOSE:
        # *1* Retrieve files:
        #    --download_all_files forces to download all files even if they are served by OPENDAP.
            echo "Downloading files to in/CMIP5:"
            echo $PASSWORD_ESGF | cdb_query CMIP5 download_files \
                                --download_all_files \
                                --openid=$OPENID_ESGF \
                                --password_from_pipe \
                                --log_files \
                                --debug \
                                --out_download_dir=./in/CMIP5/ \
                                coupled_ocean_pointers.validate.nc \
                                coupled_ocean_pointers.validate.downloaded.nc

    #Testing check: 
    if [ $(cat coupled_ocean_pointers.validate.downloaded.nc.log | grep ERROR | wc -l) -gt 0 ]; then
        exit 1
    fi
fi
