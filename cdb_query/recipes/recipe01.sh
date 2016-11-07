#!/bin/bash

NUM_PROCS=10
#Discover data:
echo "Recipe 01"
echo "Ask:"
cdb_query CMIP5 ask --ask_month=1,2,10,11,12 \
                    --debug \
                    --log_files \
                    --ask_var=tas:day-atmos-day,orog:fx-atmos-fx \
                    --ask_experiment=amip:1979-2004 \
                    --institute=NCAR --ensemble=r1i1p1 --model=CCSM4 \
                    --num_procs=${NUM_PROCS} \
                    tas_ONDJF_pointers.nc

#Testing check: 
if [ $(cat tas_ONDJF_pointers.nc.log | grep ERROR | wc -l) -gt 0 ]; then
    exit 1
fi

#List simulations:
echo "Discovered files:"
cdb_query CMIP5 list_fields -f institute \
                            -f model \
                            -f ensemble \
                            tas_ONDJF_pointers.nc

#Validate simulations:
#Exclude data_node http://esgf2.dkrz.de because it is on a tape archive (slow)
#If you do not exclude it, it will likely be excluded because of its slow
#response time.
echo "Validate:"
echo $PASSWORD_ESGF | cdb_query CMIP5 validate \
                            --debug \
                            --log_files \
                            --openid=$OPENID_ESGF \
                            --password_from_pipe \
                            --num_procs=${NUM_PROCS} \
                            --Xdata_node=http://esgf2.dkrz.de \
                            --Xdata_node=http://esgf-data1.ceda.ac.uk \
                            tas_ONDJF_pointers.nc \
                            tas_ONDJF_pointers.validate.nc
#Testing check: 
if [ $(cat tas_ONDJF_pointers.validate.nc.log | grep ERROR | wc -l) -gt 0 ]; then
    exit 1
fi

#List simulations:
echo "Validated simulations:"
cdb_query CMIP5 list_fields -f institute \
                            -f model \
                            -f ensemble \
                            tas_ONDJF_pointers.validate.nc

#CHOOSE:
    # *1* Retrieve files:
        #echo $PASSWORD_ESGF | cdb_query CMIP5 download_files \
        #                    --download_all_files \
        #                    --openid=$OPENID_ESGF \
        #                    --password_from_pipe \
        #                    --out_download_dir=./in/CMIP5/ \
        #                    tas_ONDJF_pointers.validate.nc \
        #                    tas_ONDJF_pointers.validate.downloaded.nc

    # *2* Retrieve to netCDF:
        #Retrieve the first month:
        echo "Download using OPENDAP:"
        echo $PASSWORD_ESGF | cdb_query CMIP5 download_opendap --year=1979 --month=1 \
                            --log_files \
                            --openid=$OPENID_ESGF \
                            --debug \
                            --num_dl=3 \
                            --password_from_pipe \
                            tas_ONDJF_pointers.validate.nc \
                            tas_ONDJF_pointers.validate.197901.retrieved.nc
        #Testing check: 
        if [ $(cat tas_ONDJF_pointers.validate.197901.retrieved.nc.log | grep ERROR | wc -l) -gt 0 ]; then
            exit 1
        fi
        echo "Done downloading using OPENDAP!"

        #Pick one simulation:
        #Note: this can be VERY slow!
        echo "Use NCO to look at the files:"
        if [ ! -f tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc ]; then
            # Here, we capture warnings because these days ncks tends to spit out a lot of warning messages:
            ncks -q -G :8 -g /NCAR/CCSM4/amip/day/atmos/day/r1i1p1/tas \
               tas_ONDJF_pointers.validate.197901.retrieved.nc \
               tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc &> ncks_warnings
            # Show errors:
            cat ncks_warnings | grep 'rror'
            #Remove soft_links informations:
            ncks -q -L 0 -O -x -g soft_links \
               tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc \
               tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc &> ncks_warnings
            # Show errors:
            cat ncks_warnings | grep 'rror'
        fi

        #Look at it:
        #When done, look at it. A good tool for that is ncview:
        #   ncview tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc

        #Convert hierarchical file to files on filesystem (much faster than ncks):
        #Identity reduction simply copies the data to disk
        echo "Convert to directory tree:"
        cdb_query CMIP5 reduce \
                            --log_files \
                            --debug \
                            '' \
                            --out_destination=./out/CMIP5/ \
                            tas_ONDJF_pointers.validate.197901.retrieved.nc \
                            tas_ONDJF_pointers.validate.197901.retrieved.converted.nc
        #Testing check: 
        if [ $(cat tas_ONDJF_pointers.validate.197901.retrieved.converted.nc.log | grep ERROR | wc -l) -gt 0 ]; then
            exit 1
        fi
        echo "Done converting!"

        #The files can be found in ./out/CMIP5/:
        echo "Converted files:"
        find ./out/CMIP5/ -name '*.nc'

rm tas_ONDJF_pointers.*
