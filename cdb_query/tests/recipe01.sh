#!/bin/bash

NUM_PROCS=10
#Discover data:
cdb_query CMIP5 ask --ask_month=1,2,10,11,12 \
                    --debug -s \
                    --ask_var=tas:day-atmos-day,orog:fx-atmos-fx \
                    --ask_experiment=amip:1979-2004 \
                    --institute=NCAR --ensemble=r1i1p1 --model=CCSM4 \
                    --num_procs=${NUM_PROCS} \
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
echo $PASSWORD_ESGF | cdb_query CMIP5 validate \
                            --debug -s \
                            --openid=$OPENID_ESGF \
                            --password_from_pipe \
                            --num_procs=${NUM_PROCS} \
                            --Xdata_node=http://esgf2.dkrz.de \
                            --Xdata_node=http://esgf-data1.ceda.ac.uk \
                            tas_ONDJF_pointers.nc \
                            tas_ONDJF_pointers.validate.nc

#List simulations:
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
        echo $PASSWORD_ESGF | cdb_query CMIP5 download_opendap --year=1979 --month=1 \
                            --log_files \
                            --openid=$OPENID_ESGF \
                            --debug -s \
                            --num_dl=3 \
                            --password_from_pipe \
                            tas_ONDJF_pointers.validate.nc \
                            tas_ONDJF_pointers.validate.197901.retrieved.nc

        #Pick one simulation:
        #Note: this can be VERY slow!
        if [ ! -f tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc ]; then
            ncks -G :8 -g /NCAR/CCSM4/amip/day/atmos/day/r1i1p1/tas \
               tas_ONDJF_pointers.validate.197901.retrieved.nc \
               tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc
            #Remove soft_links informations:
            ncks -L 0 -O -x -g soft_links \
               tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc \
               tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc
        fi

        #Look at it:
        #When done, look at it. A good tool for that is ncview:
        #   ncview tas_ONDJF_pointers.validate.197901.retrieved.NCAR_CCSM4_r1i1p1.nc

        #Convert hierarchical file to files on filesystem (much faster than ncks):
        #Identity reduction simply copies the data to disk
        cdb_query CMIP5 reduce \
                            --log_files \
                            --debug -s \
                            '' \
                            --out_destination=./out/CMIP5/ \
                            tas_ONDJF_pointers.validate.197901.retrieved.nc \
                            tas_ONDJF_pointers.validate.197901.retrieved.converted.nc

        #The files can be found in ./out/CMIP5/:
        find ./out/CMIP5/ -name '*.nc'
