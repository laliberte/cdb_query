Retrieve sea surface temperature and sea level pressure from pre-industrial and remap to a fixed grid
-----------------------------------------------------------------------------------------------------

.. hint:: Some experiments are not following a set calendar and it thus become difficult to ensure
          that all requested years are available. For those experiments, it is suggested to
          use 1 as a starting year and your desired length as ending year. In the following example,
          we use 1 and 499 for a 498 years time series. In this example, `cdb_query` will find
          the first 498 years time series published. It will discard experiments that have less than
          498 years. 

.. warning:: The following example can take more than a day to complete!

THe following is an example script for finding, retrieving and remapping data::

    #!/bin/bash
    cat > tos_slp_picontrol.hdr <<EndOfHDR
    {
    "header":{
    "search_list":
        [   
        "http://esgf-index1.ceda.ac.uk/esg-search/",
        "http://pcmdi9.llnl.gov/esg-search/",
        "http://esgf-data.dkrz.de/esg-search/",
        "http://esgdata.gfdl.noaa.gov/esg-search/",
        "http://esgf-node.ipsl.fr/esg-search/",
        "http://esg-datanode.jpl.nasa.gov/esg-search/"
        ],  
    "file_type_list":
        [   
        "HTTPServer"
        ],  
    "variable_list":
        {   
        "tos":["mon","ocean","Omon"],
        "psl":["mon","atmos","Amon"],
        "areacello":["fx","ocean","fx"]
        },  
    "experiment_list":
        {   
        "piControl":"1,499"
        }   
    }
    }
    EndOfHDR
    #Make search dir otherwise result in error:
    mkdir -p ./in/CMIP5
    #Discover data:
    echo -n "Discovering data: "
    date
    cdb_query_CMIP5 discover --num_procs=5 \
                            tos_slp_picontrol.hdr \
                            tos_slp_picontrol.hdr.pointers.nc

    #List simulations:
    cdb_query_CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                tos_slp_picontrol.hdr.pointers.nc


    #Find optimal set of simulations:
    echo -n "Finding optimal set: "
    date
    cdb_query_CMIP5 optimset --num_procs=5\
                             tos_slp_picontrol.hdr.pointers.nc \
                             tos_slp_picontrol.hdr.pointers.optimset.nc

    #List simulations:
    cdb_query_CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                tos_slp_picontrol.hdr.pointers.optimset.nc

    #REMAPPING HISTORICAL DATA
    cat >> newgrid_atmos.cdo <<EndOfGrid
    gridtype  = lonlat
    gridsize  = 55296
    xname     = lon
    xlongname = longitude
    xunits    = degrees_east
    yname     = lat
    ylongname = latitude
    yunits    = degrees_north
    xsize     = 288
    ysize     = 192
    xfirst    = 0
    xinc      = 1.25
    yfirst    = -90
    yinc      = 0.94240837696
    EndOfGrid

    FILE_NAME="tos_slp_picontrol.hdr.pointers.optimset"
    EXPERIMENT=piControl
    YEAR_START=1
    YEAR_END=499
    #Retrieve first month:
    cdb_query_CMIP5 remote_retrieve --experiment=$EXPERIMENT \
                                    --year=$YEAR_START \
                                    --month=1 \
                                    $FILE_NAME.nc \
                                    $FILE_NAME.0-0.retrieved.nc


    #Compute the remapping weigths:
    #Next is a loop over variables in $FILE_NAME.0-0.retrieved.nc. It is equivalent to:
    #
    # cdo gendis,newgrid_atmos.cdo $FILE_NAME.0-0.retrieved.nc $FILE_NAME.0-0.retrieved.weigths.nc
    #
    # if the the files were not hierarchical netcdf4 files.
    #
    # This is is accomplished with 10 simultaneous processes
    #
    cdb_query_CMIP5 apply --num_procs=10 \
                            -s 'cdo gendis,newgrid_atmos.cdo' \
                            $FILE_NAME.0-0.retrieved.nc \
                            $FILE_NAME.0-0.retrieved.weigths.nc


    echo -n "Starting remapping "
    date
    for YEAR in $(seq $YEAR_START $YEAR_END); do 
        cdb_query_CMIP5 remote_retrieve \
                            --experiment=$EXPERIMENT \
                            --year=$YEAR \
                            $FILE_NAME.nc \
                            $FILE_NAME.$YEAR.retrieved.nc
        #Next is a loop over variables in $FILE_NAME.0-0.retrieved.nc. It is equivalent to:
        #
        # cdo cdo remap,newgrid_atmos.cdo,$FILE_NAME.0-0.retrieved.weigths.nc $FILE_NAME.$YEAR.retrieved.nc \
        #                                  $FILE_NAME.$YEAR.retrieved.remap.nc 
        #
        # if the the files were not hierarchical netcdf4 files.
        #
        cdb_query_CMIP5 apply \
                        --experiment=$EXPERIMENT \
                        --num_procs=5 \
                        -s 'cdo -s remap,newgrid_atmos.cdo,{1}' \
                        $FILE_NAME.$YEAR.retrieved.nc \
                        $FILE_NAME.0-0.retrieved.weigths.nc \
                        $FILE_NAME.$YEAR.retrieved.remap.nc
        rm $FILE_NAME.$YEAR.retrieved.nc
    done

    echo -n "Done remapping "
    date

    #Concatenate the results:

    #First list the files:
    FILE_LIST=$(for YEAR in $(seq 1 499); do 
                    echo $FILE_NAME.$YEAR.retrieved.remap.nc; 
                done)

    #Then apply a mergetime operator:
    cdb_query_CMIP5 apply -s 'cdo mergetime' \
                    $FILE_LIST \
                    $FILE_NAME.0001-0499.retrieved.remap.nc
    
    #Finally convert to a CMIP5 filesystem tree:
    mkdir out/CMIP5
    cdb_query_CMIP5 convert $FILE_NAME.0001-0499.retrieved.remap.nc out/CMIP5/
