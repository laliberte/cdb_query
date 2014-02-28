Retrieve precipitation and remap to a fixed grid
----------------------------------------------------

THe following is an example script for finding, retrieving and remapping data::

    #!/bin/bash
    cat > pr_historical_rcp85.hdr <<EndOfHDR
    {
    "header":{
    "experiment_list":
        {
        "historical":"1970,2005",
        "rcp85":"2006,2099"
        },
    "variable_list":
        {
        "pr":["mon","atmos","Amon"]
        },
    "search_list":
        [
        "./in/CMIP5",
        "http://esgf-index1.ceda.ac.uk/esg-search/",
        "http://pcmdi9.llnl.gov/esg-search/",
        "http://esgf-data.dkrz.de/esg-search/",
        "http://esgdata.gfdl.noaa.gov/esg-search/",
        "http://esgf-node.ipsl.fr/esg-search/",
        "http://esg-datanode.jpl.nasa.gov/esg-search/"
        ],
    "file_type_list":
        [
        "local_file",
        "HTTPServer"
        ]
    }
    }
    EndOfHDR
    #Make search dir otherwise result in error:
    mkdir -p ./in/CMIP5
    #Discover data:
    echo -n "Discovering data: "
    date
    cdb_query_CMIP5 discover --num_procs=5 \
                            pr_historical_rcp85.hdr \
                            pr_historical_rcp85.hdr.pointers.nc

    #List simulations:
    cdb_query_CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                pr_historical_rcp85.hdr.pointers.nc


    #Find optimal set of simulations:
    echo -n "Finding optimal set: "
    date
    cdb_query_CMIP5 optimset --num_procs=5\
                             pr_historical_rcp85.hdr.pointers.nc \
                             pr_historical_rcp85.hdr.pointers.optimset.nc

    #List simulations:
    cdb_query_CMIP5 list_fields -f institute \
                                -f model \
                                -f ensemble \
                                pr_historical_rcp85.hdr.pointers.optimset.nc

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

    FILE_NAME="pr_historical_rcp85.hdr.pointers.optimset"
    EXPERIMENT=historical
    YEAR_START=1970
    YEAR_END=2005
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
    FILE_LIST=$(for YEAR in $(seq 1970 2005); do 
                    echo pr_historical_rcp85.hdr.pointers.optimset.$YEAR.retrieved.remap.nc; 
                done)

    #Then apply a mergetime operator:
    cdb_query_CMIP5 apply -s 'cdo mergetime' \
                    $FILE_LIST \
                    pr_historical_rcp85.hdr.pointers.optimset.1970-2005.retrieved.remap.nc
