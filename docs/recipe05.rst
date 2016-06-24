Retrieve precipitation and remap to a fixed grid
----------------------------------------------------

The following is an example script for finding, retrieving and remapping data::

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
        "pr":["mon","atmos","Amon"],
        "tas":["mon","atmos","Amon"]
        },
    "search_list":
        [
        "./in/CMIP5",
        "http://esgf-index1.ceda.ac.uk/esg-search/",
        "http://esgf-data.dkrz.de/esg-search/",
        "http://pcmdi9.llnl.gov/esg-search/",
        "http://esgdata.gfdl.noaa.gov/esg-search/",
        "http://esgf-node.ipsl.fr/esg-search/",
        "http://esg-datanode.jpl.nasa.gov/esg-search/"
        ],
    "file_type_list":
        [
        "HTTPServer",
        "local_file"
        ]
    }
    }
    EndOfHDR
    #Make search dir otherwise result in error:
    mkdir -p ./in/CMIP5
    #Discover data:
    if [ ! -f pr_historical_rcp85.hdr.pointers.nc ]; then
        echo -n "Discovering data: "
        date
        cdb_query_CMIP5 ask --num_procs=5 \
                                pr_historical_rcp85.hdr \
                                pr_historical_rcp85.hdr.pointers.nc

        #List simulations:
        cdb_query_CMIP5 list_fields -f institute \
                                    -f model \
                                    -f ensemble \
                                    pr_historical_rcp85.hdr.pointers.nc
    fi 


    #Find optimal set of simulations:
    if [ ! -f pr_historical_rcp85.hdr.pointers.validate.nc ]; then
        echo -n "Finding optimal set: "
        date
        # On April 30, 2014, 4 data nodes were down or not
        # working properly. We excluded them from the
        # optimal set analysis. This is likely to change
        # in the future and it might be worth it
        # to try including some of the excluded nodes: 
        cdb_query_CMIP5 validate \
                                 --Xdata_node=http://esg.bnu.edu.cn \
                                 --Xdata_node=http://esg2.e-inis.ie \
                                 --Xdata_node=http://pcmdi7.llnl.gov \
                                 --Xdata_node=http://pcmdi9.llnl.gov \
                                 --num_procs=5\
                                 pr_historical_rcp85.hdr.pointers.nc \
                                 pr_historical_rcp85.hdr.pointers.validate.nc

        #List simulations:
        cdb_query_CMIP5 list_fields -f institute \
                                    -f model \
                                    -f ensemble \
                                    pr_historical_rcp85.hdr.pointers.validate.nc
        date
    fi

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

    FILE_NAME="pr_historical_rcp85.hdr.pointers.validate"
    EXPERIMENT=historical
    YEAR_START=1970
    YEAR_END=2005
    #Retrieve first month:
    if [ ! -f $FILE_NAME.197001.retrieved.nc ]; then
        cdb_query_CMIP5 download --experiment=$EXPERIMENT \
                                        --year=$YEAR_START \
                                        --month=1 \
                                        $FILE_NAME.nc \
                                        $FILE_NAME.197001.retrieved.nc
    fi


    #Compute the remapping weigths:
    #Next is a loop over variables in $FILE_NAME.197001.retrieved.nc. It is equivalent to:
    #
    # cdo gendis,newgrid_atmos.cdo $FILE_NAME.197001.retrieved.nc $FILE_NAME.197001.retrieved.weigths.nc
    #
    # if the the files were not hierarchical netcdf4 files.
    #
    # This is is accomplished with 10 simultaneous processes
    #
    if [ ! -f $FILE_NAME.197001.retrieved.weigths.nc ]; then
        cdb_query_CMIP5 apply --num_procs=10 \
                                'cdo gendis,newgrid_atmos.cdo' \
                                $FILE_NAME.197001.retrieved.nc \
                                $FILE_NAME.197001.retrieved.weigths.nc
    fi

    echo -n "Starting remapping "
    date
    for YEAR in $(seq $YEAR_START $YEAR_END); do
        if [ ! -f $FILE_NAME.$YEAR.retrieved.remap.nc ]; then
            cdb_query_CMIP5 download \
                                --experiment=$EXPERIMENT \
                                --year=$YEAR \
                                $FILE_NAME.nc \
                                $FILE_NAME.$YEAR.retrieved.nc
            #Next is a loop over variables in $FILE_NAME.197001.retrieved.nc. It is equivalent to:
            #
            # cdo cdo remap,newgrid_atmos.cdo,$FILE_NAME.197001.retrieved.weigths.nc $FILE_NAME.$YEAR.retrieved.nc \
            #                                  $FILE_NAME.$YEAR.retrieved.remap.nc
            #
            # if the the files were not hierarchical netcdf4 files.
            #
            cdb_query_CMIP5 apply \
                            --experiment=$EXPERIMENT \
                            --num_procs=5 \
                            'cdo -s remap,newgrid_atmos.cdo,{1}' \
                            $FILE_NAME.$YEAR.retrieved.nc \
                            $FILE_NAME.197001.retrieved.weigths.nc \
                            $FILE_NAME.$YEAR.retrieved.remap.nc
            rm $FILE_NAME.$YEAR.retrieved.nc
        fi
    done

    echo -n "Done remapping "
    date

    #Concatenate the results:

    if [ ! -f pr_historical_rcp85.hdr.pointers.validate.1970-2005.retrieved.remap.nc ]; then
        #First list the files:
        FILE_LIST=$(for YEAR in $(seq 1970 2005); do
                        echo pr_historical_rcp85.hdr.pointers.validate.$YEAR.retrieved.remap.nc;
                    done)

        #Then apply a mergetime operator:
        cdb_query_CMIP5 apply 'cdo mergetime' \
                        $FILE_LIST \
                        pr_historical_rcp85.hdr.pointers.validate.1970-2005.retrieved.remap.nc
    fi

