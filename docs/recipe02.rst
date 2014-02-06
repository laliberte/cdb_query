Speeding up the discovery and optimset procedure (CMIP5)
--------------------------------------------------------
The discovery step can be slow, especially when using a distributed discovery (``--distrib``).
This can be sped up by querying the archive institute per institute AND do it asynchronously.

Finding the list of institutes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
It is possible to find the list of all possible institutes using ``cdb_query_CMIP5``::

    $ cdb_query_CMIP5 discover --list_only_field=institute tas_ONDJF.hdr tas_ONDJF_pointers.nc
    INM
    CNRM-CERFACS
    ICHEC
    IPSL
    MOHC

This command does not change ``tas_ONDJF_pointers.nc``.

In `BASH` one can pass this list to a variable::
    
    $ INSTITUTE_LIST=$(cdb_query_CMIP5 discover  \
                            --list_only_field=institute \
                            tas_ONDJF.hdr \
                            tas_ONDJF_pointers.nc)

Asynchronous discovery
^^^^^^^^^^^^^^^^^^^^^^
Then one can use ``gnu-parallel`` to perform the discovery and optimal set query::

    $ for INSTITUTE in $INSTITUTE_LIST; do     
        echo $INSTITUTE;   
      done | parallel -j 8 "cdb_query_CMIP5 discover \
                            --institute={} \          
                             tas_ONDJF.hdr \
                             tas_ONDJF_pointers.{}.nc; \
                        cdb_query_CMIP5 optimset \
                             tas_ONDJF_pointers.{}.nc \
                             tas_ONDJF_pointers.optimset.{}.nc"

This command uses 8 cores (``-j 8``) and queries the archive institute per institute.
It produces one netCDF file per institute and we can use `NCO` to recombine those files::

    $ ncecat --gag tas_ONDJF_pointers.optimset.*.nc tas_ONDJF_pointers.optimset.institutes.nc
    $ ncks -G :1 tas_ONDJF_pointers.optimset.institutes.nc tas_ONDJF_pointers.optimset.nc

.. hint:: If this fails it is likely that either your `NCO` version or the version of your netCDF4
          library is outdated. Consider updating to a more recent version.

The resulting file will be the same (up to some global attributes from `NCO`) as if obtained from::

    $ cdb_query_CMIP5 discover tas_ONDJF.hdr tas_ONDJF_pointers.nc

BASH script
^^^^^^^^^^^
This recipe would improve the first recipe's BASH script::

    #!/bin/bash
    cat > tas_ONDJF.hdr <<EndOfHDR
    {
    "header":{
    "experiment_list":
        {
        "amip":"1979,2004"
        },
    "months_list": [1,2,10,11,12],
    "variable_list":
        {
        "tas":["day","atmos","day"],
        "orog":["fx","atmos","fx"]
        },
    "search_list":
        [
        "./in/CMIP5",
        "http://esgf-index1.ceda.ac.uk/esg-search/"
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

    #Find the list of institutes:
    INSTITUTE_LIST=$(cdb_query_CMIP5 discover  \
                            --list_only_field=institute \
                            tas_ONDJF.hdr \
                            tas_ONDJF_pointers.nc)
    #Discover data and find optimal set,
    #institute per institute with up to 8 processes:
    for INSTITUTE in $INSTITUTE_LIST; do     
        echo $INSTITUTE;   
      done | parallel -j 8 "cdb_query_CMIP5 discover \
                            --institute={} \          
                             tas_ONDJF.hdr \
                             tas_ONDJF_pointers.{}.nc; \
                        cdb_query_CMIP5 optimset \
                             tas_ONDJF_pointers.{}.nc \
                             tas_ONDJF_pointers.optimset.{}.nc"
    #WARNING: Do not recombine after the discovery stage.
    #         This could cause unexpected behavior with the current
    #         version of the code.

    #Recombine the results:
    ncecat --gag tas_ONDJF_pointers.optimset.*.nc \
                 tas_ONDJF_pointers.optimset.institutes.nc
    ncks -G :1 tas_ONDJF_pointers.optimset.institutes.nc \
               tas_ONDJF_pointers.optimset.nc

    #Then CHOOSE your prefferred retrieval method
