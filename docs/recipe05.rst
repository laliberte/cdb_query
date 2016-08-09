5. Remap MAM precipitation and temperature to US (CMIP5)
--------------------------------------------------------

Discovery and analyzing a sample
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With operator chaining, in a BASH script::

    #!\bin\bash
    #Create new cdo grid:
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

    OPENID="your openid"
    PASSWORD="your ESGF password"
    #latlon box -124.78 -66.95 24.74 49.34 is continental us
    echo $PASSWORD | cdb_query CMIP5 ask validate reduce_soft_links download_opendap reduce \
                      --ask_month=3,4,5 \
                      --ask_var=tas:mon-atmos-Amon,pr:mon-atmos-Amon \
                      --ask_experiment=historical:1950-2005,rcp85:2006-2050 \
                      --related_experiments \
                      --Xdata_node=http://esgf2.dkrz.de \
                      --openid=$OPENID \
                      --password_from_pipe \
                      --record_validate \
                      --out_destination=./out_sample/CMIP5/ \
                      --num_procs=10 \
                      --year=2000 --month=3 \
                      --reduce_soft_links_script='nc4sl subset --lonlatbox -150.0 -50.0 20.0 55.0' \
                      'cdo -f nc \
                           -sellonlatbox,-124.78,-66.95,24.74,49.34 \
                           -remapbil,newgrid_atmos.cdo \
                           -selgrid,lonlat,curvilinear,gaussian,unstructured ' \
                      us_pr_tas_MAM_pointers.validate.200003.retrieved.converted.nc
It does:

#. Finds MAM ``pr`` and ``tas`` for 1950 to 2050 ``historical``, followed by ``rcp85``.
#. Drops simulations (``institute``, ``model``, ``ensemble``) triples that are not found in both ``historical`` and ``rcp85`` for ALL requested years.
#. Excludes (``--Xdata_node=http://esgf2.dkrz.de``) data node ``http://esgf2.dkrz.de`` because it is a tape archive and tends to be slow.
#. Retrieves certificates (``--openid=$OPENID``). Password read from the pipe (``--password_from_pipe``).
#. Records the result (``--record_validate``) of ``validate`` to ``us_pr_tas_MAM_pointers.validate.200003.retrieved.converted.nc.validate``.
#. Selects a slightly larger area than continental US for download (``--reduce_soft_links_script='nc4sl subset --lonlatbox -150.0 -50.0 20.0 55.0'``)
#. Downloads only March 2000 (``--year=2000 --month=3``).
#. Uses a bilinear remapping and focuses on the continental US (``'cdo ... '``).
#. Does this using 10 processes ``--num_procs=10``.
#. Converts the data to the CMIP5 DRS to directory ``./out_sample/CMIP5/``.
#. Writes a full description of downloaded data (pointers to it) in file ``us_pr_tas_MAM_pointers.validate.200003.retrieved.converted.nc``.

Scaling up to the whole dataset
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the data looks OK, then one can use the validate file to bypass the ``ask`` and ``validate`` steps::

    #!\bin\bash
    OPENID="your openid"
    PASSWORD="your ESGF password"
    #latlon box -124.78 -66.95 24.74 49.34 is continental us

    echo $PASSWORD | cdb_query CMIP5 reduce_soft_links download_opendap reduce \
                      --openid=$OPENID \
                      --password_from_pipe \
                      --out_destination=./out/CMIP5/ \
                      --num_procs=10 \
                      --reduce_soft_links_script='nc4sl subset --lonlatbox -150.0 -50.0 20.0 55.0' \
                      'cdo -f nc \
                           -sellonlatbox,-124.78,-66.95,24.74,49.34  \
                           -remapbil,newgrid_atmos.cdo \
                           -selgrid,lonlat,curvilinear,gaussian,unstructured ' \
                      us_pr_tas_MAM_pointers.validate.200003.retrieved.converted.nc.validate \
                      us_pr_tas_MAM_pointers.validate.retrieved.converted.nc

This will download all the data!

.. hint:: It is good practice to first download a small subset to ensure that everything outputs as expected.
          Because we record the validate step, this two-parts process comes at a very small price.
