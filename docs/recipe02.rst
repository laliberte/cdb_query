2. Speeding things up 
---------------------
The ``ask`` and ``validate`` steps can be slow.
They can be sped up by querying the archive simulation per simulation AND do it asynchronously.

Asynchronous discovery
^^^^^^^^^^^^^^^^^^^^^^
The `ask` and `validate` commands provides a basic multi-processing implementation::

    $ cdb_query CMIP5 ask --num_procs=5 \
                             ... \
                             tas_ONDJF_pointers.nc 
    $ cdb_query CMIP5 validate --num_procs=5 \
                             tas_ONDJF_pointers.nc \
                             tas_ONDJF_pointers.validate.nc

This command uses 5 processes and queries the archive simulation per simulation.

Asynchronous downloads
^^^^^^^^^^^^^^^^^^^^^^
The `download_files` and `download_opendap` commands also provides a basic multi-processing implementation.
By default, data from different data nodes is retrieved in parallel. One can allow more than one simultaneous
download per data node ::

    $ cdb_query CMIP5 download_files --num_dl=5 ...
    $ cdb_query CMIP5 download_opendap --num_dl=5 ...

These commands now allow 5 simulataneous download per data node. 
