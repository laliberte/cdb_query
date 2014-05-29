Speeding up the discovery and optimset procedure (CMIP5)
--------------------------------------------------------
The discovery step can be slow, especially when using a distributed discovery (``--distrib``).
This can be sped up by querying the archive institute per institute AND do it asynchronously.

Asynchronous discovery
^^^^^^^^^^^^^^^^^^^^^^
The `ask` and `validate` commands provides a basic multi-threaded implementation::

    $ cdb_query_CMIP5 ask --num_procs=5 \
                             tas_ONDJF.hdr \
                             tas_ONDJF_pointers.{}.nc \
    $ cdb_query_CMIP5 validate --num_procs=5 \
                             tas_ONDJF_pointers.nc \
                             tas_ONDJF_pointers.validate.nc

This command uses 5 processes and queries the archive simulation per simulation.

.. warning:: The asynchronous optimset might not work on your system. This is will
             depend on whether your certificates are detected within the asynchronous
             threads. To verify this, it is suggested that to perform a test with and
             without asynchronous discovery and make sure that the results are the same.
             Note that the output files sizes may differ significantly.
