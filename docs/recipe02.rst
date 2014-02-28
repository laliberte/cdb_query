Speeding up the discovery and optimset procedure (CMIP5)
--------------------------------------------------------
The discovery step can be slow, especially when using a distributed discovery (``--distrib``).
This can be sped up by querying the archive institute per institute AND do it asynchronously.

Asynchronous discovery
^^^^^^^^^^^^^^^^^^^^^^
The `discover` and `optimset` commands provides a basic multi-threaded implementation::

    $ cdb_query_CMIP5 discover --num_procs=5 \
                             tas_ONDJF.hdr \
                             tas_ONDJF_pointers.{}.nc \
    $ cdb_query_CMIP5 optimset --num_procs=5 \
                             tas_ONDJF_pointers.nc \
                             tas_ONDJF_pointers.optimset.nc

This command uses 5 processes and queries the archive simulation per simulation.
