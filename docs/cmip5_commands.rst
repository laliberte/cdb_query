For CMIP5 outputs
-----------------
Data from CMIP5 outputs can be analyzed using ``cdb_query_CMIP5``.

.. command-output:: cdb_query_CMIP5 --help

It contains the following subcommands which are usually used in the
following order:

`discover`
^^^^^^^^^^
This command discovers the files that could match the query.

.. hint::
    This command does not require ESGF credentials to function properly.

.. command-output:: cdb_query_CMIP5 discover --help

.. warning::
    The result from this command does not guarantee that all the criteria
    are verified for all the returned simulations. This command acts as
    a first pass.

`list_fields`
^^^^^^^^^^^^^
This command reads the output from `discover` to list the levels available in the file.

.. command-output:: cdb_query_CMIP5 list_fields --help

.. hint::
    Using ``cdb_query_CMIP5 list_fields -f path`` will list the paths found in the file.

.. hint::
    Using ``cdb_query_CMIP5 list_fields -f institute -f model -f ensemble`` will list the
    indenpendent simulations identifiers found in the file.

`optimset`
^^^^^^^^^^
This command reads the output from `discover` and eliminates simulations 
that do not satisfy one or more of the requested criteria.

.. warning::
    This command requires ESGF credentials to function properly.

.. command-output:: cdb_query_CMIP5 optimset --help

`remote_retieve`
^^^^^^^^^^^^^^^^
This command reads the output from `optimset` and retrieves the data from
its location (either local or remote) and store it in a local netCDF4 file.

.. warning::
    This command requires ESGF credentials to function properly.

.. command-output:: cdb_query_CMIP5 remote_retrieve --help
