For CORDEX outputs
------------------
Data from CORDEX outputs can be analyzed using ``cdb_query_CORDEX``.

.. command-output:: cdb_query_CORDEX --help

It contains the following subcommands which are usually used in the
following order:

`ask`
^^^^^
This command discovers the files that could match the query.

.. hint::
    This command does not require ESGF credentials to function properly.

.. command-output:: cdb_query_CORDEX ask --help

.. warning::
    The result from this command does not guarantee that all the criteria
    are verified for all the returned simulations. This command acts as
    a first pass.

`list_fields`
^^^^^^^^^^^^^
This command reads the output from `ask` to list the levels available in the file.

.. command-output:: cdb_query_CORDEX list_fields --help

.. hint::
    Using ``cdb_query_CORDEX list_fields -f path`` will list the paths found in the file.

.. hint::
    Using ``cdb_query_CORDEX list_fields -f institute -f model -f ensemble`` will list the
    indenpendent simulations identifiers found in the file.

`validate`
^^^^^^^^^^
This command reads the output from `ask` and eliminates simulations 
that do not satisfy one or more of the requested criteria.

.. warning::
    This command requires ESGF credentials to function properly.

.. command-output:: cdb_query_CORDEX validate --help

`download`
^^^^^^^^^^
This command reads the output from `validate` and retrieves the data from
its location (either local or remote) and store it in a local netCDF4 file.

.. warning::
    This command requires ESGF credentials to function properly.

.. command-output:: cdb_query_CORDEX download --help

`download_raw`
^^^^^^^^^^^^^^
This command reads the output from `validate` and retrieves the data from
its location (either local or remote) and store it in a local file.

.. warning::
    This command requires ESGF credentials to function properly.

.. command-output:: cdb_query_CORDEX download_raw --help

`apply`
^^^^^^^
This command reads the output from `download` and applies a command-line
operator. This is a glorified for-loop.

.. command-output:: cdb_query_CORDEX apply --help

`convert`
^^^^^^^^^
This command reads the output from `download` and converts the output
to the CORDEX DRS.

.. command-output:: cdb_query_CORDEX convert --help
