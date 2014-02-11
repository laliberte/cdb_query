Recipes
=======

Once installed, this package should make the command line tool ``cdb_query_CMIP5``
( and ``cdb_query_CORDEX`` ) visible to the user's path. This is typically the 
case for common python installations.

The command ``cdb_query_CMIP5`` contains several commands:

* ``cdb_query_CMIP5 discover``, searches the CMIP5 archive and produces a file with pointers to the data. 
* ``cdb_query_CMIP5 optimset`` can be used to find all the experiments that have all of
  the avialalble years. This command outputs a file that points to data for every month of the requested period.
* ``cdb_query_CMIP5 remote_retrieve``, reads the output from ``cdb_query_CMIP5 optimset``,
  as an input and returns a **single** path to file. This makes it easy to retrieve data from simple scripts.

.. hint:: The variable descriptions (time_frequency, realm, cmor_table, ...) for the CMIP5 can be found
          in the file http://cmip-pcmdi.llnl.gov/cmip5/docs/standard_output.pdf.

.. tas ONDJF CMIP5
.. include:: recipe01.rst

.. speeding up
.. include:: recipe02.rst

.. pr JJAS France CORDEX
.. include:: recipe03.rst
