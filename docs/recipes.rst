Recipes
=======

Once installed, this package should make the command line tool ``cdb_query``
visible to the user's path. This is typically the 
case for common python installations.

The command ``cdb_query CMIP5`` contains several commands:

* ``cdb_query CMIP5 ask``, searches the CMIP5 archive and produces a file with pointers to the data. 
* ``cdb_query CMIP5 validate`` can be used to find all the experiments that have all of
  the avialalble years. This command outputs a file that points to data for every month of the requested period.
* ``cdb_query CMIP5 download_files`` and ``cdb_query CMIP5 download_opendap``, reads the output from ``cdb_query CMIP5 validate``,
  as an input and returns a **single** path to file. This makes it easy to retrieve data from simple scripts.

.. hint:: The variable descriptions (time_frequency, realm, cmor_table, ...) for the CMIP5 can be found
          in files http://cmip-pcmdi.llnl.gov/cmip5/docs/standard_output.pdf, http://cmip-pcmdi.llnl.gov/cmip5/docs/standard_output.xls.

.. tas ONDJF CMIP5
.. include:: recipe01.rst

..
    .. pr JJAS France CORDEX
    .. include:: recipe02.rst

    .. remapping
    .. include:: recipe03.rst

    .. picontrol
    .. include:: recipe04.rst

    .. daily
    .. include:: recipe05.rst
