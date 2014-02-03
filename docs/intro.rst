Introduction
============

This code is intended to provide an interface between the CMIP5/CORDEX Data Reference Syntax 
(hereafter DRS) for a local repository of CMIP5/CORDEX data, the Earth System Grid Federation
(hereafter ESGF) search engine for the efficient, robust and timely computation of climate diagnostics.

This code is accessed through the command line tools ``cdb_query_CMIP5`` and ``cdb_query_CORDEX``.
It takes as an input a JSON diagnostics header file and returns a netCDF4 file with pointers
to the requested data. The resulting pointers are intended to be immutable and describe
the entire data space available to the user. At this stage of development, ``cdb_query``
cannot guarantee that all of the available data is obtained but instead it provides
a snapshot of all the acessible data with proper CMIP5/CORDEX DRS and/or well-formed ESGF search tags.

The source code for this project is available on github: https://github.com/laliberte/cdb_query
