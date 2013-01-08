Introduction
------------

This code is intended to provide an interface between the CMIP5 Data Reference Syntax 
(hereafter DRS) for a local repository of CMIP5 data, the Earth System Grid Federation
(hereafter ESGF) search engine and the efficient computation of climate diagnostics.

This code is accessed through the command line tool `cdb_query_archive`. It takes
as an input a JSON diagnostics header file and returns a JSON file with pointers
to the requested data. The resulting pointers are intended to be immutable and describe
the entire data space available to the user. At this stage of development, `cdb_query_archive`
cannot guarantee that all of the available data is obtained but instead it provides
a snapshot of all the acessible data with proper CMIP5 DRS and/or well-formed ESGF search tags.
