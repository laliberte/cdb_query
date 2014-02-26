Introduction
============

This code is intended to provide an interface between the CMIP5/CORDEX Data Reference Syntax 
(hereafter DRS) for a local repository of CMIP5/CORDEX data, the Earth System Grid Federation
(hereafter ESGF) search engine for the efficient, robust and timely computation of climate diagnostics.

The cdb_query package contains separate command sets for CMIP5 and CORDEX archives. 
The standard workflow for both packages is similar:

1. A `discover` command searches the archive for specific variables and user specified date information. 
   Its output is used by the "optimset" command.
2. The `optimset` command refines the "discover" step to check on full availability of 
   specified data over the specified date information. Its output feeds is used by the `remote_retrieve` command.
3. The `remote_retrieve` or `download` commands then retrieves the data using the OPeNDAP protocol
   or a wget command, respectively. 
4. The data retrieved from `download` is structured in a way that can be accessed by subsequent `discover` and
   other commands.
5. The data retrieved from `remote_retrieve` keep the same structure as `optimset`. It can then be processed
   using the command `apply`.

The source code for this project is available on github: https://github.com/laliberte/cdb_query
