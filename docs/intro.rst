Introduction
============

This code is intended to provide an interface between the CMIP5/CORDEX Data Reference Syntax 
(hereafter DRS) for a local repository of CMIP5/CORDEX data, the Earth System Grid Federation
(hereafter ESGF) search engine for the efficient, robust and timely computation of climate diagnostics.

The cdb_query package contains separate command sets for CMIP5 and CORDEX archives. 
The standard workflow for both packages is similar:

1. A `ask` command searches the archive for specific variables and user specified date information. 
   Its output is used by the "validate" command.
2. The `validate` command refines the "ask" step to check on full availability of 
   specified data over the specified date information. Its output feeds is used by the `download` command.
3. The `download` or `download_raw` commands then retrieves the data using the OPeNDAP protocol
   or a wget command, respectively. 
4. The data retrieved from `download_raw` is structured in a way that can be accessed by subsequent `ask` and
   other commands.
5. The data retrieved from `download` keep the same structure as `validate`. It can then be processed
   using the commands `apply` and `convert`.

The source code for this project is available on github: https://github.com/laliberte/cdb_query
