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
   specified data over the specified date information. Its output contains full metadata for all the
   available datasets along with soft links to the data. Its output is used by the `reduce_soft_links`, `download_opendap` and `download_files` commands.
3. The `reduce_soft_links` command can apply a script onto the output of `validate` to enable subsetting.
4. The `download_opendap` or `download_files` commands then retrieves the data using the OPeNDAP protocol
   or a wget command, respectively. 
5. The data retrieved from `download_files` is structured in a way that can be accessed by subsequent `ask` and
   other commands.
6. The data retrieved from `download_opendap` keep the same structure as `validate`. It can then be processed
   using the command `reduce`.

The source code for this project is available on github: https://github.com/laliberte/cdb_query
