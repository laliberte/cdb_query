cdb_query
=========

[![Build Status](https://travis-ci.org/laliberte/cdb_query.svg?branch=master](https://travis-ci.org/laliberte/cdb_query)
[![Python2](https://img.shields.io/badge/python-2-blue.svg)](https://www.python.org/downloads/)
[![Python3](https://img.shields.io/badge/python-3-blue.svg)](https://www.python.org/downloads/)
[![documentation](https://readthedocs.org/projects/cdb_query/badge/?version=latest)](http://cdb_query.readthedocs.org/en/latest/)
[![PyPI](https://img.shields.io/pypi/v/cdb_query.svg?maxAge=2592000?style=plastic)](https://pypi.python.org/pypi/cdB_query/)

Python code to manage the analysis of climate model outputs published in the CMIP5 and CORDEX archives

This package provides simple tools to process data from the CMIP5 and CORDEX archives distributed 
by the Earth System Grid Federation.

This package was developed by F. B. Laliberte and P. J. Kushner as part of the "ExArch: Climate analytics
on distributed exascale data archives" G8 Research Initiative grant.

Frederic B. Laliberte, Paul J. Kushner
Univerity of Toronto, 2017

The Natural Sciences and Engineering Research Council of Canada (NSERC/CRSNG) funded 
FBL and PJK during this project.

If using this code to retrieve and process data from the ESGF please cite:
Efficient, robust and timely analysis of Earth System Models: a database-query approach
F. Laliberte, Juckes, M., Denvil, S., Kushner, P. J., TBD.

Current ESGF Project Compatibility
----------------------------------
- CMIP5 (full, tested)
- CREATEIP (full, tested)
- CORDEX (full, not completely tested)

Current OTHER Project Compatibility
-----------------------------------
- CanSISE (full, not completely tested)

Testing
-------
A script called ``create_test_env.sh`` (available in the github repository)
can be used to create an Anaconda-based test environment::

    $ git clone https://github.com/laliberte/cdb_query.git
    $ cd cdb_query
    $ git submodule update --init --recursive 
    $ bash create_test_env.sh
    $ source $HOME/miniconda_test/bin/activate $HOME/miniconda_test
    $ source activate cdb_query_test_env
    $ export OPENID_ESGF='YOUR OPENID'
    $ export PASSWORD_ESGF='PASSWORD ASSOCIATED WITH OPENID'
    $ py.test

Usage
----
Once the testing is successful, ``cdb_query`` can be used directly::

    $ cdb_query CMIP5 ask validate download_opendap reduce \
                      --openid=$OPENID_ESGF --password=$PASSWORD_ESGF \
                      --institute=NCAR --model=CCSM4 --ensemble=r1i1p1 \
                      --year=2000 \
                      '' tas_historical_pointers.nc

This command will retrieve year 2000 of the simulation r1i1p1 from the CCSM4 model
in the subdirectory ``./NCAR``.

Version History
---------------

2.0:     Stable, production release. Key packages are now vendored.

1.9.9.x: Bug fixes. piControl experiments now work as intended. Stability. New Projects.
         Performance improvements in reduce. Logging. Travis-CI testing.

1.9.9:   Stability. Better tempfiles handling (no file descriptor leak). More robust ESGF auth.

1.9.8:   Some major improvements in the management of certificates. Added a simple wed file list scrappign capability
         for CanSISE project.

1.9.7.x: Code now compatible with CORDEX. Minor bug fixes. Import error fixes. Minor API changes.

1.9.6.x: Minor bug fixes.

1.9.5: Last major version before release 2.0. Future versions before release 2.0 will be minor bugfixes.
       Interface can be expected to be stable for the forseeable future.

1.9: Major overhaul. Code ready for new ESGF version. CMIP5 works but other projects might not.
     Last version before a LTS version.

1.6: Split the source code in two. Some of the heavy lifiting is now provided
     by the netcdf4_soft_links package.

1.5: Added tracking_id and support for different checksum methods.
     Compatibility with new ESGF architecture.
     Rewrite of important code components.
     Inclusion of modified timeaxis package.

1.4: Stability.

1.3: Added support for FTP archives. Added support for project LRFTIP. Two time series
     described by soft links can now be safely concantenated BEFORE a download.

1.2.2: Several bug fixes. Added support for project NMME.

1.2.1: Enabled certficates management within validate, download and download_raw

1.1.0: Required update for the certificates function. The certificates function in prior versions has been deprecated.

1.0.11.2: Fixed a minor bug with validate.

1.0.11: Fixed a certificate issue. Added the possiblity of making a `partial` validate where only the time stamp in files
        is checked. This allows to sidestep a full validate if download_raw is to be used.

1.0.10.1-2: Fixed a bug with h5py. Critical update for 1.0.9.

1.0.9: Added the possibility of downloading the time step BEFORE and / or AFTER the requested times in 'download' and 'download_raw'.
       Fixed an authentication problem with newer versions of NETCDF4.

1.0.8: Bug fixes with fancy opendap queries.

1.0.7: Several bug fixes with complex queries. Includes basic updating features.

1.0.6: Major fixes to validate. Fixes to download_raw.

1.0.5: Fixed the certificates manager for the POODLE bug. Fixed the handling of piControl experiments.

1.0.4: Fixed handling of CORDEX nodes, fixed validate for more than one experiment.

1.0.3: Minor changes to help.

1.0.3-rc4: Fixed a minor bug in 'validate' command were a model that should have been excluded was not.

1.0.3-rc2: Optimized the 'ask' command. Optimized the asynchronous processing for 'ask', 'validate' and 'apply'.
