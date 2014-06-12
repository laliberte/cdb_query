Installation instructions
=========================


.. attention:: The long list of requirements may seem daunting but on many
               standard linux distributions they should be relatively easy to
               fulfill. Here it is assumed that Linux is used.

This package requires:

Core requirements
-----------------

* Python 2.7.x.
* A recent version of the netCDF4 library. Ideally, it would be of the 4.3.1 vintage.
* ESGF certificates.

Before proceeding further, are you willing to use a 3rd party Linux Distribution (Free for Academic Use)?

* If yes, follow the instructions :ref:`install-distro`

* If no, follow the instructions :ref:`install-source`

At this point in the installation, it is assumed that you have a working python distribution with
the netCDF4 python package compiled.

Installing this package: `cdb_query`
------------------------------------
This package can be installed with ``pip``::

    $ pip install cdb_query

.. warning:: If you are using a virtual environment, you must always ``source $HOME/python/bin/activate`` BEFORE
             using ``cdb_query``. If you are using Anaconda, you must activate it (see :ref:`install-distro`).


Obtaining ESGF certificates
---------------------------

This package allows you to obtain and manage ESGF certificates transparently. The only
actions a user should take is 

1. Register at http://badc.nerc.ac.uk/reg/user_register_info.html or https://esg-datanode.jpl.nasa.gov/esgf-web-fe/createAccount.
   When registering, you will create a `password` and `username`. You will then receive an `openid`.

2. Click on each of the following links (one after the other) into a browser. You will then be prompted to enter your `openid` followed by
   your `password`. You should then be asked to register for a user group. Most users will choose `CMIP5 Research`. Once, you've selected a
   user group, a file should start downloading. You can stop the transfer and repeat these steps for the next link::

   http://esg.bnu.edu.cn/thredds/fileServer/cmip5/BNU/BNU-ESM/1pctCO2/3hr/atmos/clt/r1i1p1/clt_3hr_BNU-ESM_1pctCO2_r1i1p1_196101010000-199012312100.nc
   http://cmip3.dkrz.de/thredds/dodsC/cmip5/output1/BCC/bcc-csm1-1/rcp45/day/atmos/day/r1i1p1/v20120705/ta/ta_day_bcc-csm1-1_rcp45_r1i1p1_20060101-20251231.nc
   http://albedo2.dkrz.de/thredds/dodsC/cmip5/output1/LASG-CESS/FGOALS-g2/rcp45/day/atmos/day/r1i1p1/v1/ta/ta_day_FGOALS-g2_rcp45_r1i1p1_20060101-20061231.nc
   http://esgf-data1.ceda.ac.uk/thredds/dodsC/esg_dataroot/cmip5/output1/IPSL/IPSL-CM5B-LR/rcp45/day/atmos/day/r1i1p1/v20120430/ta/ta_day_IPSL-CM5B-LR_rcp45_r1i1p1_20060101-20151231.nc
   http://vesg.ipsl.fr/thredds/dodsC/esg_dataroot/CMIP5/output1/IPSL/IPSL-CM5B-LR/rcp45/day/atmos/day/r1i1p1/v20120430/ta/ta_day_IPSL-CM5B-LR_rcp45_r1i1p1_20960101-21001231.nc
   http://pcmdi9.llnl.gov/thredds/dodsC/cmip5_css02_data/cmip5/output1/CSIRO-BOM/ACCESS1-0/rcp45/fx/atmos/fx/r0i0p0/orog/1/orog_fx_ACCESS1-0_rcp45_r0i0p0.nc
   http://bmbf-ipcc-ar5.dkrz.de/thredds/dodsC/cmip5/output1/MPI-M/MPI-ESM-MR/rcp45/day/atmos/day/r2i1p1/v20120628/ta/ta_day_MPI-ESM-MR_rcp45_r2i1p1_21000101-21001231.nc
   http://esg.cnrm-game-meteo.fr/thredds/dodsC/esg_dataroot1/CMIP5/output1/CNRM-CERFACS/CNRM-CM5/rcp45/day/atmos/day/r1i1p1/v20121001/ta/ta_day_CNRM-CM5_rcp45_r1i1p1_20960101-21001231.nc

3. Run the following command::

        $ cdb_query_CMIP5 certificates username password registering_service

   where the ``registering_service`` is ``badc`` is you used the first link to register and ``jpl`` if you used the second link.


Secondary tools used in the recipes
-----------------------------------

netCDF Operators (NCO)
^^^^^^^^^^^^^^^^^^^^^^
Some of the recipes make use of `NCO`. These recipes were tested using version 4.4.0 linked against the
netcdf libraries built in :ref:`install-source`. Please consult the project's webpage for information on how to install: http://nco.sourceforge.net/.

These recipes were tested using the `NCO` built using the BASH script found in :ref:`install-nco`

NcView
^^^^^^
With all the libraries properly installed, `NcView` is now easy to install::
    
    $ wget ftp://cirrus.ucsd.edu/pub/ncview/ncview-2.1.2.tar.gz
    $ tar xvfz ncview-2.1.2.tar.gz
    $ cd ncview-2.1.2
    $ ./configure --with-netcdf_incdir=/usr/local/packages/netcdf-c-4.3.1-rc2/include/ \
                  --with-netcdf_libname=libnetcdf.so.7 \
                  --with-netcdf_libdir=/usr/local/packages/netcdf-c-4.3.1-rc2/lib/ \
                  --with-udunits2_incdir=/home/laliberte/local/nco-4.4.0/udunits-2.1.24/include \
                  --with-udunits2_libdir=/home/laliberte/local/nco-4.4.0/udunits-2.1.24/lib \
                  --prefix=$HOME/ncview-2.1.2 \
                  --with-nc-config=/usr/local/packages/netcdf-c-4.3.1-rc2/bin/nc-config 
    $ make
    $ make install

This installation installs `NcView` in ``$HOME/local/ncview-2.1.2/bin`` and this directory should be added to your path.

Climate Data Operators (CDO)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The netCDF4 files generated by `cdb_query` are not compatible with `CDO`. `NCO` can be used to extract variables and
remove the hierarchical structure. The retrieved data will then be compatible with `CDO`. With all the installed libraries,
`CDO` is relatively easy to install.


JASPER
""""""
You will need to first install `jasper`::

    $ wget http://www.ece.uvic.ca/~frodo/jasper/software/jasper-1.900.1.zip
    $ unzip jasper-1.900.1.zip
    $ cd jasper-1.900.1
    $ ./configure --with-pic --prefix=$HOME/local/jasper-1.900.1
    $ make
    $ make install

PROJ
""""
Next, you will need `proj`::
    
    $ wget http://download.osgeo.org/proj/proj-4.8.0.tar.gz
    $ tar xvfz proj-4.8.0.tar.gz
    $ cd proj-4.8.0
    $ ./configure --without-jni --prefix=$HOME/local/proj-4.8.0
    $ make check
    $ make install

GRIB-API
""""""""
Then you will need ``grib-api``::

    $ wget https://software.ecmwf.int/wiki/download/attachments/3473437/grib_api-1.11.0.tar.gz
    $ tar xvfz grib_api-1.11.0.tar.gz
    $ cd grib_api-1.11.0
    $ ./configure --with-netcdf=/usr/local/packages/netcdf-c-4.3.1-rc2/ \
                  --with-jasper=$HOME/local/jasper-1.900.1/ \
                  --prefix=$HOME/local/grib_api-1.11.0
    $ make check
    $ make install

CDO
"""

Finally, you are ready to install `CDO`::

    $ wget --no-check-certificate https://code.zmaw.de/attachments/download/6764/cdo-1.6.2.tar.gz
    $ tar xvfz cdo-1.6.2.tar.gz
    $ cd cdo-1.6.2
    $ ./configure --prefix=$HOME/local/cdo-1.6.2 \
                  --with-proj=$HOME/local/proj-4.8.0 \
                  --with-grib_api=$HOME/local/grib_api-1.11.0 \
                  --with-jasper=$HOME/local/jasper-1.900.1 \
                  --with-netcdf=/usr/local/packages/netcdf-c-4.3.1-rc2/  \
                  --with-hdf5=/usr/local/packages/hdf5/ \
                  --with-zlib=/usr/local/packages/zlib/ \
                  --with-szlib=/usr/local/packages/szip/ \
                  --with-udunits2=$HOME/local/nco-4.4.0/udunits-2.1.24/ \
                  -enable-cgribex=no CFLAGS=-DHAVE_LIBNC_DAP
    $ make check
    $ make install

where ``/usr/local/packages/zlib/``, ``/usr/local/packages/szip/``, ``/usr/local/packages/hdf5/`` and ``/usr/local/packages/netcdf-c-4.3.1-rc2/``
are the location of your ZLIB, SZIP, HDF5 and netCDF4 libraries.

This installation installs `CDO` in ``$HOME/local/cdo-1.6.1/bin`` and this directory should be added to your path.

You can check that everything was done ok::
    
    $ cdo -V
    Climate Data Operators version 1.6.2 (http://code.zmaw.de/projects/cdo)
    Compiler: gcc -std=gnu99 -DHAVE_LIBNC_DAP -pthread
    version: gcc (GCC) 4.1.2 20080704 (Red Hat 4.1.2-54)
    Compiled: (x86_64-unknown-linux-gnu) Feb  6 2014 16:30:19
    Features: PTHREADS NC4 OPeNDAP SZ Z JASPER UDUNITS2 PROJ.4
    Libraries: proj/4.8
    Filetypes: srv ext ieg grb grb2 nc nc2 nc4 nc4c 
    CDI library version : 1.6.2 of Feb  6 2014 16:30:13
    GRIB_API library version : 1.11.0
    netCDF library version : 4.3.1-rc2 of Feb  4 2014 15:06:12 $
    HDF5 library version : 1.8.11
    SERVICE library version : 1.3.1 of Feb  6 2014 16:30:08
    EXTRA library version : 1.3.1 of Feb  6 2014 16:30:05
    IEG library version : 1.3.1 of Feb  6 2014 16:30:06
    FILE library version : 1.8.2 of Feb  6 2014 16:30:05

The `Features` line indicates that netCDF4 files are accepted, OPeNDAP links can be read and that
compressed variables can be created (SZ, Z).
