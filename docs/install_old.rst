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
* A `myproxy` manager.

Before proceeding further, are you willing to use a 3rd party Linux Distribution (Free for Academic Use)?

* If yes, follow the instructions

* If no, follow the instructions 

.. attention:: If you have a knowledgeable system administrator that can help you with
               the following, it is strongly recommended that you seek help before
               attempting to compile these libraries yourself.

To check whether you have those installed, you should ask yourself the following questions:

* Do you have a `myproxy` manager? Our experience suggests that the best and easiest way to obtain a
  `myproxy` manager is through the ``myproxy`` package available on most Linux distributions.

    * It is easily installed by a system administrator with::
        
        $ yum install myproxy

    * If you are not a Linux user, you best option is to parse through
      http://www.unidata.ucar.edu/software/netcdf/docs/esg.html or 
      http://cmip-pcmdi.llnl.gov/cmip5/data_getting_started.html (points 6,7).

    * If you are a Linux user and your system administrator cannot install this package,
      you best bet is to compile only a section of the Globus Toolkit. This is a difficult 
      package to install but we have been successful with the following procedure::

          $ wget --no-check-certificate http://www.globus.org/ftppub/gt5/5.0/5.0.0/installers/src/gt5.0.0-all-source-installer.tar.bz2
          $ tar xvfj gt5.0.0-all-source-installer.tar.bz2
          $ cd gt5.0.0-all-source-installer
          $ ./configure --prefix=$HOME/local/gt-5.0.0
          $ make install myproxy
      
      Some warnings may persist but it is likely to work for the purpose of this package.

* Do you have Python 2.7.x? To check this, run::

    $ python --version
    Python 2.7.6

  If your version is 2.6.x or older, this package will NOT work. Ask your system administrator
  to install a Python 2.7.x. If this is not possible, you can try to compile it yourself::

    $ wget http://www.python.org/ftp/python/2.7.6/Python-2.7.6.tgz
    $ tar xvfz Python-2.7.6.tgz
    $ cd Python-2.7.6
    $ ./configure --prefix=$HOME/local/Python-2.7.6
    $ make
    $ make test
    $ make install

  The ``make`` is likely to mention missing libraries but as long it is completes without errors,
  it should be OK for this package. This installs python in ``$HOME/local/Python-2.7.6``::

    $ $HOME/local/Python-2.7.6/bin/python --version
    Python 2.7.6
 
.. warning:: Enthought Python Distributions (EPD), including Enthought Canopy will NOT
             work with this package. This may change in the future but as of March 3, 2014
             they do not appear to be working.

* Do you have netCDF4 installed?
    * First, check whether you have ``nc-config`` and find its version::
        
        $ nc-config --version
        netCDF 4.3.1-rc4

    * If you have a working netCDF4 version that is older than 4.3.0 but more recent than 4.2.0
      most features in this package should work.

    * It is however strongly recommended that you upgrade to 4.3.1-rc4 or a more recent version.
      If you have netCDF4 already installed, you should have ZLIB, SZIP and HDF5. Then
      you only need to install a recent version of netCDF4.
      We are suggesting version 4.3.1-rc4
      because version 4.3.1.1 appears to require a recent version of CURL to work properly
      and most common Linux distributions do not have the adequate version. For the purposes
      of this package version 4.3.1-rc4 should work just fine. It can be obtained through::

          $ wget ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4.3.1-rc4.tar.gz
      
      Please visit http://www.unidata.ucar.edu/software/netcdf/docs/netcdf-install/Quick-Instructions.html#Quick-Instructions
      for instructions on how to build this library. 

    * If you do not have ``nc-config``, then it is likely that you will need to compile the following libraries,
      compiled in this order:
          * ZLIB (tested with zlib-1.2.8)
          * SZIP compiled using ZLIB (tested with szip-2.1)
          * HDF5 compiled using SZIP and ZLIB (tested with hdf5-1.8.11)
          * netCDF4 library compiled with DAP support. DAP support requires CURL (usually installed on 
            common OS, here tested with curl-7.15.5). The version of netcdf used here is netcdf-4.3.1-rc4

          Please visit http://www.unidata.ucar.edu/software/netcdf/docs/netcdf-install/Quick-Instructions.html#Quick-Instructions
          for instructions on how to build these libraries.

    * If you are recompiling netCDF4, make sure that ``which nc-config`` points to the new netCDF4.

Python Packages
---------------

Packages installable from PyPI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The next step is to install the following python packages:

* numpy
* sqlalchemy
* Cython
* python-dateutil

These should be easy to install::

    $ pip install numpy
    $ pip install sqlalchemy
    $ pip install Cython
    $ pip install python-dateutil

If you do not have root access to your system the best approach is to
create a virtual python environment. First download and use python package `virtualenv` 
from https://pypi.python.org/pypi/virtualenv.
This step was tested using https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.11.2.tar.gz::
    
    $ wget --no-check-certificate \
          https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.11.2.tar.gz
    $ tar xvfz virtualenv-1.11.2.tar.gz
    $ cd virtualenv-1.11.2

Then before creating the virtual environment, check that your python is version 2.7.x::
    
    $python --version
    Python 2.7.6

If yes, then create a virtual environment in ``$HOME/python``::

    $ python virtualenv.py $HOME/python

Activate it::

    $ source $HOME/python/bin/activate

Finally, install the python packages you require::

    $ pip install numpy 
    $ pip install sqlalchemy
    $ pip install Cython
    $ pip install python-dateutil

Then try::

    $ export USE_NCCONFIG=1;pip install netcdf4

The package netcdf4-python does not always compile nicely using ``pip``. If it does you can skip the next section.


Packages not installable from PyPI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To install netcdf4-python from source, go to the project page https://code.google.com/p/netcdf4-python/downloads/list and
choose the file that fits your system. Here, we use ``netCDF4-1.1.0.tar.gz``::

    $ wget --no-check-certificate https://pypi.python.org/packages/source/n/netCDF4/netCDF4-1.1.0.tar.gz#md5=8e2958160c8cccfc80f61ae0427e067f
    $ tar xvfz netCDF4-1.1.0.tar.gz
    $ cd netCDF4-1.1.0

.. warning:: These steps are crucial:
            
             * Copy setup.cfg.template to setup.cfg: ``$ cp setup.cfg.template setup.cfg``
             * Open with a text editor
             * Follow the instructions in the comments for editing.
             * Get help from your system administrator if your are trying to locate the path
               to your netcdf4 and hdf5 libraries (installed at the begining).
             * Because you should have a recent netCDF4 version, you can use nc-config.
               In this case, you just have to know where it can be found in your directory tree.
               Our installation of netcdf4 was in ``/usr/local/packages/netcdf-c-4.3.1-rc2/`` so in
               the ``setup.cfg`` we set::
                    # Rename this file to setup.cfg to set build options.
                    # Follow instructions below for editing.
                    [options]
                    # if true, the nc-config script (installed with netcdf 4.1.2 and higher)
                    # will be used to determine the locations of required libraries.
                    use_ncconfig=True
                    # path to nc-config script.
                    ncconfig=/usr/local/packages/netcdf-c-4.3.2/bin/nc-config
               and left everything else untouched. 

Once ``setup.cfg`` is properly edited::
    
    $ python setup.py build
    $ python setup.py install

Run the tests::

    $ cd test; python run_all.py; cd ..

If all tests were passed, the installation was successful!

Installing this package: `cdb_query`
-------------------------------------
This package can be installed with ``pip``::

    $ pip install cdb_query

.. warning:: If you are using a virtual environment, you must always ``source $HOME/python/bin/activate`` BEFORE
             using ``cdb_query``

ESGF certificates manager
-------------------------

This will likely be the most difficult part of the installation for most users.
There are several web resources for setting up your certificates but they all
differ slightly. 

Here we assume that the users have accomplished steps 1,2,3 from http://cmip-pcmdi.llnl.gov/cmip5/data_getting_started.html)
and that they have an account on the ESGF.

Then there is a three steps procedure to obtain certificates:

Edit your ``.bash_profile``
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Add these two lines to your ``.bash_profile``::

    export X509_CERT_DIR=$HOME/.esg/certificates
    export X509_USER_PROXY=$HOME/.esg/credentials.pem

and source your ``.bash_profile``::

    $ source ~/.bash_profile

Create ``.dodsrc`` file
^^^^^^^^^^^^^^^^^^^^^^^

In your root directory, create the file ``.dodsrc`` and paste these line into it::

    # OPeNDAP client configuration file. See the OPeNDAP
    # users guide for information.
    USE_CACHE=0
    # Cache and object size are given in megabytes (20 ==> 20Mb).
    MAX_CACHE_SIZE=20
    MAX_CACHED_OBJ=5
    IGNORE_EXPIRES=0
    CACHE_ROOT=/home/laliberte/.dods_cache/
    DEFAULT_EXPIRES=86400
    ALWAYS_VALIDATE=0
    # Request servers compress responses if possible?
    # 1 (yes) or 0 (false).
    DEFLATE=0
    # Should SSL certificates and hosts be validated? SSL
    # will only work with signed certificates.
    VALIDATE_SSL=1
    # Proxy configuration (optional parts in []s).
    # You may also use the 'http_proxy' environment variable
    # but a value in this file will override that env variable.
    # PROXY_SERVER=[http://][username:password@]host[:port]
    # NO_PROXY_FOR=<host|domain>
    # AIS_DATABASE=<file or url>
    # COOKIE_JAR=.dods_cookies
    # The cookie jar is a file that holds cookies sent from
    # servers such as single signon systems. Uncomment this
    # option and provide a file name to activate this feature.
    # If the value is a filename, it will be created in this
    # directory; a full pathname can be used to force a specific
    CURL.VERBOSE=0
    CURL.COOKIEJAR=.dods_cookies
    CURL.SSL.VALIDATE=1
    CURL.SSL.CERTIFICATE=/home/laliberte/.esg/credentials.pem
    CURL.SSL.KEY=/home/laliberte/.esg/credentials.pem
    CURL.SSL.CAPATH=/home/laliberte/.esg/certificates

    HTTP.VERBOSE=0
    HTTP.COOKIEJAR=.dods_cookies
    HTTP.SSL.VALIDATE=1
    HTTP.SSL.CERTIFICATE=/home/laliberte/.esg/credentials.pem
    HTTP.SSL.KEY=/home/laliberte/.esg/credentials.pem
    HTTP.SSL.CAPATH=/home/laliberte/.esg/certificates

.. warning:: Replace all occurences of ``laliberte`` with your local username before
             closing this file!

Obtain the certificate
^^^^^^^^^^^^^^^^^^^^^^

Running the command::

    $ myproxy-logon -t 24 -T -s pcmdi9.llnl.gov -l laliberte

should then install your certificates. You have to replace ``pcmdi9.llnl.gov`` with the
server name where you have obtained your ESGF account and replace ``laliberte`` with your
ESGF username.

.. warning:: The command ``myproxy-logon`` must re-run every day.

Alternatively, users can have a look at http://www.unidata.ucar.edu/software/netcdf/docs/esg.html
or at http://cmip-pcmdi.llnl.gov/cmip5/data_getting_started.html (points 6,7)

Secondary tools used in the recipes
-----------------------------------

netCDF Operators (NCO)
^^^^^^^^^^^^^^^^^^^^^^
Some of the recipes make use of `NCO`. These recipes were tested using version 4.4.0 linked against the aforementioned
netcdf libraries. Please consult the project's webpage for information on how to install: http://nco.sourceforge.net/.

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
