.. _install-source:

Installing python and netCDF4 from source
-----------------------------------------

This section explains how to compile
* Python 2.7.x.
* A recent version of the netCDF4 library.
* Different python packages necessary for `cdb_query`

.. attention:: If you have a knowledgeable system administrator that can help you with
               the following, it is strongly recommended that you seek help before
               attempting to compile these libraries yourself.

Before attempting to compile these from scratch, check whether you have those installed.
To check this, you should ask yourself the following questions:

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
          - ZLIB (tested with zlib-1.2.8)
          - SZIP compiled using ZLIB (tested with szip-2.1)
          - HDF5 compiled using SZIP and ZLIB (tested with hdf5-1.8.11)
          - netCDF4 library compiled with DAP support. DAP support requires CURL (usually installed on 
            common OS, here tested with curl-7.15.5). The version of netcdf used here is netcdf-4.3.1-rc4

          Please visit http://www.unidata.ucar.edu/software/netcdf/docs/netcdf-install/Quick-Instructions.html#Quick-Instructions
          for instructions on how to build these libraries.

    * If you are recompiling netCDF4, make sure that ``which nc-config`` points to the new netCDF4.

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

The package netcdf4-python does not always compile nicely using ``pip``. If it does compile you can skip the next section.

Finally, try::

    $ pip install h5py
    $ pip install netcdf4

Again the package h5py does not always compile nicely using ``pip``. If it does compile you can skip the next section.
