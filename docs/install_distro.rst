.. _install-distro:

Installing python and netCDF4 from a distribution
-------------------------------------------------

Anaconda Scientific Python Distribution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following was tested with Anaconda Scientific Python Distribution (http://continuum.io/).
It is free for academic use and includes almost all of the prerequisite for `cdb_query`.

To install it, go to https://store.continuum.io/cshop/anaconda/, choose your operating system
and follow their instructions. `cdb_query` has only been tested on linux machines. 

- If you have installed the full distribution and if you have used the default installation directories,
  you should activate the distribution::

    $ source $HOME/anaconda/bin/activate $HOME/anaconda

  Recent changes to the distribution have broken some cross linking in the necessary libraries. To
  prevent these changes from affecting `cdb_query`, revert to version 2.0.1 of Anaconda::

    $ conda install anaconda=2.0.1

  Once this is done, you have to install netCDF4. This is accomplished through their installation
  application::

    $ conda install --no-deps libnetcdf=4.2.1.1 netcdf4=1.0.8 cffi=0.8.6 cryptography=0.5.4

.. warning:: Do not use `conda` to install other packages BEFORE you have made sure that `cdb_query` is working properly.

..
    Miniconda Fix
    ^^^^^^^^^^^^^
- If you have installed the lightweigth Miniconda distribution (http://conda.pydata.org/miniconda.html) and you have used the default installation directories,
  you should activate the distribution::

    $ source $HOME/miniconda/bin/activate $HOME/miniconda

  Recent changes to the distribution have broken some cross linking in the necessary libraries. To
  prevent these changes from affecting `cdb_query`, revert to version 2.0.1 of Anaconda::

    $ conda install anaconda=2.0.1

  Once this is done, you have to install netCDF4. This is accomplished through their installation
  application::

    $ conda install --no-deps libnetcdf=4.2.1.1 netcdf4=1.0.8 cffi=0.8.6 cryptography=0.5.4


  Once this is done, you have to install netCDF4. This is accomplished through their installation
  application::

    $ conda install --no-deps curl=7.30.0 \
                              h5py=2.3.0 \
                              hdf5=1.8.9 \
                              jinja2=2.7.3 \
                              libnetcdf=4.2.1.1 \
                              markupsafe=0.23 \
                              netcdf4=1.0.8 \
                              numpy=1.8.2 \
                              pip=1.5.6 \
                              setuptools=5.7 \
                              sqlalchemy=0.9.7 \
                              cffi=0.8.6 \
                              cryptography=0.5.4


..
    Canopy Enthought Python Distribution
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    .. warning:: Including Enthought Canopy will NOT
                 work with this package. This may change in the future but as of March 3, 2014
                 they do not appear to be working.

    The following was tested with Canopy Enthought Python Distribution (https://www.enthought.com)
    It is free for academic use and includes almost all of the prerequisite for `cdb_query`.

    To install it, go to https://www.enthought.com/downloads/, choose your operating system
    and follow their instructions. `cdb_query` has only been tested on linux machines. 

    On linux, once the installation is complete, you should create the command line interface. 
    The procedure is described at http://docs.enthought.com/canopy/configure/canopy-cli.html#scenario-creating-an-epd-like-python-environment.

    If you have used the default installation directories, you can now activate the distribution::

    $ source $HOME/canopy/bin/activate
    
