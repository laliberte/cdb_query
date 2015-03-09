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

  Once this is done, you have to install netCDF4 and HDF5. This is accomplished through their installation
  application::

    $ conda install netcdf4 h5py

.. warning:: Do not use `conda` to install other packages BEFORE you have made sure that `cdb_query` is working properly.

- If you have installed the lightweigth Miniconda distribution (http://conda.pydata.org/miniconda.html) and you have used the default installation directories,
  you should activate the distribution::

    $ source $HOME/miniconda/bin/activate $HOME/miniconda

  Once this is done, you have to install netCDF4. This is accomplished through their installation
  application::

    $ conda install netcdf4 h5py jinja2 pip sqlalchemy

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
    
