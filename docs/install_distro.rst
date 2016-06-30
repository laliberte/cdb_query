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

    $ conda install netcdf4 h5py Paste PasteDeploy

.. warning:: Do not use `conda` to install other packages BEFORE you have made sure that `cdb_query` is working properly.
