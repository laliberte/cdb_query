Here are the steps that were used to install the netCDF4 library.

* ZLIB::

    $ wget ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4/zlib-1.2.8.tar.gz
    $ tar xvfz zlib-1.2.8.tar.gz
    $ cd zlib-1.2.8
    $ ./configure --prefix=$HOME/local2/zlib-1.2.8
    $ make check
    $ make install

* SZIP::

    $ wget http://www.hdfgroup.org/ftp/lib-external/szip/2.1/src/szip-2.1.tar.gz
    $ tar xvfz szip-2.1.tar.gz
    $ cd szip-2.1
    $ ./configure --prefix=$HOME/local2/szip-2.1
    $ make check
    $ make install

* HDF5::

    $ wget ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4/hdf5-1.8.12.tar.gz
    $ tar xvfz hdf5-1.8.12.tar.gz
    $ cd hdf5-1.8.12
    $ ./configure --with-zlib=/home/fred1/local2/zlib-1.2.8/ --with-szlib=/home/fred1/local2/szip-2.1/ --prefix=$HOME/local2/hdf5-1.8.12
    $ make check
    $ cd tools/hd5import
    $ make
    $ cd ../../
    $ make check
    $ make install

* CURL::

    $ wget ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4/curl-7.26.0.tar.gz
    $ tar xvfz curl-7.26.0.tar.gz
    $ cd curl-7.26.0
    $ make check
    $ make install

* netCDF4::

    $ wget ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4.3.2.tar.gz
    $ tar xvfz netcdf-4.3.2.tar.gz
    $ cd netcdf-4.3.2
    $ CPPFLAGS="-I/home/fred1/local2/zlib-1.2.8/include/ -I/home/fred1/local2/szip-2.1/include -I/home/fred1/local2/hdf5-1.8.12/include"  LDFLAGS="-L/home/fred1/local2/zlib-1.2.8/lib -L/home/fred1/local2/szip-2.1/lib -L/home/fred1/local2/hdf5-1.8.12/lib" ./configure --prefix=$HOME/local2/netcdf-4.3.2
    $ make check
    $ make install
