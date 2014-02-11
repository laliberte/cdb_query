.. _install-nco:

BASH script to compile and install NCO on CentOS
================================================

The following script has been used on CentOS 5 to
successfully build `NCO`.
It should compile and install `NCO` sucessfully but it should fail at building the
`NCO` documentation. This is not a problem.
It is a modified version of
a script found on http://idolinux.blogspot.ca/2011/02/nco-netcdf-operators-build-log.html::

    #!/bin/bash
    #This script compiles NCO assuming that
    #netCDF4 and HDF5 libraries are already compiled,
    #tested and installed on the current system.

    ######################################
    # You MUST modify the following lines:
    ######################################
    INSTALL_PATH=$HOME/local/nco-4.4.0

    #set these to 0 if you have to re-run this
    #script and some of these libraries
    #compiled succesfully:
    INSTALL_ANTLR=1
    INSTALL_UDUNITS=1
    INSTALL_GSL=1

    NETCDF4_DIR="/usr/local/packages/netcdf"
    HDF5_DIR="/usr/local/packages/hdf5"
    ##############################################
    # Please do not modify anything past this line
    # before trying the script!
    ##############################################

    mkdir -p $INSTALL_PATH/src

    # ANTLR2
    # Do not change the version of ANTLR
    # NCO works ONLY with versions 2.7.x and
    # NOT with newer versions:
        APP=antlr-2.7.7
        ANTLR_PATH=$INSTALL_PATH/$APP
        if [ "$INSTALL_ANTLR" -eq "1" ]; then
            rm -rf $ANTLR_PATH
            cd $INSTALL_PATH/src

            rm -rf ${APP}
            wget http://www.antlr2.org/download/${APP}.tar.gz
            tar xzf ${APP}.tar.gz ; cd ${APP}

            CC=gcc CXX='' ./configure \
            --prefix=$ANTLR_PATH \
            --disable-csharp \
            --disable-java \
            --disable-python 2>&1 | tee $APP.config
            make 2>&1 | tee $APP.make
            make install 2>&1 | tee $APP.install
        fi


    # UDUNITS
    # Here, you can try to change the version number if
    # a newer version if avaiable.
        APP=udunits-2.1.24
        UDUNITS_PATH=$INSTALL_PATH/$APP
        if [ "$INSTALL_UDUNITS" -eq "1" ]; then
            rm -rf $UDUNITS_PATH
            cd $INSTALL_PATH/src

            rm -rf ${APP}*
            wget ftp://ftp.unidata.ucar.edu/pub/udunits/${APP}.tar.gz
            tar xzf ${APP}.tar.gz ; cd ${APP}

            CC=gcc CXX='' F77=gfortran ./configure \
            --prefix=$UDUNITS_PATH 2>&1 | tee $APP.config
            make 2>&1 | tee $APP.make
            make install 2>&1 | tee $APP.install
        fi

    #GSL
    # Here, you can try to change the version number if
    # a newer version if avaiable.
        APP=gsl-1.16
        GSL_PATH=$INSTALL_PATH/$APP
        if [ "$INSTALL_GSL" -eq "1" ]; then
            rm -rf $GSL_PATH
            cd $INSTALL_PATH/src

            rm -rf ${APP}*
            wget ftp://ftp.gnu.org/gnu/gsl/${APP}.tar.gz
            tar xzf ${APP}.tar.gz ; cd ${APP}

            ./configure \
            --prefix=$GSL_PATH \
            CFLAGS="-fexceptions" | tee $APP.config
            make 2>&1 | tee $APP.make
            make install 2>&1 | tee $APP.install
        fi

    # NCO
    # Here, you can try to change the version number if
    # a newer version if available.
        APP=nco-4.4.0
        NCO_PATH=$INSTALL_PATH/$APP
        rm -rf $NCO_PATH
        cd $INSTALL_PATH/src

        rm -rf ${APP}*
        wget http://nco.sourceforge.net/src/${APP}.tar.gz
        tar xzf ${APP}.tar.gz ; cd ${APP}

        export LD_LIBRARY_PATH=$HDF5_DIR/lib:$LD_LIBRARY_PATH
        export PATH=$HDF5_DIR/bin:$PATH
        export LD_LIBRARY_PATH=$NETCDF4_DIR/lib:$LD_LIBRARY_PATH
        export PATH=$NETCDF4_DIR/bin:$PATH
        export LD_LIBRARY_PATH=$ANTLR_PATH/lib:$LD_LIBRARY_PATH
        export PATH=$ANTLR_PATH/bin:$PATH
        export LD_LIBRARY_PATH=$UDUNITS_PATH/lib:$LD_LIBRARY_PATH
        export PATH=$UDUNITS_PATH/bin:$PATH
        export LD_LIBRARY_PATH=$GSL_PATH/lib:$LD_LIBRARY_PATH
        export PATH=$GSL_PATH/bin:$PATH

        CC=gcc CXX='' \
        NETCDF_INC=$NETCDF4_DIR/include \
        NETCDF_LIB=$NETCDF4_DIR/lib \
        NETCDF4_ROOT=$NETCDF4_DIR \
        HDF5_LIB_DIR=$HDF5_DIR/lib \
        UDUNITS2_PATH=$UDUNITS_PATH \
        LDFLAGS="-L$ANTLR_PATH/lib -lantlr \
        -lhdf5_hl -lhdf5 -L$NETCDF4_DIR/lib -lnetcdf" \
        CFLAGS="-I$HDF5_DIR/include \
        -L$HDF5_DIR/lib \
        -I$ANTLR_PATH/include \
        -L$ANTLR_PATH/lib" \
        CPPFLAGS="-I$HDF5_DIR/include \
        -L$HDF5_DIR/lib \
        -I$ANTLR_PATH/include \
        -L$ANTLR_PATH/lib" \
        ./configure \
        --prefix=$NCO_PATH \
        --enable-optimize-custom \
        --enable-netcdf-4 2>&1 | tee $APP.config
        echo "#define ENABLE_NETCDF4 1" >> config.h
        make 2>&1 | tee $APP.make
    make install 2>&1 | tee $APP.install

Once this script is completed, add ``$INSTALL_PATH/nco-4.4.0/bin`` to your path.
