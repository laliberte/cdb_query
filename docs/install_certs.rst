.. _install-certs:

`myproxy` manager
-----------------
Our experience suggests that the best and easiest way to obtain a
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
          $ ./configure --disable-system-openssl --prefix=$HOME/local/gt-5.0.0
          $ make myproxy
          $ make install myproxy
      
      Some warnings may persist but it is likely to work for the purpose of this package.

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
