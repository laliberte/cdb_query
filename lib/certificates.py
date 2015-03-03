import os, sys, string, subprocess
from myproxy_ws_get_trustroots_wget import myproxy_ws_get_trustroots_wget
from myproxy_ws_logon_wget import myproxy_ws_logon_wget


def retrieve_certificates(username,registering_service):
    home=os.getenv('HOME')
    http_proxy=os.getenv('http_proxy')
    https_proxy=os.getenv('https_proxy')
    if http_proxy != None and https_proxy== None:
      print 'You have http_proxy set but not https_proxy: download tests are likely to fail'

    esgfdir='%s/.esg4' % home

    dods=True

    ee = { 'smhi':'esg-dn1.nsc.liu.se', 'pcmdi':'pcmdi9.llnl.gov', 'ipsl':'esgf-node.ipsl.fr', 'badc':'myproxy.ceda.ac.uk',
    'dkrz':'esgf-data.dkrz.de', 'pik':'esg.pik-potsdam.de', 'jpl':'jpl-esg.jpl.nasa.gov' }
    dodsrc='%s/.dodsrc' % home

    registering_service = ee.get( registering_service, registering_service )

    for f in [esgfdir,esgfdir+'/certificates']:
      if not os.path.isdir( f ):
        os.mkdir( f )

    dodstext = """
    ## generated by enesGetCert
    CURL.VERBOSE=0
    CURL.COOKIEJAR=.dods_cookies
    CURL.SSL.VALIDATE=1
    CURL.SSL.CERTIFICATE=%(esgfdir)s/credentials.pem
    CURL.SSL.KEY=%(esgfdir)s/credentials.pem
    CURL.SSL.CAPATH=%(esgfdir)s/certificates
    HTTP.VERBOSE=0
    HTTP.COOKIEJAR=.dods_cookies
    HTTP.SSL.VALIDATE=1
    HTTP.SSL.CERTIFICATE=%(esgfdir)s/credentials.pem
    HTTP.SSL.KEY=%(esgfdir)s/credentials.pem
    HTTP.SSL.CAPATH=%(esgfdir)s/certificates
    """
    if dods:
      oo = open( dodsrc, 'w' )
      oo.write( dodstext % locals() )
      oo.close()
      #Temporary fix: write a local .dodsrc
      oo = open( '.dodsrc', 'w' )
      oo.write( dodstext % locals() )
      oo.close()

    oo = open(esgfdir+'/myproxy-ws-get-trustroots-wget.sh','w')
    oo.write(myproxy_ws_get_trustroots_wget())
    oo.close()

    subprocess.call(['bash',esgfdir+'/myproxy-ws-get-trustroots-wget.sh','-b','-U','https://'+registering_service+'/get-trustroots'])

    oo = open(esgfdir+'/myproxy-ws-logon-wget.sh','w')
    oo.write(myproxy_ws_logon_wget())
    oo.close()
    subprocess.call(['bash',esgfdir+'/myproxy-ws-logon-wget.sh','-l',username,'-U','https://'+registering_service+'/logon','-o',esgfdir+'/credentials.pem'])

    return
    #port=MyProxyClient.PROPERTY_DEFAULTS['port']
    #lifetime=MyProxyClient.PROPERTY_DEFAULTS['proxyCertLifetime']
    #
    #if MyProxyClient.X509_CERT_DIR_ENVVARNAME in os.environ:
    #    cadir = os.environ[MyProxyClient.X509_CERT_DIR_ENVVARNAME]
    #else:
    #    cadir = os.path.join(
    #                    os.path.expanduser(MyProxyClient.USER_TRUSTROOT_DIR))
    #
    #client_props = dict(caCertDir=cadir,
    #                    hostname=registering_service,
    #                    port=port,
    #                    proxyCertLifetime=lifetime,
    #                    )
    #
    #myproxy = MyProxyClient(**client_props)
    #
    #creds = myproxy.logon(username, password,
    #                      bootstrap=True,
    #                      updateTrustRoots=False)
    #return

def test_certificates():
    home=os.getenv('HOME')
    http_proxy=os.getenv('http_proxy')
    https_proxy=os.getenv('https_proxy')
    if http_proxy != None and https_proxy== None:
      print 'You have http_proxy set but not https_proxy: download tests are likely to fail'

    esgfdir='%s/.esg4' % home

    print 'Testing certificate by running wget request in spider mode'
    cmd='wget -c -nH --certificate=%(esgfdir)s/credentials.pem --private-key=%(esgfdir)s/credentials.pem --save-cookies=%(esgfdir)s/cookies --load-cookies=%(esgfdir)s/cookies --ca-directory=%(esgfdir)s/certificates --no-check-certificate --spider   http://vesg.ipsl.fr/thredds/fileServer/esg_dataroot/CMIP5/output1/IPSL/IPSL-CM5A-LR/rcp85/mon/atmos/cfMon/r1i1p1/v20111119/clhcalipso/clhcalipso_cfMon_IPSL-CM5A-LR_rcp85_r1i1p1_200601-230012.nc  1> .wgsp 2> .wgspe' % locals()
    cmd2=['wget', '-c', '-nH', '--certificate=%(esgfdir)s/credentials.pem' %locals(), '--private-key=%(esgfdir)s/credentials.pem' %locals(), '--save-cookies=%(esgfdir)s/cookies' %locals(), '--load-cookies=%(esgfdir)s/cookies' %locals(), '--ca-directory=%(esgfdir)s/certificates' %locals(), '--no-check-certificate', '--spider', 'http://vesg.ipsl.fr/thredds/fileServer/esg_dataroot/CMIP5/output1/IPSL/IPSL-CM5A-LR/rcp85/mon/atmos/cfMon/r1i1p1/v20111119/clhcalipso/clhcalipso_cfMon_IPSL-CM5A-LR_rcp85_r1i1p1_200601-230012.nc' ] 
    if http_proxy != None:
      cmd = ( 'export http_proxy=%s ; export https_proxy=%s ;' % (http_proxy, https_proxy) ) + cmd
    print cmd
    subprocess.Popen( cmd2, env=os.environ.copy(), stdout=open('.wgsp','w'), stderr=open('.wgspe','w')  ).communicate()
    ##subprocess.Popen( cmd ).readlines()
    ii = open( '.wgspe' ).readlines()
    assert string.strip( ii[-2] ) in ['Remote file exists.','200 OK'] , 'File not found -- check .wgspe for error messages'
    os.unlink( '.wgsp' )
    os.unlink( '.wgspe' )

    print 'Check 1 OK'
    print '--------------------------------------------------'

    os.popen( 'ncdump -v 1> .nctmp 2> .nc2' ).readlines()
    ii = open( '.nc2' ).readlines()
    os.unlink( '.nctmp' )
    os.unlink( '.nc2' )
    try:
      x = string.split( string.split( ii[-1] )[3], '.' )
      maj = int(x[0])
      min = int(x[1])
      ncd = maj == 4 and min >= 1 or maj > 4
      if not ncd:
        print 'Netcdf libraries do not support opendap -- check 2 will not be completed'
    except:
      print 'Failed to identify ncdump version'
      ncd = False

    if ncd:
      print 'Testing certificate by requesting header of pr_day_HadGEM2-ES_esmControl_r1i1p1_20891201-20991130.nc'
      os.popen( 'ncdump -h http://cmip-dn1.badc.rl.ac.uk/thredds/dodsC/esg_dataroot/cmip5/output1/MOHC/HadGEM2-ES/esmControl/day/atmos/day/r1i1p1/v20120423/pr/pr_day_HadGEM2-ES_esmControl_r1i1p1_20891201-20991130.nc > .tmp ; md5sum .tmp > .md5' ).readlines()
      ii = open( '.md5' ).readlines()
      assert string.split(ii[0])[0] == '8f1d9ede885a527bcbdd1ddf1a5ed699', 'Checksum of header does not match expected value -- check .tmp and .md5 for clues'

      print 'Check 2 OK'
      print '--------------------------------------------------'

      print 'try: ncview http://cmip-dn1.badc.rl.ac.uk/thredds/dodsC/esg_dataroot/cmip5/output1/MOHC/HadGEM2-ES/esmControl/day/atmos/day/r1i1p1/v20120423/pr/pr_day_HadGEM2-ES_esmControl_r1i1p1_20891201-20991130.nc'
