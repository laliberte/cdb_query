def onlineca_get_trustroots_wget():
    return """
#!/bin/bash
#
# Client script for web service interface to SLCS get-trustroots based on 
# curl and base64 commands.  Get trust roots retrieves the CA certificate 
# issuer(s) of the SLCS's SSL certificate
#
# @author P J Kershaw 07/06/2010
#
# @copyright: (C) 2010 STFC
#
# @license: BSD - See top-level LICENCE file for licence details
#
# $Id$
cmdname=$(basename $0)
cmdline_opt=`getopt hU:bc: $*`

usage="Usage: $cmdname [-h][-U get trust roots URI][-b][-c CA directory]\n
\n
   Options\n
       -h\t\t\tDisplays usage and quits.\n
       -U <uri>\t\tShort-Lived Credential service URI\n
       -b\t\t\tbootstrap trust in the Short-Lived Credential Service\n
       -c <directory path>\tDirectory to store the trusted CA (Certificate Authority) certificates.\n
       \t\t\tDefaults to ${HOME}/.globus/certificates or\n
       \t\t\t/etc/grid-security/certificates if running as root.\n
"

if [ $? != 0 ] ; then
    echo -e $usage >&2 ;
    exit 1 ;
fi

set -- $cmdline_opt

while true ; do
    case "$1" in
        -h) echo -e $usage ; exit 0 ;;
        -U) uri=$2 ; shift 2 ;;
        -b) bootstrap=1 ; shift 1 ;;
        -c) cadir=$2 ; shift 2 ;;
         --) shift ; break ;;
        *) echo "Error parsing command line" ; exit 1 ;;
    esac
done

if [ -z $uri ]; then
    echo -e Give the URI for the Short-Lived Credential service get trust roots request;
    echo -e $usage >&2 ;
    exit 1;
fi

# Set-up destination trust root directory
if [ $cadir ]; then
    if [ ! -d $cadir ]; then
        mkdir -p $cadir
    fi
    
elif [ ${X509_CERT_DIR} ]; then
    cadir=${X509_CERT_DIR}
    
elif [ "$LOGNAME" = "root" ]; then
    cadir=/etc/grid-security/certificates
    
    # Check path exists and if not make it
    if [ ! -d "/etc/grid-security" ]; then
        mkdir /etc/grid-security
    fi
       
    if [ ! -d "/etc/grid-security/certificates" ]; then
        mkdir /etc/grid-security/certificates
    fi
else
    cadir=${HOME}/.globus/certificates
    
    # Check path exists and if not make it
    if [ ! -d "${HOME}/.globus" ]; then
        mkdir ${HOME}/.globus
    fi
    
    if [ ! -d "${HOME}/.globus/certificates" ]; then
        mkdir ${HOME}/.globus/certificates
    fi
fi

# Set peer authentication based on bootstrap command line setting
if [ -z $bootstrap ]; then 
    ca_arg="--ca-directory $cadir"
else
    #echo Bootstrapping Short-Lived Credential Service root of trust.
    ca_arg="--no-check-certificate"
fi

# Make a temporary file for error output
error_output_filepath="/tmp/$UID-$RANDOM.csr"

# Post request to Short-Lived Credential Service
response=$(wget $uri  --secure-protocol TLSv1 $ca_arg -t 1 -O - 2> $error_output_filepath)

# Extract error output and clean up
error_output=$(cat $error_output_filepath)
rm -f $error_output_filepath

# Pull out the response code from the error output
wget_statcode_line="awaiting response..."
responsecode=$(echo "$error_output"|grep "$wget_statcode_line"|awk '{print $6}')
if [ "$responsecode" != "200" ]; then
    echo "Get trust roots failed"
    echo "$error_output" >&2
    exit 1
fi

# Process response
entries=$(echo $response|awk '{print $0}')
for i in $entries; do
    filename=${i%%=*}
    filecontent="$(echo ${i#*=}|awk '{for(i=1;i<length;i+=65) print substr($0,i,65)}'|openssl enc -d -base64)"
    echo "$filecontent" > $cadir/$filename
done

#echo Trust roots have been installed in $cadir.
"""
