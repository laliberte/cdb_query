import os
import subprocess

import urllib2, httplib
from cookielib import CookieJar

import netCDF4
import indices_utils

import numpy as np

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    def __init__(self, key, cert):
        urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

#def check_file_availability(url_name):
#    wget_call='wget --spider --ca-directory={0} --certificate={1} --private-key={1}'.format(os.environ['X509_CERT_DIR'],os.environ['X509_USER_PROXY']).split(' ')
#    wget_call.append(url_name)
#
#    proc=subprocess.Popen(wget_call,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
#    (out, err) = proc.communicate()
#
#    status_string='HTTP request sent, awaiting response... '
#    error_codes=[ int(line.replace(status_string,'').split(' ')[0]) for line in err.splitlines() if status_string in line]
#    length_string='Length: '
#    lengths=[ int(line.replace(length_string,'').split(' ')[0]) for line in err.splitlines() if length_string in line]
#   
#    if 200 in error_codes and max(lengths)>0:
#        return True
#    else:
#        return False

def check_file_availability(url_name):
    cj = CookieJar()
    try: 
        opener = urllib2.build_opener(HTTPSClientAuthHandler(os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY']),urllib2.HTTPCookieProcessor(cj))
        response = opener.open(url_name)

        if response.msg=='OK' and response.headers.getheaders('Content-Length')[0]:
            return True
        else:
            return False
    except:
        return False

def download_secure(url_name,dest_name):
    if check_file_availability(url_name):
        cj = CookieJar()
        opener = urllib2.build_opener(HTTPSClientAuthHandler(os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY']),urllib2.HTTPCookieProcessor(cj))
        data = opener.open(url_name)

        meta = data.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        size_string="Downloading: %s MB: %s" % (dest_name, file_size/2.0**20)
        
        directory=os.path.dirname(dest_name)
        if not os.path.exists(directory):
            os.makedirs(directory)
        dest_file = open(dest_name, 'wb')
        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = data.read(block_sz)
            if not buffer:
                break
        
            file_size_dl += len(buffer)
            dest_file.write(buffer)
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = status + chr(8)*(len(status)+1)
            #print status,

        dest_file.close()
    else:
        size_string="URL %s is missing." % (url_name)
    return size_string
            
def md5_for_file(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()

def retrieve_path(path,options):
    decomposition=path[0].split('|')
    if not (isinstance(decomposition,list) and len(decomposition)>1):
        return

    root_path=decomposition[0]
    dest_name=options.out_destination+'/'+'/'.join(path[2:])+'/'+root_path.split('/')[-1]
    try:
        md5sum=md5_for_file(open(dest_name,'r'))
    except:
        md5sum=''
    if md5sum==decomposition[1]:
        return 'File '+dest_name+' found. MD5 OK! Not retrieving.'

    size_string=download_secure(root_path,dest_name)
    try:
        md5sum=md5_for_file(open(dest_name,'r'))
    except:
        md5sum=''
    if md5sum!=decomposition[1]:
        try:
            os.remove(dest_name)
        except:
            pass
        return size_string+'\n'+'File '+dest_name+' does not have the same MD5 checksum as published on the ESGF. Removing this file...'
    else:
        return size_string+'\n'+'Checking MD5 checksum of retrieved file... MD5 OK!'

def retrieve_path_data(in_tuple,pointer_var):
    path=in_tuple[0].replace('fileServer','dodsC')
    indices=in_tuple[1]
    var=in_tuple[2]
    other_indices=in_tuple[3]

    sort_table=in_tuple[4]
    #pointer_var=in_tuple[5]

    remote_data=open_remote_netCDF(path)
    if len(indices)==1:
        retrieved_data=add_axis(grab_remote_indices(remote_data.variables[var],indices,other_indices))
    else:
        retrieved_data=grab_remote_indices(remote_data.variables[var],indices,other_indices)
    remote_data.close()
    return (retrieved_data, sort_table,pointer_var)

def open_remote_netCDF(url):
    try:
        return netCDF4.Dataset(url)
    except:
        error=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.
        '''.splitlines())
        raise dodsError(error.format(url.replace('dodsC','fileServer')))
        

class dodsError(Exception):
     def __init__(self, value):
         self.value = value
     def __str__(self):
         return repr(self.value)

def add_axis(array):
    return np.reshape(array,(1,)+array.shape)

def grab_remote_indices(variable,indices,other_indices):
    
    indices_sort=np.argsort(indices)
    other_slices=tuple([other_indices[dim] for dim in variable.dimensions if dim!='time'])
    #return variable.__getitem__((indices[indices_sort],)+other_slices)[np.argsort(indices_sort),...]
    return np.concatenate(map(lambda x: variable.__getitem__((x,)+other_slices),
                                indices_utils.convert_indices_to_slices(indices[indices_sort])
                             ),axis=0
                             )[np.argsort(indices_sort),...]
