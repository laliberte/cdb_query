import os
import shutil
import subprocess

import urllib2, httplib
from cookielib import CookieJar
import ssl

import ftplib

import copy

import indices_utils
import remote_netcdf

import numpy as np

import hashlib

import time

unique_file_id_list=['checksum_type','checksum','tracking_id']

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

def check_file_availability_wget(url_name):
    wget_call='wget --spider --ca-directory={0} --certificate={1} --private-key={1}'.format(os.environ['X509_CERT_DIR'],os.environ['X509_USER_PROXY']).split(' ')
    wget_call.append(url_name)

    proc=subprocess.Popen(wget_call,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out, err) = proc.communicate()

    status_string='HTTP request sent, awaiting response... '
    error_codes=[ int(line.replace(status_string,'').split(' ')[0]) for line in err.splitlines() if status_string in line]
    length_string='Length: '
    lengths=[ int(line.replace(length_string,'').split(' ')[0]) for line in err.splitlines() if length_string in line]
   
    if 200 in error_codes and max(lengths)>0:
        return True
    else:
        return False

def check_file_availability(url_name,stop=False):
    #If ftp, assume available:
    if len(url_name)>3 and url_name[:3]=='ftp':
        return True

    #Some monkeypathcing to get rid of SSL certificate verification:
    if hasattr(ssl, '_create_unverified_context'): 
        ssl._create_default_https_context = ssl._create_unverified_context
    cj = CookieJar()
    opener = urllib2.build_opener(HTTPSClientAuthHandler(os.environ['X509_USER_PROXY'], os.environ['X509_USER_PROXY']),urllib2.HTTPCookieProcessor(cj))
    urllib2.install_opener(opener)
    try: 
        response = opener.open(url_name)

        if response.msg=='OK' and response.headers.getheaders('Content-Length')[0]:
            return True
        else:
            return False
    except:
        if stop:
            return False
        else:
            time.sleep(15)
            return check_file_availability(url_name,stop=True)

def download_secure(url_name,dest_name,file_type,username=None,user_pass=None):
    if file_type=='HTTPServer':
        return download_secure_HTTP(url_name,dest_name)
    elif file_type=='local_file':
        return download_secure_local(url_name,dest_name)
    elif file_type=='FTPServer':
        return download_secure_FTP(url_name,dest_name,username=username,user_pass=user_pass)

def download_secure_FTP(url_name,dest_name,username=None,user_pass=None):
    if (username!=None and 
        user_pass!=None):
        #Use credentials:
        ftp=ftplib.FTP(url_name.split('/')[2],username,user_pass)

    else:
        #Do not use credentials and hope for anonymous:
        ftp=ftplib.FTP(url_name.split('/')[2])

    directory=os.path.dirname(dest_name)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(dest_name,'wb') as local_file:
        try:
            ftp.retrbinary('RETR %s' % '/'+'/'.join(url_name.split('/')[3:]), local_file.write)
        except ftplib.error_perm:
            #Permission error. Try again!
            ftp.retrbinary('RETR %s' % '/'+'/'.join(url_name.split('/')[3:]), local_file.write)
    
    ftp.close()
    file_size=np.float(os.stat(dest_name).st_size)
    return "Downloading: %s MB: %s" % (dest_name, file_size/2.0**20)

def download_secure_local(url_name,dest_name):
    directory=os.path.dirname(dest_name)
    if not os.path.exists(directory):
        os.makedirs(directory)
    shutil.copy(url_name,dest_name)
    file_size=np.float(os.stat(dest_name).st_size)
    return "Downloading: %s MB: %s" % (dest_name, file_size/2.0**20)

def download_secure_HTTP(url_name,dest_name):
    if check_file_availability(url_name):
        #Some monkeypathcing to get rid of SSL certificate verification:
        if hasattr(ssl, '_create_unverified_context'): 
            ssl._create_default_https_context = ssl._create_unverified_context
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

def sha_for_file(f, block_size=2**20):
    sha = hashlib.sha256()
    while True:
        data = f.read(block_size)
        if not data:
            break
        sha.update(data)
    return sha.hexdigest()

def checksum_for_file(checksum_type,f, block_size=2**20):
    checksum = getattr(hashlib,checksum_type.lower())()
    while True:
        data = f.read(block_size)
        if not data:
            break
        checksum.update(data)
    return checksum.hexdigest()

#def retrieve_path(path,out_destination):
def retrieve_path(in_dict,pointer_var):
    path=in_dict['path']
    out_destination=in_dict['file_path']
    version=in_dict['version']
    file_type=in_dict['file_type']
    var=in_dict['var']
    username=in_dict['username']
    user_pass=in_dict['user_pass']
    dest_name=out_destination.replace('tree','/'.join(pointer_var[:-1]))
    dest_name=dest_name.replace('var',var)
    dest_name=dest_name.replace('version',version)
    #Do not retrieve aux variables:
    if pointer_var[-1]!=var:
        return

    decomposition=path.split('|')
    if not (isinstance(decomposition,list) and len(decomposition)>1):
        return

    if (isinstance(decomposition,list) and len(decomposition)==1):
        return

    root_path=decomposition[0]
    dest_name+=root_path.split('/')[-1]
    checksum_type=decomposition[unique_file_id_list.index('checksum_type')+1]
    checksum=decomposition[unique_file_id_list.index('checksum')+1]

    if checksum=='':
        if not os.path.isfile(dest_name):
            #Downloads only if file exists!
            size_string=download_secure(root_path,dest_name,file_type,username=username,user_pass=user_pass)
            return 'Could NOT check checksum of retrieved file because checksum was not a priori available.'
        else:
            return 'File '+dest_name+' found but could NOT check checksum of existing file because checksum was not a priori available. Not retrieving.'
    else:
        try: #Works only if file exists!
            comp_checksum=checksum_for_file(checksum_type,open(dest_name,'r'))
        except:
            comp_checksum=''
        if comp_checksum==checksum:
            return 'File '+dest_name+' found. '+checksum_type+' OK! Not retrieving.'

        size_string=download_secure(root_path,dest_name,file_type,username=username,user_pass=user_pass)
        try: 
            comp_checksum=checksum_for_file(checksum_type,open(dest_name,'r'))
        except:
            comp_checksum=''
        if comp_checksum!=checksum:
            try:
                os.remove(dest_name)
            except:
                pass
            return size_string+'\n'+'File '+dest_name+' does not have the same '+checksum_type+' checksum as published on the ESGF. Removing this file...'
        else:
            return size_string+'\n'+'Checking '+checksum_type+' checksum of retrieved file... '+checksum_type+' OK!'

#def find_local_file(source_dir,data):
#    paths_list=data.variables['path'][:]
#    version_list=data.variables['version'][:]
#    checksum_list=data.variables['checksum'][:]
#    checksum_type_list=data.variables['checksum_type'][:]
#    file_type_list=data.variables['file_type'][:]
#    #THIS IS NOT DRS-SAFE:
#    tree='/'.join(data.path.split('/')[1:-2])
#    var=data.path.split('/')[-2]
#    unique_paths_list=list(np.unique([source_dir+'/'+tree+'/v'+str(version)+'/'+var+'/'+path.split('/')[-1] for path, version in zip(paths_list,version_list)]))
#    unique_checksum_list=[]
#    for path in unique_paths_list:
#        try:
#            comp_checksum=sha_for_file(open(path,'r'))
#        except:
#            comp_checksum=''
#        unique_checksum_list.append(comp_checksum)
#    new_paths_list=[]
#    new_file_type_list=[]
#    for path_id,path in enumerate(paths_list):
#        local_path=source_dir+'/'+tree+'/v'+str(version_list[path_id])+'/'+var+'/'+path.split('/')[-1]
#        if unique_checksum_list[unique_paths_list.index(local_path)]==checksum_list[path_id]:
#            new_paths_list.append(local_path)
#            new_file_type_list.append('local_file')
#        else:
#            new_paths_list.append(path)
#            new_file_type_list.append(file_type_list[path_id])
#    return new_paths_list, new_file_type_list

def retrieve_path_data(in_dict,pointer_var):
    #print 'Recovering '+'/'.join(self.tree)

    path=in_dict['path'].split('|')[0]
    var=in_dict['var']
    indices=copy.copy(in_dict['indices'])
    unsort_indices=copy.copy(in_dict['unsort_indices'])
    sort_table=in_dict['sort_table']
    file_type=in_dict['file_type']

    remote_data=remote_netcdf.remote_netCDF(path,file_type,[])
    remote_data.open_with_error()
    dimensions=remote_data.retrieve_dimension_list(var)
    for dim in dimensions:
        if dim != 'time':
            remote_dim, attributes=remote_data.retrieve_dimension(dim)
            indices[dim], unsort_indices[dim] = indices_utils.prepare_indices(
                                                            indices_utils.get_indices_from_dim(remote_dim,indices[dim]))
    
    retrieved_data=remote_data.grab_indices(var,indices,unsort_indices)
    remote_data.close()
    return (retrieved_data, sort_table,pointer_var+[var])

def get_data_node(path,file_type):
    if file_type=='HTTPServer':
        return '/'.join(path.split('/')[:3])
    elif file_type=='OPENDAP':
        return '/'.join(path.split('/')[:3])
    elif file_type=='FTPServer':
        return '/'.join(path.split('/')[:3])
    elif file_type=='local_file':
        return '/'.join(path.split('/')[:2])
    else:
        return ''



