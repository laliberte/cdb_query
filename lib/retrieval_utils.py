import os
import subprocess

import urllib2, httplib
from cookielib import CookieJar
import ssl

import copy

import indices_utils
import remote_netcdf

import numpy as np

import hashlib

import time

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

def download_secure(url_name,dest_name):
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

#def retrieve_path(path,out_destination):
def retrieve_path(in_dict,pointer_var):
    path=in_dict['path']
    out_destination=in_dict['file_path']
    version=in_dict['version']
    var=in_dict['var']
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
    if decomposition[1]=='':
        size_string=download_secure(root_path,dest_name)
        return 'Could NOT check MD5 checksum of retrieved file because checksum was not a priori available.'
    else:
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

def find_local_file(source_dir,data):
    paths_list=data.variables['path'][:]
    version_list=data.variables['version'][:]
    checksum_list=data.variables['checksum'][:]
    file_type_list=data.variables['file_type'][:]
    #THIS IS NOT DRS-SAFE:
    tree='/'.join(data.path.split('/')[1:-2])
    var=data.path.split('/')[-2]
    unique_paths_list=list(np.unique([source_dir+'/'+tree+'/v'+str(version)+'/'+var+'/'+path.split('/')[-1] for path, version in zip(paths_list,version_list)]))
    unique_checksum_list=[]
    for path in unique_paths_list:
        try:
            md5sum=md5_for_file(open(path,'r'))
        except:
            md5sum=''
        unique_checksum_list.append(md5sum)
    new_paths_list=[]
    new_file_type_list=[]
    for path_id,path in enumerate(paths_list):
        local_path=source_dir+'/'+tree+'/v'+str(version_list[path_id])+'/'+var+'/'+path.split('/')[-1]
        if unique_checksum_list[unique_paths_list.index(local_path)]==checksum_list[path_id]:
            new_paths_list.append(local_path)
            new_file_type_list.append('local_file')
        else:
            new_paths_list.append(path)
            new_file_type_list.append(file_type_list[path_id])
    return new_paths_list, new_file_type_list

def retrieve_path_data(in_dict,pointer_var):
    #print 'Recovering '+'/'.join(self.tree)

    path=in_dict['path'].replace('fileServer','dodsC').split('|')[0]
    var=in_dict['var']
    indices=copy.copy(in_dict['indices'])
    unsort_indices=copy.copy(in_dict['unsort_indices'])
    sort_table=in_dict['sort_table']

    remote_data=remote_netcdf.remote_netCDF(path,[])
    remote_data.open_with_error()
    dimensions=remote_data.Dataset.variables[var].dimensions
    for dim in dimensions:
        if dim != 'time':
            if dim in remote_data.Dataset.variables.keys():
                remote_dim = remote_data.Dataset.variables[dim][:]
            else:
                remote_dim = np.arange(len(remote_data.Dataset.dimensions[dim]))
            indices[dim], unsort_indices[dim] = indices_utils.prepare_indices(
                                                            indices_utils.get_indices_from_dim(remote_dim,indices[dim]))
    
    #try:
    retrieved_data=grab_remote_indices(remote_data.Dataset.variables[var],indices,unsort_indices)
    #except RuntimeError:
    #    retrieved_data=grab_remote_indices_pedantic(remote_data.Dataset.variables[var],indices,unsort_indices)

    remote_data.close()
    return (retrieved_data, sort_table,pointer_var+[var])

def get_data_node(path,file_type):
    if file_type=='HTTPServer':
        return '/'.join(path.split('/')[:3])
    elif file_type=='local_file':
        return '/'.join(path.split('/')[:2])
    else:
        return ''


def grab_remote_indices(variable,indices,unsort_indices):
    dimensions=variable.dimensions
    return retrieve_slice(variable,indices,unsort_indices,dimensions[0],dimensions[1:],0)

def retrieve_slice(variable,indices,unsort_indices,dim,dimensions,dim_id,getitem_tuple=tuple()):
    if len(dimensions)>0:
        return np.take(np.concatenate(map(lambda x: retrieve_slice(variable,
                                                 indices,
                                                 unsort_indices,
                                                 dimensions[0],
                                                 dimensions[1:],
                                                 dim_id+1,
                                                 getitem_tuple=getitem_tuple+(x,)),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)
    else:
        return np.take(np.concatenate(map(lambda x: variable.__getitem__(getitem_tuple+(x,)),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)

def getitem_pedantic(shape,getitem_tuple):
    getitem_tuple_fixed=()
    for item_id, item in enumerate(getitem_tuple):
        indices_list=range(shape[item_id])[item]
        if indices_list[-1]+item.step>shape[item_id]:
            #Must fix the slice:
            #getitem_tuple_fixed+=(slice(item.start,shape[item_id],item.step),)
            getitem_tuple_fixed+=(indices_list,)
        else:
            getitem_tuple_fixed+=(item,)
    return getitem_tuple_fixed
        
def grab_remote_indices_pedantic(variable,indices,unsort_indices):
    dimensions=variable.dimensions
    return retrieve_slice_pedantic(variable,indices,unsort_indices,dimensions[0],dimensions[1:],0)

def retrieve_slice_pedantic(variable,indices,unsort_indices,dim,dimensions,dim_id,getitem_tuple=tuple()):
    if len(dimensions)>0:
        return np.take(np.concatenate(map(lambda x: retrieve_slice_pedantic(variable,
                                                 indices,
                                                 unsort_indices,
                                                 dimensions[0],
                                                 dimensions[1:],
                                                 dim_id+1,
                                                 getitem_tuple=getitem_tuple+(x,)),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)
    else:
        shape=variable.shape
        print map(lambda x: getitem_tuple+(x,),indices[dim])
        print map(lambda x: getitem_pedantic(shape,getitem_tuple+(x,)),indices[dim])
        return np.take(np.concatenate(map(lambda x: variable.__getitem__(getitem_pedantic(variable.shape,getitem_tuple+(x,))),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)


def retrieve_slice_pedantic_bak(variable,indices,unsort_indices,dim,dimensions,dim_id,getitem_tuple=tuple()):
    if len(dimensions)>0:
        return np.take(np.concatenate(map(lambda x: 
                                                 remove_zero_if_added(
                                                 retrieve_slice_pedantic(variable,
                                                 indices,
                                                 unsort_indices,
                                                 dimensions[0],
                                                 dimensions[1:],
                                                 dim_id+1,
                                                 getitem_tuple=getitem_tuple+(ensure_zero(x),)),
                                                 x,dim_id),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)
    else:
        return np.take(np.concatenate(map(lambda x: remove_zero_if_added(
                                            variable.__getitem__(getitem_tuple+(ensure_zero(x),)),
                                            x,dim_id),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)

def ensure_zero(indices):
    if indices.start > 0:
        return [0,]+range(0,indices.stop)[indices]
    else:
        return indices

def remove_zero_if_added(arr,indices,dim_id):
    if indices.start > 0:
        return np.take(arr,range(1,arr.shape[dim_id]),axis=dim_id)
    else:
        return arr

