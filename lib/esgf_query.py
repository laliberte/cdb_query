import os
import copy
import warnings
from pyesgf.search import SearchConnection

import database_utils
from tree_utils import File_Expt

import multiprocessing as mproc

remote_file_types=['HTTPServer','GridFTP']

def descend_tree(pointers,header,search_path,options):
    #Create the database:
    for experiment in header['experiment_list'].keys():
        for var_name in header['variable_list'].keys():
            experiment_variable_search(pointers,search_path,header['file_type_list'],options,
                                        experiment,var_name,*header['variable_list'][var_name])

    return

def experiment_variable_search(pointers,search_path,file_type_list,options,
                                experiment,var_name,frequency,realm,mip):

    print 'Searching path ', search_path
    conn = SearchConnection(search_path, distrib=options.distrib)

    #Search the ESGF:
    ctx = conn.new_context(project='CMIP5',
                        experiment=experiment,
                        time_frequency=frequency,
                        realm=realm,
                        cmor_table=mip)
    if options.model:
        ctx=ctx.constrain(model=options.model)
    if options.center:
        ctx=ctx.constrain(institute=options.center)
    if options.rip:
        ctx=ctx.constrain(ensemble=options.rip)

    pointers.file_expt.experiment=experiment
    pointers.file_expt.var=var_name
    pointers.file_expt.realm=realm
    pointers.file_expt.frequency=frequency
    pointers.file_expt.search=search_path
    pointers.file_expt.time='all'

    #file_list_remote=[]
    #for result in ctx.search(variable=var_name):
    #    file_list_remote.extend(get_urls(result,file_type_list,var_name))
    #    print file_list_remote
    #if num_procs>1:
    #    pool=mproc.Pool(processes=num_procs)
    #    file_list_remote=pool.map(lambda x: get_urls(x,file_type_list,var_name),ctx.search(variable=var_name))
    #    pool.close()
    #else:

    file_list_remote=map(lambda x: get_urls(x,file_type_list,var_name),ctx.search(variable=var_name))
    file_list_remote=[item for sublist in file_list_remote for item in sublist]
   
    for url in file_list_remote:
        pointers.file_expt.file_type=url[0]
        url_name=url[1]
        if pointers.file_expt.file_type==url[0] in remote_file_types:
            file_description=url_name.split('/')[-10:-1]
            known_description=[ file_description[ind] for ind in [2,3,4,5,8] ]
            if known_description==[experiment,frequency,realm,mip,var_name]:
                if url[2]:
                    pointers.file_expt.path=url_name+'|'+url[2]
                else:
                    pointers.file_expt.path=url_name
                pointers.file_expt.center=file_description[0]
                pointers.file_expt.model=file_description[1]
                pointers.file_expt.rip=file_description[6]
                pointers.file_expt.mip=file_description[5]
                pointers.file_expt.version=file_description[7]
                if url_name and pointers.file_expt.version[1:]!='atest':
                    pointers.add_item()
        elif pointers.file_expt.file_type==url[0] in ['OPeNDAP']:
            file_description=url[2]
            variable=file_description[-2]
            if variable==var_name:
                pointers.file_expt.path=url_name
                pointers.file_expt.center=file_description[-9]
                pointers.file_expt.model=file_description[-8]
                pointers.file_expt.rip=file_description[-3]
                pointers.file_expt.mip=file_description[-4]
                pointers.file_expt.version='v'+file_description[-1].replace('v','')
                if url_name and pointers.file_expt.version[1:]!='atest':
                    pointers.add_item()
    print 'Done searching for variable '+var_name
    return

def get_urls(result,file_type_list,var_name):
    file_list_remote=[]
    if len(set(file_type_list).intersection(set(remote_file_types)))>0:
        #If remote file types were requested
        fil_ctx = result.file_context()
        try:
            fil = fil_ctx.search(variable=var_name)
            for item in fil:
                file_list_remote.extend(get_url_remote(item,file_type_list))
        except:
            warnings.warn('Shard {0} is unresponsive at the moment'.format(fil_ctx.shards[0]))
        
    if 'OPeNDAP' in file_type_list:
        #OPeNDAP files were requested:
        agg_ctx = result.aggregation_context()
        agg = agg_ctx.search(variable=var_name)
        for item in agg:
            file_list_remote.append(get_url_opendap(item))
    return file_list_remote

def get_url_remote(item,file_type_list):
    url_name=(None,None,None)
    try:
        keys_list=item.urls.viewkeys()
    except:
        keys_list=[]

    for key in set(keys_list).intersection(file_type_list):
        try:
            checksum=item.checksum
        except:
            checksum=None
        try:
            url_name=(key,item.urls[key][0][0],checksum)
        except:
            url_name=(None,None,None)
    if not isinstance(url_name,list): url_name=[url_name]
    return url_name

def get_url_opendap(item):
    try:
        url_name=('OPeNDAP',item.opendap_url,item.json['title'].split('.aggregation')[0].split('.'))
    except:
        url_name=(None,None,None)
    #if not isinstance(url_name,list): url_name=[url_name]
    return url_name

