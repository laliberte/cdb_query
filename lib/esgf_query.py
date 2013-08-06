import os
import copy
import warnings
from pyesgf.search import SearchConnection

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
    pointers.file_expt.mip=mip
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

    try:
        file_list_found=ctx.search(variable=var_name)
    except:
        warnings.warn('Search path {0} is unresponsive at the moment'.format(search_path))
        file_list_found=[]
    file_list_remote=map(lambda x: get_urls(x,file_type_list,var_name),file_list_found)
    file_list_remote=[item for sublist in file_list_remote for item in sublist]
   
    #for file in file_list_remote: pointers=record_url(file,pointers)
    map(lambda x: record_url(x,pointers),file_list_remote)
    print 'Done searching for variable '+var_name
    return

def record_url(remote_file_desc,pointers):
    pointers.file_expt.path=remote_file_desc['url']
    if remote_file_desc['file_type'] in remote_file_types and remote_file_desc['checksum']:
        pointers.file_expt.path+='|'+remote_file_desc['checksum']

    for val in ['file_type','center','model','rip','version']:
        setattr(pointers.file_expt,val,remote_file_desc[val])

    #Convert unicode to string:
    for val in dir(pointers.file_expt):
        if val[0]!='_' and val!='case_id':
            setattr(pointers.file_expt,val,str(getattr(pointers.file_expt,val)))

    known_fields=['experiment','var','frequency','realm','mip']
    list_of_knowns=[ getattr(pointers.file_expt,field) for field in known_fields] 
    list_of_retrieved=[ remote_file_desc[field] for field in known_fields] 
    if (remote_file_desc['version'][1:]!='atest' and 
        len([i for i,j in zip(list_of_knowns,list_of_retrieved) if i==j])==len(list_of_knowns)):
        pointers.add_item()
    return pointers

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

correspondence_dict={'experiment':'experiment',
                     'mip':'cmor_table', 
                     'realm':'realm', 
                     'version':'version', 
                     'rip':'ensemble',
                     'frequency':'time_frequency', 
                     'var':'variable', 
                     'checksum':'checksum',
                     'model':'model',
                     'center':'institute'
                     }

def get_url_remote(item,file_type_list):
    url_name=[]
    try:
        keys_list=item.urls.viewkeys()
    except:
        keys_list=[]

    for key in set(keys_list).intersection(file_type_list):
        file_info=dict()
        file_info['file_type']=key
        try:
            file_info['url']=item.urls[key][0][0]
        except:
            file_info['url']=None
        for val in correspondence_dict.keys():
            try:
                file_info[val]=item.json[correspondence_dict[val]]
                if isinstance(file_info[val],list): file_info[val]=str(file_info[val][0])
            except:
                file_info[val]=None
        url_name.append(file_info)
    return url_name

def get_url_opendap(item):
    url_name['file_type']='OPeNDAP'
    try:
        url_name['url']=item.opendap_url
    except:
        url_name['url']=None
    for val in correspondence_dict.keys():
        try:
            url_name[val]=item.json[correspondence_dict[val]]
        except:
            url_name[val]=None
    return url_name

