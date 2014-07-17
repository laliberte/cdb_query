import os
import copy
import warnings
from pyesgf.search import SearchConnection

import multiprocessing as mproc
import retrieval_utils

import urllib2

def descend_tree(database,search_path,options,list_level=None):
    #Create the database:
    only_list=[]
    for experiment in database.header['experiment_list'].keys():
        for var_name in database.header['variable_list'].keys():
            only_list.append(experiment_variable_search(database.nc_Database,search_path,database.header['file_type_list'],options,
                                        experiment,var_name,database.header['variable_list'][var_name],list_level=list_level))
    return [item for sublist in only_list for item in sublist]
    #if len(only_list)>0:
    #    return set(only_list[0]).intersection(*only_list)
    #else:
    #    return []

def experiment_variable_search(nc_Database,search_path,file_type_list,options,
                                experiment,var_name,var_desc,list_level=None):

    conn = SearchConnection(search_path, distrib=options.distrib)

    #Search the ESGF:
    ctx = conn.new_context(project=nc_Database.drs.project,
                        experiment=experiment)
    ctx=ctx.constrain(**{field:var_desc[field_id] for field_id, field in enumerate(nc_Database.drs.var_specs)})

    
    for field in nc_Database.drs.slicing_args:
        if field in dir(options) and getattr(options,field)!=None:
            ctx=ctx.constrain(**{field:getattr(options,field)})
    #if options.model:
    #    ctx=ctx.constrain(model=options.model)
    #if options.institute:
    #    ctx=ctx.constrain(institute=options.institute)
    #if options.ensemble:
    #    ctx=ctx.constrain(ensemble=options.ensemble)

    nc_Database.file_expt.experiment=experiment
    nc_Database.file_expt.var=var_name
    nc_Database.file_expt.time=0
    for field_id, field in enumerate(nc_Database.drs.var_specs):
        setattr(nc_Database.file_expt,field,var_desc[field_id])

    if list_level!=None:
        import socket
        try:
            return ctx.facet_counts[list_level].keys()
        except socket.error as e:
            print search_path+' is not responding. '+e.strerror
            print 'This is not fatal. Data broadcast by '+search_path+' will simply NOT be considered.'
            return []
        except urllib2.HTTPError as e:
            print search_path+' is not responding. '
            print e
            print 'This is not fatal. Data broadcast by '+search_path+' will simply NOT be considered.'
            #print search_path+' is not responding. '+e.strerror
            return []
    else:
        file_list_remote=[]
        file_list_found=[]
        try:
            file_list_found=ctx.search(variable=var_name)
            file_list_remote=map(lambda x: get_urls(nc_Database.drs,x,file_type_list,var_name),file_list_found)
            file_list_remote=[item for sublist in file_list_remote for item in sublist]
        except:
            warnings.warn('Search path {0} is unresponsive at the moment'.format(search_path),UserWarning)
       
        map(lambda x: record_url(x,nc_Database),file_list_remote)
        return []

def record_url(remote_file_desc,nc_Database):
    nc_Database.file_expt.path=remote_file_desc['url']
    nc_Database.file_expt.data_node=retrieval_utils.get_data_node(remote_file_desc['url'],remote_file_desc['file_type'])
    if remote_file_desc['file_type'] in nc_Database.drs.remote_file_types and remote_file_desc['checksum']:
        nc_Database.file_expt.path+='|'+remote_file_desc['checksum']

    for val in nc_Database.drs.remote_fields:
        setattr(nc_Database.file_expt,val,remote_file_desc[val])

    #Convert unicode to string:
    for val in dir(nc_Database.file_expt):
        if val[0]!='_' and val!='case_id':
            setattr(nc_Database.file_expt,val,str(getattr(nc_Database.file_expt,val)))

    list_of_knowns=[ getattr(nc_Database.file_expt,field) for field in nc_Database.drs.known_fields] 
    list_of_retrieved=[ remote_file_desc[field] for field in nc_Database.drs.known_fields] 
    if remote_file_desc['version']:
        if (remote_file_desc['version'][1:]!='atest' and
            len([i for i,j in zip(list_of_knowns,list_of_retrieved) if i==j])==len(list_of_knowns)):
            nc_Database.session.add(copy.deepcopy(nc_Database.file_expt))
            nc_Database.session.commit()
    return nc_Database

def get_urls(drs,result,file_type_list,var_name):
    file_list_remote=[]
    if len(set(file_type_list).intersection(set(drs.remote_file_types)))>0:
        #If remote file types were requested
        fil_ctx = result.file_context()
        try:
            shard_name=fil_ctx.shards[0]
        except:
            shard_name=None
        try:
            fil = fil_ctx.search(variable=var_name)
            for item in fil:
                file_list_remote.extend(get_url_remote(item,file_type_list,drs))
        except:
            if shard_name==None:
                 warnings.warn('An unknown shard is unresponsive at the moment'.format(shard_name))
            else:
                warnings.warn('Shard {0} is unresponsive at the moment'.format(shard_name))
        
    return file_list_remote

def get_url_remote(item,file_type_list,drs):
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
        for val in drs.official_drs+['checksum']:
            try:
                if val=='var':
                    #this is a temporary fix to a poor design decision.
                    #to really fix this, will have to change names 'var' to variables.
                    file_info[val]=item.json['variable']
                elif val=='version':
                    #Version is poorly implemented... Try a fix:
                    version=item.json['instance_id'].split('.')[-3]
                    #Previous fix. Does not work for CORDEX
                    #version=item.json['id'].split('.')[9]
                    if version[0]=='v':
                        file_info[val]=version
                elif val=='model_version':
                    file_info[val]='v'+item.json['version']
                else:
                    file_info[val]=item.json[val]

                if isinstance(file_info[val],list): file_info[val]=str(file_info[val][0])
            except:
                file_info[val]=None
        #if (file_info['checksum']!=None and 
        #    set(item.urls.keys()).issuperset(drs.required_file_types)):
        if file_info['checksum']!=None:
            url_name.append(file_info)
    return url_name
