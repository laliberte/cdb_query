import os
import copy
import warnings
from pyesgf.search import SearchConnection

from tree_utils import File_Expt

import multiprocessing as mproc
import retrieval_utils

def descend_tree(pointers,project_drs,header,search_path,options,list_level=None):
    #Create the database:
    only_list=[]
    for experiment in header['experiment_list'].keys():
        for var_name in header['variable_list'].keys():
            if list_level:
                only_list.append(experiment_variable_search(pointers,project_drs,search_path,header['file_type_list'],options,
                                            experiment,var_name,header['variable_list'][var_name],list_level=list_level))
            else:
                experiment_variable_search(pointers,project_drs,search_path,header['file_type_list'],options,
                                            experiment,var_name,header['variable_list'][var_name],list_level=list_level)
    if list_level:
        return [item for sublist in only_list for item in sublist]
    else:
        return

def experiment_variable_search(pointers,project_drs,search_path,file_type_list,options,
                                experiment,var_name,var_desc,list_level=None):

    conn = SearchConnection(search_path, distrib=options.distrib)

    #Search the ESGF:
    ctx = conn.new_context(project=project_drs.project,
                        experiment=experiment)
    ctx=ctx.constrain(**{field:var_desc[field_id] for field_id, field in enumerate(project_drs.var_specs)})

    if options.model:
        ctx=ctx.constrain(model=options.model)
    if options.institute:
        ctx=ctx.constrain(institute=options.institute)
    if options.ensemble:
        ctx=ctx.constrain(ensemble=options.ensemble)

    pointers.file_expt.experiment=experiment
    pointers.file_expt.var=var_name
    pointers.file_expt.search=search_path
    pointers.file_expt.time=0
    for field_id, field in enumerate(project_drs.var_specs):
        setattr(pointers.file_expt,field,var_desc[field_id])

    if list_level!=None:
        return ctx.facet_counts[list_level].keys()
    else:
        try:
            file_list_found=ctx.search(variable=var_name)
        except:
            warnings.warn('Search path {0} is unresponsive at the moment'.format(search_path),UserWarning)
            file_list_found=[]
        file_list_remote=map(lambda x: get_urls(project_drs,x,file_type_list,var_name),file_list_found)
        file_list_remote=[item for sublist in file_list_remote for item in sublist]
       
        map(lambda x: record_url(x,pointers,project_drs),file_list_remote)
        return

def record_url(remote_file_desc,pointers,project_drs):
    pointers.file_expt.path=remote_file_desc['url']
    if remote_file_desc['file_type'] in project_drs.remote_file_types and remote_file_desc['checksum']:
        pointers.file_expt.path+='|'+remote_file_desc['checksum']
        #file_available = retrieval_utils.check_file_availability(pointers.file_expt.path.split('|')[0])
        #if not file_available:
        #    return

    for val in project_drs.remote_fields:
        setattr(pointers.file_expt,val,remote_file_desc[val])

    #Convert unicode to string:
    for val in dir(pointers.file_expt):
        if val[0]!='_' and val!='case_id':
            setattr(pointers.file_expt,val,str(getattr(pointers.file_expt,val)))

    #for val in dir(pointers.file_expt):
    #    if val[0]!='_':
    #        print val,getattr(pointers.file_expt,val)

    list_of_knowns=[ getattr(pointers.file_expt,field) for field in project_drs.known_fields] 
    list_of_retrieved=[ remote_file_desc[field] for field in project_drs.known_fields] 
    if remote_file_desc['version']:
        if (remote_file_desc['version'][1:]!='atest' and
            len([i for i,j in zip(list_of_knowns,list_of_retrieved) if i==j])==len(list_of_knowns)):
            pointers.add_item()
    return pointers

def get_urls(project_drs,result,file_type_list,var_name):
    file_list_remote=[]
    if len(set(file_type_list).intersection(set(project_drs.remote_file_types)))>0:
        #If remote file types were requested
        fil_ctx = result.file_context()
        try:
            fil = fil_ctx.search(variable=var_name)
            for item in fil:
                file_list_remote.extend(get_url_remote(item,file_type_list,project_drs))
        except:
            warnings.warn('Shard {0} is unresponsive at the moment'.format(fil_ctx.shards[0]))
            #pass
        
    #if 'OPeNDAP' in file_type_list:
    #    #OPeNDAP files were requested:
    #    agg_ctx = result.aggregation_context()
    #    agg = agg_ctx.search(variable=var_name)
    #    for item in agg:
    #        file_list_remote.append(get_url_opendap(item))
    return file_list_remote

#correspondence_dict={'experiment':'experiment',
#                     'cmor_table':'cmor_table', 
#                     'realm':'realm', 
#                     'version':'version', 
#                     'ensemble':'ensemble',
#                     'time_frequency':'time_frequency', 
#                     'var':'variable', 
#                     'checksum':'checksum',
#                     'model':'model',
#                     'institute':'institute'
#                     }

def get_url_remote(item,file_type_list,project_drs):
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
        for val in project_drs.official_drs+['checksum']:
            try:
                if val=='var':
                    #this is a temporary fix to a poor design decision.
                    #to really fix this, will have to change names 'var' to variables.
                    file_info[val]=item.json['variable']
                else:
                    file_info[val]=item.json[val]
                if val=='version':
                    #Version is poorly implemented... Try a fix:
                    version=item.json['id'].split('.')[9]
                    if version[0]=='v':
                        file_info[val]=version
                if isinstance(file_info[val],list): file_info[val]=str(file_info[val][0])
            except:
                file_info[val]=None
        if file_info['checksum']:
            url_name.append(file_info)
    return url_name

#def get_url_opendap(item):
#    url_name['file_type']='OPeNDAP'
#    try:
#        url_name['url']=item.opendap_url
#    except:
#        url_name['url']=None
#    for val in correspondence_dict.keys():
#        try:
#            url_name[val]=item.json[correspondence_dict[val]]
#        except:
#            url_name[val]=None
#    return url_name

