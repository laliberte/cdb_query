from __future__ import nested_scopes, generators, division, absolute_import, with_statement, print_function, unicode_literals

#External:
import copy
import warnings
import socket
import requests
import httplib
import datetime

#External but related:
import netcdf4_soft_links.remote_netcdf.remote_netcdf as remote_netcdf

#Internal:
from .pyesgf_connection import SearchConnection

unique_file_id_list=['checksum_type','checksum','tracking_id']

class browser:
    def __init__(self,search_path,options,session=None):
        self.options=options
        self.session=session
        self.search_path=search_path

    def test_valid(self):
        #Try to connect with timeout:
        try:
            get_kwargs={'timeout':20,'stream':True,'allow_redirects':True}
            if self.session!=None:
                response=self.session.get(self.search_path+'search',**get_kwargs)
            else:
                response=requests.get(self.search_path+'search',**get_kwargs)
            test=response.ok
            response.close()
        except requests.exceptions.ReadTimeout as e:
            test=False
            pass
        except requests.exceptions.ConnectionError as e:
            test=False
            pass
        return test
        #except Exception as e:
        #    return False

    def close(self):
        return

    def descend_tree(self,database,list_level=None):
        list_names=[('experiment','experiment_list'),('var','variable_list')]
        lists_to_loop=dict()
        for id in list_names:
            if id[0] in dir(self.options) and getattr(self.options,id[0]) != None:
                lists_to_loop[id[1]] = getattr(self.options,id[0])
            else:
                lists_to_loop[id[1]]=database.header[id[1]].keys()
        for id in lists_to_loop.keys():
            if not isinstance(lists_to_loop[id],list): lists_to_loop[id]=[lists_to_loop[id]]

        #Create the database:
        only_list=[]
        for var_name in lists_to_loop['variable_list']:
            for experiment in lists_to_loop['experiment_list']:
                only_list.append(experiment_variable_search_recursive(database.nc_Database.drs.slicing_args.keys(),database.nc_Database,self.search_path,database.header['file_type_list'],self.options,
                                            experiment,var_name,database.header['variable_list'][var_name],list_level=list_level,session=self.session))
        return [item for sublist in only_list for item in sublist]

def experiment_variable_search_recursive(slicing_args,nc_Database,search_path,file_type_list,options,
                                        experiment,var_name,var_desc,list_level=None,session=None):
    if isinstance(slicing_args,list) and len(slicing_args)>0:
        #Go down slicing arguments:
        if ( slicing_args[0] in dir(options) and 
             getattr(options,slicing_args[0])!=None ):
            only_list=[]
            for field_option in getattr(options,slicing_args[0]):
                options_copy = copy.copy(options)

                setattr(options_copy,slicing_args[0],[field_option,])
                only_list.append(experiment_variable_search_recursive(slicing_args[1:],nc_Database,search_path,file_type_list,options_copy,
                                                         experiment,var_name,var_desc,list_level=list_level))
            return [item for sublist in only_list for item in sublist]
        else:
            return experiment_variable_search_recursive(slicing_args[1:],nc_Database,search_path,file_type_list,options,
                                                         experiment,var_name,var_desc,list_level=list_level)
    else:
        #When done, perform the search:
        return experiment_variable_search(nc_Database,search_path,file_type_list,options,
                                         experiment,var_name,var_desc,list_level=list_level,session=session)

def experiment_variable_search(nc_Database,search_path,file_type_list,options,
                                experiment,var_name,var_desc,list_level=None,session=None):
    nc_Database.file_expt.experiment=experiment
    nc_Database.file_expt.var=var_name
    nc_Database.file_expt.time=0
    for field_id, field in enumerate(nc_Database.drs.var_specs):
        setattr(nc_Database.file_expt,field,var_desc[field_id])

    #Assumes that all slicing arguments in options are length-one list:
    conn_kwargs={'distrib':options.distrib}

    if session != None:
        conn = SearchConnection(search_path, session=session)
    else:
        if 'ask_cache' in dir(options) and options.ask_cache:
            conn_kwargs['cache']=options.ask_cache.split(',')[0]
            if len(options.ask_cache.split(','))>1:
                conn_kwargs['expire_after']=datetime.timedelta(hours=float(options.ask_cache.split(',')[1]))
        conn=SearchConnection(search_path, **conn_kwargs)

    #Search the ESGF:
    top_ctx = conn.new_context(project=nc_Database.drs.project,
                        experiment=experiment,variable=var_name)
    if ( 'product' in dir(nc_Database.drs) and 
         top_ctx.hit_count == 0 ):
        #Try using project to define product name. Fix for CREATEIP.
        top_ctx = conn.new_context(product=nc_Database.drs.product,
                            experiment=experiment,variable=var_name)

    constraints_dict={field:var_desc[field_id] for field_id, field in enumerate(nc_Database.drs.var_specs)}
    #This is where the lenght-one list is important:
    #First constrain using fields that are not related to simulation_desc:
    constraints_dict.update(**{field:getattr(options,field)[0] for field in nc_Database.drs.slicing_args.keys()
                                            if field in dir(options) and getattr(options,field)!=None and not field in nc_Database.drs.simulations_desc})
    if 'aliases' in dir(nc_Database.drs):
        #Go down the simulation description and ensure that at each level an aliases could be used
        #This implementation does not allwo for aliases in variable descriptions
        constraints_dict.update(**{field:getattr(options,field)[0] for field in nc_Database.drs.slicing_args.keys()
                                                if field in dir(options) and 
                                                   getattr(options,field)!=None and 
                                                   field in nc_Database.drs.simulations_desc and
                                                   not field in nc_Database.drs.aliases.keys()})
        for field in  nc_Database.drs.simulations_desc:
            if ( field in dir(options) and 
                 getattr(options,field)!=None and 
                 field in nc_Database.drs.aliases.keys()):
                ctx=top_ctx.constrain(**constraints_dict)
                try:
                    constraints_dict.update(**{allow_aliases(nc_Database.drs,field,ctx.facet_counts.keys()):getattr(options,field)[0]})
                except KeyError:
                    pass
    else:
        #If no aliases, simply extend constraints to simulations_desc:
        constraints_dict.update(**{field:getattr(options,field)[0] for field in nc_Database.drs.slicing_args.keys()
                                                if field in dir(options) and getattr(options,field)!=None and field in nc_Database.drs.simulations_desc})
    #Consstrain with an adequate constraints dict
    ctx=top_ctx.constrain(**constraints_dict)

    if list_level!=None:
        try:
            return ctx.facet_counts[allow_aliases(nc_Database.drs,list_level,ctx.facet_counts.keys())].keys()
        except socket.error as e:
            print(search_path+' is not responding. '+e.strerror)
            print('This is not fatal. Data broadcast by '+search_path+' will simply NOT be considered.')
            return []
        except httplib.BadStatusLine as e:
            return []
        except requests.HTTPError as e:
            print(search_path+' is not responding. ')
            print(e)
            print('This is not fatal. Data broadcast by '+search_path+' will simply NOT be considered.')
            return []
        except (KeyError, ValueError) as e:
            #list_level is not available. Happens when nodes are not configured to handle data from the MIP.
            return []
    else:
        file_list_remote=[]
        file_list_found=[]
        try:
            file_list_found=ctx.search(variable=var_name)
            file_list_remote=map(lambda x: get_urls(nc_Database.drs,x,file_type_list,var_name),file_list_found)
            file_list_remote=[item for sublist in file_list_remote for item in sublist]
        except Exception as e:
            warnings.warn('Search path {0} is unresponsive at the moment'.format(search_path),UserWarning)

        map(lambda x: record_url(x,nc_Database),file_list_remote)
        return []

def allow_aliases(project_drs,key,available_keys):
    # Check if aliases are allowed and return compatible alias.
    aliases = find_aliases(project_drs,key)
    if available_keys != None:
        compatible_keys = set(aliases).intersection(available_keys)
        return compatible_keys.pop()
    else:
        return key

def find_aliases(project_drs,key):
    if ('aliases' in dir(project_drs) and
        key in project_drs.aliases.keys()):
        return project_drs.aliases[key]
    else:
        return [key,]

def record_url(remote_file_desc,nc_Database):
    nc_Database.file_expt.path=remote_file_desc['url']
    nc_Database.file_expt.data_node=remote_netcdf.get_data_node(remote_file_desc['url'],remote_file_desc['file_type'])
    for unique_file_id in unique_file_id_list:
        if remote_file_desc['file_type'] in nc_Database.drs.remote_file_types and remote_file_desc[unique_file_id]!=None:
            nc_Database.file_expt.path+='|'+remote_file_desc[unique_file_id]
        else:
            nc_Database.file_expt.path+='|'

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
        fil = fil_ctx.search(variable=var_name)
        #try:
        fil = fil_ctx.search(variable=var_name)
        for item in fil:
            file_list_remote.extend(get_url_remote(item,file_type_list,drs))
        
    return file_list_remote

def get_url_remote(item,file_type_list,drs):
    url_name=[]
    try:
        keys_list=item.urls.viewkeys()
    except:
        keys_list=[]

    for key in set(keys_list).intersection(file_type_list):
        file_info=create_file_info_dict(key,item,drs)
        
        if (any([ file_info[unique_file_id]!=None for unique_file_id in unique_file_id_list]) or
            drs.project in ['NMME']):
            url_name.append(file_info)
    return url_name

def create_file_info_dict(key,item,drs):
    file_info=dict()
    file_info['file_type']=key
    try:
        if key=='OPENDAP':
            file_info['url']=item.urls[key][0][0].replace('.html','')
        else:
            file_info['url']=item.urls[key][0][0]
    except Exception as e:
        file_info['url']=None
    for val in drs.official_drs+unique_file_id_list:
        try:
            if val=='var':
                #this is a temporary fix to a poor design decision on my part.
                #to really fix this, will have to change names 'var' to 'variable'.
                #if (isinstance(item.json['variable'],list) and
                #   len(item.json['variable']) == 1):
                #    file_info[val]=item.json['variable'][0]
                #else:
                file_info[val]=item.json['variable']
            elif val=='version':
                if 'version' in item.json.keys():
                    version=item.json[val]
                else:
                    #Version is poorly implemented... Try a fix:
                    version=item.json['instance_id'].split('.')[-3]
                if version[0]=='v':
                    file_info[val]=version
                else:
                    file_info[val]='v'+version
            elif ( val=='ensemble' and 
                   drs.project in ['NMME'] ):
                #Fix for NMME
                file_info[val]=item.json['instance_id'].split('_')[-2]
            elif ( val=='product' and
                   'product' in dir(drs) ):
                #Fix for CREATE-IP
                file_info[val] = drs.product
            else:
                file_info[val]=item.json[allow_aliases(drs,val,item.json.keys())]

            if isinstance(file_info[val],list): file_info[val]=str(file_info[val][0])
        except Exception as e:
            file_info[val]=None
    return file_info
    #if (file_info['checksum']!=None and 
    #    set(item.urls.keys()).issuperset(drs.required_file_types)):
