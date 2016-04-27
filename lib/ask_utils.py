#External:
import copy
import nc_Database
from operator import itemgetter
import os
import random
import sqlalchemy
import multiprocessing
import json
import netCDF4

#Internal:
import ftp_query
import filesystem_query
import esgf_query
import cdb_query_archive_class

##### PATH #####
def find_path(nc_Database,file_expt,semaphores=dict()):
    for val in dir(file_expt):
        if val[0]!='_' and val!='case_id':
            getattr(file_expt,val)
    nc_Database.session.add(file_expt)
    nc_Database.session.commit()
    return

def ask(database,options,q_manager=None):
    return ask_with_database(database,options)

def ask_with_database(database,options):
    database.define_database(options)
    only_list=[]

    only_list.append(ask_database(database,options))
    #if options.ensemble!=None:
    #    options_copy=copy.copy(options)
    #    options_copy.ensemble=['r0i0p0',]
    #    only_list.append(ask_database(database,options_copy))

    if options.list_only_field!=None:
        output=set([item for sublist in only_list for item in sublist])
    else:
        intersection(database)
        #List data_nodes:
        database.header['data_node_list']=database.nc_Database.list_data_nodes(options)
        output =database.nc_Database.write_database(database.header,options,'record_paths')
        #Remove data_nodes:
        with netCDF4.Dataset(output,'a') as dataset:
            delattr(dataset,'data_node_list')
    database.close_database()
    return output

def ask_database(database,options):
    #Copy and shuffle search path for optimal multithreaded asky:
    search_path_list=copy.copy(database.header['search_list'])
    random.shuffle(search_path_list)

    only_list=[]
    for search_path in search_path_list:
        if os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path)))):
            #Local filesystem archive
            browser=filesystem_query.browser(search_path,options)
        elif ('ftp' in search_path and search_path[:3]=='ftp'):
            #FTP filesystem archive
            browser=ftp_query.browser(search_path,options)
        elif ('http' in search_path and 'esg-search' in search_path):
            #ESGF catalogue archive query
            browser=esgf_query.browser(search_path,options)
        else:
            browser=None

        if browser!=None:
            only_list.append(browser.descend_tree(database,list_level=options.list_only_field))
            browser.close()
    return [item for sublist in only_list for item in sublist]

def find_model_list(diagnostic,project_drs,model_list,experiment):
    for var_name in diagnostic.header['variable_list'].keys():
        time_frequency=diagnostic.header['variable_list'][var_name][project_drs.var_specs.index('time_frequency')]
        if not time_frequency in ['fx','en']:
            #Do this without fx variables:
            conditions=[
                         nc_Database.File_Expt.var==var_name,
                         nc_Database.File_Expt.experiment==experiment
                       ]
            for field_id, field in enumerate(project_drs.var_specs):
                conditions.append(getattr(nc_Database.File_Expt,field)==diagnostic.header['variable_list'][var_name][field_id])

            model_list_var=diagnostic.nc_Database.session.query(*[getattr(nc_Database.File_Expt,desc) for desc in project_drs.simulations_desc]
                                    ).filter(sqlalchemy.and_(*conditions)).distinct().all()
            model_list=set(model_list).intersection(set(model_list_var))

    #Do it for fx variables:
    model_list_fx=[remove_ensemble(model,project_drs) for model in model_list]
    for var_name in diagnostic.header['variable_list'].keys():
        time_frequency=diagnostic.header['variable_list'][var_name][project_drs.var_specs.index('time_frequency')]
        if time_frequency in ['fx','en']:
            conditions=[
                         nc_Database.File_Expt.var==var_name,
                         nc_Database.File_Expt.experiment==experiment
                       ]
            for field_id, field in enumerate(project_drs.var_specs):
                conditions.append(getattr(nc_Database.File_Expt,field)==diagnostic.header['variable_list'][var_name][field_id])
            model_list_var=diagnostic.nc_Database.session.query(
                                    *[getattr(nc_Database.File_Expt,desc) for desc in project_drs.simulations_desc if desc!='ensemble']
                                       ).filter(sqlalchemy.and_(*conditions)).distinct().all()
            model_list_fx=set(model_list_fx).intersection(set(model_list_var))
    model_list_combined=[model for model in model_list if remove_ensemble(model,project_drs) in model_list_fx]
    return model_list_combined

def intersection(database):
    #Step one: find all the institute / model tuples with all the requested variables
    simulations_list=database.nc_Database.simulations_list()

    #Remove fixed variables:
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']
    model_list=copy.copy(simulations_list_no_fx)

    if not 'experiment' in database.drs.simulations_desc:
        for experiment in database.header['experiment_list'].keys():
            model_list = find_model_list(database,database.drs,model_list,experiment)
        model_list_combined=model_list
    else:
        list_of_model_list=[find_model_list(database,database.drs,model_list,experiment) 
                                           for experiment in database.header['experiment_list'].keys()]
        model_list_combined=set().union(*list_of_model_list)

    #Step two: find the models to remove:
    #model_list_combined=[model for model in model_list if remove_ensemble(model,database.drs) in model_list_fx]
    models_to_remove=set(simulations_list_no_fx).difference(model_list_combined)
    #models_to_remove=set(simulations_list_no_fx).difference(model_list)

    #Step three: remove from database:
    for model in models_to_remove:
        conditions=[ getattr(nc_Database.File_Expt,field)==model[field_id] for field_id, field in enumerate(database.drs.simulations_desc)]
        database.nc_Database.session.query(nc_Database.File_Expt).filter(*conditions).delete()

    #Step four: remove remaining models that only have fixed variables:
    simulations_list=database.nc_Database.simulations_list()

    #Remove fixed variables:
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']

    #Remove fixed variables:
    models_to_remove=set(
                        [remove_ensemble(simulation,database.drs) for simulation in simulations_list]
                         ).difference([remove_ensemble(simulation,database.drs) for simulation in simulations_list_no_fx])

    for model in models_to_remove:
        conditions=[ getattr(nc_Database.File_Expt,field)==model[field_id] for field_id, field in enumerate(remove_ensemble(database.drs.simulations_desc,database.drs))]
        database.nc_Database.session.query(nc_Database.File_Expt).filter(*conditions).delete()
    return

def remove_ensemble(simulation,project_drs):
    simulations_desc_indices_without_ensemble=range(0,len(project_drs.simulations_desc))
    simulations_desc_indices_without_ensemble.remove(project_drs.simulations_desc.index('ensemble'))
    return itemgetter(*simulations_desc_indices_without_ensemble)(simulation)

def wrapper_ask_simulations_recursive(args):
    return [(args[-1],)+item for item in ask_simulations_recursive(*args[:-1],async=False)]

def ask_simulations_recursive(database,options,simulations_desc,async=True):
    #Recursively find possible simulation list
    options_copy=copy.copy(options)
    if isinstance(simulations_desc,list) and len(simulations_desc)>1:
        options_copy.list_only_field=simulations_desc[0]
        output=ask_with_database(database,options_copy)
        options_copy.list_only_field=None
        args_list=[]
        for val in output:
            if (getattr(options_copy,simulations_desc[0]) == None):
                #Here it is important to pass a list:
                setattr(options_copy,simulations_desc[0],[val,])
                args_list.append((copy.copy(database),copy.copy(options_copy),simulations_desc[1:],val))
                setattr(options_copy,simulations_desc[0],None)
            elif (val in getattr(options_copy,simulations_desc[0])):
                args_list.append((copy.copy(database),copy.copy(options_copy),simulations_desc[1:],val))
        if ('num_procs' in dir(options_copy) and options_copy.num_procs>1 and async==True and len(args_list)>0
            and not ('serial' in dir(options_copy) and options_copy.serial)):
            pool=multiprocessing.Pool(processes=min(options_copy.num_procs,len(args_list),5))
            try:
                simulations_list=[item for sublist in pool.map(wrapper_ask_simulations_recursive,args_list) for item in sublist]
            finally:
                pool.terminate()
                pool.join()
        else:
            simulations_list=[item for sublist in map(wrapper_ask_simulations_recursive,args_list) for item in sublist]
    else:
        options_copy.list_only_field=simulations_desc[0]
        simulations_list=[(item,) for item in ask_with_database(database,options_copy)]
        options_copy.list_only_field=None
    return simulations_list

