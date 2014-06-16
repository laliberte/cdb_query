import copy
import nc_Database
from operator import itemgetter
import os

import filesystem_query
import esgf_query

import sqlalchemy

import multiprocessing

##### PATH #####
def find_path(nc_Database,file_expt,semaphores=None):
    for val in dir(file_expt):
        if val[0]!='_' and val!='case_id':
            getattr(file_expt,val)
    nc_Database.session.add(file_expt)
    nc_Database.session.commit()
    return

def discover(database,options):
    database.define_database(options)
    only_list=[]

    only_list.append(discover_database(database,options))
    if options.ensemble!=None:
        options_copy=copy.copy(options)
        options_copy.ensemble='r0i0p0'
        only_list.append(discover_database(database,options_copy))

    if options.list_only_field!=None:
        output=set([item for sublist in only_list for item in sublist])
    else:
        intersection(database)
        #List data_nodes:
        database.header['data_node_list']=database.nc_Database.list_data_nodes(options)
        dataset, output =database.nc_Database.write_database(database.header,options,'record_paths')
        #Remove data_nodes:
        delattr(dataset,'data_node_list')
        dataset.close()


    database.close_database()
    return output

def discover_database(database,options):
    only_list=[]
    #Local filesystem archive
    local_paths=[search_path for search_path in 
                    database.header['search_list']
                    if os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
    for search_path in local_paths:
        only_list.append(filesystem_query.descend_tree(database,search_path,options,list_level=options.list_only_field))

    #ESGF search
    remote_paths=[search_path for search_path in 
                    database.header['search_list']
                    if not os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
    for search_path in remote_paths:
        only_list.append(esgf_query.descend_tree(database,search_path,options,list_level=options.list_only_field))
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

def wrapper_discover_simulations_recursive(args):
    return [(args[-1],)+item for item in discover_simulations_recursive(*args[:-1],async=False)]

def discover_simulations_recursive(database,options,simulations_desc,async=True):
    if isinstance(simulations_desc,list) and len(simulations_desc)>1:
        options.list_only_field=simulations_desc[0]
        output=discover(database,options)
        options.list_only_field=None
        args_list=[]
        for val in output:
            setattr(options,simulations_desc[0],val)
            args_list.append((copy.copy(database),copy.copy(options),simulations_desc[1:],val))
            setattr(options,simulations_desc[0],None)
        if 'num_procs' in dir(options) and options.num_procs>1 and async==True:
            pool=multiprocessing.Pool(processes=min(options.num_procs,len(args_list)))
            simulations_list=[item for sublist in pool.map(wrapper_discover_simulations_recursive,args_list) for item in sublist]
            pool.close()
        else:
            simulations_list=[item for sublist in map(wrapper_discover_simulations_recursive,args_list) for item in sublist]
    else:
        options.list_only_field=simulations_desc[0]
        simulations_list=[(item,) for item in discover(database,options)]
        options.list_only_field=None
    return simulations_list

