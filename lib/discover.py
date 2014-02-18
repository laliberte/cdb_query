import copy
from nc_Database import File_Expt
from operator import itemgetter

import sqlalchemy

##### PATH #####
def find_path(nc_Database,file_expt):
    for val in dir(file_expt):
        if val[0]!='_' and val!='case_id':
            getattr(file_expt,val)
    nc_Database.session.add(file_expt)
    nc_Database.session.commit()
    return

def discover(database,options):
    database.nc_Database=nc_Database.nc_Database(database.drs)

    only_list=[]

    #Local filesystem archive
    local_paths=[search_path for search_path in 
                    database.header['search_list']
                    if os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
    for search_path in local_paths:
        only_list.append(filesystem_query.descend_tree(database,search_path,list_level=options.list_only_field))

    #ESGF search
    remote_paths=[search_path for search_path in 
                    database.header['search_list']
                    if not os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
    for search_path in remote_paths:
        only_list.append(esgf_query.descend_tree(database,search_path,options,list_level=options.list_only_field))

    if options.list_only_field!=None:
        for field_name in set([item for sublist in only_list for item in sublist]):
            print field_name
    else:
        intersection(database)
        #List data_nodes:
        database.header['data_node_list']=database.nc_Database.list_data_nodes()
        database.nc_Database.create_netcdf_container(database.header,options,'record_paths')
    return

def intersection(database):
    #Step one: find all the institute / model tuples with all the requested variables
    simulations_list=database.nc_Database.simulations_list()

    #Remove fixed variables:
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']
    model_list=copy.copy(simulations_list_no_fx)

    for experiment in database.header['experiment_list'].keys():
        for var_name in database.header['variable_list'].keys():
            time_frequency=database.header['variable_list'][var_name][database.drs.var_specs.index('time_frequency')]
            if not time_frequency in ['fx','en']:
                #Do this without fx variables:
                conditions=[
                             File_Expt.var==var_name,
                             File_Expt.experiment==experiment
                           ]
                for field_id, field in enumerate(database.drs.var_specs):
                    conditions.append(getattr(File_Expt,field)==database.header['variable_list'][var_name][field_id])

                model_list_var=database.nc_Database.session.query(
                                         File_Expt.institute,
                                         File_Expt.model,
                                         File_Expt.ensemble
                                        ).filter(sqlalchemy.and_(*conditions)).distinct().all()
                model_list=set(model_list).intersection(set(model_list_var))

        #Do it for fx variables:
        model_list_fx=[model[:-1] for model in model_list]
        for var_name in database.header['variable_list'].keys():
            time_frequency=database.header['variable_list'][var_name][database.drs.var_specs.index('time_frequency')]
            if time_frequency in ['fx','en']:
                conditions=[
                             File_Expt.var==var_name,
                             File_Expt.experiment==experiment
                           ]
                for field_id, field in enumerate(database.drs.var_specs):
                    conditions.append(getattr(File_Expt,field)==database.header['variable_list'][var_name][field_id])
                model_list_var=database.nc_Database.session.query(
                                         File_Expt.institute,
                                         File_Expt.model,
                                           ).filter(sqlalchemy.and_(*conditions)).distinct().all()
                model_list_fx=set(model_list_fx).intersection(set(model_list_var))


    #Step two: find the models to remove:
    model_list_combined=[model for model in model_list if remove_ensemble(model,database.drs) in model_list_fx]
    models_to_remove=set(simulations_list_no_fx).difference(model_list_combined)

    #Step three: remove from database:
    for model in models_to_remove:
        conditions=[ getattr(File_Expt,field)==model[field_id] for field_id, field in enumerate(database.drs.simulations_desc)]
        database.nc_Database.session.query(File_Expt).filter(*conditions).delete()

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
        conditions=[ getattr(File_Expt,field)==model[field_id] for field_id, field in enumerate(remove_ensemble(database.drs.simulations_desc,database.drs))]
        database.nc_Database.session.query(File_Expt).filter(*conditions).delete()

    return

def remove_ensemble(simulation,project_drs):
    simulations_desc_indices_without_ensemble=range(0,len(project_drs.simulations_desc))
    simulations_desc_indices_without_ensemble.remove(project_drs.simulations_desc.index('ensemble'))
    return itemgetter(*simulations_desc_indices_without_ensemble)(simulation)

