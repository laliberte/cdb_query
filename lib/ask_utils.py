#from __future__ import nested_scopes, generators, division, absolute_import, with_statement, print_function, unicode_literals

#External:
import copy
from operator import itemgetter
import os
import random
import sqlalchemy
import multiprocessing
import json
import netCDF4

#Internal:
import nc_Database, ftp_query, filesystem_query, esgf_query, cdb_query_archive_class


#################################################################
#   ROUTINES AND FUNCTIONS TO QUERY A DATABASE (LOCAL, FTP, ESGF)
#

def ask(database,options,q_manager=None,sessions=dict()):
    '''
    Function to query a database. All components are passed through the `options`,
    the result of a parsed command line string.
    '''
    #Setup databse:
    database.define_database(options)

    #Perform the query. The output is a list of possible
    #fields in --list_only_field has been specified:
    if 'ask' in sessions.keys():
        session=session=sessions['ask']
    else:
        session=None

    only_list=ask_database(database,options,session=session)

    if options.list_only_field!=None:
        #If --list_only_field has been specified, output this list:
        output=set(only_list)
    else:
        #Find the intersection of the database, so that all the requested variables are available:
        intersection(database)
        #List data_nodes and record to header:
        database.header['data_node_list']=database.nc_Database.list_data_nodes(options)

        #Write the database to file. Only record paths:
        output =database.nc_Database.write_database(database.header,options,'record_paths')
        #Remove data_node list from output file:
        with netCDF4.Dataset(output,'a') as dataset:
            delattr(dataset,'data_node_list')
    #Close the database and clear memory
    database.close_database()
    #Return --list_only_field or output filename:
    return output

def ask_database(database,options,session=None):
    '''
    Function that loops through search paths to complete the query.
    '''
    #Copy and shuffle search path for optimal multithreaded ask:
    search_path_list=copy.copy(database.header['search_list'])
    random.shuffle(search_path_list)

    #Create empty list:
    only_list=[]
    for search_path in search_path_list:
        #Decide what type of query and load query_specific browser:
        if os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path)))):
            #Local filesystem archive
            browser=filesystem_query.browser(search_path,options)
        elif ('ftp' in search_path and search_path[:3]=='ftp'):
            #FTP filesystem archive
            browser=ftp_query.browser(search_path,options)
        elif ('http' in search_path and 'esg-search' in search_path):
            #ESGF catalogue archive query
            browser=esgf_query.browser(search_path,options,session=session)
        else:
            browser=None

        if browser!=None:
            #Test if browser is working. Only important for ESGF queries:
            if browser.test_valid():
                only_list.append(browser.descend_tree(database,list_level=options.list_only_field))
            else:
                print('Search_path '+search_path+' is not accessible. It will not be considered.')
            browser.close()
    #Convert list of list into list:
    return [item for sublist in only_list for item in sublist]

def find_model_list(database,model_list,experiment):
    '''
    Function to find the what subset of `model_list` can be found in 
    `database` for experiment `experiment`.
    '''
    #Copy input model_list to avoid modifying the original:
    model_list_copy=copy.copy(model_list)

    #Loop through requested variables:
    for var_name in database.header['variable_list'].keys():
        time_frequency=database.header['variable_list'][var_name][database.drs.var_specs.index('time_frequency')]
        #Here time frequency 'en' is for an ensemble mean, i.e. a variable that is common to all ensemble members.
        #So are fx variables.
        if not time_frequency in ['fx','en']:
            #Do this without fx variables:
            conditions=[
                         nc_Database.File_Expt.var==var_name,
                         nc_Database.File_Expt.experiment==experiment
                       ]
            for field_id, field in enumerate(database.drs.var_specs):
                conditions.append(getattr(nc_Database.File_Expt,field)==database.header['variable_list'][var_name][field_id])

            #Find the model_list for that one variable:
            model_list_var=database.nc_Database.session.query(*[getattr(nc_Database.File_Expt,desc) for desc in database.drs.simulations_desc]
                                    ).filter(sqlalchemy.and_(*conditions)).distinct().all()
            #Remove from model_list:
            model_list_copy=set(model_list_copy).intersection(set(model_list_var))

    #Perform the same for variables shared by all elements of the ensemble:
    model_list_fx=[remove_ensemble(model,database.drs) for model in model_list_copy]
    for var_name in database.header['variable_list'].keys():
        time_frequency=database.header['variable_list'][var_name][database.drs.var_specs.index('time_frequency')]
        if time_frequency in ['fx','en']:
            conditions=[
                         nc_Database.File_Expt.var==var_name,
                         nc_Database.File_Expt.experiment==experiment
                       ]
            for field_id, field in enumerate(database.drs.var_specs):
                conditions.append(getattr(nc_Database.File_Expt,field)==database.header['variable_list'][var_name][field_id])
            #For these variables remove ensemble description:
            model_list_var=database.nc_Database.session.query(
                                    *[getattr(nc_Database.File_Expt,desc) for desc in database.drs.simulations_desc if desc!='ensemble']
                                       ).filter(sqlalchemy.and_(*conditions)).distinct().all()
            model_list_fx=set(model_list_fx).intersection(set(model_list_var))
    model_list_combined=[model for model in model_list_copy if remove_ensemble(model,database.drs) in model_list_fx]
    return model_list_combined

def intersection(database):
    #Step one: find all the institute / model tuples with all the requested variables
    simulations_list=database.nc_Database.simulations_list()

    #Remove fixed variables:
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']
    model_list=copy.copy(simulations_list_no_fx)

    if not 'experiment' in database.drs.simulations_desc:
        #If simulations are not defined using experiments, 
        #successively remove models by going through all the experiments:
        for experiment in database.header['experiment_list'].keys():
            model_list = find_model_list(database,model_list,experiment)
        #Assigned the limited model_list:
        model_list_combined=model_list
    else:
        #If they are, then list all simulations and find the combined model list:
        list_of_model_list=[find_model_list(database,model_list,experiment) 
                                           for experiment in database.header['experiment_list'].keys()]
        model_list_combined=set().union(*list_of_model_list)

    #Step two: find the models to remove:
    models_to_remove=set(simulations_list_no_fx).difference(model_list_combined)

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
        #Remove the models from the database:
        conditions=[ getattr(nc_Database.File_Expt,field)==model[field_id] for field_id, field in enumerate(remove_ensemble(database.drs.simulations_desc,database.drs))]
        database.nc_Database.session.query(nc_Database.File_Expt).filter(*conditions).delete()
    return

def remove_ensemble(simulation,project_drs):
    '''
    Function to remove ensemble description from simulation description.
    '''
    simulations_desc_indices_without_ensemble=range(0,len(project_drs.simulations_desc))
    simulations_desc_indices_without_ensemble.remove(project_drs.simulations_desc.index('ensemble'))
    return itemgetter(*simulations_desc_indices_without_ensemble)(simulation)

def wrapper_ask_simulations_recursive(args):
    '''
    Wrapper to make ask_simulations_recursive pickable.
    '''
    return [(args[-1],)+item for item in ask_simulations_recursive(*args[:-1],async=False)]

def ask_simulations_recursive(database,options,simulations_desc,async=True):
    '''
    Function to recursively find possible simulation list
    '''
    options_copy=copy.copy(options)
    if isinstance(simulations_desc,list) and len(simulations_desc)>1:
        #--list_only_field set to first simulations_desc.
        options_copy.list_only_field=simulations_desc[0]

        #Ask to list only that field:
        output=ask(database,options_copy)

        options_copy.list_only_field=None
        args_list=[]

        #Loop through the listed fields:
        #if len(output)>0:
        #    print(output)
        for val in output:
            if (getattr(options_copy,simulations_desc[0]) == None):
                #Slice the next ask with this value.
                #Here it is important to pass a list:
                setattr(options_copy,simulations_desc[0],[val,])
                #Put in args list and reset:
                args_list.append((copy.copy(database),copy.copy(options_copy),simulations_desc[1:],val))
                setattr(options_copy,simulations_desc[0],None)
            elif (val in getattr(options_copy,simulations_desc[0])):
                #val was already sliced:
                args_list.append((copy.copy(database),copy.copy(options_copy),simulations_desc[1:],val))
        if ('num_procs' in dir(options_copy) and options_copy.num_procs>1 and async==True and len(args_list)>0
            and not ('serial' in dir(options_copy) and options_copy.serial)):
            #Use at most 5 processors in multiprocessing was requested (siginifcant speed up):
            pool=multiprocessing.Pool(processes=min(options_copy.num_procs,len(args_list),5))
            try:
                simulations_list=[item for sublist in pool.map(wrapper_ask_simulations_recursive,args_list) for item in sublist]
            finally:
                pool.terminate()
                pool.join()
        else:
            #Only use multiprocessing for the first level:
            simulations_list=[item for sublist in map(wrapper_ask_simulations_recursive,args_list) for item in sublist]
    else:
        #Recursion termination condition:
        options_copy.list_only_field=simulations_desc[0]
        simulations_list=[(item,) for item in ask(database,options_copy)]
        options_copy.list_only_field=None
    return simulations_list

def find_path(nc_Database,file_expt,time_slices=dict(),semaphores=dict(),session=None,remote_netcdf_kwargs=dict()):
    for val in dir(file_expt):
        if val[0]!='_' and val!='case_id':
            getattr(file_expt,val)
    nc_Database.session.add(file_expt)
    nc_Database.session.commit()
    return
