#External:
import netCDF4
import copy
import os
import shutil
import json
import timeit
import time
import numpy as np
import multiprocessing
import random
import sys
import getpass
import datetime
import requests
import logging

#External but related:
import netcdf4_soft_links.certificates.certificates as certificates
import netcdf4_soft_links.retrieval_manager as retrieval_manager
import netcdf4_soft_links.remote_netcdf.remote_netcdf as remote_netcdf

#Internal:
from .utils import ask_utils, validate_utils, reduce_utils, downloads_utils, find_functions
from .nc_Database import db_manager, db_utils
from . import parsers, commands_parser

def ask(database, options, q_manager=None, sessions=dict()):
    #Load header:
    database.load_header(options)

    #Simplify the header:
    database.union_header()

    #Only a listing of a few fields was requested.
    if ('list_only_field' in dir(options) and options.list_only_field!=None):
        for field_name in ask_utils.ask(database,options):
            print(field_name)
        return

    #Find the simulation list:
    #Check if a specific simulation was sliced:
    single_simulation_requested = []
    for desc in database.drs.simulations_desc:
        if (getattr(options,desc) != None):
            if (len(getattr(options,desc)) == 1 or 
                (desc=='ensemble' and 'r0i0p0' in getattr(options,desc)
                 and len(getattr(options,desc)) == 2)):
                #Either a single simulation or two ensembles if one is r0i0p0
                single_simulation_requested.append(getattr(options, desc))

    if len(single_simulation_requested) == len(database.drs.simulations_desc):
        simulations_list = [ tuple(single_simulation_requested)]
    else:
        simulations_list = ask_utils.ask_simulations_recursive(database,options,database.drs.simulations_desc)

    #Remove fixed variable:
    if 'ensemble' in database.drs.simulations_desc:
        simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']
    else:
        simulations_list_no_fx = copy.copy(simulations_list)

    if not ('silent' in dir(options) and options.silent) and len(simulations_list_no_fx)>1:
        print("This is a list of simulations that COULD satisfy the query:")
        for simulation in simulations_list_no_fx:
            print(','.join(simulation))
        print("cdb_query will now attempt to confirm that these simulations have all the requested variables."
               "This can take some time. Please abort if there are not enough simulations for your needs.")
    
    vars_list = ask_utils.ask_var_list(database,simulations_list_no_fx,options)
    database.put_or_process(ask_utils.ask,vars_list,options,q_manager,sessions)
    return

def validate(database,options,q_manager=None,sessions=dict()):
    database.load_header(options)

    if not 'data_node_list' in database.header.keys():
        data_node_list, url_list, simulations_list = database.find_data_nodes_and_simulations(options)
        if not options.no_check_availability:
            data_node_list, Xdata_node_list = rank_data_nodes(options,data_node_list,url_list,q_manager)
        else:
            Xdata_node_list=[]
    else:
        simulations_list=[]
        data_node_list=database.header['data_node_list']
        Xdata_node_list=[]
    database.drs.data_node_list = data_node_list

    #Some data_nodes might have been dropped. Restrict options accordingly:
    options_copy = copy.copy(options)
    options_copy.data_node=data_node_list
    if 'Xdata_node' in dir(options_copy) and isinstance(options_copy.Xdata_node,list):
        options_copy.Xdata_node = list(set(options_copy.Xdata_node+Xdata_node_list))
    else:
        options_copy.Xdata_node = Xdata_node_list

    #Find the atomic simulations:
    if simulations_list==[]:
        simulations_list=database.list_fields_local(options_copy,database.drs.simulations_desc, soft_links=False)
    #Remove fixed variable:
    if 'ensemble' in database.drs.simulations_desc:
        simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']
    else:
        simulations_list_no_fx = copy.copy(simulations_list)

    #Activate queues and semaphores:
    if q_manager != None:
        for data_node in data_node_list:
            if len(set(['validate']).intersection(q_manager.queues_names))>0:
                q_manager.validate_semaphores.add_new_data_node(data_node)
            #Add the data nodes if downloads are required
            if len(set(['download_files','download_opendap']).intersection(q_manager.queues_names))>0:
                q_manager.download.semaphores.add_new_data_node(data_node)
                q_manager.download.queues.add_new_data_node(data_node)

    vars_list=ask_utils.ask_var_list(database,simulations_list_no_fx,options_copy)
    database.put_or_process(validate_utils.validate,vars_list,options_copy,q_manager,sessions)
    return


def download_files(database,options,q_manager=None,sessions=dict()):
    if q_manager != None:
        data_node_list, url_list, simulations_list =database.find_data_nodes_and_simulations(options)
        for data_node in data_node_list:
            q_manager.download.semaphores.add_new_data_node(data_node)
            q_manager.download.queues.add_new_data_node(data_node)
        if multiprocessing.current_process().name == 'MainProcess':
            #If this is the main process, can start download processes:
            q_manager.start_download_processes()

    #Recover the database meta data:
    if ( not 'script' in dir(options) or options.script==''):
        #No reduction: do not split in variables...
        vars_list=[ [reduce_utils.make_list(None) for item in database.drs.official_drs_no_version] ] 
    else:
        vars_list=reduce_utils.reduce_var_list(database,options)

    #Users have requested time types to be kept
    times_list = downloads_utils.time_split(database,options,check_split=(len(vars_list)==1))
    database.put_or_process(downloads_utils.download_files,vars_list,options,q_manager,sessions,times_list=times_list)
    return

def reduce_soft_links(database,options,q_manager=None,sessions=dict()):
    vars_list=reduce_utils.reduce_var_list(database,options)
    database.put_or_process(reduce_utils.reduce_soft_links,vars_list,options,q_manager,sessions)
    return

def download_opendap(database,options,q_manager=None,sessions=dict()):
    if q_manager != None:
        data_node_list, url_list, simulations_list =database.find_data_nodes_and_simulations(options)
        for data_node in data_node_list:
            q_manager.download.semaphores.add_new_data_node(data_node)
            q_manager.download.queues.add_new_data_node(data_node)
        if multiprocessing.current_process().name == 'MainProcess':
            #If this is the main process, can start download processes:
            q_manager.start_download_processes()

    if ( not 'script' in dir(options) or options.script==''):
        #No reduction: do not split in variables...
        vars_list=[ [reduce_utils.make_list(None) for item in database.drs.official_drs_no_version] ] 
    else:
        vars_list=reduce_utils.reduce_var_list(database,options)

    times_list = downloads_utils.time_split(database,options,check_split=(len(vars_list)==1))
    database.put_or_process(downloads_utils.download_opendap,vars_list,options,q_manager,sessions,times_list=times_list)
    return

#def gather(database,options,q_manager=None,sessions=dict()):
#    reduce(database,options,q_manager=q_manager)
#    return

def reduce(database,options,q_manager=None,sessions=dict()):
    if (options.script=='' and 
        ('in_extra_netcdf_files' in dir(options) and 
          len(options.in_extra_netcdf_files)>0) ):
        raise SyntaxErrorr('The identity script \'\' can only be used when no extra netcdf files are specified.')

    vars_list = reduce_utils.reduce_var_list(database,options)
    times_list = downloads_utils.time_split(database,options,check_split=(len(vars_list)==1))
    database.put_or_process(reduce_utils.reduce_variable, vars_list, options, q_manager, sessions, times_list=times_list)
    return

def merge(database,options,q_manager=None,sessions=dict()):
    database.load_header(options)
    with netCDF4.Dataset(options.out_netcdf_file,'w') as output:
        db_manager.record_header(output,database.header)
        for file_name in [options.in_netcdf_file,]+options.in_extra_netcdf_files:
            db_utils.record_to_netcdf_file_from_file_name(options,file_name,output,database.drs)
    return

def list_fields(database,options,q_manager=None,sessions=dict()):
    fields_list=database.list_fields_local(options,options.field)
    for field in fields_list:
        print ','.join(field)
    return

class Database_Manager:
    def __init__(self,project_drs):
        self.drs=project_drs
        return

    def list_fields_local(self,options,fields_to_list, soft_links=True):
        self.load_database(options, find_functions.simple, soft_links=soft_links)
        fields_list=self.nc_Database.list_fields(fields_to_list)
        self.close_database()
        return fields_list

    def put_or_process(self,function_handle,vars_list,options,q_manager,sessions,times_list=[(None,None,None,None),]):

        if (len(vars_list)==0 or len(times_list)==0):
            #There is no variables to find in the input. 
            #Delete input and decrement expected function.
            q_manager.remove(options)

            if len(vars_list)>0:
                if not ('silent' in dir(options) and options.silent):
                    for var in vars_list:
                        logging.warning(' '.join([ opt+': '+str(var[opt_id]) 
                                                   for opt_id, opt in enumerate(self.drs.official_drs_no_version)]))
                    logging.warning('Were excluded because no date matched times requested')
            return

        if not ((len(vars_list)==1 and len(times_list)==1) or
            'serial' in dir(options) and options.serial):
            #Randomize to minimize strain on consumers:
            random.shuffle(vars_list)
            for var_id, var in enumerate(vars_list):
                for time_id, time in enumerate(times_list):
                    options_copy = make_new_options_from_lists(options,var,time,self.drs.official_drs_no_version)
                    #Set the priority to var_id + time_id to get a good mix:
                    options_copy.priority = var_id + time_id

                    if len(times_list)>1:
                        #Find times list again:
                        var_times_list = downloads_utils.time_split(self,options_copy)
                    #Submit only if the times_list is not empty:
                    if len(times_list)==1 or len(var_times_list)>0:
                        q_manager.increment_expected_and_put(options_copy, copyfile=True)
            q_manager.remove(options)
            return
        else:
            #Compute single element!
            if ('serial' in dir(options) and options.serial):
                options_copy = copy.copy(options)
            else: 
                options_copy = make_new_options_from_lists(options,vars_list[0],times_list[0],self.drs.official_drs_no_version)

            #Compute function:
            function_handle(self, options_copy, q_manager=q_manager, sessions=sessions)

            #Reset trial counter:
            options_copy.trial=0

            q_manager.put_to_next(options_copy)
            return

    def find_data_nodes_and_simulations(self,options):
        #We have to time the response of the data node.
        self.load_database(options, find_functions.simple)
        simulations_list=self.nc_Database.list_fields(self.drs.simulations_desc)

        data_node_list=[item[0] for item in self.nc_Database.list_fields(['data_node'])]
        url_list=[[item.split('|')[0] for item in self.nc_Database.list_paths_by_data_node(data_node)]
                            for data_node in data_node_list ]
        #url_list=[self.nc_Database.list_paths_by_data_node(data_node).split('|')
        #            for data_node in data_node_list ]
        self.close_database()
        return data_node_list,url_list, simulations_list

    def union_header(self):
        #This method creates a simplified header

        #Create the diagnostic description dictionary:
        self.header_simple={}

        #Find all the requested realms, frequencies and variables:
        variable_list=['var_list']+[field+'_list' for field in self.drs.var_specs]
        for list_name in variable_list: self.header_simple[list_name]=[]
        for var_name in self.header['variable_list'].keys():
            self.header_simple['var_list'].append(var_name)
            for list_id, list_name in enumerate(list(variable_list[1:])):
                self.header_simple[list_name].append(self.header['variable_list'][var_name][list_id])

        #Find all the requested experiments and years:
        experiment_list=['experiment_list','years_list']
        for list_name in experiment_list: self.header_simple[list_name]=[]
        for experiment_name in self.header['experiment_list'].keys():
            self.header_simple['experiment_list'].append(experiment_name)
            for list_name in list(experiment_list[1:]):
                self.header_simple[list_name].append(self.header['experiment_list'][experiment_name])
                
        #Find the unique members:
        for list_name in self.header_simple.keys(): self.header_simple[list_name]=list(set(self.header_simple[list_name]))
        return

    def load_header(self,options):
        if ( commands_parser._get_command_name(options) == 'ask' ):
            self.header=dict()
            try:
                self.header['experiment_list']={item.split(':')[0]:item.split(':')[1].replace('-',',') for item in options.ask_experiment}
                self.header['month_list']=[item for item in options.ask_month]
                self.header['search_list']=[item for item in options.search_path if not item in options.Xsearch_path]
                self.header['variable_list']={item.split(':')[0]:item.split(':')[1].split('-') for item in options.ask_var}
                #Need to do this to allwo choices:
                if len(options.ask_file_type)==0:
                    self.header['file_type_list'] = parsers.file_type_list
                else:
                    self.header['file_type_list']=[item for item in options.ask_file_type]
            except IndexError:
                raise SyntaxError('Query improperly specified. Check --ask_var and --ask_experiment')
        else:
            self.define_database(options)
            self.header=self.nc_Database.load_header()
            self.close_database()
        return
        
    def load_database(self,options,find_function,
                      soft_links=True,time_slices=dict(),
                      semaphores=dict(),session=None,remote_netcdf_kwargs=dict()):
        self.define_database(options)
        if 'header' in dir(self):
            self.nc_Database.header=self.header
        self.nc_Database.populate_database(options,find_function,
                                           soft_links=soft_links,
                                           time_slices=time_slices,semaphores=semaphores,
                                           session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
        if 'ensemble' in dir(options) and options.ensemble!=None:
            #Always include r0i0p0 when ensemble was sliced:
            options_copy=copy.copy(options)
            options_copy.ensemble='r0i0p0'
            self.nc_Database.populate_database(options_copy,find_function,soft_links=soft_links,
                                               semaphores=semaphores,session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
        return

    def define_database(self,options):
        if 'in_netcdf_file' in dir(options):
            self.nc_Database = db_manager.nc_Database(self.drs, database_file=options.in_netcdf_file)
            return
        else:
            self.nc_Database = db_manager.nc_Database(self.drs)
            return

    def close_database(self):
        self.nc_Database.close_database()
        del self.nc_Database
        return

def rank_data_nodes(options,data_node_list,url_list,q_manager):
    data_node_list_timed=[]
    data_node_timing=[]
    for data_node_id, data_node in enumerate(data_node_list):
        if db_utils.is_level_name_included_and_not_excluded('data_node',options,data_node):
            url=url_list[data_node_id]
            if not ('silent' in dir(options) and options.silent):
                if 'log_files' in dir(options) and options.log_files:
                    logging.info('Querying '+url[0]+' to measure response time of data node... ')
                else:
                    print('Querying '+url[0]+' to measure response time of data node... ')

            #Add credentials:
            credentials_kwargs={opt: getattr(options,opt) for opt in ['openid','username','password','use_certificates', 'timeout'
                                                                         ] if opt in dir(options)}

            #Create a session for timing:
            session=requests.Session()
            with Timer() as timed_exec:
                try:
                    is_available = remote_netcdf.remote_netCDF(url[0],url[1],semaphores=q_manager.validate_semaphores,
                                                                        session=session,
                                                                       **credentials_kwargs).is_available(num_trials=1)
                except Exception as e:
                    is_available = False
                    if (str(e).startswith('The kind of user must be selected') or
                         ('debug' in dir(options) and options.debug)):
                        raise

            if not is_available:
                if not ('silent' in dir(options) and options.silent):
                    if timed_exec.interval > options.timeout:
                        logging.warning('Data node '+data_node+' excluded because it did not respond (timeout).')
                    else:
                        logging.warning('Data node '+data_node+' excluded because it did not respond.')
            else:
                #Try opening a link on the data node. If it does not work, remove this data node.
                number_of_trials=3
                exclude_data_node=False
                try:
                    timing=0.0
                    for trial in range(number_of_trials):
                        #simple loop. Pass the session that should have all the proper cookies:
                        with Timer() as timed_exec:
                            is_available=remote_netcdf.remote_netCDF(url[0],url[1],semaphores=q_manager.validate_semaphores,
                                                                                session=session,
                                                                               **credentials_kwargs).is_available(num_trials=1)
                        timing+=timed_exec.interval
                    data_node_timing.append(timing)
                    data_node_list_timed.append(data_node)
                    if not ('silent' in dir(options) and options.silent):
                        if 'log_files' in dir(options) and options.log_files:
                            logging.info('Done!')
                        else:
                            print('Done!')
                except Exception as e:
                    if (str(e).startswith('The kind of user must be selected') or
                         ('debug' in dir(options) and options.debug)):
                        raise
                    exclude_data_node=True
                except:
                    exclude_data_node=True

                if not ('silent' in dir(options) and options.silent) and exclude_data_node:
                    logging.warning('Data node '+data_node+' excluded because it did not respond.')
            #Close the session:
            session.close()
    return list(np.array(data_node_list_timed)[np.argsort(data_node_timing)]),list(set(data_node_list).difference(data_node_list_timed))

def make_new_options_from_lists(options,var_item,time_item,official_drs_no_version):
    options_copy=copy.copy(options)
    reduce_utils.set_new_var_options(options_copy, var_item, official_drs_no_version)
    reduce_utils.set_new_time_options(options_copy, time_item)

    if ( commands_parser._get_command_name(options) in ['ask','validate'] and
        'ensemble' in official_drs_no_version and
        'ensemble' in dir(options_copy) and options_copy.ensemble != None
        and not 'r0i0p0' in options_copy.ensemble):
        #Added 'fixed' variables:
        options_copy.ensemble.append('r0i0p0')
    return options_copy


#http://preshing.com/20110924/timing-your-code-using-pythons-with-statement/
class Timer:    
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

