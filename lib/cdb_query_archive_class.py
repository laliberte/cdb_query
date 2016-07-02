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

#External but related:
import netcdf4_soft_links.certificates as certificates
import netcdf4_soft_links.retrieval_manager as retrieval_manager
import netcdf4_soft_links.remote_netcdf as remote_netcdf

#Internal:
import ask_utils
import validate_utils
import nc_Database
import nc_Database_utils
import nc_Database_reduce
import downloads
import cdb_query_archive_parsers

def ask_var_list(database,simulations_list,options):
    if 'keep_field' in dir(options):
        drs_to_eliminate=[field for field in database.drs.simulations_desc if
                                             not field in options.keep_field]
    else:
        drs_to_eliminate=database.drs.simulations_desc
    return [ [make_list(item) for item in var_list] for var_list in 
                set([
                    tuple([ 
                            tuple(sorted(set(make_list(var[database.drs.simulations_desc.index(field)])))) 
                        if field in drs_to_eliminate else None
                        for field in database.drs.official_drs_no_version]) for var in 
                        simulations_list ])]

def ask(database,options,q_manager=None,sessions=dict()):
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
    single_simulation_requested=[]
    for desc in database.drs.simulations_desc:
        if (getattr(options,desc) !=None):
            if (len(getattr(options,desc))==1 or 
                (desc=='ensemble' and 'r0i0p0' in getattr(options,desc)
                 and len(getattr(options,desc))==2)):
                #Either a single simulation or two ensembles if one is r0i0p0
                single_simulation_requested.append(getattr(options,desc))

    if len(single_simulation_requested)==len(database.drs.simulations_desc):
        simulations_list=[tuple(single_simulation_requested)]
    else:
        simulations_list=ask_utils.ask_simulations_recursive(database,options,database.drs.simulations_desc)

    #Remove fixed variable:
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']
    if not ('silent' in dir(options) and options.silent) and len(simulations_list_no_fx)>1:
        print "This is a list of simulations that COULD satisfy the query:"
        for simulation in simulations_list_no_fx:
            print ','.join(simulation)
        print "cdb_query will now attempt to confirm that these simulations have all the requested variables."
        print "This can take some time. Please abort if there are not enough simulations for your needs."
    
    vars_list=ask_var_list(database,simulations_list_no_fx,options)
    database.put_or_process('ask',ask_utils.ask,vars_list,options,q_manager,sessions)
    return

def validate(database,options,q_manager=None,sessions=dict()):
    database.load_header(options)

    if not 'data_node_list' in database.header.keys():
        data_node_list, url_list, simulations_list =database.find_data_nodes_and_simulations(options)
        if not options.no_check_availability:
            data_node_list, Xdata_node_list=rank_data_nodes(options,data_node_list,url_list,q_manager)
        else:
            Xdata_node_list=[]
    else:
        simulations_list=[]
        data_node_list=database.header['data_node_list']
        Xdata_node_list=[]
    database.drs.data_node_list=data_node_list

    #Some data_nodes might have been dropped. Restrict options accordingly:
    options_copy=copy.copy(options)
    options_copy.data_node=data_node_list
    if 'Xdata_node' in dir(options_copy) and isinstance(options_copy.Xdata_node,list):
        options_copy.Xdata_node=list(set(options_copy.Xdata_node+Xdata_node_list))
    else:
        options_copy.Xdata_node=Xdata_node_list

    #Find the atomic simulations:
    if simulations_list==[]:
        simulations_list=database.list_fields_local(options_copy,database.drs.simulations_desc)
    #Remove fixed variable:
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']

    #Activate queues and semaphores:
    if q_manager != None:
        for data_node in data_node_list:
            if len(set(['validate']).intersection(q_manager.queues_names))>0:
                q_manager.validate_semaphores.add_new_data_node(data_node)
            #Add the data nodes if downloads are required
            if len(set(['download_files','download_opendap']).intersection(q_manager.queues_names))>0:
                q_manager.download.semaphores.add_new_data_node(data_node)
                q_manager.download.queues.add_new_data_node(data_node)

    vars_list=ask_var_list(database,simulations_list_no_fx,options_copy)
    database.put_or_process('validate',validate_utils.validate,vars_list,options_copy,q_manager,sessions)
    return

#Do it by simulation, except if one simulation field should be kept for further operations:
def reduce_var_list(database,options):
    if ('keep_field' in dir(options) and options.keep_field!=None):
        drs_to_eliminate=[field for field in database.drs.official_drs_no_version if
                                             not field in options.keep_field]
    else:
        drs_to_eliminate=database.drs.official_drs_no_version
    return [ [ make_list(item) for item in var_list] for var_list in 
                set([
                    tuple([ 
                        tuple(sorted(set(make_list(var[drs_to_eliminate.index(field)]))))
                        if field in drs_to_eliminate else None
                        for field in database.drs.official_drs_no_version]) for var in 
                        database.list_fields_local(options,drs_to_eliminate) ])]

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
        vars_list=[ [make_list(None) for item in database.drs.official_drs_no_version] ] 
    else:
        vars_list=reduce_var_list(database,options)


    if len(vars_list)==1:
        #Users have requested time types to be kept
        times_list=downloads.time_split(database,options)
    else:
        times_list=[(None,None,None,None),]
    database.put_or_process('download_files',downloads.download_files,vars_list,options,q_manager,sessions,times_list=times_list)
    return

def reduce_soft_links(database,options,q_manager=None,sessions=dict()):
    vars_list=reduce_var_list(database,options)
    database.put_or_process('reduce_soft_links',nc_Database_reduce.reduce_soft_links,vars_list,options,q_manager,sessions)
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
        vars_list=[ [make_list(None) for item in database.drs.official_drs_no_version] ] 
    else:
        vars_list=reduce_var_list(database,options)

    if len(vars_list)==1:
        #Users have requested time types to be kept
        times_list=downloads.time_split(database,options)
    else:
        times_list=[(None,None,None,None),]
    database.put_or_process('download_opendap',downloads.download_opendap,vars_list,options,q_manager,sessions,times_list=times_list)
    return

def gather(database,options,q_manager=None,sessions=dict()):
    reduce(database,options,q_manager=q_manager)
    return

def reduce(database,options,q_manager=None,sessions=dict()):
    if (options.script=='' and 
        ('in_extra_netcdf_files' in dir(options) and 
          len(options.in_extra_netcdf_files)>0) ):
        raise InputErrorr('The identity script \'\' can only be used when no extra netcdf files are specified.')

    vars_list=reduce_var_list(database,options)
    if len(vars_list)==1:
        #Users have requested time types to be kept
        times_list=downloads.time_split(database,options)
    else:
        times_list=[(None,None,None,None),]
    database.put_or_process('reduce',nc_Database_reduce.reduce_variable,vars_list,options,q_manager,sessions,times_list=times_list)
    return

def merge(database,options,q_manager=None,sessions=dict()):
    database.load_header(options)
    with netCDF4.Dataset(options.out_netcdf_file,'w') as output:
        nc_Database.record_header(output,database.header)
        for file_name in [options.in_netcdf_file,]+options.in_extra_netcdf_files:
            nc_Database_utils.record_to_netcdf_file_from_file_name(options,file_name,output,database.drs)
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

    def list_fields_local(self,options,fields_to_list):
        self.load_database(options,find_simple)
        fields_list=self.nc_Database.list_fields(fields_to_list)
        self.close_database()
        return fields_list

    def put_or_process(self,function_name,function_handle,vars_list,options,q_manager,sessions,times_list=[(None,None,None,None),]):

        if (len(vars_list)==0 or len(times_list)==0):
            #There is no variables to find in the input. 
            #Delete input and decrement expected function.
            if q_manager.remove((function_name,options)):
                os.remove(options.in_netcdf_file)

            if len(vars_list)>0:
                if not ('silent' in dir(options) and options.silent):
                    for var in vars_list:
                        print ' '.join([ opt+': '+str(var[opt_id]) for opt_id, opt in enumerate(self.drs.official_drs_no_version)])
                    print 'Were excluded because no date matched times requested'
            return

        if not ((len(vars_list)==1 and len(times_list)==1) or
            'serial' in dir(options) and options.serial):
            #Randomize to minimize strain on consumers:
            random.shuffle(vars_list)
            for var_id,var in enumerate(vars_list):
                for time_id, time in enumerate(times_list):
                    options_copy=make_new_options_from_lists(options,var,time,function_name,self.drs.official_drs_no_version)
                    #Set the priority to time_id:
                    options_copy.priority=time_id

                    if len(times_list)>1:
                        #Find times list again:
                        var_times_list=downloads.time_split(self,options_copy)
                    #Submit only if the times_list is not empty:
                    if len(times_list)==1 or len(var_times_list)>0:
                        new_file_name=q_manager.increment_expected_and_put((function_name,options_copy))
                        if new_file_name!='':
                            shutil.copyfile(options.in_netcdf_file,new_file_name)
            if q_manager.remove((function_name,options)):
                os.remove(options.in_netcdf_file)
            return
        else:
            #Compute single element!
            if ('serial' in dir(options) and options.serial):
                options_copy=copy.copy(options)
            else: 
                options_copy=make_new_options_from_lists(options,vars_list[0],times_list[0],function_name,self.drs.official_drs_no_version)

            #Compute function:
            output_file_name=function_handle(self,options_copy,q_manager=q_manager,sessions=sessions)

            if output_file_name==None:
                #No file was written and the next function should not expect anything:
                #next_function_name=q_manager.queues_names[q_manager.queues_names.index(function_name)+1]
                #getattr(q_manager,next_function_name+'_expected').decrement()
                #if ('in_netcdf_file' in dir(options) and
                #    q_manager.queues_names.index(function_name)>0):
                if q_manager.remove((function_name,options)):
                    os.remove(options.in_netcdf_file)
                return
            else:
                #Reset trial counter:
                options_copy.trial=0

                #Remove temporary input files if not the first function:
                if 'in_netcdf_file' in dir(options):
                    previous_in_netcdf_file=options_copy.in_netcdf_file
                #Set to output_file
                options_copy.in_netcdf_file=output_file_name
                if q_manager.put_to_next((function_name,options_copy)):
                    os.remove(previous_in_netcdf_file)
                return

    def find_data_nodes_and_simulations(self,options):
        #We have to time the response of the data node.
        self.load_database(options,find_simple)
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
        if ('ask' in dir(options) and options.ask):
            self.header=dict()
            self.header['experiment_list']={item.split(':')[0]:item.split(':')[1].replace('-',',') for item in options.ask_experiment}
            self.header['month_list']=[item for item in options.ask_month]
            self.header['search_list']=[item for item in options.search_path if not item in options.Xsearch_path]
            self.header['variable_list']={item.split(':')[0]:item.split(':')[1].split('-') for item in options.ask_var}
            #Need to do this to allwo choices:
            if len(options.ask_file_type)==0:
                self.header['file_type_list']=cdb_query_archive_parsers.file_type_list
            else:
                self.header['file_type_list']=[item for item in options.ask_file_type]
        else:
            self.define_database(options)
            self.header=self.nc_Database.load_header()
            self.close_database()
        return
        
    def load_database(self,options,find_function,time_slices=dict(),semaphores=dict(),session=None,remote_netcdf_kwargs=dict()):
        self.define_database(options)
        if 'header' in dir(self):
            self.nc_Database.header=self.header
        self.nc_Database.populate_database(options,find_function,time_slices=time_slices,semaphores=semaphores,session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
        if 'ensemble' in dir(options) and options.ensemble!=None:
            #Always include r0i0p0 when ensemble was sliced:
            options_copy=copy.copy(options)
            options_copy.ensemble='r0i0p0'
            self.nc_Database.populate_database(options_copy,find_function,semaphores=semaphores,session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
        return

    def define_database(self,options):
        if 'in_netcdf_file' in dir(options):
            self.nc_Database=nc_Database.nc_Database(self.drs,database_file=options.in_netcdf_file)
            return
        else:
            self.nc_Database=nc_Database.nc_Database(self.drs)
            return

    def close_database(self):
        self.nc_Database.close_database()
        del self.nc_Database
        return

def make_list(item):
    if isinstance(item,list):
        return item
    elif (isinstance(item,set) or isinstance(item,tuple)):
        return list(item)
    else:
        if item!=None:
            return [item,]
        else:
            return None

def make_new_options_from_lists(options,var_item,time_item,function_name,official_drs_no_version):
    options_copy=copy.copy(options)
    for opt_id, opt in enumerate(official_drs_no_version):
        if var_item[opt_id]!=None:
            setattr(options_copy,opt,make_list(var_item[opt_id]))
    for opt_id, opt in enumerate(['year','month','day','hour']):
        if time_item[opt_id]!=None and opt in dir(options_copy):
            setattr(options_copy,opt,make_list(time_item[opt_id]))

    if (function_name in ['ask','validate'] and
        'ensemble' in official_drs_no_version and
        'ensemble' in dir(options_copy) and options_copy.ensemble != None
        and not 'r0i0p0' in options_copy.ensemble):
        #Added 'fixed' variables:
        options_copy.ensemble.append('r0i0p0')
    return options_copy


def rank_data_nodes(options,data_node_list,url_list,q_manager):
    data_node_list_timed=[]
    data_node_timing=[]
    for data_node_id, data_node in enumerate(data_node_list):
        url=url_list[data_node_id]
        if not ('silent' in dir(options) and options.silent):
            print 'Querying '+url[0]+' to measure response time of data node... '

        with Timer() as timed_exec:
            is_available=remote_netcdf.remote_netCDF(url[0],url[1],semaphores=q_manager.validate_semaphores).is_available(num_trials=1)

        if not is_available:
            if not ('silent' in dir(options) and options.silent):
                if timed_exec.interval>options.timeout:
                    print('Data node '+data_node+' excluded because it did not respond (timeout).')
                else:
                    print('Data node '+data_node+' excluded because it did not respond.')
        else:
            #Try opening a link on the data node. If it does not work remove this data node.
            number_of_trials=3
            try:
                import_string='import netcdf4_soft_links.remote_netcdf as remote_netcdf;import time;'
                load_string='remote_data=remote_netcdf.remote_netCDF(\''+url[0]+'\',\''+url[1]+'\');remote_data.is_available(num_trials=1);time.sleep(0.1);'
                timing=timeit.timeit(import_string+load_string,number=number_of_trials)
                data_node_timing.append(timing)
                data_node_list_timed.append(data_node)
                if not ('silent' in dir(options) and options.silent):
                    print('Done!')
            except:
                if not ('silent' in dir(options) and options.silent):
                    print('Data node '+data_node+' excluded because it did not respond.')
    return list(np.array(data_node_list_timed)[np.argsort(data_node_timing)]),list(set(data_node_list).difference(data_node_list_timed))

#http://preshing.com/20110924/timing-your-code-using-pythons-with-statement/
class Timer:    
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

def find_simple(pointers,file_expt,time_slices=dict(),semaphores=None,session=None,remote_netcdf_kwargs=dict()):
    pointers.session.add(file_expt)
    pointers.session.commit()
    return
