import discover
import optimset

import netCDF4

import nc_Database
import netcdf_soft_links

import copy
import os

import json
import timeit

import numpy as np

import multiprocessing

import random

import retrieval_utils

import datetime

import netcdf_utils


class SimpleTree:
    def __init__(self,options,project_drs):

        self.drs=project_drs
        return

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

    def discover(self,options):
        #Load header:
        self.header=json.load(open(options.in_diagnostic_headers_file,'r'))['header']
        #Simplify the header:
        self.union_header()

        if options.list_only_field!=None:
            for field_name in discover.discover(self,options):
                print field_name
            return
        else:
            if options.num_procs==1:
                filepath=discover.discover(self,options)
                try:
                    os.rename(filepath,filepath.replace('.pid'+str(os.getpid()),''))
                except OSError:
                    pass
            else:
                #Find the atomic simulations:
                simulations_list=discover.discover_simulations_recursive(self,options,self.drs.simulations_desc)

                output=distributed_recovery(discover.discover,self,options,simulations_list)

                #Close dataset
                output.close()
        return

    def optimset(self,options):
        self.load_header(options)
        #if options.data_nodes!=None:
        #    self.header['data_node_list']=options.data_nodes

        if not 'data_node_list' in self.header.keys():
            self.header['data_node_list']=self.retrieve_and_rank_data_nodes(options)

        if options.num_procs==1:
            filepath=optimset.optimset(self,options)
            try:
                os.rename(filepath,filepath.replace('.pid'+str(os.getpid()),''))
            except OSError:
                pass
        else:
            #Find the atomic simulations:
            simulations_list=self.list_fields_local(options,self.drs.simulations_desc)

            output=distributed_recovery(optimset.optimset,self,options,simulations_list)
            #Close datasets:
            output.close()
        return

    def remote_retrieve(self,options):
        #Recover the database meta data:
        self.load_header(options)

        paths_list, data_node_list, queues = self.setup_remote_retrieve_and_download(options)
        #Check if years should be relative, eg for piControl:
        options.min_year=None
        for experiment in self.header['experiment_list']:
            min_year=int(self.header['experiment_list'][experiment].split(',')[0])
            if min_year<10:
                options.min_year=min_year
                print 'Using min year {0} for experiment {1}'.format(str(min_year),experiment)

        #Create queues:
        output=netCDF4.Dataset(options.out_diagnostic_netcdf_file,'w')
        self.define_database(options)
        self.nc_Database.retrieve_database(options,output,queues)
        self.close_database()
        launch_download_and_remote_retrieve(output,data_node_list,queues,False)
        return

    def download(self,options):
        #Recover the database meta data:
        self.load_header(options)

        paths_list, data_node_list, queues = self.setup_remote_retrieve_and_download(options)
        #First find the unique paths:
        unique_paths_list=list(np.unique([path[0].split('/')[-1] for path in paths_list]))

        #Then attribute paths randomly:
        random.shuffle(paths_list)
        for path in paths_list:
            if path[0].split('/')[-1] in unique_paths_list:
                if retrieval_utils.check_file_availability(path[0].split('|')[0]): 
                    queues[retrieval_utils.get_data_node(*path[:2])].put((retrieval_utils.retrieve_path,)+path+(options,))
                    unique_paths_list.remove(path[0].split('/')[-1])
        #Redo it without checking availability in order for the error messages to reach the user:
        for path in paths_list:
            if path[0].split('/')[-1] in unique_paths_list:
                queues[retrieval_utils.get_data_node(*path[:2])].put((retrieval_utils.retrieve_path,)+path+(options,))
                unique_paths_list.remove(path[0].split('/')[-1])
        launch_download_and_remote_retrieve([],data_node_list,queues,True)
        return

    def setup_remote_retrieve_and_download(self,options):
        self.load_database(options,find_simple)

        #Find data node list:
        data_node_list=self.nc_Database.list_data_nodes()
        paths_list=self.nc_Database.list_paths()
        self.close_database()

        queues=define_queues(options,data_node_list)
        #Redefine data nodes:
        data_node_list=queues.keys()
        data_node_list.remove('end')
        return paths_list, data_node_list, queues

    def retrieve_and_rank_data_nodes(self,options):
        #We have to time the response of the data node.
        self.load_database(options,find_simple)
        data_node_list=self.nc_Database.list_data_nodes()
        data_node_timing=[]
        data_node_list_timed=[]
        for data_node in data_node_list:
            url=self.nc_Database.list_paths_by_data_node(data_node)[0].split('|')[0].replace('fileServer','dodsC')
            #Try opening a link on the data node. If it does not work do not use this data_node
            number_of_trials=10
            try:
                timing=timeit.timeit('import cdb_query.retrieval_utils; cdb_query.retrieval_utils.test_remote_netCDF(\''+url+'\');',number=number_of_trials)
                data_node_timing.append(timing)
                data_node_list_timed.append(data_node)
            except:
                pass
        self.close_database()
        #print data_node_list
        #print data_node_list_timed
        #print data_node_timing
        return list(np.array(data_node_list_timed)[np.argsort(data_node_timing)])+list(set(data_node_list).difference(data_node_list_timed))

    def list_fields(self,options):
        #slice with options:
        fields_list=self.list_fields_local(options,options.field)
        for field in fields_list:
            print ','.join(field)
        return

    def list_fields_local(self,options,fields_to_list):
        self.load_database(options,find_simple)
        fields_list=self.nc_Database.list_fields(fields_to_list)
        self.close_database()
        return fields_list

    def define_database(self,options):
        if 'in_diagnostic_netcdf_file' in dir(options):
            self.nc_Database=nc_Database.nc_Database(self.drs,database_file=options.in_diagnostic_netcdf_file)
        else:
            self.nc_Database=nc_Database.nc_Database(self.drs)
        return

    def load_header(self,options):
        self.define_database(options)
        self.header=self.nc_Database.load_header()
        self.close_database()
        return

    def load_database(self,options,find_function):
        self.define_database(options)
        self.nc_Database.populate_database(options,find_function)
        if 'ensemble' in dir(options) and options.ensemble!=None:
            #Always include r0i0p0 when ensemble was sliced:
            options_copy=copy.copy(options)
            options_copy.ensemble='r0i0p0'
            self.nc_Database.populate_database(options_copy,find_function)
        return

    def close_database(self):
        self.nc_Database.close_database()
        del self.nc_Database
        return


def distributed_recovery(function_handle,database,options,simulations_list,args=tuple()):

    #Open output file:
    output_file_name=options.out_diagnostic_netcdf_file
    output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')

    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']

    manager=multiprocessing.Manager()
    queue=manager.Queue()
    #Set up the discovery simulation per simulation:
    args_list=[]
    for simulation_id,simulation in enumerate(simulations_list_no_fx):
        options_copy=copy.copy(options)
        for desc_id, desc in enumerate(database.drs.simulations_desc):
            setattr(options_copy,desc,simulation[desc_id])
        args_list.append((function_handle,copy.copy(database),options_copy)+args+(queue,))
    
    #Span up to options.num_procs processes and each child process analyzes only one simulation
    pool=multiprocessing.Pool(processes=options.num_procs,maxtasksperchild=1)
    result=pool.map_async(worker_query,args_list,chunksize=1)
    for arg in args_list:
        filename=queue.get()
        netcdf_soft_links.record_to_file(output_root,netCDF4.Dataset(filename,'r'))
        output_root.sync()
    pool.close()
    pool.join()

    return output_root

def worker_query(tuple):
    return tuple[-1].put(tuple[0](*tuple[1:-1]))

def worker_retrieve(input, output):
    for tuple in iter(input.get, 'STOP'):
        result = tuple[0](tuple[1:-1],tuple[-1])
        output.put(result)
    return

def launch_download_and_remote_retrieve(output,data_node_list,queues,download_flag):
    #Second step: Process the queues:
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('Retrieving from data nodes:')
    for data_node in data_node_list:
        print data_node,' ',queues[data_node].qsize()

    num_files=0
    processes=dict()
    for data_node in data_node_list:
        num_files+=queues[data_node].qsize()
        processes[data_node]=multiprocessing.Process(target=worker_retrieve, args=(queues[data_node], queues['end']))
        queues[data_node].put('STOP')
        processes[data_node].start()

    open_queues_list=copy.copy(data_node_list)
    #Third step: Close the queues:
    if download_flag:
        for i in range(num_files):
            print '\t', queues['end'].get()
    else:
        for i in range(num_files):
            netcdf_utils.assign_tree(output,*queues['end'].get())
            output.sync()
            for data_node in data_node_list:
                if queues[data_node].qsize()==0 and data_node in open_queues_list:
                    print 'Finished retrieving from data node '+data_node
                    open_queues_list.remove(data_node)
        output.close()
    print('Done!')
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return
        
def find_simple(pointers,file_expt):
    #for item in dir(file_expt):
    #    if item[0]!='_':
    #        print getattr(file_expt,item)
    pointers.session.add(file_expt)
    pointers.session.commit()
    return

def define_queues(options,data_node_list):
    #from multiprocessing import Manager
    #manager=Manager()
    queues={data_node : multiprocessing.Queue() for data_node in data_node_list}
    #sem=manager.Semaphore()
    #semaphores={data_node : manager.Semaphore() for data_node in data_node_list}
    #semaphores={data_node : sem for data_node in data_node_list}
    queues['end']= multiprocessing.Queue()
    if 'source_dir' in dir(options) and options.source_dir!=None:
        queues[retrieval_utils.get_data_node(options.source_dir,'local_file')]=multiprocessing.Queue()
    return queues
