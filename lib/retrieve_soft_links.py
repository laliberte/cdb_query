
import numpy as np

import netCDF4
import datetime

import copy

import os

import cdb_query_archive_class

import retrieval_utils

import netcdf_utils

import indices_utils

import nc_Database

import random

def define_queues(options,data_node_list):
    #from multiprocessing import Manager
    from multiprocessing import Queue
    #manager=Manager()
    queues={data_node : Queue() for data_node in data_node_list}
    #sem=manager.Semaphore()
    #semaphores={data_node : manager.Semaphore() for data_node in data_node_list}
    #semaphores={data_node : sem for data_node in data_node_list}
    queues['end']= Queue()
    if 'source_dir' in dir(options) and options.source_dir!=None:
        queues[retrieval_utils.get_data_node(options.source_dir,'local_file')]=Queue()
    return queues

def worker(input, output):
    for tuple in iter(input.get, 'STOP'):
        result = tuple[0](tuple[1:-1],tuple[-1])
        output.put(result)
    return

def retrieve_data(options,project_drs):
    from multiprocessing import Process, current_process

    #Recover the database meta data:
    database=cdb_query_archive_class.SimpleTree(options,project_drs)
    database.load_header(options)
    database.load_database(options,cdb_query_archive_class.find_simple)

    #Find data node list:
    data_node_list=database.nc_Database.list_data_nodes()
    paths_list=database.nc_Database.list_paths()
    simulations_list=database.nc_Database.list_fields(database.drs.simulations_desc)
    database.close_database()

    #Check if years should be relative, eg for piControl:
    for experiment in database.header['experiment_list']:
        min_year=int(database.header['experiment_list'][experiment].split(',')[0])
        if min_year<10:
            options.min_year=min_year
            print 'Using min year {0} for experiment {1}'.format(str(min_year),experiment)

    #Create queues:
    queues=define_queues(options,data_node_list)
    #Redefine data nodes:
    data_node_list=queues.keys()
    data_node_list.remove('end')

    #First step: Define the queues:
    if not options.netcdf:
        #First find the unique paths:
        unique_paths_list=list(np.unique([path[0].split('/')[-1] for path in paths_list]))

        #Then attribute paths randomly:
        random.shuffle(paths_list)
        for path in paths_list:
            if path[0].split('/')[-1] in unique_paths_list:
                queues[retrieval_utils.get_data_node(*path[:2])].put((retrieval_utils.retrieve_path,)+path+(options,))
                unique_paths_list.remove(path[0].split('/')[-1])
    else:
        output=netCDF4.Dataset(options.out_diagnostic_netcdf_file,'w')
        data=netCDF4.Dataset(options.in_diagnostic_netcdf_file,'r')
        descend_tree_recursive(options,data,output,queues)
        data.close()
        output.sync()

    #Second step: Process the queues:
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('Retrieving from data nodes:')
    for data_node in data_node_list:
        print data_node,' ',queues[data_node].qsize()

    num_files=0
    processes=dict()
    for data_node in data_node_list:
        num_files+=queues[data_node].qsize()
        processes[data_node]=Process(target=worker, args=(queues[data_node], queues['end']))
        queues[data_node].put('STOP')
        processes[data_node].start()

    open_queues_list=copy.copy(data_node_list)
    #Third step: Close the queues:
    if not options.netcdf:
        for i in range(num_files):
            print '\t', queues['end'].get()
    else:
        for i in range(num_files):
            #print i, queues['num_files']
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


def retrieve_remote_vars(data,output,queues,year=None,month=None,min_year=None,source_dir=None):
    if not 'time' in output.dimensions.keys():
        time_axis=data.variables['time'][:]
        time_restriction=np.ones(time_axis.shape,dtype=np.bool)
        if year!=None or month!=None:
            date_axis=netcdf_utils.get_date_axis(data.variables['time'])
            if year!=None:
                year_axis=np.array([date.year for date in date_axis])
                if min_year!=None:
                    #Important for piControl:
                    time_restriction=np.logical_and(time_restriction,year_axis-year_axis.min()+min_year== year)
                else:
                    time_restriction=np.logical_and(time_restriction,year_axis== year)
            if month!=None:
                month_axis=np.array([date.month for date in date_axis])
                time_restriction=np.logical_and(time_restriction,month_axis== month)
        netcdf_utils.create_time_axis(output,data,time_axis[time_restriction])

    vars_to_retrieve=[var for var in data.variables.keys() if  var in data.groups['soft_links'].variables.keys()]

    #Replicate all the other variables:
    for var in set(data.variables.keys()).difference(vars_to_retrieve):
        if not var in output.variables.keys():
            output=netcdf_utils.replicate_netcdf_var(output,data,var)
            output.variables[var][:]=data.variables[var][:]


    #Get list of paths:
    paths_list=data.groups['soft_links'].variables['path'][:]
    paths_id_list=data.groups['soft_links'].variables['path_id'][:]
    file_type_list=data.groups['soft_links'].variables['file_type'][:]
    if source_dir!=None:
        #Check if the file has already been retrieved:
        paths_list,file_type_list=retrieval_utils.find_local_file(source_dir,data.groups['soft_links'])

    for var_to_retrieve in vars_to_retrieve:
        paths_link=data.groups['soft_links'].variables[var_to_retrieve][time_restriction,0]
        indices_link=data.groups['soft_links'].variables[var_to_retrieve][time_restriction,1]

        #Convert paths_link to id in path dimension:
        paths_link=np.array([list(paths_id_list).index(path_id) for path_id in paths_link])

        #Sort the paths so that we query each only once:
        sorting_paths=np.argsort(paths_link)
        unique_paths_list_id=np.unique(paths_link[sorting_paths])
        sorted_paths_link=paths_link[sorting_paths]
        sorted_indices_link=indices_link[sorting_paths]
        
        #Replicate variable to output:
        output=netcdf_utils.replicate_netcdf_var(output,data,var_to_retrieve,chunksize=-1)

        #Get attributes from variable to retrieve:
        #remote_path=paths_list[unique_paths_list_id[0]].replace('fileServer','dodsC')
        #remote_file_type=file_type_list[list(paths_list).index(paths_list[unique_paths_list_id[0]])]
        #remote_data_node=netcdf_utils.get_data_node(remote_path,remote_file_type)
        #semaphores[remote_data_node].acquire()
        #remote_data=retrieval_utils.open_remote_netCDF(remote_path)
        #remote_data.close()
        #semaphores[remote_data_node].release()

        dimensions=dict()
        unsort_dimensions=dict()
        dims_length=[]
        for dim in output.variables[var_to_retrieve].dimensions:
            if dim != 'time':
                dimensions[dim] = output.variables[dim][:]
                unsort_dimensions[dim] = None
                dims_length.append(len(dimensions[dim]))

        #Maximum number of time step per request:
        max_request=450 #maximum request in Mb
        max_time_steps=np.floor(max_request*1024*1024/(32*np.prod(dims_length)))
        for path_id in unique_paths_list_id:
            path=paths_list[paths_id_list[path_id]]


            file_type=file_type_list[list(paths_list).index(path)]
             
            time_indices=sorted_indices_link[sorted_paths_link==path_id]
            num_time_chunk=int(np.ceil(len(time_indices)/float(max_time_steps)))
            for time_chunk in range(num_time_chunk):
                dimensions['time'], unsort_dimensions['time'] = indices_utils.prepare_indices(time_indices[time_chunk*max_time_steps:(time_chunk+1)*max_time_steps])
                
                #Get the file tree:
                tree=data.path.split('/')[1:]+[var_to_retrieve]
                args = (path,var_to_retrieve,dimensions,unsort_dimensions,np.argsort(sorting_paths)[sorted_paths_link==path_id],tree)
                queues[retrieval_utils.get_data_node(path,file_type)].put((retrieval_utils.retrieve_path_data,)+copy.deepcopy(args))
    return 

    
