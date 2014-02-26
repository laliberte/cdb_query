
import numpy as np
import math

import netCDF4
import datetime

import hashlib

import copy

import os

import cdb_query_archive_class
import io_tools

import json

import retrieval_utils

import netcdf_utils

import indices_utils

import nc_Database

import random

def recover_time(file):
    file_name=str(file['path']).replace('fileServer','dodsC').split('|')[0]

    try:
        data=netCDF4.Dataset(file_name)
            
        if 'calendar' in dir(data.variables['time']):
            calendar=data.variables['time'].calendar
        else:
            calendar='standard'
        time_axis=(netCDF4.num2date(data.variables['time'][:],
                                     units=data.variables['time'].units,
                                     calendar=calendar)
                        )
    except:
        time_axis=np.empty((0,))

    try:
        data.close()
    except:
        pass

    table_desc=[
               ('paths','a255'),
               ('indices','uint32')
               ]
    table=np.empty(time_axis.shape, dtype=table_desc)
    if len(time_axis)>0:
        table['paths']=np.array([str(file_name) for item in time_axis])
        table['indices']=range(0,len(time_axis))
    return time_axis,table

def create_netcdf_pointers_file(header,output,source_files,paths_ordering,id_list):
    if not 'soft_links' in output.groups.keys():
        output_grp=output.createGroup('soft_links')
    else:
        output_grp=output.groups['soft_links']

    #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
    output_grp.createDimension('path',len(paths_ordering))
    for id in ['version','path_id']:
        output_grp.createVariable(id,np.uint32,('path',))[:]=paths_ordering[id]
    for id in id_list:
        temp=output_grp.createVariable(id,str,('path',))
        for file_id, file in enumerate(paths_ordering['path']):
            temp[file_id]=str(paths_ordering[id][file_id])
    return 

def record_meta_data(header,output,paths_list,var,time_frequency,experiment):
    if not time_frequency in ['fx','clim']:
        #Retrieve time and meta:
        retrieve_time_and_meta_data(header,output,paths_list,var,experiment)
    else:
        #Retrieve fixed and clim variables:
        retrieve_fx(output,paths_list,var)
    return

def record_paths(header,output,paths_list,var,time_frequency,experiment):
    #First order source files by preference:
    sorts_list=['version','file_type_id','data_node_id','path_id']
    id_list=['data_node','file_type','path','checksum','search']
    paths_ordering = order_paths_by_preference(paths_list,header,sorts_list,id_list)
    create_netcdf_pointers_file(header,output,paths_list,paths_ordering,id_list)
    return

def retrieve_time_and_meta_data(header,output,source_files,var,experiment):
    sorts_list=['version','file_type_id','data_node_id','path_id']
    id_list=['data_node','file_type','path','checksum','search']
    paths_ordering = order_paths_by_preference(source_files,header,sorts_list,id_list)

    #Recover time axis for all files:
    time_axis, table=map(np.concatenate,
                             zip(*map(recover_time,np.nditer(paths_ordering)))
                             )

    #Open the first file and use its metadata to populate container file:
    data=netCDF4.Dataset(table['paths'][0])
    netcdf_utils.replicate_netcdf_file(output,data)

    #Convert time axis to numbers and find the unique time axis:
    time_axis = netCDF4.date2num(time_axis,units=data.variables['time'].units,calendar=data.variables['time'].calendar)
    time_axis_unique = np.unique(time_axis)

    time_axis_unique_date=netCDF4.num2date(time_axis_unique,units=data.variables['time'].units,calendar=data.variables['time'].calendar)

    #Include a filter on years: 
    time_desc={}
    years_range=range(*[ int(year) for year in header['experiment_list'][experiment].split(',')])
    years_range+=[years_range[-1]+1]
    if int(header['experiment_list'][experiment].split(',')[0])<10:
        #This is important for piControl
        years_range=list(np.array(years_range)+np.min([date.year for date in time_axis_unique_date]))
        #min_year=np.min([date.year for date in time_axis_unique_date])
    if 'months_list' in header.keys():
        months_range=header['months_list']
    else:
        months_range=range(1,13)

    valid_times=np.array([True  if (date.year in years_range and 
                                 date.month in months_range) else False for date in  time_axis_unique_date])
    time_axis_unique=time_axis_unique[valid_times]

    #Create time axis in ouptut:
    netcdf_utils.create_time_axis(output,data,time_axis_unique)

    #CREATE LOOK-UP TABLE:
    paths_indices=np.empty(time_axis.shape,dtype=np.uint32)
    time_indices=np.empty(time_axis.shape,dtype=np.uint32)

    paths_list=[path for path in paths_ordering['path'] ]
    for path_id, path in enumerate(paths_list):
        paths_indices[path.replace('fileServer','dodsC')==table['paths']]=path_id

    #Remove paths that are not necessary over the requested time range:
    paths_id_list=list(np.unique([np.min(paths_indices[time==time_axis])  for time_id, time in enumerate(time_axis_unique)]))
    useful_file_name_list=[path.split('/')[-1] for path in paths_ordering['path'][paths_id_list] ]
    for file_id, file in enumerate(paths_ordering):
        if not file_id in paths_id_list:
            file_name=paths_ordering['path'][file_id].split('/')[-1]
            if file_name in useful_file_name_list:
                equivalent_file_id=paths_id_list[useful_file_name_list.index(file_name)]
                if paths_ordering['checksum'][file_id]==paths_ordering['checksum'][equivalent_file_id]:
                    paths_id_list.append(file_id)
    paths_ordering=paths_ordering[np.sort(paths_id_list)]
    #Finally, set the path_id field to be following the indices in paths_ordering:
    paths_ordering['path_id']=range(len(paths_ordering))

    #Recompute the indices to paths:
    paths_list=[path for path in paths_ordering['path'] ]
    for path_id, path in enumerate(paths_list):
        paths_indices[path.replace('fileServer','dodsC')==table['paths']]=path_id

    #USE VERSION 0.9 OF SOFT LINKS BECAUSE NCO AND OTHER TOOLS ARE NOT READY FOR COMPOUND TYPES.
    create_variable_soft_links(data,output,var,time_axis,time_axis_unique,paths_indices,table)
    #create_variable_soft_links(data,output,var,time_axis,time_axis_unique,paths_indices,paths_list,table)
    #create_variable_soft_links2(data,output,var,time_axis,time_axis_unique,paths_indices,paths_list,table)
    #output.variables[var].setncattr('cdb_query_dimensions',','.join(data.variables[var].dimensions))
    data.close()

    create_netcdf_pointers_file(header,output,source_files,paths_ordering,id_list)

    #remove data_node_list header
    #del header['data_node_list']
    return

#def create_variable_soft_links(data,output,var,time_axis,time_axis_unique,paths_indices,paths_list,table):
#    #CREATE LOOK-UP TABLE:
#    output.createDimension('soft_link',len(paths_list))
#    soft_link_path=output.createVariable('soft_link_path',np.str,('soft_link',))
#    for path_id,path in enumerate(paths_list):
#        soft_link_path[path_id]=np.str(path)
#    soft_link_time=output.createVariable('soft_link_time',np.uint32,('time',))
#
#    #replicate_netcdf_var(output,data,var,datatype=np.uint32,fill_value=np.iinfo(np.uint32).max,chunksize=-1)
#    #var_out=output.variables[var]
#    replicate_netcdf_var_dimensions(output,data,var)
#    dtype=np.byte
#    var_out = output.createVariable(var,dtype,data.variables[var].dimensions+('soft_link',),zlib=True,fill_value=np.iinfo(dtype).max)
#    replicate_netcdf_var_att(output,data,var)
#    for var in data.variables.keys():
#        if not 'time' in data.variables[var].dimensions:
#            output=replicate_netcdf_var(output,data,var)
#            output.variables[var][:]=data.variables[var][:]
#    print output
#
#    #Create soft links:
#    for time_id, time in enumerate(time_axis_unique):
#        path_index=np.min(paths_indices[time==time_axis])
#        var_out[time_id,...,path_index]=1
#        soft_link_time[time_id]=table['indices'][np.logical_and(paths_indices==path_index,time==time_axis)][0]
#    return

def create_variable_soft_links(data,output,var,time_axis,time_axis_unique,paths_indices,table):
    netcdf_utils.replicate_netcdf_var(output,data,var)
    for other_var in data.variables.keys():
        if not 'time' in data.variables[other_var].dimensions:
            output=netcdf_utils.replicate_netcdf_var(output,data,other_var)
            output.variables[other_var][:]=data.variables[other_var][:]

    if not 'soft_links' in output.groups.keys():
        output_grp=output.createGroup('soft_links')
    else:
        output_grp=output.groups['soft_links']
    #CREATE LOOK-UP TABLE:
    output_grp.createDimension('indices',2)
    indices=output_grp.createVariable('indices',np.str,('indices',))
    indices[0]='path'
    indices[1]='time'

    var_out = output_grp.createVariable(var,np.uint32,('time','indices'),zlib=False,fill_value=np.iinfo(np.uint32).max)
    #netcdf_utils.replicate_netcdf_var_att(output_grp,data,var)
    #netcdf_utils.replicate_netcdf_var_dimensions(output_grp,data,var)

    #Create soft links:
    for time_id, time in enumerate(time_axis_unique):
        var_out[time_id,0]=np.min(paths_indices[time==time_axis])
        var_out[time_id,1]=table['indices'][np.logical_and(paths_indices==var_out[time_id,0],time==time_axis)][0]
    return

#def create_variable_soft_links2(data,output,var,time_axis,time_axis_unique,paths_indices,paths_list,table):
#    #CREATE LOOK-UP TABLE:
#    common_substring=long_substr(paths_list)
#    soft_link_numpy=np.dtype([(path.replace('/','&#47;'),np.byte) for path in paths_list]+[('time',np.uint16)])
#    soft_link=output.createCompoundType(soft_link_numpy,'soft_link')
#    netcdf_utils.replicate_netcdf_var(output,data,var,chunksize=-1,datatype=soft_link)
#    for var in data.variables.keys():
#        if not 'time' in data.variables[var].dimensions:
#            output=netcdf_utils.replicate_netcdf_var(output,data,var)
#            output.variables[var][:]=data.variables[var][:]
#    shape = (1,)+output.variables[var].shape[1:]
#
#    #Create soft links:
#    var_out=output.variables[var]
#    for time_id, time in enumerate(time_axis_unique):
#        var_temp=np.zeros(shape,dtype=soft_link_numpy)
#
#        path_id=np.min(paths_indices[time==time_axis])
#        var_temp[paths_list[path_id].replace('/','&#47;')]=1
#        var_temp['time']=table['indices'][np.logical_and(paths_indices==path_id,time==time_axis)][0]
#        var_out[time_id,...]=var_temp
#    return
#
#def long_substr(data):
#    #from http://stackoverflow.com/questions/2892931/longest-common-substring-from-more-than-two-strings-python
#    substr = ''
#    if len(data) > 1 and len(data[0]) > 0:
#        for i in range(len(data[0])):
#            for j in range(len(data[0])-i+1):
#                if j > len(substr) and all(data[0][i:i+j] in x for x in data):
#                    substr = data[0][i:i+j]
#    return substr

def retrieve_fx(output,paths_list,var):
    most_recent_version='v'+str(np.max([int(item['version'][1:]) for item in paths_list]))
    path=paths_list[[item['version'] for item in paths_list].index(most_recent_version)]
    for att in path.keys():
        if att!='path':      
            output.setncattr(att,path[att])
    output.setncattr('path',path['path'].split('|')[0])
    output.setncattr('checksum',path['path'].split('|')[1])
    remote_data=retrieval_utils.open_remote_netCDF(path['path'].split('|')[0].replace('fileServer','dodsC'))
    for var_name in remote_data.variables.keys():
        netcdf_utils.replicate_netcdf_var(output,remote_data,var_name)
        output.variables[var_name][:]=remote_data.variables[var_name][:]
    return

def order_paths_by_preference(source_files,header,sorts_list,id_list):
    #FIND ORDERING:
    paths_desc=[]
    for id in sorts_list:
        paths_desc.append((id,np.uint32))
    for id in id_list:
        paths_desc.append((id,'a255'))
    paths_ordering=np.empty((len(source_files),), dtype=paths_desc)
    for file_id, file in enumerate(source_files):
        paths_ordering['path'][file_id]=file['path'].split('|')[0]
        paths_ordering['path_id'][file_id]=file_id
        paths_ordering['checksum'][file_id]=file['path'].split('|')[1]
        paths_ordering['search'][file_id]=file['search']
        paths_ordering['version'][file_id]=np.uint32(file['version'][1:])
        paths_ordering['file_type'][file_id]=file['file_type']
        paths_ordering['data_node'][file_id]=get_data_node(file['path'],paths_ordering['file_type'][file_id])

    #Sort paths from most desired to least desired:
    #First order desiredness for least to most:
    data_node_order=copy.copy(header['data_node_list'])[::-1]#list(np.unique(paths_ordering['data_node']))
    file_type_order=copy.copy(header['file_type_list'])[::-1]#list(np.unique(paths_ordering['file_type']))
    for file_id, file in enumerate(source_files):
        paths_ordering['data_node_id'][file_id]=data_node_order.index(paths_ordering['data_node'][file_id])
        paths_ordering['file_type_id'][file_id]=file_type_order.index(paths_ordering['file_type'][file_id])
    #'version' is implicitly from least to most

    #sort and reverse order to get from most to least:
    return np.sort(paths_ordering,order=sorts_list)[::-1]

def get_data_node(path,file_type):
    if file_type=='HTTPServer':
        return '/'.join(path.split('/')[:3])
    elif file_type=='local_file':
        return '/'.join(path.split('/')[:2])
    else:
        return ''

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
        queues[get_data_node(options.source_dir,'local_file')]=Queue()
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
                queues[get_data_node(*path[:2])].put((retrieval_utils.retrieve_path,)+path+(options,))
                unique_paths_list.remove(path[0].split('/')[-1])
    else:
        #if options.num_procs==1:
        output=netCDF4.Dataset(options.out_diagnostic_netcdf_file,'w')
        data=netCDF4.Dataset(options.in_diagnostic_netcdf_file,'r')
        descend_tree_recursive(options,data,output,queues)
        data.close()
        #else:
        #    #Distributed analysis:
        #    output=cdb_query_archive_class.distributed_recovery(descend_tree,database,options,simulations_list,args=(queues,))
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
            assign_tree(output,*queues['end'].get())
            output.sync()
            for data_node in data_node_list:
                if queues[data_node].qsize()==0 and data_node in open_queues_list:
                    print 'Finished retrieving from data node '+data_node
                    open_queues_list.remove(data_node)
        output.close()
    print('Done!')
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return

def assign_tree(output,val,sort_table,tree):
    if len(tree)>1:
        assign_tree(output.groups[tree[0]],val,sort_table,tree[1:])
    else:
        output.variables[tree[0]][sort_table]=val
    return

def descend_tree(database,options,queues):
    data=netCDF4.Dataset(options.in_diagnostic_netcdf_file,'r')
    output_file_name=options.out_diagnostic_netcdf_file
    output=netCDF4.Dataset(output_file_name+'.pid'+str(os.getpid()),'w',format='NETCDF4',diskless=True,persist=True)
    descend_tree_recursive(options,data,output,queues)
    if 'ensemble' in dir(options) and options.ensemble!=None:
        #Always include r0i0p0 when ensemble was sliced:
        options_copy=copy.copy(options)
        options_copy.ensemble='r0i0p0'
        descend_tree_recursive(options_copy,data,output,queues)
    filepath=output.filepath()
    output.close()
    data.close()
    return filepath

def descend_tree_recursive(options,data,output,queues):
    if 'soft_links' in data.groups.keys():
        retrieve_remote_vars(options,data,output,queues)
    elif len(data.groups.keys())>0:
        for group in data.groups.keys():
            level_name=data.groups[group].getncattr('level_name')
            if ((not level_name in dir(options)) or 
                (getattr(options,level_name)==None) or 
                (getattr(options,level_name)==group)): 
                if not group in output.groups.keys():
                    output_grp=output.createGroup(group)
                else:
                    output_grp=output.groups[group]
                for att in data.groups[group].ncattrs():
                    if not att in output_grp.ncattrs():
                        output_grp.setncattr(att,data.groups[group].getncattr(att))
                descend_tree_recursive(options,data.groups[group],output_grp,queues)
    else:
        #Fixed variables. Do not retrieve, just copy:
        for var in data.variables.keys():
            output_fx=netcdf_utils.replicate_netcdf_var(output,data,var)
            output_fx.variables[var][:]=data.variables[var][:]
        output_fx.sync()
    return 
            
def retrieve_remote_vars(options,data,output,queues):
    if not 'time' in output.dimensions.keys():
        time_axis=data.variables['time'][:]
        time_restriction=np.ones(time_axis.shape,dtype=np.bool)
        if options.year!=None or options.month!=None:
            date_axis=netcdf_utils.get_date_axis(data.variables['time'])
            if options.year!=None:
                year_axis=np.array([date.year for date in date_axis])
                if 'min_year' in dir(options):
                    #Important for piControl:
                    time_restriction=np.logical_and(time_restriction,year_axis-year_axis.min()+options.min_year== options.year)
                else:
                    time_restriction=np.logical_and(time_restriction,year_axis== options.year)
            if options.month!=None:
                month_axis=np.array([date.month for date in date_axis])
                time_restriction=np.logical_and(time_restriction,month_axis== options.month)
        netcdf_utils.create_time_axis(output,data,time_axis[time_restriction])

    vars_to_retrieve=[var for var in data.variables.keys() if  var in data.groups['soft_links'].variables.keys()]

    #Replicate all the other variables:
    for var in set(data.variables.keys()).difference(vars_to_retrieve):
        if not var in output.variables.keys():
            output=netcdf_utils.replicate_netcdf_var(output,data,var)
            output.variables[var][:]=data.variables[var][:]

    #Get the file tree:
    tree=data.path.split('/')[1:]

    #Get list of paths:
    paths_list=data.groups['soft_links'].variables['path'][:]
    paths_id_list=data.groups['soft_links'].variables['path_id'][:]
    file_type_list=data.groups['soft_links'].variables['file_type'][:]
    if options.source_dir!=None:
        #Check if the file has already been retrieved:
        paths_list,file_type_list=retrieval_utils.find_local_file(options,data.groups['soft_links'])

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
                
                queues[get_data_node(path,file_type)].put((retrieval_utils.retrieve_path_data,
                                                                         path,
                                                                         var_to_retrieve,
                                                                         dimensions,
                                                                         unsort_dimensions,
                                                                         np.argsort(sorting_paths)[sorted_paths_link==path_id],
                                                                         copy.copy(tree)+[var_to_retrieve]))
                #print path, sorted_soft_link[sorted_soft_link[:,0]==path_id,1], copy.copy(tree)+[var_to_retrieve]
    return 

    
def record_to_file(output_root,output):
    netcdf_utils.replicate_netcdf_file(output_root,output)
    netcdf_utils.replicate_full_netcdf_recursive(output_root,output)
    filepath=output.filepath()
    output.close()
    try:
        os.remove(filepath)
    except OSError:
        pass
    return
