
import numpy as np
import math

import netCDF4
import datetime

import hashlib

import copy

import os

import cdb_query_archive_class
import io_tools

import retrieval_utils
from itertools import groupby, count

def md5_for_file(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()

def get_year_axis(path_name):
    try:
        #print 'Loading file... ',
        #print path_name
        data=netCDF4.Dataset(path_name)
        dimensions_list=data.dimensions.keys()
        if 'time' not in dimensions_list:
            raise Error('time is missing from variable')
        date_axis = get_date_axis(data.variables['time'])
        #print ' Done!'
        data.close()
        year_axis=np.array([date.year for date in date_axis])
        month_axis=np.array([date.month for date in date_axis])
    except:
        return None, None

    return year_axis, month_axis

def get_date_axis(time_var):
    if time_var.units=='day as %Y%m%d.%f':
        date_axis=get_date_axis_absolute(time_var)
    else:
        date_axis=get_date_axis_relative(time_var)
    return date_axis

def get_date_axis_relative(time_var):
    if 'calendar' in dir(time_var):
        date_axis = netCDF4.num2date(time_var[:],units=time_var.units,calendar=time_var.calendar)
    else:
        date_axis = netCDF4.num2date(time_var[:],units=time_var.units)
    return date_axis

def get_date_axis_absolute(time_var):
    return map(convert_to_date_absolute,time_var[:])

def convert_to_date_absolute(absolute_time):
    year=int(math.floor(absolute_time/1e4))
    remainder=absolute_time-year*1e4
    month=int(math.floor(remainder/1e2))
    remainder-=month*1e2
    day=int(math.floor(remainder))
    remainder-=day
    remainder*=24.0
    hour=int(math.floor(remainder))
    remainder-=hour
    remainder*=60.0
    minute=int(math.floor(remainder))
    remainder-=minute
    remainder*=60.0
    seconds=int(math.floor(remainder))
    return datetime.datetime(year,month,day,hour,minute,seconds)
    
def replicate_netcdf_file(output,data):
    for att in data.ncattrs():
        att_val=getattr(data,att)
        if 'encode' in dir(att_val):
            att_val=att_val.encode('ascii','replace')
        setattr(output,att,att_val)
    return output

def replicate_netcdf_var_dimensions(output,data,var):
    for dims in data.variables[var].dimensions:
        if dims not in output.dimensions.keys():
            output.createDimension(dims,len(data.dimensions[dims]))
            if dims in data.variables.keys():
                dim_var = output.createVariable(dims,data.variables[dims].dtype,(dims,))
                dim_var[:] = data.variables[dims][:]
                output = replicate_netcdf_var(output,data,dims)
                if 'bounds' in output.variables[dims].ncattrs():
                    output=replicate_netcdf_var(output,data,output.variables[dims].getncattr('bounds'))
    return output

def replicate_netcdf_var(output,data,var,datatype=None,fill_value=None,add_dim=None,chunksize=None):
    if not datatype: datatype=data.variables[var].dtype
    output=replicate_netcdf_var_dimensions(output,data,var)

    if var not in output.variables.keys():
        dimensions=data.variables[var].dimensions
        if add_dim:
            dimensions+=(add_dim,)
        if chunksize:
            if chunksize==-1:
                chunksizes=tuple([1 if dim=='time' else len(output.dimensions[dim]) for dim in dimensions])
            else:
                #chunksizes=tuple([1 if output.dimensions[dim].isunlimited() else 10 for dim in dimensions])
                chunksizes=tuple([1 if dim=='time' else chunksize for dim in dimensions])
            output.createVariable(var,datatype,dimensions,zlib=True,fill_value=fill_value,chunksizes=chunksizes)
        else:
            output.createVariable(var,datatype,dimensions,zlib=True,fill_value=fill_value)
    output = replicate_netcdf_var_att(output,data,var)
    output.sync()
    return output

def replicate_netcdf_var_att(output,data,var):
    for att in data.variables[var].ncattrs():
        att_val=getattr(data.variables[var],att)
        if att[0]!='_':
            if 'encode' in dir(att_val):
                att_val=att_val.encode('ascii','replace')
            setattr(output.variables[var],att,att_val)
    return output

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
        data.close()
    except:
        time_axis=np.empty((0,))

    table_desc=[
               ('paths','a255'),
               ('indices','uint32')
               ]
    table=np.empty(time_axis.shape, dtype=table_desc)
    if len(time_axis)>0:
        table['paths']=np.array([str(file_name) for item in time_axis])
        table['indices']=range(0,len(time_axis))
    return time_axis,table

def create_tree(output_top,tree):
    level_name=tree[0][1]
    if not level_name in output_top.groups.keys(): 
        output=output_top.createGroup(level_name)
        output.level_name=tree[0][0]
    else:
        output=output_top.groups[level_name]
    if len(tree)>1:
        output=create_tree(output,tree[1:])
    return output

def create_netcdf_pointers_file(header,output,source_files,paths_ordering,id_list):

    #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
    output.createDimension('path',len(paths_ordering))
    output.createVariable('version',np.uint32,('path',))[:]=paths_ordering['version']
    for id in id_list:
        temp=output.createVariable(id,str,('path',))
        for file_id, file in enumerate(paths_ordering['path']):
            temp[file_id]=str(paths_ordering[id][file_id])
    output.sync()
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
    sorts_list=['version','file_type_id','data_node_id']
    id_list=['data_node','file_type','path','checksum','search']
    paths_ordering = order_paths_by_preference(paths_list,header,sorts_list,id_list)
    create_netcdf_pointers_file(header,output,paths_list,paths_ordering,id_list)
    return

def retrieve_time_and_meta_data(header,output,source_files,var,experiment):
    sorts_list=['version','file_type_id','data_node_id']
    id_list=['data_node','file_type','path','checksum','search']
    paths_ordering = order_paths_by_preference(source_files,header,sorts_list,id_list)

    #Recover time axis for all files:
    time_axis, table=map(np.concatenate,
                             zip(*map(recover_time,np.nditer(paths_ordering)))
                             )

    #Open the first file and use its metadata to populate container file:
    data=netCDF4.Dataset(table['paths'][0])
    replicate_netcdf_file(output,data)

    #Convert time axis to numbers and find the unique time axis:
    time_axis = netCDF4.date2num(time_axis,units=data.variables['time'].units,calendar=data.variables['time'].calendar)
    time_axis_unique = np.unique(time_axis)

    time_axis_unique_date=netCDF4.num2date(time_axis_unique,units=data.variables['time'].units,calendar=data.variables['time'].calendar)

    #Include a filter on years: 
    years_range=range(*[ int(year) for year in header['experiment_list'][experiment].split(',')])
    years_range+=[years_range[-1]+1]
    if int(header['experiment_list'][experiment].split(',')[0])<10:
        #This is important for piControl
        years_range=list(np.array(years_range)+np.min([date.year for date in time_axis_unique_date]))
    if 'months_list' in header.keys():
        months_range=header['months_list']
    else:
        months_range=range(1,13)

    valid_times=np.array([True  if (date.year in years_range and 
                                 date.month in months_range) else False for date in  time_axis_unique_date])
    time_axis_unique=time_axis_unique[valid_times]

    #Create time axis in ouptut:
    create_time_axis(output,data,time_axis_unique)

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

    #Recompute the indices to paths:
    paths_list=[path for path in paths_ordering['path'] ]
    for path_id, path in enumerate(paths_list):
        paths_indices[path.replace('fileServer','dodsC')==table['paths']]=path_id

    #USE VERSION 4 OF SOFT LINKS BECAUSE NCO AND OTHER TOOLS ARE NOT READY FOR COMPOUND TYPES.
    create_variable_soft_links(data,output,var,time_axis,time_axis_unique,paths_indices,table)
    output.variables[var].setncattr('cdb_query_dimensions',','.join(data.variables[var].dimensions))
    data.close()

    create_netcdf_pointers_file(header,output,source_files,paths_ordering,id_list)

    output.sync()
    return

def create_variable_soft_links(data,output,var,time_axis,time_axis_unique,paths_indices,table):
    #CREATE LOOK-UP TABLE:
    output.createDimension('indices',2)
    indices=output.createVariable('indices',np.str,('indices',))
    indices[0]='path'
    indices[1]='time'

    var_out = output.createVariable(var,np.uint32,('time','indices'),zlib=False,fill_value=np.iinfo(np.uint32).max)
    replicate_netcdf_var_att(output,data,var)
    replicate_netcdf_var_dimensions(output,data,var)
    for var in data.variables.keys():
        if not 'time' in data.variables[var].dimensions:
            output=replicate_netcdf_var(output,data,var)
            output.variables[var][:]=data.variables[var][:]

    #Create soft links:
    for time_id, time in enumerate(time_axis_unique):
        var_out[time_id,0]=np.min(paths_indices[time==time_axis])
        var_out[time_id,1]=table['indices'][np.logical_and(paths_indices==var_out[time_id,0],time==time_axis)][0]
        if time_id % 100 == 0:
            output.sync()
    output.sync()
    return

#def create_variable_soft_links3(data,soft_link_desc,output_root,output,var,time_axis,time_axis_unique,time_indices,paths_indices,paths_ordering,id_list,table):
#    #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
#    output.createDimension('path',len(paths_ordering))
#    output.createVariable('version',np.uint32,('path',))[:]=paths_ordering['version']
#    for id in id_list:
#        temp=output.createVariable(id,str,('path',))
#        for file_id, file in enumerate(paths_ordering['path']):
#            temp[file_id]=str(paths_ordering[id][file_id])
#
#    #CREATE LOOK-UP TABLE:
#    soft_link=output.createCompoundType(soft_link_desc['numpy'],'soft_link')
#    var_out = output.createVariable(var,soft_link,('time',),zlib=False)
#    replicate_netcdf_var_att(output,data,var)
#    data.close()
#    
#    #Full description:
#    #replicate_netcdf_var(output,data,var,datatype=soft_link,chunksize=-1)
#    #var_out=output.variables[var]
#    #data.close()
#
#    #Create soft links:
#    paths_list=[str(path).replace('Server','dodsC') for path in np.nditer(paths_ordering['path'])]
#    paths_list=list(paths_ordering['path'])
#    for time_id, time in enumerate(time_axis_unique):
#        var_out[time_id,...]=(paths_list.index(str(table['paths'][time==time_axis][0]).replace('dodsC','fileServer')),
#                          table['indices'][time==time_axis][0])
#        if time_id % 100 == 0:
#            output_root.sync()
#    output_root.sync()
#        #var_out[time_id]=(paths_list.index(str(table['paths'][time==time_axis][0]).replace('dodsC','fileServer')),
#        #                  table['indices'][time==time_axis][0])
#    return
#
#
#def create_variable_soft_links2(data,output,var,time_indices,paths_indices,paths_ordering,id_list,table):
#    #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
#    output.createDimension('path',len(paths_ordering))
#    output.createVariable('version',np.uint32,('path',))[:]=paths_ordering['version']
#    for id in id_list:
#        temp=output.createVariable(id,str,('path',))
#        for file_id, file in enumerate(paths_ordering['path']):
#            temp[file_id]=str(paths_ordering[id][file_id])
#
#    #soft_link_numpy=np.dtype([(str(path_id),np.uint32) for 
#    #                     path_id, path in enumerate(paths_ordering['path'])])
#    #soft_link=output.createCompoundType(soft_link_numpy,'soft_link')
#    replicate_netcdf_var(output,data,var,datatype=np.uint32,fill_value=np.iinfo(np.uint32).max,add_dim='path')
#    data.close()
#    var_out=output.variables[var]
#    for time_id, time in enumerate(time_indices):
#        temp=np.ones(var_out.shape[1:],dtype=np.uint32)*np.iinfo(np.uint32).max
#        temp[...,paths_indices[time_id]]=table['indices'][time_id]
#        var_out[time,...]=np.reshape(temp,(1,)+temp.shape)
#    return
#
#def create_variable_soft_links(data,output,var,time_indices,paths_indices,paths_ordering,id_list,table):
#    #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
#    output.createDimension('path',len(paths_ordering))
#    output.createVariable('version',np.uint32,('path',))[:]=paths_ordering['version']
#    for id in id_list:
#        temp=output.createVariable(id,str,('path',))
#        for file_id, file in enumerate(paths_ordering['path']):
#            temp[file_id]=str(paths_ordering[id][file_id])
#
#    var_out = output.createVariable(var,np.uint32,('time','path'),zlib=True,fill_value=np.iinfo(np.uint32).max)
#    replicate_netcdf_var_att(output,data,var)
#    data.close()
#
#    temp=np.ones(var_out.shape,dtype=np.uint32)*np.iinfo(np.uint32).max
#    temp[time_indices,paths_indices]=table['indices']
#    var_out[:]=temp
#    return

def create_time_axis(output,data,time_axis):
    output.createDimension('time',len(time_axis))
    time = output.createVariable('time','d',('time',))
    time.calendar=str(data.variables['time'].calendar)
    time.units=str(data.variables['time'].units)
    time[:]=time_axis
    return

def retrieve_fx(output,paths_list,var):
    most_recent_version='v'+str(np.max([int(item['version'][1:]) for item in paths_list]))
    path=paths_list[[item['version'] for item in paths_list].index(most_recent_version)]
    for att in path.keys():
        if att!='path':      
            output.setncattr(att,path[att])
    output.setncattr('path',path['path'].split('|')[0])
    output.setncattr('checksum',path['path'].split('|')[1])
    remote_data=netCDF4.Dataset(path['path'].split('|')[0].replace('fileServer','dodsC'))
    replicate_netcdf_var(output,remote_data,var)
    output.variables[var][:]=remote_data.variables[var][:]
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

def create_local_netcdf(options,out_netcdf_file,tuple_list):
    output=netCDF4.Dataset(out_netcdf_file,'w')
    time_axis_datetime=[item[2] for item in tuple_list]
    for item_id, item in enumerate(tuple_list):
        data=netCDF4.Dataset(item[0].replace('fileServer','dodsC'),'r')
        if item_id==0:
            output=replicate_netcdf_file(output,data)
            time_axis=netCDF4.date2num(time_axis_datetime,units=data.variables['time'].units, calendar=netcdf_calendar(data))
            create_time_axis(output,data,time_axis)
            ouptut=replicate_netcdf_var(output,data,options.var)
            var_out=output.variables[options.var]
        temp=data.variables[options.var][item[1],...]
        var_out[item_id,...]=temp
        output.sync()
        print output
        data.close()
    output.close()
    return

def netcdf_calendar(data):
    if 'calendar' in dir(data.variables['time']):
        calendar=data.variables['time'].calendar
    else:
        calendar='standard'
    return calendar

def retrieve_data(options,project_drs):
    if not options.netcdf:
        paths_list=cdb_query_archive_class.SimpleTree(io_tools.open_netcdf(options,project_drs),options,project_drs).get_paths_list(options)
        for path in paths_list:
            retrieve_path(options,path)
    else:
        data=netCDF4.Dataset(options.in_diagnostic_netcdf_file,'r')
        output=netCDF4.Dataset(options.out_destination,'w')
        output=descend_tree(options,data,output)
        data.close()
        output.close()
    return

def descend_tree(options,data,output):
    output=descend_tree_recursive(options,data,output)
    return output

def descend_tree_recursive(options,data,output):
    if len(data.groups.keys())>0:
        for group in data.groups.keys():
            if not group in output.groups.keys():
                output_grp=output.createGroup(group)
            else:
                output_grp=output.groups[group]
            for att in data.groups[group].ncattrs():
                if not att in output_grp.ncattrs():
                    output_grp.setncattr(att,getattr(data.groups[group],att))
            output_grp=descend_tree_recursive(options,data.groups[group],output_grp)
    else:
        if 'time' in data.dimensions.keys():
            output=retrieve_remote_vars(options,data,output)
        else:
            #Fixed variables. Do not retrieve, just copy:
            for var in data.variables.keys():
                output=replicate_netcdf_var(output,data,var)
                output.variables[var][:]=data.variables[var][:]
            
    return output
            
def retrieve_remote_vars(options,data,output):
    if not 'time' in output.dimensions.keys():
        create_time_axis(output,data,data.variables['time'][:])
    vars_to_retrieve=[var for var in data.variables.keys() if 'indices' in data.variables[var].dimensions and var!='indices']

    #Replicate all the other variables:
    for var in set(data.variables.keys()).difference(vars_to_retrieve):
        if not var in ['checksum','data_node','file_type','search','version']:
            output=replicate_netcdf_var(output,data,var)
            output.variables[var][:]=data.variables[var][:]

    for var_to_retrieve in vars_to_retrieve:
        soft_link=np.ma.array(data.variables[var_to_retrieve][:]).data

        #Sort the paths so that we query each only once:
        sorting_paths=np.argsort(soft_link[:,0])
        unique_paths_list_id=np.unique(soft_link[sorting_paths,0])
        sorted_soft_link=soft_link[sorting_paths,:]


        #Get list of paths:
        paths_list=data.variables['path'][:]
        
        #Get attributes from variable to retrieve:
        remote_data=netCDF4.Dataset(paths_list[unique_paths_list_id[0]].replace('fileServer','dodsC'))
        output=replicate_netcdf_var(output,remote_data,var_to_retrieve,chunksize=-1)
        source_dimensions=dict()
        for dim in data.variables[var_to_retrieve].getncattr('cdb_query_dimensions').split(','):
            if dim != 'time':
                source_dimensions[dim]=remote_data.variables[dim][:]
                source_dimensions[dim]=np.arange(max(source_dimensions[dim].shape))[np.in1d(output.variables[dim][:],
                                                                                       source_dimensions[dim])]
                if len(convert_indices_to_slices(source_dimensions[dim]))==1:
                    source_dimensions[dim]=slice(*convert_indices_to_slices(source_dimensions[dim])[0])
        remote_data.close()

        #Retrieve the data path by path. This is the most efficient method since paths only have to be queried once
        retrieved_data=map(retrieve_path_data,
                            zip(paths_list[unique_paths_list_id],
                                [sorted_soft_link[sorted_soft_link[:,0]==path_id,1] for path_id in unique_paths_list_id],
                                [var_to_retrieve for path_id in unique_paths_list_id],
                                [source_dimensions for path_id in unique_paths_list_id]
                                )
                            )

        #print np.argsort(sorting_paths)
        #Assign retrieved variables to output. We sort back the time axis:
        output.variables[var_to_retrieve][:]=np.concatenate(retrieved_data,axis=0)[np.argsort(sorting_paths),...]

        output.sync()
    return output

def retrieve_path_data(in_tuple):
    path=in_tuple[0].replace('fileServer','dodsC')
    indices=in_tuple[1]
    var=in_tuple[2]
    other_indices=in_tuple[3]

    remote_data=netCDF4.Dataset(path)
    if len(indices)==1:
        return add_axis(grab_remote_indices(remote_data.variables[var],indices,other_indices))
    else:
        return grab_remote_indices(remote_data.variables[var],indices,other_indices)

def add_axis(array):
    return np.reshape(array,(1,)+array.shape)

def grab_remote_indices(variable,indices,other_indices):
    
    indices_sort=np.argsort(indices)
    other_slices=tuple([other_indices[dim] for dim in variable.dimensions if dim!='time'])
    #return np.concatenate(map(lambda x: variable[slice(*x),...],
    return np.concatenate(map(lambda x: variable.__getitem__((slice(*x),)+other_slices),
                                convert_indices_to_slices(indices[indices_sort])
                             ),axis=0
                             )[np.argsort(indices_sort),...]

def convert_indices_to_slices(indices):
    slices = []
    for key, it in groupby(enumerate(indices), lambda x: x[1] - x[0]):
        indices = [y for x, y in it]
        if len(indices) == 1:
            slices.append([indices[0]])
        else:
            slices.append([indices[0], indices[-1]+1])
    return slices

def retrieve_path(options,path):
    decomposition=path[0].split('|')
    if not (isinstance(decomposition,list) and len(decomposition)>1):
        return

    root_path=decomposition[0]
    dest_name=options.out_destination+'/'.join(path[1:])+'/'+root_path.split('/')[-1]
    try:
        md5sum=md5_for_file(open(dest_name,'r'))
    except:
        md5sum=''
    if md5sum==decomposition[1]:
        print 'File '+dest_name+' found.'
        print 'MD5 OK! Not retrieving.'
        return

    retrieval_utils.download_secure(root_path,dest_name)
    try:
        md5sum=md5_for_file(open(dest_name,'r'))
    except:
        md5sum=''
    print 'Checking MD5 checksum of retrieved file...'
    if md5sum!=decomposition[1]:
        print('File '+dest_name+' does not have the same MD5 checksum as published on the ESGF. Removing this file...')
        try:
            os.remove(dest_name)
        except:
            pass
    else:
        print 'MD5 OK!'
    return
    

