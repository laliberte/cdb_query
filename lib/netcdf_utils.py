
import numpy as np
import math

import netCDF4
import datetime

import hashlib

import copy

import os

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

def replicate_netcdf_var(output,data,var,datatype=None,fill_value=None,add_dim=None,chunksize=None):
    if not datatype: datatype=data.variables[var].dtype
    for dims in data.variables[var].dimensions:
        if dims not in output.dimensions.keys():
            output.createDimension(dims,len(data.dimensions[dims]))
            dim_var = output.createVariable(dims,data.variables[dims].dtype,(dims,))
            dim_var[:] = data.variables[dims][:]
            output = replicate_netcdf_var(output,data,dims)

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

def concatenate_paths(header,output_root,output,soft_link_desc,source_files,var):
    sorts_list=['version','domain_id','file_type_id']
    id_list=['domain','file_type','path','checksum','search']
    #First order source files by preference:
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

    #Create time axis in ouptut:
    create_time_axis(output,data,time_axis_unique)

    #CREATE LOOK-UP TABLE:
    paths_list=[path for path in paths_ordering['path'] ]
    paths_indices=np.empty(time_axis.shape,dtype=np.uint32)
    time_indices=np.empty(time_axis.shape,dtype=np.uint32)

    for path_id, path in enumerate(paths_list):
        paths_indices[path.replace('fileServer','dodsC')==table['paths']]=path_id

    #for time_id, time in enumerate(time_axis_unique):
    #    time_indices[time==time_axis]=time_id

    #USE VERSION 4 OF SOFT LINKS BECAUSE NCO AND OTHER TOOLS ARE NOT READY FOR COMPOUND TYPES.
    create_variable_soft_links4(data,output_root,output,var,time_axis,time_axis_unique,paths_indices,paths_ordering,id_list,table)
    output.sync()
    return

def create_variable_soft_links4(data,output_root,output,var,time_axis,time_axis_unique,paths_indices,paths_ordering,id_list,table):
    #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
    output.createDimension('path',len(paths_ordering))
    output.createVariable('version',np.uint32,('path',))[:]=paths_ordering['version']
    for id in id_list:
        temp=output.createVariable(id,str,('path',))
        for file_id, file in enumerate(paths_ordering['path']):
            temp[file_id]=str(paths_ordering[id][file_id])

    #CREATE LOOK-UP TABLE:
    output.createDimension('indices',2)
    indices=output.createVariable('indices',np.str,('indices',))
    indices[0]='path'
    indices[1]='time'

    var_out = output.createVariable(var,np.uint32,('time','indices'),zlib=False,fill_value=np.iinfo(np.uint32).max)
    replicate_netcdf_var_att(output,data,var)
    data.close()

    #Create soft links:
    for time_id, time in enumerate(time_axis_unique):
        var_out[time_id,0]=np.min(paths_indices[time==time_axis])
        var_out[time_id,1]=table['indices'][np.logical_and(paths_indices==var_out[time_id,0],time==time_axis)][0]
        if time_id % 100 == 0:
            output_root.sync()
    output_root.sync()
    return

def create_variable_soft_links3(data,soft_link_desc,output_root,output,var,time_axis,time_axis_unique,time_indices,paths_indices,paths_ordering,id_list,table):
    #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
    output.createDimension('path',len(paths_ordering))
    output.createVariable('version',np.uint32,('path',))[:]=paths_ordering['version']
    for id in id_list:
        temp=output.createVariable(id,str,('path',))
        for file_id, file in enumerate(paths_ordering['path']):
            temp[file_id]=str(paths_ordering[id][file_id])

    #CREATE LOOK-UP TABLE:
    soft_link=output.createCompoundType(soft_link_desc['numpy'],'soft_link')
    var_out = output.createVariable(var,soft_link,('time',),zlib=False)
    replicate_netcdf_var_att(output,data,var)
    data.close()
    
    #Full description:
    #replicate_netcdf_var(output,data,var,datatype=soft_link,chunksize=-1)
    #var_out=output.variables[var]
    #data.close()

    #Create soft links:
    paths_list=[str(path).replace('Server','dodsC') for path in np.nditer(paths_ordering['path'])]
    paths_list=list(paths_ordering['path'])
    for time_id, time in enumerate(time_axis_unique):
        var_out[time_id,...]=(paths_list.index(str(table['paths'][time==time_axis][0]).replace('dodsC','fileServer')),
                          table['indices'][time==time_axis][0])
        if time_id % 100 == 0:
            output_root.sync()
    output_root.sync()
        #var_out[time_id]=(paths_list.index(str(table['paths'][time==time_axis][0]).replace('dodsC','fileServer')),
        #                  table['indices'][time==time_axis][0])
    return


def create_variable_soft_links2(data,output,var,time_indices,paths_indices,paths_ordering,id_list,table):
    #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
    output.createDimension('path',len(paths_ordering))
    output.createVariable('version',np.uint32,('path',))[:]=paths_ordering['version']
    for id in id_list:
        temp=output.createVariable(id,str,('path',))
        for file_id, file in enumerate(paths_ordering['path']):
            temp[file_id]=str(paths_ordering[id][file_id])

    #soft_link_numpy=np.dtype([(str(path_id),np.uint32) for 
    #                     path_id, path in enumerate(paths_ordering['path'])])
    #soft_link=output.createCompoundType(soft_link_numpy,'soft_link')
    replicate_netcdf_var(output,data,var,datatype=np.uint32,fill_value=np.iinfo(np.uint32).max,add_dim='path')
    data.close()
    var_out=output.variables[var]
    for time_id, time in enumerate(time_indices):
        temp=np.ones(var_out.shape[1:],dtype=np.uint32)*np.iinfo(np.uint32).max
        temp[...,paths_indices[time_id]]=table['indices'][time_id]
        var_out[time,...]=np.reshape(temp,(1,)+temp.shape)
    return

def create_variable_soft_links(data,output,var,time_indices,paths_indices,paths_ordering,id_list,table):
    #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
    output.createDimension('path',len(paths_ordering))
    output.createVariable('version',np.uint32,('path',))[:]=paths_ordering['version']
    for id in id_list:
        temp=output.createVariable(id,str,('path',))
        for file_id, file in enumerate(paths_ordering['path']):
            temp[file_id]=str(paths_ordering[id][file_id])

    var_out = output.createVariable(var,np.uint32,('time','path'),zlib=True,fill_value=np.iinfo(np.uint32).max)
    replicate_netcdf_var_att(output,data,var)
    data.close()

    temp=np.ones(var_out.shape,dtype=np.uint32)*np.iinfo(np.uint32).max
    temp[time_indices,paths_indices]=table['indices']
    var_out[:]=temp
    return

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
        paths_ordering['domain'][file_id]=get_domain(file['path'],paths_ordering['file_type'][file_id])

    #Sort paths from most desired to least desired:
    #First order desiredness for least to most:
    domain_order=copy.copy(header['domain_list'])[::-1]#list(np.unique(paths_ordering['domain']))
    file_type_order=copy.copy(header['file_type_list'])[::-1]#list(np.unique(paths_ordering['file_type']))
    for file_id, file in enumerate(source_files):
        paths_ordering['domain_id'][file_id]=domain_order.index(paths_ordering['domain'][file_id])
        paths_ordering['file_type_id'][file_id]=file_type_order.index(paths_ordering['file_type'][file_id])
    #'version' is implicitly from least to most

    #sort and reverse order to get from most to least:
    return np.sort(paths_ordering,order=sorts_list)[::-1]

def get_domain(path,file_type):
    if file_type=='HTTPServer':
        return '/'.join(path.split('/')[:3])
    elif file_type=='local_file':
        return '/'.join(path.split('/')[:-1])
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

#READ NETCDF FILES
def descend_tree(options,data,timestamp):
    output_tuple=descend_tree_recursive(options,options.drs,data,timestamp)
    return output_tuple

def descend_tree_recursive(options,tree_desc,data,timestamp):
    if isinstance(tree_desc,list) and len(tree_desc)>0:
        local_tree_desc=tree_desc[0]
        next_tree_desc=tree_desc[1:]
        if ( local_tree_desc in dir(options) and 
            getattr(options,local_tree_desc) in data.groups.keys()):
            output=descend_tree_recursive(
                                  options,
                                  next_tree_desc,
                                  data.groups[getattr(options,local_tree_desc)],
                                  timestamp)
    else:   

        time_axis=data.variables['time'][:]
        timestamp_val=netCDF4.date2num(timestamp,units=data.variables['time'].units, calendar=netcdf_calendar(data))
        try:
            time_id=list(time_axis).index(timestamp_val)
        except:
            raise IOError('Timestamp not in file')
        temp=np.ma.array(data.variables[getattr(options,'var')][time_id,:]).data
        #Output the path from the first nonmasked element. The paths were sorted earlier
        #path_index=np.ma.nonzero(temp+1)[0]
        #time_index_in_path=temp[path_index]
        path_index=temp[0]
        time_index_in_path=temp[1]
        file_desc={}
        for id in ['path','version','checksum','domain']:
            file_desc[id]=data.variables[id][:]

        #Next, we look for identical paths:
        domains_with_identical_files=[file_desc['domain'][i] for i in range(0,len(file_desc['path'])) if 
                                    file_desc['path'][i].split('/')[-1] == file_desc['path'][path_index].split('/')[-1]
                                and file_desc['version'][i] == file_desc['version'][path_index]
                                and file_desc['checksum'][i] == file_desc['checksum'][path_index]
                                ]
        if options.domain in domains_with_identical_files:
            alternative_file=[file_desc['path'][i] for i in range(0,len(file_desc['path'])) if 
                                    file_desc['path'][i].split('/')[-1] == file_desc['path'][path_index].split('/')[-1]
                                and file_desc['version'][i] == file_desc['version'][path_index]
                                and file_desc['checksum'][i] == file_desc['checksum'][path_index]
                                and file_desc['domain'][i] == options.domain
                                ]
            output={'path':alternative_file[0],
                    'index':time_index_in_path,
                    'timestamp':timestamp,
                    'domains':domains_with_identical_files}
        else:
            #output_tuple=(file_desc['path'][path_index], time_index_in_path, timestamp, domains_with_identical_files)
            output={'path':file_desc['path'][path_index],
                    'index':time_index_in_path,
                    'timestamp':timestamp,
                    'domains':domains_with_identical_files}
    return output
