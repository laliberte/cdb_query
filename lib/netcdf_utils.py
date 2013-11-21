
import numpy as np
import math

import netCDF4
import datetime

import hashlib

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
    output.sync()
    return output

def replicate_netcdf_var(output,data,var,datatype=None):
    if not datatype: datatype=data.variables[var].dtype
    for dims in data.variables[var].dimensions:
        if dims not in output.dimensions.keys():
            output.createDimension(dims,len(data.dimensions[dims]))
            dim_var = output.createVariable(dims,data.variables[dims].dtype,(dims,))
            dim_var[:] = data.variables[dims][:]
            output = replicate_netcdf_var(output,data,dims)

    if var not in output.variables.keys():
        output.createVariable(var,datatype,data.variables[var].dimensions,zlib=True)
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

def recover_time_old(file):
    file_name=file.split('|')[0]
    start_id=int(file.split('|')[1])
    end_id=int(file.split('|')[2])

    data=netCDF4.Dataset(file_name)
    if 'calendar' in dir(data.variables['time']):
        calendar=data.variables['time'].calendar
    else:
        calendar='standard'
    time_axis=(netCDF4.num2date(data.variables['time'],
                                 units=data.variables['time'].units,
                                 calendar=calendar)
                    )[start_id:end_id+1]
    table_desc=[
               ('paths','a255'),
               ('indices','uint32')
               ]
    table=np.empty(time_axis.shape, dtype=table_desc)
    table['paths']=np.array([str(file_name) for item in time_axis])
    table['indices']=range(start_id,end_id+1)
    data.close()
    return time_axis,table

def recover_time(file):
    file_name=file['path'].replace('fileServer','dodsC').split('|')[0]

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

def concatenate_paths(output_file,source_files,frequency_time,var,checksum=False):
    time_axis, table=map(np.concatenate,
                             zip(*map(recover_time,source_files))
                             )

    data=netCDF4.Dataset(table['paths'][0])
    time_axis = netCDF4.date2num(time_axis,units=data.variables['time'].units,calendar=data.variables['time'].calendar)
    time_axis_unique = np.unique(time_axis)

    time_axis_datetime= netCDF4.num2date(time_axis_unique,units=data.variables['time'].units,calendar=data.variables['time'].calendar)
    output_file_name=(output_file+'_'+time_axis_datetime[0].strftime(''.join(frequency_time)) +'_'+
                                     time_axis_datetime[-1].strftime(''.join(frequency_time)) +'.nc' )
    output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')
    output=output_root.createGroup('model_id')
    replicate_netcdf_file(output,data)

    output.createDimension('time',len(time_axis_unique))
    time = output.createVariable('time','d',('time',))
    time.calendar=str(data.variables['time'].calendar)
    time.units=str(data.variables['time'].units)
    time[:]=time_axis_unique

    output.createDimension('path',len(source_files))
    paths = output.createVariable('path',str,('path',))
    checksum = output.createVariable('checksum',str,('path',))
    version = output.createVariable('version',np.uint32,('path',))
    search = output.createVariable('search',str,('path',))
    domain = output.createVariable('domain',str,('path',))
    file_type = output.createVariable('file_type',str,('path',))
    for file_id, file in enumerate(source_files):
        paths[file_id]=file['path'].split('|')[0]
        checksum[file_id]=file['path'].split('|')[1]
        version[file_id]=np.uint32(file['version'][1:])
        search[file_id]=file['search']
        domain[file_id]='/'.join(file['path'].split('/')[:3])
        file_type[file_id]=file['file_type']

    #paths_list=[path.replace('fileServer','dodsC') for path in paths[:] ]
    paths_list=[path for path in paths[:] ]

    paths_indices=np.empty(time_axis.shape,dtype=np.uint32)
    for path_id, path in enumerate(paths_list):
        paths_indices[path.replace('fileServer','dodsC')==table['paths']]=path_id
    time_indices=np.empty(time_axis.shape,dtype=np.uint32)
    for time_id, time in enumerate(time_axis_unique):
        time_indices[time==time_axis]=time_id

    var_out = output.createVariable(var,np.uint32,('time','path'),zlib=True)
    replicate_netcdf_var_att(output,data,var)
    temp=np.empty_like(var_out)
    temp[time_indices,paths_indices]=table['indices']
    var_out=temp
        
    output_root.close()
    data.close()

    return

#def modify_init(func):
#    return (lambda cls: setattr(cls,'__init__',func(m)))
#
#class cdb_Dataset(netCDF4.Dataset):
#    pass
#
#class cdb_Variable(netCDF4.Variable):
#    def __getitem__(self,elem):
#        return netCDF4.Variable.__getitem__(self,elem)

