
import numpy as np
import math

import netCDF4
import datetime

import hashlib

import os
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
    for att in data.variables[var].ncattrs():
        att_val=getattr(data.variables[var],att)
        if att[0]!='_':
            if 'encode' in dir(att_val):
                att_val=att_val.encode('ascii','replace')
            setattr(output.variables[var],att,att_val)
    output.sync()
    return output

def recover_time(file):
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

def concatenate_paths(output_file,source_files,frequency_time,var,checksum=False):
    time_axis, table=map(np.concatenate,
                             zip(*map(recover_time,source_files))
                             )
    sort_id=np.argsort(time_axis)
    table=table[sort_id]
    time_axis=time_axis[sort_id]

    data=netCDF4.Dataset(table['paths'][0])

    output_file_name=(output_file+'_'+time_axis[0].strftime(''.join(frequency_time)) +'_'+
                                     time_axis[-1].strftime(''.join(frequency_time)) +'.nc' )
    output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')
    output=output_root.createGroup('model_id')
    replicate_netcdf_file(output,data)

    output.createDimension('time',len(time_axis))
    time = output.createVariable('time','d',('time',))
    time.calendar=str(data.variables['time'].calendar)
    time.units=str(data.variables['time'].units)
    time[:]=netCDF4.date2num(time_axis,units=time.units,calendar=time.calendar)

    datatype=[('data',data.variables[var].dtype)]
    for dim in data.variables[var].dimensions: datatype.append((str(dim),np.uint32))
    for path in np.unique(table['paths']): datatype.append((str(path).replace('/','0x2f'),np.uint8))
    cdb_type=output.createCompoundType(np.dtype(datatype),var+'_type')
    replicate_netcdf_var(output,data,var,datatype=cdb_type)
    record_array=np.ma.empty(output.variables[var].shape[1:],np.dtype(datatype))
    print var
    for time_id, time in enumerate(time_axis):
        print time_id
        path_name=str(table['paths'][time_id]).replace('/','0x2f')
        record_array[path_name][:]=1
        record_array['time'][:]=table['indices'][time_id]
        output.variables[var][time_id,...]=record_array
        output.sync()
    #ptr_group=output.createGroup('paths')
    #paths=ptr_group.createVariable(var,str,('time',),zlib=True)
    #ind_group=output.createGroup('indices')
    #indices=ind_group.createVariable(var,'u4',('time',),zlib=True)
    #if checksum:
    #    chk_group=output.createGroup('checksums')
    #    checksums=chk_group.createVariable(var,'u4',('time',),zlib=True)

    #for id, path in enumerate(table['paths']): paths[id]=str(path)
    #indices[:]=table['indices']
    #if checksum:
    #    checksums[:]=np.vectorize(lambda x: md5_for_netcdf(x['paths'],var,x['indices']))(table)

    output_root.close()
    data.close()
    return

#def md5_for_netcdf(netcdf_file,variable,time_index):
#    data=Dataset(netcdf_file,'r')
#    print dir(data.variables[variable].filters)
#    array=data.variables[variable][time_index,...]
#    data.close()
#    print array.shape
#    md5sum=hashlib.md5(array).digest()
#    print md5sum
#    return md5sum

#def modify_init(func):
#    return (lambda cls: setattr(cls,'__init__',func(m)))
#
#class cdb_Dataset(netCDF4.Dataset):
#    pass
#
#class cdb_Variable(netCDF4.Variable):
#    def __getitem__(self,elem):
#        return netCDF4.Variable.__getitem__(self,elem)

