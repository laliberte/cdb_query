
import numpy as np
import math

from netCDF4 import Dataset, num2date,date2num
import datetime

def get_year_axis(path_name):
    try:
        #print 'Loading file... ',
        #print path_name
        data=Dataset(path_name)
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
        date_axis = num2date(time_var[:],units=time_var.units,calendar=time_var.calendar)
    else:
        date_axis = num2date(time_var[:],units=time_var.units)
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

def replicate_netcdf_var(output,data,var):
    for dims in data.variables[var].dimensions:
        if dims not in output.dimensions.keys():
            output.createDimension(dims,len(data.dimensions[dims]))
            dim_var = output.createVariable(dims,'d',(dims,))
            dim_var[:] = data.variables[dims][:]
            output = replicate_netcdf_var(output,data,dims)

    if var not in output.variables.keys():
        output.createVariable(var,'d',data.variables[var].dimensions)
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

    data=Dataset(file_name)
    time_axis=(num2date(data.variables['time'],
                                 units=data.variables['time'].units,
                                 calendar=data.variables['time'].calendar)
                    )[start_id:end_id+1]
    name_axis=np.array([file_name for item in time_axis])
    data.close()
    return time_axis,name_axis

def concatenate_pointers(output_file,source_files,frequency_time,var):
    time_axis, name_axis=map(np.concatenate,
                             zip(*map(recover_time,source_files))
                             )
    sort_id=np.argsort(time_axis)
    data=Dataset(name_axis[sort_id][0])

    output_file_name=(output_file+'_'+time_axis[sort_id[0]].strftime(''.join(frequency_time)) +'_'+
                                     time_axis[sort_id[-1]].strftime(''.join(frequency_time)) )
    output=Dataset(output_file,'w',format='NETCDF4')
    replicate_netcdf_file(output,data)

    output.createDimension('time',len(time_axis))
    time = output.createVariable('time','d',('time',))
    time.calendar=str(data.variables['time'].calendar)
    time.units=str(data.variables['time'].units)
    time[:]=date2num(time_axis[sort_id],units=time.units,calendar=time.calendar)

    replicate_netcdf_var(output,data,var)
    ptr_group=output.createGroup('pointers')
    pointers=ptr_group.createVariable(var,str,('time',),zlib=True)
    for id, name in enumerate(name_axis[sort_id]): pointers[id]=str(name)
    output.sync()
    output.close()
    data.close()
    return
