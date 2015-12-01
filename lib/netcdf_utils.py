
import numpy as np
import math

import netCDF4
import h5py
import datetime


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
    units=time_var.units
    if 'calendar' in dir(time_var):
        calendar=time_var.calendar
    else:
        calendar=None
    return get_date_axis_units(time_var[:],units,calendar)

def get_date_axis_units(time_axis,units,calendar):
    if units=='day as %Y%m%d.%f':
        date_axis=get_date_axis_absolute(time_axis)
    else:
        date_axis=get_date_axis_relative(time_axis,units,calendar)
    return date_axis

def get_date_axis_relative(time_axis,units,calendar):
    if calendar!=None:
        try:
            date_axis = netCDF4.num2date(time_axis,units=units,calendar=calendar)
        except ValueError:
            if (units=='days since 0-01-01 00:00:00' and
                calendar=='365_day'):
                date_axis = netCDF4.num2date(time_axis-365.0,units='days since 1-01-01 00:00:00',calendar=calendar)
            else:
                raise
    else:
        date_axis = netCDF4.num2date(time_axis,units=units)
    return date_axis

def get_date_axis_absolute(time_axis):
    return map(convert_to_date_absolute,time_axis)

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

#def replicate_full_netcdf_recursive(output,data,options=None):
def replicate_full_netcdf_recursive(output,data,hdf5=None,check_empty=False):
    for var_name in data.variables.keys():
        replicate_and_copy_variable(output,data,var_name,hdf5=hdf5,check_empty=check_empty)
    if len(data.groups.keys())>0:
        for group in data.groups.keys():
            output_grp=replicate_group(output,data,group)
            if hdf5!=None:
                replicate_full_netcdf_recursive(output_grp,data.groups[group],hdf5=hdf5[group],check_empty=check_empty)
            else:
                replicate_full_netcdf_recursive(output_grp,data.groups[group],check_empty=check_empty)
    return

def replicate_and_copy_variable(output,data,var_name,datatype=None,fill_value=None,add_dim=None,chunksize=None,zlib=None,hdf5=None,check_empty=False):
    replicate_netcdf_var(output,data,var_name,datatype=datatype,fill_value=fill_value,add_dim=add_dim,chunksize=chunksize,zlib=zlib)

    if len(data.variables[var_name].shape)>0:
        if ( 'soft_links' in data.groups.keys() and 
              var_name in data.groups['soft_links'].variables.keys()
              and check_empty):
            #Variable has a soft link.
            return output

        variable_size=min(data.variables[var_name].shape)
        #Use the hdf5 library to find the real size of the stored array:
        if hdf5!=None:
            variable_size=hdf5[var_name].size
            storage_space=hdf5[var_name].id.get_storage_size()

        if variable_size>0:
            max_request=450.0 #maximum request in Mb
            #max_request=9000.0 #maximum request in Mb
            max_time_steps=max(
                            int(np.floor(max_request*1024*1024/(32*np.prod(data.variables[var_name].shape[1:])))),
                            1)

            num_time_chunk=int(np.ceil(data.variables[var_name].shape[0]/float(max_time_steps)))

            if (len(data.variables[var_name].shape)>1 and
                num_time_chunk>1):
                for time_chunk in range(num_time_chunk):
                    time_slice=slice(time_chunk*max_time_steps,
                                     min((time_chunk+1)*max_time_steps,data.variables[var_name].shape[0])
                                     ,1)
                                     #(time_chunk+1)*max_time_steps,1)
                    #output.variables[var_name][time_slice,...]=data.variables[var_name][time_slice,...]
                    temp=data.variables[var_name][time_slice,...]
                    #Assign only if not masked everywhere:
                    if not 'mask' in dir(temp) or not check_empty:
                        output.variables[var_name][time_slice,...]=temp
                    else: 
                        #Only write the variable if it is not empty:
                        if not temp.mask.all():
                            output.variables[var_name][time_slice,...]=temp
            else:
                #output.variables[var_name][:]=data.variables[var_name][:]
                #temp=np.reshape(data.variables[var_name][:],data.variables[var_name].shape)
                temp=data.variables[var_name][:]
                if not 'mask' in dir(temp) or not check_empty:
                    #if output.variables[var_name].shape!=temp.shape:
                    #    print data.variables[var_name].shape,  output.variables[var_name].shape,temp.shape
                    #    print data
                    #    print output
                    #    print output.path
                    #output_hdf5=None
                    #for item in h5py.h5f.get_obj_ids():
                    #    if 'name' in dir(item) and item.name==output.filepath():
                    #        output_hdf5=h5py.File(item)
                    #if output_hdf5!=None:
                    #    dset=output_hdf5[output.path].get(var_name)
                    #    dset=temp
                    #else:
                    output.variables[var_name][:]=temp
                else: 
                    #Only write the variable if it is not empty:
                    if not temp.mask.all():
                        output.variables[var_name][:]=temp
    elif len(data.variables[var_name].dimensions)==0:
        #scalar variable:
        output.variables[var_name][:]=data.variables[var_name][:]
    return output

def replicate_group(output,data,group_name):
    output_grp=create_group(output,data,group_name)
    replicate_netcdf_file(output_grp,data.groups[group_name])
    return output_grp

def create_group(output,data,group_name):
    if not group_name in output.groups.keys():
        output_grp=output.createGroup(group_name)
    else:
        output_grp=output.groups[group_name]
    return output_grp
    
def replicate_netcdf_file(output,data):
    for att in data.ncattrs():
        att_val=data.getncattr(att)
        if 'encode' in dir(att_val):
            att_val=str(att_val.encode('ascii','replace'))
        if (not att in output.ncattrs() and
            att != 'cdb_query_temp'):
            try:
                setattr(output,att,att_val)
            except:
                output.setncattr(att,att_val)
    return output

def replicate_netcdf_var_dimensions(output,data,var):
    for dims in data.variables[var].dimensions:
        if dims not in output.dimensions.keys() and dims in data.dimensions.keys():
            if data.dimensions[dims].isunlimited():
                output.createDimension(dims,None)
            else:
                output.createDimension(dims,len(data.dimensions[dims]))
            if dims in data.variables.keys():
                dim_var = output.createVariable(dims,data.variables[dims].dtype,(dims,))
                dim_var[:] = data.variables[dims][:]
                output = replicate_netcdf_var(output,data,dims)
                if ('bounds' in output.variables[dims].ncattrs() and
                    output.variables[dims].getncattr('bounds') in data.variables.keys()):
                    output=replicate_netcdf_var(output,data,output.variables[dims].getncattr('bounds'))
                    output.variables[output.variables[dims].getncattr('bounds')][:]=data.variables[output.variables[dims].getncattr('bounds')][:]
            else:
                #Create a dummy dimension variable:
                dim_var = output.createVariable(dims,np.float,(dims,))
                dim_var[:]=np.arange(len(data.dimensions[dims]))
    return output

def replicate_netcdf_var(output,data,var,datatype=None,fill_value=None,add_dim=None,chunksize=None,zlib=None):
    output=replicate_netcdf_var_dimensions(output,data,var)
    if datatype==None: datatype=data.variables[var].datatype
    if (isinstance(datatype,netCDF4.CompoundType) and
        not datatype.name in output.cmptypes.keys()):
        datatype=output.createCompoundType(datatype.dtype,datatype.name)

    kwargs=dict()
    if (fill_value==None and 
        '_FillValue' in dir(data.variables[var]) and 
        datatype==data.variables[var].datatype):
            kwargs['fill_value']=data.variables[var]._FillValue
    else:
        kwargs['fill_value']=fill_value

    if zlib==None:
        if data.variables[var].filters()==None:
            kwargs['zlib']=False
        else:
            for item in data.variables[var].filters():
                kwargs[item]=data.variables[var].filters()[item]
    else:
        kwargs['zlib']=zlib
    
    if var not in output.variables.keys():
        dimensions=data.variables[var].dimensions
        if add_dim:
            dimensions+=(add_dim,)
        if chunksize!=None:
            if chunksize==-1:
                chunksizes=tuple([1 if dim=='time' else data.variables[var].shape[dim_id] for dim_id,dim in enumerate(dimensions)])
            else:
                #chunksizes=tuple([1 if output.dimensions[dim].isunlimited() else 10 for dim in dimensions])
                #chunksizes=tuple([1 if dim=='time' else chunksize for dim in dimensions])
                chunksizes=tuple([1 if dim=='time' else chunksize for dim_id,dim in enumerate(dimensions)])
            kwargs['chunksizes']=chunksizes
        else:
            if data.variables[var].chunking()=='contiguous':
                kwargs['contiguous']=True
                kwargs['zlib']=False
            else:
                kwargs['chunksizes']=data.variables[var].chunking()
        output.createVariable(var,datatype,dimensions,**kwargs)
    output = replicate_netcdf_var_att(output,data,var)
    return output

def replicate_netcdf_var_att(output,data,var):
    for att in data.variables[var].ncattrs():
        att_val=data.variables[var].getncattr(att)
        if att[0]!='_':
            if 'encode' in dir(att_val):
                att_val=att_val.encode('ascii','replace')
            if 'encode' in dir(att):
                att=att.encode('ascii','replace')
            try:
                setattr(output.variables[var],att,att_val)
            except:
                output.variables[var].setncattr(att,att_val)
    return output

def create_time_axis(output,data,time_axis):
    #output.createDimension('time',len(time_axis))
    output.createDimension('time',None)
    time = output.createVariable('time','d',('time',))
    if data==None:
        time.calendar='standard'
        time.units='days since '+str(time_axis[0])
    else:
        time.calendar=netcdf_calendar(data)
        time.units=str(data.variables['time'].units)
    time[:]=time_axis
    return

def create_time_axis_date(output,time_axis,units,calendar):
    #output.createDimension('time',len(time_axis))
    output.createDimension('time',None)
    time = output.createVariable('time','d',('time',))
    time.calendar=calendar
    time.units=units
    time[:]=netCDF4.date2num(time_axis,time.units,calendar=time.calendar)
    return

def netcdf_calendar(data):
    if 'calendar' in data.variables['time'].ncattrs():
        calendar=data.variables['time'].calendar
    else:
        calendar='standard'
    if 'encode' in dir(calendar):
        calendar=calendar.encode('ascii','replace')
    return calendar

def netcdf_time_units(data):
    if 'units' in dir(data.variables['time']):
        units=data.variables['time'].units
    else:
        units=None
    return calendar

def create_date_axis_from_time_axis(time_axis,attributes_dict):
    units=attributes_dict['units']
    if 'calendar' in attributes_dict.keys(): 
        calendar=attributes_dict['calendar']
    else:
        calendar='standard'

    if units=='day as %Y%m%d.%f':
        date_axis=np.array(map(convert_to_date_absolute,native_time_axis))
    else:
        try:
            #Put cmip5_rewrite_time_axis here:
            date_axis=get_date_axis_relative(time_axis,units,calendar)
            #date_axis=netCDF4.num2date(time_axis,units=units,calendar=calendar)
        except TypeError:
            time_axis=np.array([]) 
    return date_axis

def assign_tree(output,val,sort_table,tree):
    if len(tree)>1:
        if tree[0]!='':
            assign_tree(output.groups[tree[0]],val,sort_table,tree[1:])
        else:
            assign_tree(output,val,sort_table,tree[1:])
    else:
        output.variables[tree[0]][sort_table]=val
    return
