
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

import indices_utils

import multiprocessing
import subprocess


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

#def replicate_full_netcdf_recursive(output,data,options=None):
def replicate_full_netcdf_recursive(output,data):
    for var in data.variables.keys():
        replicate_netcdf_var(output,data,var)
        output.variables[var][:]=data.variables[var][:]
    if len(data.groups.keys())>0:
        for group in data.groups.keys():
            #if (options==None or
            #    (not data.groups[group].getncattr('level_name') in dir(options)) or 
            #    (getattr(options,data.groups[group].getncattr('level_name'))==None) or 
            #    (getattr(options,data.groups[group].getncattr('level_name'))==group)): 
            if not group in output.groups.keys():
                output_grp=output.createGroup(group)
            else:
                output_grp=output.groups[group]
            for att in data.groups[group].ncattrs():
                if not att in output_grp.ncattrs():
                    output_grp.setncattr(att,data.groups[group].getncattr(att))
            replicate_full_netcdf_recursive(output_grp,data.groups[group])
    return
    
def replicate_netcdf_file(output,data):
    for att in data.ncattrs():
        att_val=data.getncattr(att)
        if 'encode' in dir(att_val):
            att_val=att_val.encode('ascii','replace')
        output.setncattr(att,att_val)
    return output

def replicate_netcdf_var_dimensions(output,data,var):
    for dims in data.variables[var].dimensions:
        if dims not in output.dimensions.keys() and dims in data.dimensions.keys():
            output.createDimension(dims,len(data.dimensions[dims]))
            if dims in data.variables.keys():
                dim_var = output.createVariable(dims,data.variables[dims].dtype,(dims,))
                dim_var[:] = data.variables[dims][:]
                output = replicate_netcdf_var(output,data,dims)
                if 'bounds' in output.variables[dims].ncattrs():
                    output=replicate_netcdf_var(output,data,output.variables[dims].getncattr('bounds'))
                    output.variables[output.variables[dims].getncattr('bounds')][:]=data.variables[output.variables[dims].getncattr('bounds')][:]
    return output

def replicate_netcdf_var(output,data,var,datatype=None,fill_value=None,add_dim=None,chunksize=None):
    output=replicate_netcdf_var_dimensions(output,data,var)
    if datatype==None: datatype=data.variables[var].datatype
    if (isinstance(datatype,netCDF4.CompoundType) and
        not datatype.name in output.cmptypes.keys()):
        datatype=output.createCompoundType(datatype.dtype,datatype.name)
    
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
    return output

def replicate_netcdf_var_att(output,data,var):
    for att in data.variables[var].ncattrs():
        att_val=data.variables[var].getncattr(att)
        if att[0]!='_':
            if 'encode' in dir(att_val):
                att_val=att_val.encode('ascii','replace')
            output.variables[var].setncattr(att,att_val)
    return output

def create_time_axis(output,data,time_axis):
    output.createDimension('time',len(time_axis))
    time = output.createVariable('time','d',('time',))
    time.calendar=str(data.variables['time'].calendar)
    time.units=str(data.variables['time'].units)
    time[:]=time_axis
    return

def get_data_node(path,file_type):
    if file_type=='HTTPServer':
        return '/'.join(path.split('/')[:3])
    elif file_type=='local_file':
        return '/'.join(path.split('/')[:2])
    else:
        return ''

def netcdf_calendar(data):
    if 'calendar' in dir(data.variables['time']):
        calendar=data.variables['time'].calendar
    else:
        calendar='standard'
    return calendar

def apply(options,project_drs):
    if options.script=='': return
    #Recover the database meta data:
    database=cdb_query_archive_class.SimpleTree(options,project_drs)
    database.load_header(options)
    database.load_database(options,cdb_query_archive_class.find_simple)

    vars_list=database.nc_Database.list_fields(database.drs.official_drs_no_version)
    database.close_database()

    output_root=distributed_apply(apply_to_variable,database,options,vars_list)
    output_root.close()
    return

def apply_to_variable(database,options):
    input_file_name=options.in_diagnostic_netcdf_file
    files_list=[input_file_name,]+options.in_extra_netcdf_files

    temp_files_list=[]
    for file in files_list:
        data=netCDF4.Dataset(file,'r')
        temp_file=file+'.pid'+str(os.getpid())
        output_tmp=netCDF4.Dataset(temp_file,'w',format='NETCDF4',diskless=True,persist=True)
        extract_netcdf_variable_recursive(output_tmp,data,options)
        temp_files_list.append(temp_file)
        output_tmp.close()
        data.close()

    script_to_call=options.script
    for file_id, file in enumerate(temp_files_list):
        if not '{'+str(file_id)+'}' in options.script:
            script_to_call+=' {'+str(file_id)+'}'

    output_file_name=options.out_netcdf_file+'.pid'+str(os.getpid())
    temp_output_file_name= output_file_name+'.tmp'
    script_to_call+=' '+temp_output_file_name

    var=[getattr(options,opt) for opt in database.drs.official_drs_no_version]
    print '/'.join(var)
    out=subprocess.call(script_to_call.format(*temp_files_list),shell=True)

    try:
        for file in temp_files_list:
            os.remove(file)
    except OSError:
        pass
    return temp_output_file_name, var

def replace_netcdf_variable_recursive(output,data,level_desc,tree):
    level_name=level_desc[0]
    group_name=level_desc[1]
    if not level_desc[1] in output.groups.keys():
        output_grp=output.createGroup(group_name)
    else:
        output_grp=output.groups[group_name]
    if 'level_name' not in output_grp.ncattrs():
        output_grp.setncattr('level_name',level_name)

    if len(tree)>0:
        replace_netcdf_variable_recursive(output_grp,data,tree[0],tree[1:])
    else:
        for att in data.ncattrs():
            if not att in output_grp.ncattrs():
                output_grp.setncattr(att,data.getncattr(att))
        for var in data.variables.keys():
            replicate_netcdf_var(output_grp,data,var)
            if np.min(data.variables[var].shape)>0:
                output_grp.variables[var][:]=data.variables[var][:]
    return
        
    

def extract_netcdf_variable_recursive(output,data,options):
    for var in data.variables.keys():
        replicate_netcdf_var(output,data,var)
        output.variables[var][:]=data.variables[var][:]
    if len(data.groups.keys())>0:
        for group in data.groups.keys():
            if (
                (not data.groups[group].getncattr('level_name') in dir(options)) or 
                (getattr(options,data.groups[group].getncattr('level_name'))==None) or 
                (getattr(options,data.groups[group].getncattr('level_name'))==group)): 
                for att in data.groups[group].ncattrs():
                    if ( not att in output.ncattrs() and att!='level_name'):
                        output.setncattr(att,data.groups[group].getncattr(att))
                extract_netcdf_variable_recursive(output,data.groups[group],options)
    return

def worker(tuple):
    return tuple[-1].put(tuple[0](*tuple[1:-1]))
    
def distributed_apply(function_handle,database,options,vars_list,args=tuple()):

        #Open output file:
        output_file_name=options.out_netcdf_file
        output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')

        manager=multiprocessing.Manager()
        queue=manager.Queue()
        #Set up the discovery var per var:
        args_list=[]
        for var_id,var in enumerate(vars_list):
            options_copy=copy.copy(options)
            for opt_id, opt in enumerate(database.drs.official_drs_no_version):
                setattr(options_copy,opt,var[opt_id])
            args_list.append((function_handle,copy.copy(database),options_copy)+args+(queue,))
        
        #Span up to options.num_procs processes and each child process analyzes only one simulation
        if options.num_procs>1:
            pool=multiprocessing.Pool(processes=options.num_procs,maxtasksperchild=1)
            result=pool.map_async(worker,args_list,chunksize=1)
            #Record files to main file:
            for arg in args_list:
                temp_file_name, var=queue.get()
                data=netCDF4.Dataset(temp_file_name,'r')
                tree=zip(database.drs.official_drs_no_version,var)
                replace_netcdf_variable_recursive(output_root,data,tree[0],tree[1:])
                data.close()
                try:
                    os.remove(temp_file_name)
                except OSError:
                    pass
                output_root.sync()

                pool.close()
                pool.join()
        else:
            for arg in args_list:
                worker(arg)
                temp_file_name, var=queue.get()
                data=netCDF4.Dataset(temp_file_name,'r')
                tree=zip(database.drs.official_drs_no_version,var)
                replace_netcdf_variable_recursive(output_root,data,tree[0],tree[1:])
                data.close()
                try:
                    os.remove(temp_file_name)
                except OSError:
                    pass
                output_root.sync()


        return output_root
