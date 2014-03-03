
import numpy as np
import math

import netCDF4
import datetime

import hashlib

import copy

import os

import cdb_query_archive_class

import json

import retrieval_utils

import indices_utils

import multiprocessing
import subprocess

import netcdf_soft_links

import nc_Database

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
    for var_name in data.variables.keys():
        replicate_and_copy_variable(output,data,var_name)
    if len(data.groups.keys())>0:
        for group in data.groups.keys():
            output_grp=replicate_group(output,data,group)
            replicate_full_netcdf_recursive(output_grp,data.groups[group])
    return

def replicate_and_copy_variable(output,data,var_name):
    replicate_netcdf_var(output,data,var_name)
    if len(data.variables[var_name].shape)>0 and max(data.variables[var_name].shape)>0:
        output.variables[var_name][:]=data.variables[var_name][:]
    return

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
            att_val=att_val.encode('ascii','replace')
        if not att in output.ncattrs():
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

def replicate_netcdf_var(output,data,var,datatype=None,fill_value=None,add_dim=None,chunksize=None,zlib=None):
    output=replicate_netcdf_var_dimensions(output,data,var)
    if datatype==None: datatype=data.variables[var].datatype
    if (isinstance(datatype,netCDF4.CompoundType) and
        not datatype.name in output.cmptypes.keys()):
        datatype=output.createCompoundType(datatype.dtype,datatype.name)

    kwargs=dict()
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
        if chunksize:
            if chunksize==-1:
                chunksizes=tuple([1 if dim=='time' else len(output.dimensions[dim]) for dim in dimensions])
            else:
                #chunksizes=tuple([1 if output.dimensions[dim].isunlimited() else 10 for dim in dimensions])
                chunksizes=tuple([1 if dim=='time' else chunksize for dim in dimensions])
            output.createVariable(var,datatype,dimensions,chunksizes=chunksizes,**kwargs)
        else:
            if data.variables[var].chunking()=='contiguous':
                kwargs['contiguous']=True
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
            output.variables[var].setncattr(att,att_val)
    return output

def create_time_axis(output,data,time_axis):
    output.createDimension('time',len(time_axis))
    time = output.createVariable('time','d',('time',))
    time.calendar=str(data.variables['time'].calendar)
    time.units=str(data.variables['time'].units)
    time[:]=time_axis
    return

def netcdf_calendar(data):
    if 'calendar' in dir(data.variables['time']):
        calendar=data.variables['time'].calendar
    else:
        calendar='standard'
    return calendar

def convert(options,project_drs):
    database=cdb_query_archive_class.SimpleTree(options,project_drs)
    #Recover the database meta data:
    vars_list=database.list_fields_local(options,database.drs.official_drs_no_version)
    database.load_header(options)

    #Find the list of arguments to pass to convert_variable:
    args_list=[]
    for var_id,var in enumerate(vars_list):
        options_copy=copy.copy(options)
        for opt_id, opt in enumerate(database.drs.official_drs_no_version):
            setattr(options_copy,opt,var[opt_id])
        args_list.append((copy.copy(database),options_copy))
    if options.num_procs>1:
        pool=multiprocessing.Pool(processes=options.num_procs,maxtasksperchild=1)
        result=pool.map(convert_to_variable_tuple,args_list,chunksize=1)
        pool.close()
        pool.join()
    else:
        result=map(convert_to_variable_tuple,args_list)
    return

def convert_to_variable_tuple(x):
    return convert_to_variable(*x)

def convert_to_variable(database,options):
    input_file_name=options.in_diagnostic_netcdf_file

    options.version='v'+datetime.datetime.now().strftime('%Y%m%d')
    var=[getattr(options,opt) for opt in database.drs.official_drs]
    file_name=[getattr(options,opt) for opt in database.drs.filename_drs]
    output_file_name=options.out_destination+'/'+'/'.join(var)+'/'+'_'.join(file_name)
    temp_output_file_name=output_file_name+'.pid'+str(os.getpid())

    try:
        directory=os.path.dirname(temp_output_file_name)
        if not os.path.exists(directory):
            os.makedirs(directory)
    except:
        pass

    data=netCDF4.Dataset(input_file_name,'r')
    output_tmp=netCDF4.Dataset(temp_output_file_name,'w',format='NETCDF4',diskless=True,persist=True)
    #extract_netcdf_variable_recursive(output_tmp,data,options)
    #tree=zip(database.drs.official_drs_no_version,var)
    var=[getattr(options,opt) for opt in database.drs.official_drs_no_version]
    tree=zip(database.drs.official_drs_no_version,var)
    extract_netcdf_variable_recursive(output_tmp,data,tree[0],tree[1:],options)
    data.close()

    #Get the time:
    timestamp=convert_dates_to_timestamps(output_tmp,options.time_frequency)
    output_tmp.close()
    os.rename(temp_output_file_name,output_file_name+timestamp+'.nc')
    return

def convert_dates_to_timestamps(output_tmp,time_frequency):
    conversion=dict()
    conversion['year']=(lambda x: str(x.year).zfill(4))
    conversion['mon']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2))
    conversion['day']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2)+str(x.day).zfill(2))
    conversion['6hr']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2)+str(x.day).zfill(2)+str(x.hour).zfill(2)+str(x.minute).zfill(2)+str(x.second).zfill(2))
    conversion['3hr']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2)+str(x.day).zfill(2)+str(x.hour).zfill(2)+str(x.minute).zfill(2)+str(x.second).zfill(2))
    if time_frequency!='fx':
        date_axis=get_date_axis(output_tmp.variables['time'])[[0,-1]]
        return '_'+'-'.join([conversion[time_frequency](date) for date in date_axis])
    else:
        return ''


def assign_tree(output,val,sort_table,tree):
    if len(tree)>1:
        assign_tree(output.groups[tree[0]],val,sort_table,tree[1:])
    else:
        output.variables[tree[0]][sort_table]=val
    return

def apply(options,project_drs):
    if options.script=='': return
    database=cdb_query_archive_class.SimpleTree(options,project_drs)
    #Recover the database meta data:
    if options.field!=None:
        drs_to_eliminate=[field for field in database.drs.official_drs_no_version if
                                             not field in options.field]
    else:
        drs_to_eliminate=database.drs.official_drs_no_version
    vars_list=[[ var[drs_to_eliminate.index(field)] if field in drs_to_eliminate else None
                        for field in database.drs.official_drs_no_version] for var in 
                        database.list_fields_local(options,drs_to_eliminate) ]

    output_root=distributed_apply(apply_to_variable,database,options,vars_list)
    database.record_header(options,output_root)
    output_root.close()
    return

def apply_to_variable(database,options):
    input_file_name=options.in_diagnostic_netcdf_file
    files_list=[input_file_name,]+options.in_extra_netcdf_files

    output_file_name=options.out_netcdf_file+'.pid'+str(os.getpid())
    temp_output_file_name= output_file_name+'.tmp'

    var=[getattr(options,opt) for opt in database.drs.official_drs_no_version]
    tree=zip(database.drs.official_drs_no_version,var)

    temp_files_list=[]
    for file in files_list:
        data=netCDF4.Dataset(file,'r')
        temp_file=file+'.pid'+str(os.getpid())
        output_tmp=netCDF4.Dataset(temp_file,'w',format='NETCDF4',diskless=True,persist=True)
        extract_netcdf_variable_recursive(output_tmp,data,tree[0],tree[1:],options)
        temp_files_list.append(temp_file)
        output_tmp.close()
        data.close()

    script_to_call=options.script
    for file_id, file in enumerate(temp_files_list):
        if not '{'+str(file_id)+'}' in options.script:
            script_to_call+=' {'+str(file_id)+'}'

    script_to_call+=' '+temp_output_file_name

    #print '/'.join(var)
    #print script_to_call.format(*temp_files_list)
    out=subprocess.call(script_to_call.format(*temp_files_list),shell=True)

    #import shlex
    #args = shlex.split(script_to_call.format(*temp_files_list))
    #out=subprocess.call(args,shell=True)
    #out=subprocess.Popen(script_to_call.format(*temp_files_list),shell=True,
    #                     stdout=subprocess.PIPE,
    #                     stderr=subprocess.PIPE)
    #out.wait()

    try:
        for file in temp_files_list:
            os.remove(file)
    except OSError:
        pass
    return (temp_output_file_name, var)

def extract_netcdf_variable_recursive(output,data,level_desc,tree,options):
    level_name=level_desc[0]
    group_name=level_desc[1]
    if group_name==None:
        for group in data.groups.keys():
            if ( nc_Database.is_level_name_included_and_not_excluded(level_name,options,group) and
                 nc_Database.retrieve_tree_recursive_check_not_empty(options,data.groups[group])):
                output_grp=replicate_group(output,data,group)
                if len(tree)>0:
                    extract_netcdf_variable_recursive(output_grp,data.groups[group],tree[0],tree[1:],options)
                else:
                    netcdf_pointers=netcdf_soft_links.read_netCDF_pointers(data.groups[group])
                    netcdf_pointers.replicate(output_grp)
    else:
        if len(tree)>0:
            extract_netcdf_variable_recursive(output,data.groups[group_name],tree[0],tree[1:],options)
        else:
            netcdf_pointers=netcdf_soft_links.read_netCDF_pointers(data.groups[group_name])
            netcdf_pointers.replicate(output)
    return


def worker(tuple):
    result=tuple[0](*tuple[1:-1])
    tuple[-1].put(result)
    return
    
def distributed_apply(function_handle,database,options,vars_list,args=tuple()):

        #Open output file:
        output_file_name=options.out_netcdf_file
        output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')

        manager=multiprocessing.Manager()
        queue=manager.Queue()
        #queue=multiprocessing.Queue()
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
                description=queue.get()
                temp_file_name=description[0]
                var=description[1]
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

def replace_netcdf_variable_recursive(output,data,level_desc,tree):
    level_name=level_desc[0]
    group_name=level_desc[1]
    if group_name==None:
        for group in data.groups.keys():
            output_grp=create_group(output,data,group)
            output_grp.setncattr('level_name',level_name)
            if len(tree)>0:
                    replace_netcdf_variable_recursive(output_grp,data.groups[group],tree[0],tree[1:])
            else:
                netcdf_pointers=netcdf_soft_links.read_netCDF_pointers(data.groups[group])
                netcdf_pointers.replicate(output_grp)
    else:
        output_grp=create_group(output,data,group_name)
        output_grp.setncattr('level_name',level_name)
        if len(tree)>0:
            replace_netcdf_variable_recursive(output_grp,data,tree[0],tree[1:])
        else:
            netcdf_pointers=netcdf_soft_links.read_netCDF_pointers(data)
            netcdf_pointers.replicate(output_grp)
    return

    
