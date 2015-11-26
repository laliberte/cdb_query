import netcdf_utils
import datetime

import os

import netCDF4

import cdb_query_archive_class

import nc_Database_utils

import copy

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
        try:
            result=pool.map(convert_to_variable_tuple,args_list,chunksize=1)
        finally:
            pool.terminate()
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
    #nc_Database_utils.extract_netcdf_variable_recursive(output_tmp,data,options)
    #tree=zip(database.drs.official_drs_no_version,var)
    var=[getattr(options,opt) for opt in database.drs.official_drs_no_version]
    tree=zip(database.drs.official_drs_no_version,var)
    nc_Database_utils.extract_netcdf_variable_recursive(output_tmp,data,tree[0],tree[1:],options)
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
        date_axis=netcdf_utils.get_date_axis(output_tmp.variables['time'])[[0,-1]]
        return '_'+'-'.join([conversion[time_frequency](date) for date in date_axis])
    else:
        return ''
