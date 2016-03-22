#External:
import datetime
import os
import netCDF4
import copy
import multiprocessing

#External but related:
import netcdf4_soft_links.netcdf_utils as netcdf_utils

#Internal:
import nc_Database_utils

def convert_to_variable(project_drs,options,recovered_file_list=[],recovery_queue=None):
    input_file_name=options.in_netcdf_file

    options.version='v'+datetime.datetime.now().strftime('%Y%m%d')
    var=[getattr(options,opt) for opt in project_drs.official_drs]
    file_name=[getattr(options,opt) for opt in project_drs.filename_drs]
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
    #tree=zip(project_drs.official_drs_no_version,var)
    var=[getattr(options,opt) for opt in project_drs.official_drs_no_version]
    tree=zip(project_drs.official_drs_no_version,var)
    nc_Database_utils.extract_netcdf_variable(output_tmp,data,tree,options)
    data.close()

    #Get the time:
    timestamp=convert_dates_to_timestamps(output_tmp,options.time_frequency)
    output_tmp.close()
    if timestamp=='':
        os.remove(temp_output_file_name)
    else:
        os.rename(temp_output_file_name,output_file_name+timestamp+'.nc')
    return

def convert_dates_to_timestamps(output_tmp,time_frequency):
    conversion=dict()
    conversion['year']=(lambda x: str(x.year).zfill(4))
    conversion['mon']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2))
    conversion['day']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2)+str(x.day).zfill(2))
    conversion['6hr']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2)+str(x.day).zfill(2)+str(x.hour).zfill(2)+str(x.minute).zfill(2)+str(x.second).zfill(2))
    conversion['3hr']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2)+str(x.day).zfill(2)+str(x.hour).zfill(2)+str(x.minute).zfill(2)+str(x.second).zfill(2))
    if time_frequency!='fx' and len(output_tmp.variables['time'])>0:
        date_axis=netcdf_utils.get_date_axis(output_tmp.variables['time'])[[0,-1]]
        return '_'+'-'.join([conversion[time_frequency](date) for date in date_axis])
    else:
        return ''
