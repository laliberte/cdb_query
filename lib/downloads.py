#External:
import netCDF4
import os
import shutil
import copy
import numpy as np

#External but related:
import netcdf4_soft_links.remote_netcdf as remote_netcdf

#Internal:
import cdb_query_archive_class

def download_files(database,options,q_manager=None,sessions=dict()):
    return download(database,'download_files',options,q_manager,sessions)

def download_opendap(database,options,q_manager=None,sessions=dict()):
    return download(database,'download_opendap',options,q_manager,sessions)

def download(database,retrieval_type,options,q_manager,sessions):
    options_copy=copy.copy(options)
    if 'out_download_dir' in dir(options):
        #Describe the tree pattern:
        if database.drs.official_drs.index('var')>database.drs.official_drs.index('version'):
            options_copy.out_download_dir+='/tree/version/var/'
        else:
            options_copy.out_download_dir+='/tree/var/version/'

    if ('swap_dir' in dir(options_copy) and options_copy.swap_dir!='.'):
        options_copy.out_netcdf_file=options_copy.swap_dir+'/'+os.path.basename(options_copy.out_netcdf_file)

    if 'validate' in sessions.keys():
        session=sessions['validate']
    else:
        session=None

    #Recover the database meta data:
    database.load_header(options_copy)
    #Check if years should be relative, eg for piControl:
    options_copy.min_year=None
    if 'experiment_list' in database.header.keys():
        for experiment in database.header['experiment_list']:
            min_year=int(database.header['experiment_list'][experiment].split(',')[0])
            if min_year<10:
                options_copy.min_year=min_year
                if not ('silent' in dir(options_copy) and options_copy.silent):
                    print 'Using min year {0} for experiment {1}'.format(str(min_year),experiment)

    time_slicing_names=['year','month','day','hour']
    is_time_slicing_requested=np.any([ True if time_slicing in dir(options_copy) and getattr(options_copy,time_slicing)!=None else False for time_slicing in time_slicing_names])
    #Find the data that needs to be recovered:
    database.load_database(options_copy,cdb_query_archive_class.find_simple)
    file_type_list=[item[0] for item in database.nc_Database.list_fields(['file_type',])]
    if (not set(file_type_list).issubset(remote_netcdf.local_queryable_file_types) or
       is_time_slicing_requested):
        #If the data is not all local or if a time slice was requested, "download"
        with netCDF4.Dataset(options_copy.out_netcdf_file,'w') as output:
            output.set_fill_off()
            output=database.nc_Database.retrieve_database(output,options_copy,q_manager=q_manager,session=session,retrieval_type=retrieval_type)
        database.close_database()
    else:
        #Else, simply copy:
        shutil.copyfile(options.in_netcdf_file,options_copy.out_netcdf_file)
        database.close_database()
    return options_copy.out_netcdf_file

def time_split(database,options):
    options_copy=copy.copy(options)
    #Do not use previous and next to determine time:
    for type in ['previous','next']:
        if type in dir(options_copy): setattr(options_copy,type,0)
    #Recover the database meta data:
    database.load_header(options_copy)
    options_copy.min_year=None
    if 'experiment_list' in database.header.keys():
        for experiment in database.header['experiment_list']:
            min_year=int(database.header['experiment_list'][experiment].split(',')[0])
            if min_year<10:
                options_copy.min_year=min_year
                if not ('silent' in dir(options_copy) and options_copy.silent):
                    print 'Using min year {0} for experiment {1}'.format(str(min_year),experiment)
    #Find the data that needs to be recovered:
    database.load_database(options_copy,cdb_query_archive_class.find_simple)
    dates_axis=database.nc_Database.retrieve_dates(options_copy)
    database.close_database()
    if len(dates_axis)>0:
        if ('loop_through_time' in dir(options) and len(options.loop_through_time)>0):
            loop_names=['year','month','day','hour']
            time_list=recursive_time_list(dates_axis,loop_names,[ True if loop in options_copy.loop_through_time else False for loop in loop_names],[],[])
            valid_time_list=list(set(time_list))
            if len(valid_time_list)>0:
                return valid_time_list
            else:
                return []
        else:
            return [(None,None,None,None),]
    else:
        #There are no dates corresponding to the slicing
        return []

def recursive_time_list(dates_axis,loop_names,loop_values,time_unit_names,time_unit_values):
    dates_axis_tmp=dates_axis
    for time_unit,value in zip(time_unit_names,time_unit_values):
        dates_axis_tmp=dates_axis_tmp[np.array(map(lambda x: getattr(x,time_unit)==value,dates_axis_tmp))]
    if loop_values[0]:
        unique_loop_list=np.unique(map(lambda x: getattr(x,loop_names[0]), dates_axis_tmp))
        if len(loop_names)>1:
            return [ item for sublist in map(lambda y: map(lambda x: (y,)+x,recursive_time_list(dates_axis_tmp,loop_names[1:],loop_values[1:],time_unit_names+[loop_names[0],],time_unit_values+[y,])),unique_loop_list)
                            for item in sublist]
        else:
            return map(lambda y:(y,),unique_loop_list)
    else:
        if len(loop_names)>1:
            return map(lambda x:(None,)+x,recursive_time_list(dates_axis_tmp,loop_names[1:],loop_values[1:],time_unit_names,time_unit_values))
        else:
            return [(None,),]
        
