#External:
import netCDF4
import os
import copy
import numpy as np

#Internal:
import cdb_query_archive_class

def download_files(database,options,q_manager=None):
    return download(database,'download_files',options,q_manager)

def download_opendap(database,options,q_manager=None):
    return download(database,'download_opendap',options,q_manager)

#def load(database,options,q_manager=None):
#    return download(database,'load',options,q_manager)

def download(database,retrieval_type,options,q_manager):
    options_copy=copy.copy(options)
    if 'out_download_dir' in dir(options):
        #Describe the tree pattern:
        if database.drs.official_drs.index('var')>database.drs.official_drs.index('version'):
            options_copy.out_download_dir+='/tree/version/var/'
        else:
            options_copy.out_download_dir+='/tree/var/version/'

    if ('swap_dir' in dir(options_copy) and options_copy.swap_dir!='.'):
        options_copy.out_netcdf_file=options_copy.swap_dir+'/'+os.path.basename(options_copy.out_netcdf_file)

    output=netCDF4.Dataset(options_copy.out_netcdf_file,'w')
    output.set_fill_off()
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

    #Find the data that needs to be recovered:
    database.load_database(options_copy,cdb_query_archive_class.find_simple)
    output=database.nc_Database.retrieve_database(output,options_copy,q_manager=q_manager,retrieval_type=retrieval_type)
    output.close()
    database.close_database()
    return options_copy.out_netcdf_file

def time_split(database,options):
    if not ('loop_through_time' in dir(options) and len(options.loop_through_time)>0):
        return [(None,None,None,None),]
    elif np.all([ True if (getattr(options,loop)!=None and len(getattr(options,loop))==1)
                            else False for loop in options.loop_through_time]):
        #The time list has already been set, do not redo it!
        return [(None,None,None,None),]
    else:
        options_copy=copy.copy(options)
        #Do not use previous and next to determine time:
        for type in ['previous','next']:
            if type in dir(options_copy): setattr(options,type,0)
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
            loop_names=['year','month','day','hour']
            time_list=recursive_time_list(dates_axis,loop_names,[ True if loop in options_copy.loop_through_time else False for loop in loop_names],[],[])
            return list(set(time_list))
        else:
            return [(None,None,None,None),]

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
        
