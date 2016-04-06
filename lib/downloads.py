#External:
import netCDF4
import os
import copy

#Internal:
import cdb_query_archive_class

def download_files(database,options,queues_manager=None):
    return download(database,'download_files',options,queues_manager)

def download_opendap(database,options,queues_manager=None):
    return download(database,'download_opendap',options,queues_manager)

#def load(database,options,queues_manager=None):
#    return download(database,'load',options,queues_manager)

def download(database,retrieval_type,options,queues_manager):
    options_copy=copy.copy(options)
    if 'out_destination' in dir(options):
        #Describe the tree pattern:
        if database.drs.official_drs.index('var')>database.drs.official_drs.index('version'):
            options_copy.out_destination+='/tree/version/var/'
        else:
            options_copy.out_destination+='/tree/var/version/'

    if ('swap_dir' in dir(options_copy) and options_copy.swap_dir!='.'):
        options_copy.out_netcdf_file=options_copy.swap_dir+'/'+os.path.basename(options_copy.out_netcdf_file)

    output=netCDF4.Dataset(options_copy.out_netcdf_file,'w')
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
    output=database.nc_Database.retrieve_database(output,options_copy,queues_manager=queues_manager,retrieval_type=retrieval_type)
    output.close()
    database.close_database()
    return options_copy.out_netcdf_file

def time_split(database,options,queues_manager=None):
    output_file_name=options.out_netcdf_file
    if ('swap_dir' in dir(options) and options.swap_dir!='.'):
        output_file_name=options.swap_dir+'/'+os.path.basename(output_file_name)

    #Recover the database meta data:
    database.load_header(options)
    options.min_year=None
    if 'experiment_list' in database.header.keys():
        for experiment in database.header['experiment_list']:
            min_year=int(database.header['experiment_list'][experiment].split(',')[0])
            if min_year<10:
                options.min_year=min_year
                if not ('silent' in dir(options) and options.silent):
                    print 'Using min year {0} for experiment {1}'.format(str(min_year),experiment)
    #Find the data that needs to be recovered:
    database.load_database(options,cdb_query_archive_class.find_simple)
    dates_axis=database.nc_Database.retrieve_dates(options)
    database.close_database()
    return output_file_name
