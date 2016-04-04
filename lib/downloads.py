import cdb_query_archive_class

def download_files(database,options,queues_manager=None):
    output=options.out_destination
    #Describe the tree pattern:
    if self.drs.official_drs.index('var')>self.drs.official_drs.index('version'):
        output+='/tree/version/var/'
    else:
        output+='/tree/var/version/'
    
    download(database,options,output,queues_manager)
    return

def download_opendap(database,options,queues_manager=None):
    output_file_name=options.out_netcdf_file
    if ('swap_dir' in dir(options) and options.swap_dir!='.'):
        output_file_name=options.swap_dir+'/'+os.path.basename(output_file_name)
    download(database,options,output_file_name,queues_manager)
    return

def load(database,options,queues_manager=None):
    output_file_name=options.out_netcdf_file
    if ('swap_dir' in dir(options) and options.swap_dir!='.'):
        output_file_name=options.swap_dir+'/'+os.path.basename(output_file_name)
    #download_and_download_raw(database,options,output_file_name,queues_manager)
    return

def download(database,options,output_file_name,queues_manager):
    output=netCDF4.Dataset(output_file_name,'w')

    #Recover the database meta data:
    database.load_header(options)
    #Check if years should be relative, eg for piControl:
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
    database.nc_Database.retrieve_database(options,output,queues_manager=queues_manager)
    database.close_database()
    return

def time_split(database,options,queues_manager=None):
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
    print dates_axis
    database.close_database()
    return options.out_netcdf_file
