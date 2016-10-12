#External:
import copy
import os.path

#External but related:
import netcdf4_soft_links.remote_netcdf.remote_netcdf as remote_netcdf

def path(database,file_expt,
         time_slices=dict(),semaphores=dict(),
         session=None,remote_netcdf_kwargs=dict()):
    for val in dir(file_expt):
        if val[0]!='_' and val!='case_id':
            getattr(file_expt,val)
    database.session.add(file_expt)
    database.session.commit()
    return

def simple(database,file_expt,
           time_slices=dict(),semaphores=None,
           session=None,remote_netcdf_kwargs=dict()):
    database.session.add(file_expt)
    database.session.commit()
    return

def time(database,file_expt,
         time_slices=dict(),semaphores=dict(),
         session=None,remote_netcdf_kwargs=dict()):
    #Will check both availability and queryability:
    time_file(database,file_expt,time_slices=time_slices,semaphores=semaphores,
                                                    session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
    return

def time_available(database,file_expt,
                   time_slices=dict(),semaphores=dict(),
                   session=None,remote_netcdf_kwargs=dict()):
    #same as time but keeps non-queryable files:
    time_file(database,file_expt,file_available=True,time_slices=time_slices,semaphores=semaphores,
                                                            session=session,remote_netcdf_kwargs=remote_netcdf_kwargs)
    return

def time_file(database,file_expt,file_available=False,
              time_slices=dict(),semaphores=dict(),
              session=None,remote_netcdf_kwargs=dict()):
    #If the path is a remote file, we must use the time stamp
    filename = os.path.basename(file_expt.path)
    
    #Check if file has fixed time_frequency or is a climatology:
    if file_expt.time_frequency in ['fx']:
        database.session.add(file_expt)
        database.session.commit()
        return
    else:
        time_stamp=filename.replace('.nc','').split('_')[-1].split('|')[0]

    if not file_expt.experiment in database.header['experiment_list']:
        return

    #Find the years that were requested:
    years_requested=[int(year) for year in database.header['experiment_list'][file_expt.experiment].split(',')]
    years_list_requested=range(*years_requested)
    years_list_requested.append(years_requested[1])

    #Flag to check if the time axis is requested as relative:
    picontrol_min_time=(years_list_requested[0]<=10)

    #Recover date range from filename:
    years_range = [int(date[:4]) for date in time_stamp.split('-')]
    #Check for yearly data
    if len(time_stamp.split('-')[0])>4:
        months_range=[int(date[4:6]) for date in time_stamp.split('-')]
    else:
        months_range=range(1,13)
    years_list=range(*years_range)
    years_list.append(years_range[1])

    #Tweaked to allow for relative years:
    if not picontrol_min_time: years_list=[ year for year in years_list if year in years_list_requested]

    #Time was further sliced:
    if ('year' in time_slices and
         time_slices['year']!=None):
        years_list=[year for year in years_list if year in time_slices['year']]

    months_list=range(1,13)
    if ('month' in time_slices and
        time_slices['month']!=None):
        months_list=[month for month in months_list if month in time_slices['month']]

    #Record in the database:
    checked_availability=False
    for year_id,year in enumerate(years_list):
        for month in months_list:
            if  ( not ( (year==years_range[0] and month<months_range[0]) or
                     (year==years_range[1] and month>months_range[1])   ) ):
                if (not file_available and
                   not checked_availability):
                    checked_availability=True
                    if file_expt.file_type in ['local_file']:
                        file_available=True
                    else:
                        remote_data=remote_netcdf.remote_netCDF(file_expt.path.split('|')[0],
                                                                file_expt.file_type,
                                                                semaphores=semaphores,
                                                                session=session,
                                                                **remote_netcdf_kwargs)
                        file_available=remote_data.is_available()
                _attribute_time(database,file_expt,file_available,year,month)
    return

def _attribute_time(database,file_expt,file_available,year,month):
    #Record checksum of local files:
    #if file_expt.file_type in ['local_file'] and len(file_expt.path.split('|')[1])==0:
    #    #Record checksum
    #    file_expt.path+=retrieval_utils.md5_for_file(open(file_expt.path.split('|')[0],'r'))

    if file_available:
        #If file is avaible and queryable, keep it:
        file_expt_copy = copy.deepcopy(file_expt)
        setattr(file_expt_copy,'time',str(year).zfill(4)+str(month).zfill(2))
        database.session.add(file_expt_copy)
        database.session.commit()
    return
