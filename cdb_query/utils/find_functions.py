# External:
import copy
import os.path

# External but related:
from ..netcdf4_soft_links import remote_netcdf


def path(database, file_expt,
         time_slices=dict(), semaphores=dict(),
         session=None, remote_netcdf_kwargs=dict()):
    for val in dir(file_expt):
        if val[0] != '_' and val != 'case_id':
            getattr(file_expt, val)
    database.session.add(file_expt)
    database.session.commit()
    return


def simple(database, file_expt, time_slices=dict(), semaphores=None,
           session=None, remote_netcdf_kwargs=dict()):
    database.session.add(file_expt)
    database.session.commit()
    return


def time(database, file_expt,
         time_slices=dict(), semaphores=dict(),
         session=None, remote_netcdf_kwargs=dict()):
    # Will check both availability and queryability:
    time_file(database, file_expt, time_slices=time_slices,
              semaphores=semaphores, session=session,
              remote_netcdf_kwargs=remote_netcdf_kwargs)
    return


def time_available(database, file_expt,
                   time_slices=dict(), semaphores=dict(),
                   session=None, remote_netcdf_kwargs=dict()):
    # same as time but keeps non-queryable files:
    time_file(database, file_expt, file_available=True,
              time_slices=time_slices,
              semaphores=semaphores, session=session,
              remote_netcdf_kwargs=remote_netcdf_kwargs)
    return


def time_file(database, file_expt, file_available=False,
              time_slices=dict(), semaphores=dict(),
              session=None, remote_netcdf_kwargs=dict()):
    # If the path is a remote file, we must use the time stamp
    filename = os.path.basename(file_expt.path)

    # Check if file has fixed time_frequency or is a climatology:
    if file_expt.time_frequency in ['fx']:
        database.session.add(file_expt)
        database.session.commit()
        return
    else:
        time_stamp = (filename.replace('.nc', '')
                      .split('_')[-1].split('|')[0])

    if file_expt.experiment not in database.header['experiment_list']:
        return

    # Find the years that were requested:
    (years_list_requested,
     picontrol_min_time) = get_years_list_from_periods(database
                                                       .header
                                                       ['experiment_list']
                                                       [file_expt.experiment])

    # Recover date range from filename:
    years_range = [int(date[:4]) for date in time_stamp.split('-')]
    # Check for yearly data
    if len(time_stamp.split('-')[0]) > 4:
        months_range = [int(date[4:6]) for date in time_stamp.split('-')]
    else:
        months_range = list(range(1, 13))
    years_list = list(range(*years_range))
    years_list.append(years_range[1])

    # Tweaked to allow for relative years:
    if not picontrol_min_time:
        years_list = [year for year in years_list
                      if year in years_list_requested]

    # Time was further sliced:
    if ('year' in time_slices and
       time_slices['year'] is not None):
        years_list = [year for year in years_list
                      if year in time_slices['year']]

    months_list = list(range(1, 13))
    if ('month' in time_slices and
       time_slices['month'] is not None):
        months_list = [month for month in months_list
                       if month in time_slices['month']]

    # Record in the database:
    checked_availability = False
    for year_id, year in enumerate(years_list):
        for month in months_list:
            if (not((year == years_range[0] and month < months_range[0]) or
                    (year == years_range[1] and month > months_range[1]))):
                if (not file_available and
                   not checked_availability):
                    checked_availability = True
                    if file_expt.file_type in ['local_file']:
                        file_available = True
                    else:
                        remote_data = (remote_netcdf.remote_netcdf
                                       .remote_netCDF(file_expt.path
                                                      .split('|')[0],
                                                      file_expt.file_type,
                                                      semaphores=semaphores,
                                                      session=session,
                                                      **remote_netcdf_kwargs))
                        file_available = remote_data.is_available()
                _attribute_time(database, file_expt, file_available,
                                year, month)
    return


def get_years_list_from_periods(period_list):
    # Determine the requested time list:
    if not isinstance(period_list, list):
        period_list = [period_list]
    years_list = []
    for period in period_list:
        years_range = [int(year) for year in period.split(',')]
        years_list.extend(range(*years_range))
        years_list.append(years_range[1])
    years_list = sorted(set(years_list))
    # Flag to check if the time axis is requested as relative:
    picontrol_min_time = (years_list[0] <= 10)
    return years_list, picontrol_min_time


def _attribute_time(database, file_expt, file_available, year, month):
    # Record checksum of local files:

    if file_available:
        # If file is avaible and queryable, keep it:
        file_expt_copy = copy.deepcopy(file_expt)
        setattr(file_expt_copy, 'time',
                str(year).zfill(4) + str(month).zfill(2))
        database.session.add(file_expt_copy)
        database.session.commit()
    return
