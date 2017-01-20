# External:
import netCDF4
import copy
import numpy as np
import datetime

# External but related:
from ..netcdf4_soft_links import remote_netcdf, requests_sessions

# Internal:
from . import find_functions
from .. import parsers


def download_files(database, options, q_manager=None, sessions=dict()):
    return download(database, 'download_files', options, q_manager, sessions)


def download_opendap(database, options, q_manager=None, sessions=dict()):
    return download(database, 'download_opendap', options, q_manager, sessions)


def download(database, retrieval_type, options, q_manager, sessions):
    options_copy = copy.copy(options)
    if hasattr(options, 'out_download_dir'):
        # Describe the tree pattern:
        if 'version' not in database.drs.official_drs:
            options_copy.out_download_dir += '/tree/'
        elif (database.drs.official_drs[-1] == 'var' and
              database.drs.official_drs[-2] == 'version'):
            options_copy.out_download_dir += '/tree/version/var/'
        elif (database.drs.official_drs[-2] == 'var' and
              database.drs.official_drs[-1] == 'version'):
            options_copy.out_download_dir += '/tree/var/version/'
        else:
            raise NotImplementedError('If version is in the project drs, it '
                                      'must be either the last or '
                                      'next-to-last field.')

    if 'validate' in sessions:
        session = sessions['validate']
    else:
        remote_netcdf_kwargs = dict()
        if (hasattr(options, 'validate_cache') and
           getattr(options, 'validate_cache')):
            remote_netcdf_kwargs['cache'] = (getattr(options, 'validate_cache')
                                             .split(',')[0])
            if len(getattr(options, 'validate_cache').split(',')) > 1:
                (remote_netcdf_kwargs
                 ['expire_after']) = (datetime
                                      .timedelta(hours=float(getattr(
                                                              options,
                                                              'validate_cache')
                                                             .split(',')[1])))
        session = (requests_sessions
                   .create_single_session(**remote_netcdf_kwargs))

    # Recover the database meta data:
    database.load_header(options_copy)
    # Check if years should be relative, eg for piControl:
    options_copy.min_year = min_year_from_header(database.header, options_copy)

    time_slicing_names = ['year', 'month', 'day', 'hour']
    is_time_slicing_requested = np.any([True
                                        if (hasattr(options_copy,
                                                    time_slicing) and
                                            getattr(options_copy,
                                                    time_slicing) is not None)
                                        else False
                                        for time_slicing
                                        in time_slicing_names])
    # Find the data that needs to be recovered:
    database.load_database(options_copy, find_functions.simple)
    file_type_list = [item[0] for item
                      in database.nc_Database.list_fields(['file_type'])]

    if (not (set(file_type_list)
             .issubset(remote_netcdf.remote_netcdf
                       .local_queryable_file_types)) or
       is_time_slicing_requested):
        # If the data is not all local or if a time slice
        # was requested, "download"
        with netCDF4.Dataset(options_copy.out_netcdf_file, 'w') as output:
            output.set_fill_off()
            output = (database.nc_Database
                      .retrieve_database(output, options_copy,
                                         q_manager=q_manager,
                                         session=session,
                                         retrieval_type=retrieval_type))
        database.close_database()
    else:
        # Else, simply copy:
        parsers._copyfile(options, 'in_netcdf_file',
                          options_copy, 'out_netcdf_file')
        database.close_database()
    return


def _is_len_one_list(x):
    if isinstance(x, list) and len(x) == 1:
        return True
    else:
        return False


def time_split(database, options, check_split=True):
    loop_names = ['year', 'month', 'day', 'hour']

    if (not check_split or
        (options.time_frequency is not None and
         set(options.time_frequency).issubset(['fx', 'clim'])) or
       not hasattr(options, 'loop_through_time')):
        # Some time frequencies should not be time split:
        return [tuple([getattr(options, loop) for loop in loop_names])]
    elif np.all([_is_len_one_list(getattr(options, loop))
                 for loop in loop_names if loop in options.loop_through_time]):
        return [tuple([getattr(options, loop)[0]
                       if loop in options.loop_through_time
                       else getattr(options, loop) for loop in loop_names])]

    options_copy = copy.copy(options)
    # Do not use previous and next to determine time:
    for offset in ['previous', 'next']:
        if hasattr(options_copy, offset):
            setattr(options_copy, offset, 0)
    # Recover the database meta data:
    database.load_header(options_copy)
    options_copy.min_year = min_year_from_header(database.header, options_copy)

    database.load_database(options_copy, find_functions.simple)
    dates_axis = database.nc_Database.retrieve_dates(options_copy)
    database.close_database()

    if len(dates_axis) > 0:
        time_list = recursive_time_list(options_copy, dates_axis, loop_names,
                                        [True
                                         if loop in (options_copy
                                                     .loop_through_time)
                                         else False for loop in loop_names],
                                        [], [])
        valid_time_list = list(set(time_list))
        if len(valid_time_list) > 0:
            return valid_time_list
        else:
            return []
    else:
        # There are no dates corresponding to the slicing
        return []


def min_year_from_header(header, options):
    if 'experiment_list' in header:
        for experiment in header['experiment_list']:
            experiment_spec_list = header['experiment_list'][experiment]
            if not isinstance(experiment_spec_list, list):
                experiment_spec_list = [experiment_spec_list]
            min_year = np.min([int(experiment_spec.split(',')[0])
                               for experiment_spec in
                               experiment_spec_list])
            if min_year < 10:
                if not ('silent' in dir(options) and options.silent):
                    print('Using min year {0} for experiment {1}'
                          .format(str(min_year), experiment))
                return min_year


def recursive_time_list(options, dates_axis, loop_names, loop_values,
                        time_unit_names, time_unit_values):
    dates_axis_tmp = dates_axis
    for time_unit, value in zip(time_unit_names, time_unit_values):
        dates_axis_tmp = dates_axis_tmp[np.array([getattr(x,
                                                          time_unit) == value
                                                  for x in dates_axis_tmp])]

    if loop_values[0]:
        unique_loop_list = np.unique([getattr(x, loop_names[0])
                                      for x in dates_axis_tmp])
        if len(loop_names) > 1:
            return [item for sublist in map(lambda y:
                                            map(lambda x: (y,)+x,
                                                recursive_time_list(
                                                   options,
                                                   dates_axis_tmp,
                                                   loop_names[1:],
                                                   loop_values[1:],
                                                   time_unit_names +
                                                   [loop_names[0]],
                                                   time_unit_values + [y])),
                                            unique_loop_list)
                    for item in sublist]
        else:
            return map(lambda y: (y,), unique_loop_list)
    else:
        if len(loop_names) > 1:
            return [(_make_tuple(getattr(options, loop_names[0])),) + x
                    for x in recursive_time_list(options,
                                                 dates_axis_tmp,
                                                 loop_names[1:],
                                                 loop_values[1:],
                                                 time_unit_names,
                                                 time_unit_values)]
        else:
            return [(_make_tuple(getattr(options, loop_names[0])),)]


def _make_tuple(x):
    if isinstance(x, list):
        return tuple(x)
    else:
        return x
