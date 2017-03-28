# External:
import netCDF4
# import h5netcdf.legacyapi as netCDF4_h5
# from contextlib import suppress
import copy
import datetime
import os
import numpy as np
import logging

# External but related:
from ..netcdf4_soft_links import (soft_links, remote_netcdf, ncutils)

_logger = logging.getLogger(__name__)

level_key = 'level_name'


def _read_Dataset(file_name, **kwargs):
    # try:
    #    # Make close clean when the same file is opened twice:
    #    netCDF4_h5.Dataset.close = _catch_runtime(netCDF4_h5.Dataset.close)
    #    return netCDF4_h5.Dataset(file_name, **kwargs)
    # except Exception:
    return netCDF4.Dataset(file_name, **kwargs)


# def _catch_runtime(func):
#     def func_wrapper(*args, **kwargs):
#         with suppress(RuntimeError):
#             return func(*args, **kwargs)
#     return func_wrapper


def is_level_name_included_and_not_excluded(level_name, options, group):
    if hasattr(options, level_name):
        if isinstance(getattr(options, level_name), list):
            included = ((getattr(options, level_name) == []) or
                        (group in getattr(options, level_name)))
        else:
            included = ((getattr(options, level_name) is None) or
                        (getattr(options, level_name) == group))
    else:
        included = True

    if hasattr(options, 'X' + level_name):
        if isinstance(getattr(options, 'X' + level_name), list):
            not_excluded = ((getattr(options, 'X' + level_name) == []) or
                            (group not in getattr(options, 'X' + level_name)))
        else:
            not_excluded = ((getattr(options, 'X' + level_name) is None) or
                            (getattr(options, 'X' + level_name) != group))
    else:
        not_excluded = True
    return included and not_excluded


def tree_recursive_check_not_empty(options, data, check=True, slicing=True):
    if 'soft_links' in data.groups:
        if check:
            options_dict = {opt: getattr(options, opt) for opt
                            in ['previous', 'next', 'year', 'month', 'day',
                                'hour'] if hasattr(options, opt)}
            remote_data = (soft_links.read_soft_links
                           .read_netCDF_pointers(data, **options_dict))
            return check_soft_links_size(remote_data)
        else:
            return True
    elif len(data.groups.keys()) > 0:
        empty_list = []
        for group in data.groups:
            level_name = ncutils.core.getncattr(data.groups[group], level_key)
            if is_level_name_included_and_not_excluded(level_name, options,
                                                       group):
                if slicing:
                    (empty_list
                     .append(tree_recursive_check_not_empty(options,
                                                            data.groups[group],
                                                            check=check)))
                else:
                    empty_list.append(True)
            else:
                empty_list.append(False)
        return any(empty_list)
    else:
        if len(data.variables.keys()) > 0:
            return True
        else:
            return False


def check_soft_links_size(remote_data):
    if remote_data.time_var is not None:
        # Check if time slice is leading to zero time dimension:
        if np.any(remote_data.time_restriction):
            return True
        else:
            return False
    else:
        return True


# EXTRACTIONS:
def extract_netcdf_variable(output, data, tree, options,
                            session=None, retrieval_type='reduce',
                            check_empty=False, q_manager=None):
    return extract_netcdf_variable_recursive(output, data,
                                             tree[0], tree[1:],
                                             retrieval_type,
                                             options, check_empty,
                                             q_manager, session)


def extract_netcdf_variable_recursive(output, data,
                                      level_desc, tree,
                                      retrieval_type,
                                      options, check_empty,
                                      q_manager, session):
    level_name = level_desc[0]
    group_name = level_desc[1]
    if group_name is None or isinstance(group_name, list):
        for group in data.groups:
            if (is_level_name_included_and_not_excluded(level_name,
                                                        options, group) and
               tree_recursive_check_not_empty(options, data.groups[group])):
                output_grp = ncutils.replicate.replicate_group(data, output,
                                                               group)
                extract_retrieve_or_replicate(group, output_grp, data,
                                              tree, retrieval_type,
                                              options, q_manager,
                                              check_empty, session)
    else:
        if (len(tree) > 0 and group_name == ''):
            extract_netcdf_variable_recursive(output, data,
                                              tree[0], tree[1:],
                                              retrieval_type,
                                              options, check_empty,
                                              q_manager, session)
        else:
            extract_retrieve_or_replicate(group_name, output, data,
                                          tree, retrieval_type,
                                          options, q_manager,
                                          check_empty, session)
    return


def extract_retrieve_or_replicate(group_name, output, data,
                                  tree, retrieval_type,
                                  options, q_manager,
                                  check_empty, session):
    if len(tree) > 0:
        if group_name in data.groups:
            extract_netcdf_variable_recursive(output, data.groups[group_name],
                                              tree[0], tree[1:],
                                              retrieval_type,
                                              options, check_empty,
                                              q_manager, session)
    else:
        retrieve_or_replicate(output, data, group_name, retrieval_type,
                              options, check_empty, q_manager, session)
    return


def retrieve_or_replicate(output_grp, data, group, retrieval_type,
                          options, check_empty, q_manager, session):

    remote_netcdf_kwargs = dict()
    if (hasattr(options, 'validate_cache') and
       getattr(options, 'validate_cache')):
        remote_netcdf_kwargs['cache'] = getattr(options,
                                                'validate_cache').split(',')[0]
        if len(getattr(options, 'validate_cache').split(',')) > 1:
            td = datetime.timedelta(hours=float(getattr(options,
                                                        'validate_cache')
                                                .split(',')[1]))
            remote_netcdf_kwargs['expire_after'] = td

    remote_netcdf_kwargs.update({opt: getattr(options, opt) for opt
                                 in ['openid', 'username', 'password',
                                     'use_certificates']
                                 if hasattr(options, opt)})
    options_dict = {opt: getattr(options, opt) for opt
                    in ['previous', 'next', 'year', 'month', 'day', 'hour',
                        'download_all_files', 'download_all_opendap']
                    if hasattr(options, opt)}

    options_dict['remote_netcdf_kwargs'] = remote_netcdf_kwargs

    netcdf_pointers = (soft_links.read_soft_links
                       .read_netCDF_pointers(data.groups[group],
                                             q_manager=q_manager,
                                             session=session,
                                             **options_dict))
    if retrieval_type == 'reduce_soft_links':
        # If applying to soft links, replicate.
        netcdf_pointers.replicate(output_grp, check_empty=check_empty,
                                  zlib=True)
    elif retrieval_type == 'reduce':
        if ('soft_links' not in data.groups[group].groups):
            # If there are no soft links, replicate.
            netcdf_pointers.replicate(output_grp, check_empty=check_empty,
                                      zlib=True)
        else:
            # There are soft links and they are supposed to be loaded:
            netcdf_pointers._retrieve(output_grp, 'load',
                                      filepath=options.out_netcdf_file,
                                      zlib=True)
    elif retrieval_type == 'download_files':
        netcdf_pointers._retrieve(output_grp, retrieval_type,
                                  filepath=options.out_netcdf_file,
                                  out_dir=options.out_download_dir,
                                  zlib=True)
    elif retrieval_type == 'download_opendap':
        netcdf_pointers._retrieve(output_grp, retrieval_type,
                                  filepath=options.out_netcdf_file,
                                  zlib=True)
    else:
        netcdf_pointers._retrieve(output_grp, retrieval_type,
                                  filepath=options.out_netcdf_file,
                                  zlib=True)
    return


# PUT BACK IN DATABASE:
def record_to_netcdf_file_from_file_name(options, temp_file_name, output,
                                         project_drs, check_empty=False):
    # Temporarily disable usage of h5netcdf here because of
    # incompatiblity between libraries.
    # with closing(_read_Dataset(temp_file_name, mode='r')) as data:
    with netCDF4.Dataset(temp_file_name, 'r') as data:
        var = [_fix_list_to_none(getattr(options, opt))
               if (hasattr(options, opt) and
                   getattr(options, opt) is not None) else None
               for opt in project_drs.official_drs_no_version]
        tree = list(zip(project_drs.official_drs_no_version, var))

        # Do not check empty:
        replace_netcdf_variable_recursive(output, data,
                                          tree[0], tree[1:], options,
                                          check_empty=check_empty)

    return


def replace_netcdf_variable_recursive(output, data,
                                      level_desc, tree, options,
                                      check_empty=False):
    level_name = level_desc[0]
    group_name = level_desc[1]
    if group_name is None or isinstance(group_name, list):
        for group in data.groups:
            if tree_recursive_check_not_empty(options, data.groups[group],
                                              slicing=False, check=False):
                output_grp = ncutils.replicate.create_group(data, output,
                                                            group)
                replace_netcdf_variable_recursive_replicate(
                            output_grp, data.groups[group], level_name, group,
                            tree, options, check_empty=check_empty)
    else:
        output_grp = ncutils.replicate.create_group(data, output, group_name)
        if group_name in data.groups:
            data_grp = data.groups[group_name]
        else:
            data_grp = data
        replace_netcdf_variable_recursive_replicate(
                    output_grp, data_grp, level_name, group_name,
                    tree, options,  check_empty=check_empty)
    return


def replace_netcdf_variable_recursive_replicate(output_grp, data_grp,
                                                level_name, group_name,
                                                tree, options,
                                                check_empty=False):
    if len(tree) > 0 or (group_name not in output_grp.groups):
        ncutils.core.setncattr(output_grp, level_key, level_name)
    if len(tree) > 0:
        replace_netcdf_variable_recursive(output_grp, data_grp,
                                          tree[0], tree[1:], options,
                                          check_empty=check_empty)
    else:
        netcdf_pointers = (soft_links.read_soft_links
                           .read_netCDF_pointers(data_grp))
        _logger.debug('Appending to out_netcdf_file leaf ' + output_grp.path)
        netcdf_pointers.append(output_grp, check_empty=check_empty,
                               zlib=True)
    return


# PUT INTO FILESYSTEM DATABASE
def record_to_output_directory(output_file_name, project_drs, options):
    # with closing(_read_Dataset(output_file_name, mode='r')) as data:
    with netCDF4.Dataset(output_file_name, 'r') as data:
        out_dir = options.out_destination
        with netCDF4.Dataset(output_file_name+'.tmp', 'w') as output:
            write_netcdf_variable_recursive(output, out_dir, data,
                                            project_drs.official_drs,
                                            project_drs, options,
                                            check_empty=True)
    return output_file_name + '.tmp'


def write_netcdf_variable_recursive(output, out_dir, data, tree,
                                    project_drs, options,
                                    check_empty=False):
    level_name = tree[0]
    if level_name == 'version':
        version = ('v' +
                   datetime.datetime.now().strftime('%Y%m%d'))
        sub_out_dir = make_sub_dir(out_dir, version)
        options_copy = copy.copy(options)
        setattr(options_copy, level_name, [version])
        write_netcdf_variable_recursive_replicate(
                output, sub_out_dir, data, version, tree,
                project_drs, options_copy, check_empty=check_empty)
    else:
        group_name = _fix_list(getattr(options, level_name))
        if group_name is None or isinstance(group_name, list):
            for group in data.groups:
                sub_out_dir = make_sub_dir(out_dir, group)
                output_grp = ncutils.replicate.create_group(data, output,
                                                            group)
                ncutils.core.setncattr(output_grp, level_key, group)

                options_copy = copy.copy(options)
                setattr(options_copy, level_name, [group])
                write_netcdf_variable_recursive_replicate(
                    output_grp, sub_out_dir, data.groups[group], group, tree,
                    project_drs, options_copy, check_empty=check_empty)
        else:
            sub_out_dir = make_sub_dir(out_dir, group_name)
            output_grp = ncutils.replicate.create_group(data, output,
                                                        group_name)
            ncutils.core.setncattr(output_grp, level_key, group_name)

            options_copy = copy.copy(options)
            setattr(options_copy, level_name, [group_name])
            write_netcdf_variable_recursive_replicate(
                output_grp, sub_out_dir, data, group_name, tree,
                project_drs, options_copy, check_empty=check_empty)
    return


def write_netcdf_variable_recursive_replicate(output, sub_out_dir, data_grp,
                                              group_name, tree, project_drs,
                                              options, check_empty=False):
    if len(tree) > 1:
        write_netcdf_variable_recursive(output, sub_out_dir, data_grp,
                                        tree[1:], project_drs, options,
                                        check_empty=check_empty)
    else:
        # Must propagate infos down to this level
        # var_with_version = [getattr(options, opt)[0] for opt
        #                     in project_drs.official_drs]
        output_file_name = '_'.join([getattr(options, opt)[0] for opt
                                     in project_drs.filename_drs])
        time_frequency = getattr(options, 'time_frequency')[0]
        # Get the time:
        timestamp = convert_dates_to_timestamps(data_grp, time_frequency)

        if (timestamp == '' and
           time_frequency not in ['fx', 'clim']):
            return None
        elif timestamp == '':
            output_file_name += '.nc'
        else:
            output_file_name += timestamp + '.nc'

        output_file_name = sub_out_dir + '/' + output_file_name
        with netCDF4.Dataset(output_file_name, 'w') as output_data:
            netcdf_pointers = (soft_links.read_soft_links
                               .read_netCDF_pointers(data_grp))
            netcdf_pointers.replicate(output_data, check_empty=check_empty,
                                      chunksize=-1, zlib=True)

        unique_file_id_list = ['checksum_type', 'checksum', 'tracking_id']
        path = os.path.abspath(
                    os.path.expanduser(os.path.expandvars(output_file_name)))
        paths_list = [{'path': '|'.join([path]+['' for id
                                                in unique_file_id_list]),
                       'version': options.version[0],
                       'file_type': 'local_file',
                       'data_node': remote_netcdf.remote_netcdf
                                    .get_data_node(path, 'local_file')}]

        netcdf_pointers = (soft_links.create_soft_links
                           .create_netCDF_pointers(
                                paths_list, time_frequency, options.year,
                                options.month, ['local_file'],
                                [paths_list[0]['data_node']],
                                record_other_vars=True))
        netcdf_pointers.record_meta_data(output, options.var[0])
    return


def convert_dates_to_timestamps(output_tmp, time_frequency):
    conversion = dict()
    conversion['year'] = (lambda x: str(x.year).zfill(4))
    conversion['mon'] = (lambda x: (str(x.year).zfill(4) +
                                    str(x.month).zfill(2)))
    conversion['day'] = (lambda x: (str(x.year).zfill(4) +
                                    str(x.month).zfill(2) +
                                    str(x.day).zfill(2)))
    conversion['6hr'] = (lambda x: (str(x.year).zfill(4) +
                                    str(x.month).zfill(2) +
                                    str(x.day).zfill(2) +
                                    str(x.hour).zfill(2) +
                                    str(x.minute).zfill(2) +
                                    str(x.second).zfill(2)))
    conversion['3hr'] = conversion['6hr']
    if (time_frequency != 'fx' and
        'time' in output_tmp.variables and
       len(output_tmp.variables['time']) > 0):
        date_axis = ncutils.time.get_date_axis(output_tmp, 'time')[[0, -1]]
        return '_' + '-'.join([conversion[time_frequency](date) for date
                               in date_axis])
    else:
        return ''


def make_sub_dir(out_dir, group):
    try:
        sub_out_dir = out_dir + '/' + group
        if not os.path.exists(sub_out_dir):
            os.makedirs(sub_out_dir)
    except OSError:
        pass
    return sub_out_dir


def _fix_list_to_none(x):
    return x[0] if (x is not None and len(x) == 1) else None


def _fix_list(x):
    return x[0] if (x is not None and len(x) == 1) else x
