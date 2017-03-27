# External:
import netCDF4
import copy
import subprocess
import os
import tempfile
import numpy as np
import shutil
import errno
from contextlib import closing
import logging

# Internal:
from ..nc_Database import db_utils
from . import downloads_utils

_logger = logging.getLogger(__name__)


def _fix_list(x):
    if len(x) == 1:
        return x[0]
    else:
        return x


def _convert_list(x):
    if x is None:
        return ''
    try:
        return str(x)
    except TypeError:
        # Input is list:
        return ' '.join(str(y) for y in x)


def make_list(item):
    if isinstance(item, list):
        return item
    elif (isinstance(item, set) or isinstance(item, tuple)):
        return list(item)
    else:
        if item is not None:
            return [item]
        else:
            return None


def set_new_var_options(options_copy, var_item, official_drs_no_version):
    for opt_id, opt in enumerate(official_drs_no_version):
        if var_item[opt_id] is not None:
            setattr(options_copy, opt, make_list(var_item[opt_id]))
    return


def set_new_time_options(options_copy, time_item):
    for opt_id, opt in enumerate(['year', 'month', 'day', 'hour']):
        if (time_item[opt_id] is not None and
           opt in dir(options_copy)):
            setattr(options_copy, opt, make_list(time_item[opt_id]))
    return


# Do it by simulation, except if one simulation field should be kept
# for further operations:
def reduce_var_list(database, options):
    if (hasattr(options, 'keep_field') and options.keep_field is not None):
        drs_to_eliminate = [field for field
                            in database.drs.official_drs_no_version
                            if field not in options.keep_field]
    else:
        drs_to_eliminate = database.drs.official_drs_no_version
    var_list = [[make_list(item) for item in var_list]
                for var_list in
                set([tuple([tuple(sorted(set(make_list(var[drs_to_eliminate
                                                           .index(field)]))))
                            if field in drs_to_eliminate else None
                            for field in database.drs.official_drs_no_version])
                     for var in
                     (database
                      .list_fields_local(options, drs_to_eliminate,
                                         soft_links=False))])]
    if len(var_list) > 1 and 'ensemble' in database.drs.official_drs_no_version:
        # This is a fix necessary for MOHC models.
        if 'var' in drs_to_eliminate:
            var_index = database.drs.official_drs_no_version.index('var')
            var_names = set([tuple(x[var_index]) for x in var_list])
            if len(var_names) == 1:
                ensemble_index = (database.drs
                                  .official_drs_no_version
                                  .index('ensemble'))
                ensemble_names = np.unique(
                                    np.concatenate([tuple(x[ensemble_index])
                                                    for x in var_list]))
                if 'r0i0p0' in ensemble_names:
                    for var in var_list:
                        if 'r0i0p0' in var[ensemble_index]:
                            return [var]
    return var_list


def reduce_soft_links(database, options, q_manager=None, sessions=dict()):

    vars_list = reduce_var_list(database, options)
    with netCDF4.Dataset(options.out_netcdf_file, 'w', diskless=True,
                         persist=True) as output:
        for var in vars_list:
            options_copy = copy.copy(options)
            set_new_var_options(options_copy, var,
                                database.drs.official_drs_no_version)
            _logger.debug('Reducing soft_links ' + str(var))
            tmp_out_fn_one_var = reduce_sl_or_var(
                                    database, options_copy,
                                    q_manager=q_manager,
                                    sessions=sessions,
                                    retrieval_type='reduce_soft_links',
                                    script=options.reduce_soft_links_script)
            db_utils.record_to_netcdf_file_from_file_name(options_copy,
                                                          tmp_out_fn_one_var,
                                                          output, database.drs)
            _logger.debug('Done reducing soft_links ' + str(var))

            try:
                os.remove(tmp_out_fn_one_var)
            except Exception:
                pass
    return


def reduce_variable(database, options, q_manager=None, sessions=dict(),
                    retrieval_type='reduce'):
    vars_list = reduce_var_list(database, options)
    with netCDF4.Dataset(options.out_netcdf_file, 'w', diskless=True,
                         persist=True) as output:
        for var in vars_list:
            options_copy = copy.copy(options)
            set_new_var_options(options_copy, var,
                                database.drs.official_drs_no_version)
            times_list = downloads_utils.time_split(database, options_copy)
            for time in times_list:
                options_copy_time = copy.copy(options_copy)
                set_new_time_options(options_copy_time, time)
                _logger.debug('Reducing variables ' + str(var) + ' ' +
                              str(time))
                tmp_out_fn_one_var = reduce_sl_or_var(
                                        database, options_copy_time,
                                        q_manager=q_manager, sessions=sessions,
                                        retrieval_type='reduce',
                                        script=options.script)
                db_utils.record_to_netcdf_file_from_file_name(
                                                          options_copy_time,
                                                          tmp_out_fn_one_var,
                                                          output, database.drs)
                _logger.debug('Done Reducing variables ' + str(var) + ' ' +
                              str(time))

                try:
                    os.remove(tmp_out_fn_one_var)
                except Exception:
                    pass
    return


def reduce_sl_or_var(database, options, q_manager=None, sessions=dict(),
                     retrieval_type='reduce', script=''):
    # The leaf(ves) considered here:
    # Warning! Does not allow --serial option:
    var = [_fix_list(getattr(options, opt))
           if (getattr(options, opt) is not None
               and hasattr(options, 'keep_field')
               and opt not in options.keep_field)
           else None for opt in database.drs.official_drs_no_version]
    tree = list(zip(database.drs.official_drs_no_version, var))

    time_desc = ['year', 'month', 'day', 'hour']
    time_var = [_fix_list(getattr(options, opt))
                if getattr(options, opt) is not None
                else None for opt in time_desc]
    time_tree = list(zip(time_desc, time_var))

    # Decide whether to add fixed variables:
    tree_fx, options_fx = get_fixed_var_tree(database.drs, options, var)

    # Define temp_output_file_name:
    fileno, temp_output_file_name = tempfile.mkstemp(dir=options.swap_dir)
    # must close fileno
    os.close(fileno)

    file_name_list = get_input_file_names(database.drs, options, script)
    temp_file_name_list = []

    if 'validate' in sessions:
        session = sessions['validate']
    else:
        session = None

    for file_name in file_name_list:
        fileno, temp_file_name = tempfile.mkstemp(dir=options.swap_dir)
        # must close fileno
        os.close(fileno)

        extract_single_tree(temp_file_name, file_name, tree, tree_fx,
                            options, options_fx,
                            retrieval_type=retrieval_type,
                            session=session,
                            check_empty=(retrieval_type == 'reduce'),
                            q_manager=q_manager)
        temp_file_name_list.append(temp_file_name)

    if hasattr(options, 'sample') and options.sample:
        try:
            os.makedirs(options.out_destination)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        for file in temp_file_name_list:
            shutil.copy(file, (options.out_destination + '/' +
                               os.path.basename(file)))

    if script.strip() == '':
        os.rename(temp_file_name_list[0], temp_output_file_name)
    else:
        # If script is not empty, call script:
        temp_file_name_list.append(temp_output_file_name)
        script_to_call = script
        for file_id, file in enumerate(temp_file_name_list):
            if '{' + str(file_id) + '}' not in script:
                script_to_call += ' {' + str(file_id) + '}'

        # Remove temp_output_file_name to avoid programs that
        # request before overwriting:
        os.remove(temp_output_file_name)
        try:
            environment = dict([(val[0], _convert_list(val[1]))
                                for val in tree + time_tree])
            _logger.debug(environment)
            output = subprocess.check_output(script_to_call
                                             .format(*temp_file_name_list),
                                             shell=True,
                                             env=environment,
                                             stderr=subprocess.STDOUT)
            # Capture subprocess errors to print output:
            for line in iter(output.splitlines()):
                if hasattr(line, 'decode'):
                    line = line.decode('ascii', 'replace')
                _logger.info(line)
        except subprocess.CalledProcessError as e:
            # Capture subprocess errors to print output:
            for line in iter(e.output.splitlines()):
                if hasattr(line, 'decode'):
                    line = line.decode('ascii', 'replace')
                _logger.info(line)
            raise

    try:
        for file in temp_file_name_list[:-1]:
            os.remove(file)
    except OSError:
        pass

    if retrieval_type == 'reduce_soft_links':
        processed_output_file_name = temp_output_file_name + '.tmp'
        with netCDF4.Dataset(processed_output_file_name, 'w') as output:
            (db_utils
             .record_to_netcdf_file_from_file_name(options,
                                                   temp_output_file_name,
                                                   output, database.drs))
    else:
        # This is the last function in the chain.
        # Convert and create soft links:
        processed_output_file_name = (db_utils
                                      .record_to_output_directory(
                                           temp_output_file_name, database.drs,
                                           options))
    try:
        os.remove(temp_output_file_name)
        os.rename(processed_output_file_name, temp_output_file_name)
    except OSError:
        pass
    return temp_output_file_name


def extract_single_tree(temp_file, src_file, tree, tree_fx,
                        options, options_fx, q_manager=None,
                        session=None, retrieval_type='reduce',
                        check_empty=False):
    with closing(db_utils._read_Dataset(src_file, mode='r')) as data:
        with netCDF4.Dataset(temp_file, 'w', format='NETCDF4',
                             diskless=True, persist=True) as output_tmp:
            if (hasattr(options, 'add_fixed') and options.add_fixed):
                db_utils.extract_netcdf_variable(output_tmp, data, tree_fx,
                                                 options_fx, session=session,
                                                 retrieval_type=retrieval_type,
                                                 check_empty=True,
                                                 q_manager=q_manager)

            db_utils.extract_netcdf_variable(output_tmp, data, tree, options,
                                             session=session,
                                             retrieval_type=retrieval_type,
                                             check_empty=check_empty,
                                             q_manager=q_manager)
    return


def get_fixed_var_tree(project_drs, options, var):
    if not (hasattr(options, 'add_fixed') and options.add_fixed):
        return None, None

    # Specification for fixed vars:
    var_fx = [getattr(options, opt)
              if opt not in project_drs.var_specs + ['var']
              else None
              for opt in project_drs.official_drs_no_version]
    var_fx = copy.copy(var)
    var_fx[project_drs.official_drs_no_version.index('ensemble')] = 'r0i0p0'
    var_fx[project_drs.official_drs_no_version.index('var')] = None
    for opt in project_drs.var_specs + ['var']:
        if (opt in ['time_frequency', 'cmor_table'] and
           var[project_drs.official_drs_no_version.index(opt)] is not None):
            var_fx[project_drs.official_drs_no_version.index(opt)] = 'fx'

    tree_fx = list(zip(project_drs.official_drs_no_version, var_fx))
    options_fx = copy.copy(options)
    for opt_id, opt in enumerate(tree_fx):
        if getattr(options_fx, opt[0]) != tree_fx[opt_id]:
            setattr(options_fx, opt[0], opt[1])
            if (hasattr(options_fx, 'X' + opt[0]) and
                isinstance(getattr(options_fx, 'X' + opt[0]), list) and
               opt[1] in getattr(options_fx, 'X' + opt[0])):
                getattr(options_fx, 'X' + opt[0]).remove(opt[1])
    return tree_fx, options_fx


def get_input_file_names(project_drs, options, script):
    if (script.strip() == '' and
        (hasattr(options, 'in_extra_netcdf_files') and
         len(options.in_extra_netcdf_files) > 0)):
        raise Exception('The identity script \'\' can only be used when no'
                        ' extra netcdf files are specified.')

    input_file_name = options.in_netcdf_file
    file_name_list = [input_file_name]
    if hasattr(options, 'in_extra_netcdf_files'):
        file_name_list += options.in_extra_netcdf_files
    return file_name_list
