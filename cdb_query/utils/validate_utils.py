# External:
import copy
from operator import itemgetter
import sqlalchemy
import numpy as np
import datetime

# Internal:
from ..nc_Database import db_manager
from ..nc_Database.db_utils \
    import is_level_name_included_and_not_excluded as inc_not_exc
from . import find_functions

queryable_file_types = ['OPENDAP', 'local_file']


def obtain_time_list(database, project_drs, var_name,
                     var_spec, experiment, model):
    conditions = ([db_manager.File_Expt.var == var_name] +
                  [db_manager.File_Expt.experiment == experiment] +
                  [getattr(db_manager.File_Expt, desc) == model[desc_id]
                   for desc_id, desc
                   in enumerate(project_drs.simulations_desc)])
    for field_id, field in enumerate(project_drs.var_specs):
        conditions.append(getattr(db_manager.File_Expt,
                                  field) == var_spec[field_id])
    time_list_var = [x[0] for x
                     in (database
                         .nc_Database
                         .session
                         .query(db_manager.File_Expt.time)
                         .filter(sqlalchemy.and_(*conditions))
                         .distinct()
                         .all())]
    return time_list_var


def delete_in_time_list(database, project_drs, var_name, var_spec,
                        experiment, model, time_list):
    """
    Reverse of obtain time list: deletes from database
    """
    conditions = ([db_manager.File_Expt.var == var_name] +
                  [db_manager.File_Expt.experiment == experiment] +
                  [getattr(db_manager.File_Expt, desc) == model[desc_id]
                   for desc_id, desc
                   in enumerate(project_drs.simulations_desc)])
    for field_id, field in enumerate(project_drs.var_specs):
        conditions.append(getattr(db_manager.File_Expt,
                                  field) == var_spec[field_id])
    combined_conditions = sqlalchemy.and_(*conditions)
    if len(time_list) > 0:
        # Do not delete too many values at the same time:
        num_sublists = (len(time_list) // 900) + 1
        for time_sub_list in np.array_split(time_list,
                                            num_sublists,
                                            axis=0):
            sub_conditions = sqlalchemy.or_(
                               *[db_manager.File_Expt.time == time_val
                                 for time_val in time_sub_list])
            (database.nc_Database.session
                                 .query(db_manager.File_Expt.time)
                                 .filter(sqlalchemy.and_(combined_conditions,
                                                         sub_conditions))
                                 .delete())
    return


def find_time_list(database, experiment, time_slices):
    (years_list,
     picontrol_min_time) = (find_functions
                            .get_years_list_from_periods(database
                                                         .header
                                                         ['experiment_list']
                                                         [experiment]))

    time_list = []
    for year in years_list:
        if (('year' in time_slices and
             (time_slices['year'] is None or
              year in time_slices['year'])) or
           ('year' not in time_slices)):
            for month in get_diag_month_list(database):
                if (('month' in time_slices and
                     (time_slices['month'] is None or
                      month in time_slices['month'])) or
                   ('month' not in time_slices)):
                    time_list.append(str(year).zfill(4) +
                                     str(month).zfill(2))
    return time_list, picontrol_min_time


def find_model_list(database, project_drs, model_list,
                    experiment, options, time_list,
                    picontrol_min_time):

    # Remove other experiments:
    model_list_no_other_exp = include_simulations(project_drs.simulations_desc,
                                                  'experiment', experiment,
                                                  model_list)
    model_list_copy = copy.copy(model_list_no_other_exp)

    for model in model_list_no_other_exp:
        missing_vars = []
        if picontrol_min_time:
            # Fix for experiments without a standard time range:
            min_time_list = []
            for var_name in database.header['variable_list']:
                for var_spec in database.header['variable_list'][var_name]:
                    if (var_spec[project_drs
                                 .var_specs
                                 .index('time_frequency')]
                       not in ['fx', 'clim']):
                        time_list_var = obtain_time_list(database, project_drs,
                                                         var_name, var_spec,
                                                         experiment, model)
                        if len(time_list_var) > 0:
                            (min_time_list
                             .append(int(np.floor(np.min([int(time)
                                                          for time
                                                          in time_list_var]) /
                                                  100.0)*100)))
            try:
                min_time = np.min(min_time_list)
            except ValueError as e:
                if e.message == ('zero-size array to reduction '
                                 'operation minimum which has no identity'):
                    min_time = 0
                else:
                    raise
        else:
            min_time = 0

        if (hasattr(options, 'missing_years') and
           options.missing_years):
            # When missing years are allowed, ensure that
            # all variables have the same times!
            all_times = set()
            valid_times = dict()
            for var_name in database.header['variable_list']:
                for var_spec in database.header['variable_list'][var_name]:
                    if (var_spec[project_drs
                                 .var_specs
                                 .index('time_frequency')] not in
                       ['fx', 'clim']):
                        inclusions_and_exclusions = [(inc_not_exc('var',
                                                                  options,
                                                                  var_name)),
                                                     (inc_not_exc('experiment',
                                                                  options,
                                                                  experiment))]

                        for field_id, field in enumerate(database
                                                         .drs.var_specs):
                            (inclusions_and_exclusions
                             .append(inc_not_exc(field, options,
                                                 var_spec[field_id])))

                        if np.all(inclusions_and_exclusions):
                            time_list_var = obtain_time_list(database,
                                                             project_drs,
                                                             var_name,
                                                             var_spec,
                                                             experiment,
                                                             model)
                            all_times.update(time_list_var)

                            time_list_var = [(str(int(time)-int(min_time))
                                              .zfill(6))
                                             for time in time_list_var]
                            valid_times[var_name] = (set(time_list)
                                                     .intersection
                                                     (time_list_var))
                            
            intersect_time = set.intersection(*valid_times.values())
            if len(intersect_time) == 0:
                missing_vars.append(','.join(valid_times.keys()) +
                                    ':' + 'have no common times.')
            else:
                # This finds the symmetric difference of multiple sets
                # i.e. it finds the elements that
                not_in_all_times = all_times.difference(
                                        [(str(int(time) + int(min_time))
                                         .zfill(6))
                                         for time in intersect_time])
                for var_name in database.header['variable_list']:
                    for var_spec in database.header['variable_list'][var_name]:
                        if (var_spec[project_drs
                                     .var_specs
                                     .index('time_frequency')]
                           not in ['fx', 'clim']):
                            delete_in_time_list(database,
                                                project_drs,
                                                var_name,
                                                var_spec,
                                                experiment,
                                                model,
                                                list(not_in_all_times))
        else:
            # When missing years are not allowed, ensure that all variables
            # have the requested times!
            for var_name in database.header['variable_list']:
                for var_spec in database.header['variable_list'][var_name]:
                    if (var_spec[project_drs.var_specs.index('time_frequency')]
                       not in ['fx', 'clim']):
                        inclusions_and_exclusions = [inc_not_exc('var',
                                                                 options,
                                                                 var_name),
                                                     inc_not_exc('experiment',
                                                                 options,
                                                                 experiment)]

                        for field_id, field in enumerate(database
                                                         .drs.var_specs):
                            (inclusions_and_exclusions
                             .append(inc_not_exc(field, options,
                                                 var_spec[field_id])))

                        if np.all(inclusions_and_exclusions):
                            time_list_var = obtain_time_list(database,
                                                             project_drs,
                                                             var_name,
                                                             var_spec,
                                                             experiment, model)
                            time_list_var = [str(int(time)-int(min_time))
                                             .zfill(6)
                                             for time in time_list_var]
                            if not set(time_list).issubset(time_list_var):
                                (missing_vars
                                 .append(var_name + ':' +
                                         ','.join(var_spec) +
                                         ' for some months: ' +
                                         (','
                                          .join(
                                           sorted(
                                            set(time[:4] for time
                                                in (set(time_list)
                                                    .difference(time_list_var))
                                                )
                                           )))))
        if len(missing_vars) > 0:
            if 'experiment' in project_drs.simulations_desc:
                if experiment == model[project_drs
                                       .simulations_desc
                                       .index('experiment')]:
                    print('_'.join(model) + ' excluded because it '
                          'is missing variables:\n'),
                    for item in missing_vars:
                        print(item)
            else:
                print('_'.join(model) + ' excluded because experiment ' +
                      experiment + ' is missing variables:\n'),
                for item in missing_vars:
                    print(item)
            model_list_copy.remove(model)
    return model_list_copy


def get_diag_month_list(database):
    if 'month_list' in database.header.keys():
        diag_month_list = database.header['month_list']
    else:
        diag_month_list = range(1, 13)
    return diag_month_list


def validate(database, options, q_manager=None, sessions=dict()):
    if 'data_node_list' in dir(database.drs):
        database.header['data_node_list'] = database.drs.data_node_list
    else:
        (data_node_list,
         url_list,
         simulations_list) = (database
                              .find_data_nodes_and_simulations(options))
        if (len(data_node_list) > 1 and
           not options.no_check_availability):
            data_node_list = (database
                              .rank_data_nodes(options,
                                               data_node_list,
                                               url_list))
        database.header['data_node_list'] = data_node_list

    semaphores = q_manager.validate_semaphores
    remote_netcdf_kwargs = dict()
    if 'validate_cache' in dir(options) and options.validate_cache:
        remote_netcdf_kwargs['cache'] = options.validate_cache.split(',')[0]
        if len(options.validate_cache.split(',')) > 1:
            remote_netcdf_kwargs['expire_after'] = (datetime
                                                    .timedelta(hours=float(
                                                               options
                                                               .validate_cache
                                                               .split(',')[1])
                                                               ))
    # Add credentials:
    remote_netcdf_kwargs.update({opt: getattr(options, opt)
                                 for opt in
                                 ['openid', 'username',
                                  'password', 'use_certifices']
                                 if opt in dir(options)})

    if 'validate' in sessions.keys():
        session = sessions['validate']
    else:
        session = None

    time_slices = db_manager.time_slices_from_options(options)

    if options.no_check_availability:
        # Does not check whether files are available /
        # queryable before proceeding.
        database.load_database(options, find_functions.time_available,
                               time_slices=time_slices,
                               semaphores=semaphores,
                               session=session,
                               remote_netcdf_kwargs=remote_netcdf_kwargs)
        # Find the list of institute / model with all the months
        # for all the years / experiments and variables requested:
        intersection(database, options, time_slices=time_slices)
        (database
         .nc_Database
         .write_database(database.header, options,
                         'record_paths',
                         semaphores=semaphores,
                         session=session,
                         remote_netcdf_kwargs=remote_netcdf_kwargs))
    else:
        # Checks that files are available.
        database.load_database(options, find_functions.time,
                               time_slices=time_slices,
                               semaphores=semaphores,
                               session=session,
                               remote_netcdf_kwargs=remote_netcdf_kwargs)
        # Find the list of institute / model with all the months
        # for all the years / experiments and variables requested:
        intersection(database, options, time_slices=time_slices)
        (database
         .nc_Database
         .write_database(database.header, options,
                         'record_meta_data',
                         semaphores=semaphores,
                         session=session,
                         remote_netcdf_kwargs=remote_netcdf_kwargs))
    database.close_database()
    return


def exclude_simulations(simulations_desc, field,
                        field_value,
                        simulations_list):
    if field in simulations_desc:
        simulations_list_limited = [simulation for simulation
                                    in simulations_list if
                                    simulation[simulations_desc
                                               .index(field)] != field_value]
    else:
        simulations_list_limited = copy.copy(simulations_list)
    return simulations_list_limited


def include_simulations(simulations_desc, field, field_value,
                        simulations_list):
    if field in simulations_desc:
        simulations_list_limited = [simulation for simulation
                                    in simulations_list if
                                    simulation[simulations_desc
                                               .index(field)] == field_value]
    else:
        simulations_list_limited = copy.copy(simulations_list)
    return simulations_list_limited


def intersection(database, options, time_slices=dict()):
    # This function finds the models that satisfy all the criteria

    # Step one: find all the institute / model tuples with
    #           all the requested variables
    #           for all months of all years for all experiments.
    simulations_list = database.nc_Database.simulations_list()

    simulations_list_no_fx = exclude_simulations(database.drs.simulations_desc,
                                                 'ensemble', 'r0i0p0',
                                                 simulations_list)

    model_list = copy.copy(simulations_list_no_fx)

    if 'experiment' not in database.drs.simulations_desc:
        for experiment in database.header['experiment_list']:
            if inc_not_exc('experiment', options, experiment):
                # Only check if experiment was not sliced
                time_list, picontrol_min_time = find_time_list(database,
                                                               experiment,
                                                               time_slices)
                if len(time_list) > 0:
                    # When time was sliced, exclude models only if there
                    # were some requested times:
                    model_list = find_model_list(database, database.drs,
                                                 model_list, experiment,
                                                 options, time_list,
                                                 picontrol_min_time)
        model_list_combined = model_list
    else:
        model_list_combined = (set()
                               .union(*[find_model_list(
                                                database, database.drs,
                                                model_list,
                                                experiment, options,
                                                *find_time_list(database,
                                                                experiment,
                                                                time_slices))
                                        for experiment
                                        in (database
                                            .header['experiment_list'])]))

    # Step two: create the new paths dictionary:
    variable_list_requested = []
    for var_name in database.header['variable_list']:
        for var_spec in database.header['variable_list'][var_name]:
            variable_list_requested.append((var_name,) + tuple(var_spec))

    # Step three: find the models to remove:
    models_to_remove = (set(simulations_list_no_fx)
                        .difference(model_list_combined))

    # Step four: remove from database:
    for model in models_to_remove:
        conditions = [getattr(db_manager.File_Expt, field) == model[field_id]
                      for field_id, field in enumerate(database
                                                       .drs.simulations_desc)]
        (database.nc_Database.session
                             .query(db_manager.File_Expt)
                             .filter(*conditions)
                             .delete())

    # Remove fixed variables:
    simulations_list = database.nc_Database.simulations_list()
    if 'ensemble' in database.drs.simulations_desc:
        simulations_list_no_fx = [simulation
                                  for simulation in simulations_list if
                                  simulation[database
                                             .drs
                                             .simulations_desc
                                             .index('ensemble')] != 'r0i0p0']
    else:
        simulations_list_no_fx = copy.copy(simulations_list)
    models_to_remove = (set([remove_ensemble(simulation, database.drs)
                             for simulation in simulations_list])
                        .difference([remove_ensemble(simulation,
                                                     database.drs)
                                     for simulation
                                     in simulations_list_no_fx]))

    for model in models_to_remove:
        conditions = [getattr(db_manager.File_Expt, field) == model[field_id]
                      for field_id, field in
                      enumerate(remove_ensemble(database
                                                .drs
                                                .simulations_desc,
                                                database.drs))]
        (database.nc_Database.session
                             .query(db_manager.File_Expt)
                             .filter(*conditions)
                             .delete())

    return


def remove_ensemble(simulation, project_drs):
    if 'ensemble' in project_drs.simulations_desc:
        sims_desc_idx_without_ensemble = range(0,
                                               len(project_drs
                                                   .simulations_desc))
        sims_desc_idx_without_ensemble.remove(project_drs
                                              .simulations_desc
                                              .index('ensemble'))
        return itemgetter(*sims_desc_idx_without_ensemble)(simulation)
    else:
        return simulation
