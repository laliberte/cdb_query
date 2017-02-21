# External:
import sqlalchemy
import sqlalchemy.orm
import json
import netCDF4
import copy
import numpy as np
from functools import reduce
from contextlib import closing

# External but related:
from ..netcdf4_soft_links import (remote_netcdf, soft_links,
                                  retrieval_manager, ncutils)

# Internal:
from .db_utils import _read_Dataset as read_Dataset
from .db_utils import is_level_name_included_and_not_excluded \
               as is_ln_inc_and_not_exc
from . import db_utils
from .. import commands_parser
from ..utils import find_functions

level_key = db_utils.level_key


class nc_Database:
    def __init__(self, project_drs, database_file=None):
        # Defines the tree structure:
        self.drs = project_drs
        self.db_file = database_file

        self._setup_database()
        return

    def _setup_database(self):
        # Create an in-memory sqlite database, for easy subselecting.
        # Uses sqlalchemy
        self.engine = sqlalchemy.create_engine('sqlite:///:memory:',
                                               echo=False)
        self.metadata = sqlalchemy.MetaData(bind=self.engine)

        self.time_db = sqlalchemy.Table(
                        'time_db', self.metadata,
                        sqlalchemy.Column('case_id', sqlalchemy.Integer,
                                          primary_key=True),
                        *(sqlalchemy.Column(level_name, sqlalchemy.String(255))
                          for level_name in self.drs.base_drs))
        self.metadata.create_all()
        sqlalchemy.orm.clear_mappers()
        sqlalchemy.orm.mapper(File_Expt, self.time_db)

        self.file_expt = File_Expt(self.drs.base_drs)
        self.session = sqlalchemy.orm.create_session(bind=self.engine,
                                                     autocommit=False,
                                                     autoflush=True)
        return

    def close_database(self):
        self.session.close()
        self.engine.dispose()
        self._setup_database()
        return

    def load_header(self):
        # Load header:
        header = dict()
        with closing(read_Dataset(self.db_file, mode='r')) as dataset:
            for att in (set(self.drs.header_desc)
                        .intersection(dataset.ncattrs())):
                header[att] = json.loads(ncutils.core.getncattr(dataset, att))
        return header

    def populate_database(self, options, find_function, soft_links=True,
                          time_slices=dict(), semaphores=dict(),
                          session=None, remote_netcdf_kwargs=dict()):
        self.file_expt.time = '0'
        with closing(read_Dataset(self.db_file, mode='r')) as dataset:
            populate_database_recursive(
                                self, dataset, options, find_function,
                                soft_links=soft_links,
                                time_slices=time_slices,
                                semaphores=semaphores, session=session,
                                remote_netcdf_kwargs=remote_netcdf_kwargs)

        # Allow complex queries:
        if (hasattr(options, 'field') and
            options.field != [] and
           options.field is not None):
            if ((hasattr(options, 'complex_query') and
                 options.complex_query != []) or
                (hasattr(options, 'Xcomplex_query') and
                 options.Xcomplex_query != [])):
                list_query = self.list_fields(options.field)
                for query in list_query:
                    if ((options.complex_query != [] and
                         query not in options.complex_query) or
                        (options.Xcomplex_query != [] and
                         query in options.Xcomplex_query)):
                        conditions = [(getattr(File_Expt, field) ==
                                       query[field_id])
                                      for field_id, field
                                      in enumerate(options.field)]
                        (self.session
                         .query(File_Expt)
                         .filter(*conditions)
                         .delete())
        return

    def simulations_list(self):
        subset_desc = (getattr(File_Expt, item)
                       for item in self.drs.simulations_desc)
        simulations_list = self.list_subset(subset_desc)
        return simulations_list

    def list_subset(self, subset):
        subset_list = self.session.query(*subset).distinct().all()
        return subset_list

    def list_fields(self, fields_to_list):
        fields_list = sorted(list(set(
                        self.list_subset((getattr(File_Expt, field)
                                          for field in fields_to_list)))))
        return fields_list

    def list_data_nodes(self, options):
        data_node_list = self.list_subset((File_Expt.data_node, ))
        return [data_node[0] for data_node in data_node_list
                if is_ln_inc_and_not_exc('data_node', options, data_node[0])]

    def list_paths_by_data_node(self, data_node):
        return (self.session
                .query(*(File_Expt.path, File_Expt.file_type))
                .filter(File_Expt.data_node == data_node).first())

    def list_paths(self):
        subset = tuple([File_Expt.path] + [getattr(File_Expt, item)
                                           for item in self.drs.official_drs])
        return sorted(list(set(self.list_subset(subset))))

    def write_database(self, header, options, record_function_handle,
                       semaphores=dict(), session=None,
                       remote_netcdf_kwargs=dict()):
        # List all the trees:
        drs_list = copy.copy(self.drs.base_drs)

        drs_to_remove = [drs for drs in ['path', 'data_node', 'file_type',
                                         'version', 'time']
                         if drs in drs_list]
        for drs in drs_to_remove:
            drs_list.remove(drs)
        # Remove the time:
        drs_to_remove.remove('time')

        # Check if time was sliced:
        time_slices = time_slices_from_options(options)

        # Find the unique tuples:
        trees_list = self.list_subset([getattr(File_Expt, level)
                                       for level in drs_list])

        with netCDF4.Dataset(options.out_netcdf_file,
                             'w', format='NETCDF4',
                             diskless=True, persist=True) as output_root:
            record_header(output_root, header, options=options)

            # Define time subset:
            if 'month_list' in header:
                months_list = header['month_list']
            else:
                months_list = range(1, 13)

            for tree in trees_list:
                time_frequency = tree[drs_list.index('time_frequency')]
                experiment = tree[drs_list.index('experiment')]
                var = tree[drs_list.index('var')]
                conditions = [getattr(File_Expt, level) == value
                              for level, value in zip(drs_list, tree)]
                out_tuples = [getattr(File_Expt, level)
                              for level in drs_to_remove]
                # Find list of paths:
                paths_list = [{drs_name: path[drs_id]
                               for drs_id, drs_name
                               in enumerate(drs_to_remove)}
                              for path in (self.session
                                           .query(*out_tuples)
                                           .filter(sqlalchemy
                                                   .and_(*conditions))
                                           .distinct().all())]

                output = create_tree(output_root, list(zip(drs_list, tree)))
                # Record data:
                (years_list,
                 picontrol_min_time) = (find_functions
                                        .get_years_list_from_periods(
                                            header['experiment_list']
                                            [experiment]))
                if record_function_handle == 'record_paths':
                    check_dimensions = False
                else:
                    check_dimensions = True
                    if (hasattr(options, 'missing_years') and
                        options.missing_years):
                        # Get the years_list from the database:
                        years_list = [int(x[0][:-2]) for x
                                      in (self
                                          .session
                                          .query(File_Expt.time)
                                          .filter(sqlalchemy.and_(*conditions))
                                          .distinct()
                                          .all())]
                        picontrol_min_time = False

                # Time was further sliced:
                if ('year' in time_slices and
                   time_slices['year'] is not None):
                    if years_list is None:
                        years_list = time_slices['year']
                    else:
                        years_list = [year for year in years_list
                                      if year in time_slices['year']]

                if ('month' in time_slices and
                   time_slices['month'] is not None):
                    months_list = [month for month in months_list
                                   if month in time_slices['month']]

                netcdf_pointers = (soft_links.create_soft_links
                                   .create_netCDF_pointers(
                                    paths_list, time_frequency,
                                    years_list, months_list,
                                    header['file_type_list'],
                                    header['data_node_list'],
                                    semaphores=semaphores,
                                    check_dimensions=check_dimensions,
                                    session=session,
                                    remote_netcdf_kwargs=remote_netcdf_kwargs))

                getattr(netcdf_pointers, record_function_handle)(output, var)

                # Remove recorded data from database:
                (self.session
                     .query(*out_tuples)
                     .filter(sqlalchemy.and_(*conditions))
                     .delete())
        return

    def retrieve_database(self, output, options, q_manager=None,
                          session=None, retrieval_type='reduce'):
        # Recover the database meta data:
        tree = list(zip(self.drs.official_drs_no_version,
                        [None for field in self.drs.official_drs_no_version]))
        with closing(read_Dataset(self.db_file, mode='r')) as dataset:
            if retrieval_type in ['download_files', 'download_opendap']:
                q_manager.download.set_opened()
                db_utils.extract_netcdf_variable(output, dataset, tree,
                                                 options,
                                                 q_manager=q_manager.download,
                                                 session=session,
                                                 retrieval_type=retrieval_type)
                q_manager.download.set_closed()
                data_node_list = self.list_data_nodes(options)
                output = retrieval_manager.launch_download(output,
                                                           data_node_list,
                                                           q_manager.download,
                                                           options)
            else:
                db_utils.extract_netcdf_variable(output, dataset, tree,
                                                 options,
                                                 retrieval_type=retrieval_type)
        return output

    def retrieve_dates(self, options):
        # Recover the database meta data:
        with closing(read_Dataset(self.db_file, mode='r')) as dataset:
            dates_axis = np.unique(retrieve_dates_recursive(dataset, options))
        return dates_axis


#####################################################################
#####################################################################
#  DATABASE CONVERSION
#####################################################################
#####################################################################
def time_slices_from_options(options):
    time_slices = dict()
    # Slice time if record_validate was already performed:
    if ('record_validate' not in commands_parser._get_command_names(options) or
        'record_validate' == commands_parser._get_command_names(options)[-1] or
        (commands_parser._get_command_names(options).index('record_validate') <
         options.max_command_number)):
        for time_type in ['month', 'year']:
            if hasattr(options, time_type):
                time_slices[time_type] = getattr(options, time_type)
    return time_slices


file_unique_id_list = ['checksum_type', 'checksum', 'tracking_id']


def populate_database_recursive(nc_Database, data, options, find_function,
                                soft_links=True, time_slices=dict(),
                                semaphores=dict(), session=None,
                                remote_netcdf_kwargs=dict()):
    if soft_links and 'soft_links' in data.groups:
        soft_links = data.groups['soft_links']
        paths = soft_links.variables['path'][:]
        for path_id, path in enumerate(paths):
            # id_list = ['file_type','search']
            id_list = ['file_type']
            for idx in id_list:
                setattr(nc_Database.file_expt, idx,
                        ncutils.core.maybe_conv_bytes_to_str(
                            soft_links.variables[idx][path_id]))

            # Check if data_node was included:
            data_node = (remote_netcdf.remote_netcdf
                         .get_data_node(
                            ncutils.core.maybe_conv_bytes_to_str(
                                soft_links.variables['path'][path_id]),
                            ncutils.core.maybe_conv_bytes_to_str(
                                soft_links.variables['file_type'][path_id])))

            if is_ln_inc_and_not_exc('data_node', options, data_node):
                file_path = '|'.join(
                        [ncutils.core.maybe_conv_bytes_to_str(
                            soft_links.variables['path'][path_id])] +
                        [ncutils.core.maybe_conv_bytes_to_str(
                            soft_links.variables[unique_file_id][path_id])
                         for unique_file_id in file_unique_id_list])

                setattr(nc_Database.file_expt, 'path', file_path)
                setattr(nc_Database.file_expt, 'version',
                        'v' + str(ncutils.core.maybe_conv_bytes_to_str(
                                    soft_links.variables['version'][path_id])))
                setattr(nc_Database.file_expt, 'data_node', data_node)

                find_function(nc_Database,
                              copy.deepcopy(nc_Database.file_expt),
                              time_slices=time_slices, semaphores=semaphores,
                              session=session,
                              remote_netcdf_kwargs=remote_netcdf_kwargs)
    elif len(data.groups.keys()) > 0 and 'soft_links' not in data.groups:
        for group in data.groups:
            level_name = ncutils.core.getncattr(data.groups[group], level_key)
            if is_ln_inc_and_not_exc(level_name, options, group):
                setattr(nc_Database.file_expt,
                        ncutils.core.getncattr(data.groups[group], level_key),
                        group)
                populate_database_recursive(
                                nc_Database, data.groups[group],
                                options, find_function,
                                soft_links=soft_links,
                                time_slices=time_slices,
                                semaphores=semaphores, session=session,
                                remote_netcdf_kwargs=remote_netcdf_kwargs)
    elif 'path' in data.ncattrs():
        # for fx variables:
        # id_list=['file_type','search']
        id_list = ['file_type']
        for idx in id_list:
            setattr(nc_Database.file_expt, idx,
                    ncutils.core.getncattr(data, idx))

        # Check if data_node was included:
        data_node = (remote_netcdf.remote_netcdf
                     .get_data_node(
                            ncutils.core.getncattr(data, 'path'),
                            ncutils.core.getncattr(data, 'file_type')))
        if is_ln_inc_and_not_exc('data_node', options, data_node):
            file_path = '|'.join([ncutils.core.getncattr(data, 'path')] +
                                 [ncutils.core.getncattr(data, file_unique_id)
                                  if file_unique_id in data.ncattrs() else ''
                                  for file_unique_id in file_unique_id_list])

            setattr(nc_Database.file_expt, 'path', file_path)
            setattr(nc_Database.file_expt, 'version',
                    str(ncutils.core.getncattr(data, 'version')))

            setattr(nc_Database.file_expt, 'data_node',
                    remote_netcdf.remote_netcdf
                    .get_data_node(
                           nc_Database.file_expt.path,
                           nc_Database.file_expt.file_type))
            find_function(nc_Database, copy.deepcopy(nc_Database.file_expt),
                          time_slices=time_slices, semaphores=semaphores,
                          session=session,
                          remote_netcdf_kwargs=remote_netcdf_kwargs)
    else:
        # for retrieved datasets:
        id_list = ['file_type', 'path', 'version']
        for idx in id_list:
            setattr(nc_Database.file_expt, idx, '')
        if len(data.variables.keys()) > 0:
            setattr(nc_Database.file_expt, 'data_node',
                    remote_netcdf.remote_netcdf
                    .get_data_node(
                            nc_Database.file_expt.path,
                            nc_Database.file_expt.file_type))
            find_function(nc_Database, copy.deepcopy(nc_Database.file_expt))
    return


def create_tree(output_root, tree):
    return create_tree_recursive(output_root, tree)


def create_tree_recursive(output_top, tree):
    level_name = tree[0][1]
    if level_name not in output_top.groups:
        output = output_top.createGroup(level_name)
        ncutils.core.setncattr(output, 'level_name', tree[0][0])
    else:
        output = output_top.groups[level_name]
    if len(tree) > 1:
        output = create_tree_recursive(output, tree[1:])
    return output


def retrieve_dates_recursive(data, options):
    if 'soft_links' in data.groups:
        options_dict = {opt: getattr(options, opt)
                        for opt in ['previous', 'next', 'year',
                                    'month', 'day', 'hour']
                        if hasattr(options, opt)}
        remote_data = (soft_links.read_soft_links
                       .read_netCDF_pointers(data, **options_dict))
        if (db_utils.check_soft_links_size(remote_data) and
           remote_data.time_var is not None):
            return (remote_data
                    .date_axis[remote_data.time_restriction]
                    [remote_data.time_restriction_sort])
        else:
            return np.array([])
    elif len(data.groups.keys()) > 0:
        time_axes = [retrieve_dates_recursive(data.groups[group], options)
                     for group in data.groups
                     if is_ln_inc_and_not_exc(
                          ncutils.core.getncattr(data.groups[group],
                                                 level_key),
                          options, group)]

        time_axes = _drop_empty(time_axes)
        if len(time_axes) > 0:
            if (hasattr(options, 'restrictive_loop') and
               options.restrictive_loop):
                return reduce(np.intersect1d, time_axes)
            else:
                return np.concatenate(time_axes)
        else:
            return np.array([])
    else:
        return np.array([])


def _drop_empty(array_list):
    return [item for item in array_list if len(item) > 0]


class File_Expt(object):
    # Create a class that can be used with sqlachemy:
    def __init__(self, diag_tree_desc):
        for tree_desc in diag_tree_desc:
            setattr(self, tree_desc, '')


def record_header(output_root, header, options=None):
    if (options is not None and
        hasattr(options, 'no_check_availability') and
       options.no_check_availability):
        record_header(output_root,
                      {val: header[val] for val in header
                       if val != 'data_node_list'})
    else:
        for val in header:
            ncutils.core.setncattr(output_root, val, json.dumps(header[val]))
    return
