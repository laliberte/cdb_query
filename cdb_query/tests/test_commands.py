from cdb_query import commands
from cdb_query.nc_Database import db_manager
from cdb_query.remote_archive import CMIP5
import netCDF4
from argparse import Namespace

import pytest


@pytest.fixture
def options(tmpdir):
    return Namespace(command='ask',
                     command_number=0,
                     ask_var=['tas:day-atmos-day', 'vas:day-atmos-day',
                              'tas:mon-atmos-Amon'],
                     ask_experiment=['historical:1980-1990',
                                     'historical:1995-2005',
                                     'rcp85:2010-2020'],
                     search_path=['local', 'remote'],
                     Xsearch_path=['remote'],
                     ask_month=[1, 2, 8],
                     ask_file_type=['OPENDAP', 'local_file'],
                     out_netcdf_file=tmpdir.join('out.nc'))


def test_load_headers(options):
    """ Test simple header loading """
    project_drs = CMIP5.DRS
    db = commands.Database_Manager(project_drs)
    db.load_header(options)
    expected = {'month_list': [1, 2, 8],
                'file_type_list': ['OPENDAP', 'local_file'],
                'experiment_list': {'historical': ['1980,1990', '1995,2005'],
                                    'rcp85': ['2010,2020']},
                'search_list': ['local'],
                'variable_list': {'vas': [['day', 'atmos', 'day']],
                                  'tas': [['day', 'atmos', 'day'],
                                          ['mon', 'atmos', 'Amon']]}}
    assert db.header == expected


def test_load_headers_syntax_error(options):
    """ Test syntax error """
    project_drs = CMIP5.DRS
    db = commands.Database_Manager(project_drs)
    options.ask_var = ['tas:day-atmos-day', 'vasday-atmos']
    with pytest.raises(SyntaxError) as e:
        db.load_header(options)
    assert ('Query improperly specified. '
            'Check --ask_var and --ask_experiment') in str(e)


def test_load_headers_not_ask(options):
    """ Test header loading when not ask command """
    project_drs = CMIP5.DRS
    db = commands.Database_Manager(project_drs)
    db.load_header(options)
    # This check
    db.header['data_node_list'] = ['test1']
    options.no_check_availability = True
    with netCDF4.Dataset(options.out_netcdf_file,
                         'w', format='NETCDF4',
                         diskless=True, persist=True) as output_root:
        db_manager.record_header(output_root, db.header, options=options)
    db.header = dict()
    options.command = 'validate'
    options.in_netcdf_file = options.out_netcdf_file
    db.load_header(options)
    expected = {u'month_list': [1, 2, 8],
                u'file_type_list': [u'OPENDAP', u'local_file'],
                u'experiment_list': {u'historical': [u'1980,1990',
                                                     u'1995,2005'],
                                     u'rcp85': [u'2010,2020']},
                u'search_list': [u'local'],
                u'variable_list': {u'vas': [[u'day', u'atmos', u'day']],
                                   u'tas': [[u'day', u'atmos', u'day'],
                                            [u'mon', u'atmos', u'Amon']]}}
    assert db.header == expected


def test_union_header(options):
    """ Test taking union of header """
    project_drs = CMIP5.DRS
    db = commands.Database_Manager(project_drs)
    db.load_header(options)
    db.union_header()
    expected = {'var_list': ['vas', 'tas'],
                'experiment_list': ['historical', 'rcp85'],
                'time_frequency_list': ['day', 'mon'],
                'cmor_table_list': ['day', 'Amon'], 'realm_list': ['atmos']}
    assert db.header_simple == expected
