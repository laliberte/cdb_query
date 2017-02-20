from cdb_query import core
from cdb_query.netcdf4_soft_links import ncutils
from cdb_query.data.testing_data import generate_test_files
from cdb_query.data.testing_data_fx import generate_test_files \
                                    as generate_test_files_fx
import shlex
import os
import pytest
import datetime
import netCDF4
import numpy as np


openid = os.environ.get('OPENID_ESGF')
password = os.environ.get('PASSWORD_ESGF')

skip_auth = True
if openid is not None and password is not None:
    skip_auth = False


@pytest.fixture(scope='session')
def tmpfiles(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp('recipe01')
    generator = generate_test_files(tmpdir)
    test_file1, data1 = next(generator)
    test_file2, data2 = next(generator)
    generator = generate_test_files_fx(tmpdir)
    test_file_fx, data_fx = next(generator)
    return {'ask': tmpdir.join('tas_ONDJF_pointers.nc'),
            'validate': tmpdir.join('tas_ONDJF_pointers.validate.nc'),
            'download_opendap': tmpdir.join('tas_ONDJF_pointers.retrieved.nc'),
            'reduce': tmpdir.join('tas_ONDJF_pointers.retrieved.reduced.nc'),
            'outdir': tmpdir.mkdir('out'),
            'indir': tmpdir.join('CMIP5/'),
            'swap_dir': tmpdir,
            'data': [data1, data2, data_fx]}


def test_recipe01_local_ask(tmpfiles, capsys):
    # Ask:
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 ask -O
                            --ask_month=1,2
                            --serial
                            --debug
                            --openid={0}
                            --password={1}
                            --swap_dir={2}
                            --ask_var=tas:day-atmos-day,orog:fx-atmos-fx
                            --ask_experiment=amip:1980-1980
                            --search_path={3}
                            --institute=NCAR --model=CCSM4 --ensemble=r1i1p1
                            --ensemble=r0i0p0
                            {4}
        '''.format(openid, password, tmpfiles['swap_dir'],
                   tmpfiles['indir'], tmpfiles['ask'])))
    out, err = capsys.readouterr()

    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 list_fields -f institute
                                    -f model
                                    -f ensemble
                                    {0}
        '''.format(tmpfiles['ask'])))
    out2, err2 = capsys.readouterr()
    assert out2 == "NCAR,CCSM4,r0i0p0\nNCAR,CCSM4,r1i1p1\n"


def test_recipe01_local_validate(tmpfiles, capsys):
    # Validate simulations:
    # Exclude data_node http://esgf2.dkrz.de because it is on a
    # tape archive (slow).
    # If you do not exclude it, it will likely be excluded because of its slow
    # response time.
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 validate -O
                                --serial
                                --debug
                                --openid={0}
                                --password={1}
                                --swap_dir={2}
                                {3} {4}
        '''.format(openid, password, tmpfiles['swap_dir'],
                   tmpfiles['ask'], tmpfiles['validate'])))
    out, err = capsys.readouterr()

    # List simulations:
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 list_fields -f institute
                                    -f model
                                    -f ensemble
                                    {0}
        '''.format(tmpfiles['validate'])))
    out2, err2 = capsys.readouterr()
    assert out2 == "NCAR,CCSM4,r0i0p0\nNCAR,CCSM4,r1i1p1\n"
    with netCDF4.Dataset(tmpfiles['validate']) as dataset:
        time_axis = np.sort(dataset['NCAR/CCSM4/amip/day/atmos/day/'
                                    'r1i1p1/tas/time'][:])
    np.testing.assert_equal(time_axis, np.arange(60))


def test_recipe01_local_reduce(tmpfiles):
    # Convert hierarchical file to files on filesystem (much faster than ncks):
    # Identity reduction simply copies the data to disk
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 reduce -O
                        --serial
                        --debug
                        --month=1
                        --swap_dir={0}
                        --add_fixed
                        --var=tas
                        --out_destination={1}
                        'cp'
                        {2} {3}
        '''.format(tmpfiles['swap_dir'], tmpfiles['outdir'],
                   tmpfiles['validate'],
                   tmpfiles['reduce'])))

    version = ('v' +
               datetime.datetime.now().strftime('%Y%m%d'))
    out_file = tmpfiles['outdir'].join(
                        'NCAR', 'CCSM4', 'amip', 'day', 'atmos',
                        'day', 'r1i1p1', version, 'tas',
                        'tas_day_CCSM4_amip_r1i1p1_19800101-19800131.nc')
    assert out_file.check()

    with netCDF4.Dataset(out_file) as dataset:
        assert 'tas' in dataset.variables
        tas = dataset.variables['tas'][:]

    expected = np.concatenate([tmpfiles['data'][0]['tas'][:30, ...],
                               tmpfiles['data'][1]['tas'][:1, ...]], axis=0)
    np.testing.assert_equal(tas, expected)


def test_recipe01_local_reduce_loop(tmpfiles):
    # Convert hierarchical file to files on filesystem (much faster than ncks):
    # Identity reduction simply copies the data to disk

    # Here we simply rename a variable and ensure that it is kept along:
    command = ('python -c \'import netCDF4, sys, os; '
               'os.rename(sys.argv[1], sys.argv[2]); '
               'data = netCDF4.Dataset(sys.argv[2], mode=\\"a\\"); '
               'data.renameVariable(\\"tas\\", \\"tas_new\\"); '
               'data.close()\'')
    cl_call = (
        '''
        cdb_query CMIP5 reduce -O
                        --serial
                        --debug
                        --month=1
                        --swap_dir={0}
                        --add_fixed
                        --day=1,2,3,4,5
                        -l day
                        --var=tas
                        --out_destination={1}
                        "{4}"
                        {2} {3}
        '''.format(tmpfiles['swap_dir'], tmpfiles['outdir'],
                   tmpfiles['validate'],
                   tmpfiles['reduce'], command))
    core.cdb_query_from_list(shlex.split(cl_call))

    version = ('v' +
               datetime.datetime.now().strftime('%Y%m%d'))
    for day in range(1, 6):
        out_file = tmpfiles['outdir'].join(
                        'NCAR', 'CCSM4', 'amip', 'day', 'atmos',
                        'day', 'r1i1p1', version, 'tas',
                        'tas_day_CCSM4_amip_r1i1p1_1980010{0}-1980010{0}.nc'
                        .format(day))
        assert out_file.check()

    with netCDF4.Dataset(out_file) as dataset:
        assert 'tas_new' in dataset.variables
        tas = dataset.variables['tas_new'][:]

    expected = tmpfiles['data'][0]['tas'][4, ...],
    np.testing.assert_equal(np.squeeze(tas), np.squeeze(expected))

    with netCDF4.Dataset(tmpfiles['reduce']) as dataset:
        path = 'NCAR/CCSM4/amip/day/atmos/day/r1i1p1/tas/soft_links'
        group = dataset
        for level in path.split('/'):
            assert level in group.groups
            group = group.groups[level]
        assert 'tas_new' in group.parent.variables
        assert 'tas_new' in group.variables
        assert ncutils.dataset_compat._dim_len(group, 'time') == 5
