from cdb_query import core
import shlex
import os
import datetime
import numpy as np
import pytest
import netCDF4

openid = os.environ.get('OPENID_ESGF')
password = os.environ.get('PASSWORD_ESGF')

skip_auth = True
if openid is not None and password is not None:
    skip_auth = False


@pytest.fixture(scope='module')
def tmpfiles(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp('recipe01')
    return {'ask': tmpdir.join('tas_ONDJF_pointers.nc'),
            'validate': tmpdir.join('tas_ONDJF_pointers.validate.nc'),
            'download_opendap': tmpdir.join('tas_ONDJF_pointers.retrieved.nc'),
            'reduce': tmpdir.join('tas_ONDJF_pointers.retrieved.reduced.nc'),
            'outdir': tmpdir.mkdir('out'),
            'swap_dir': tmpdir}


@pytest.mark.skipif(skip_auth, reason=('Without auth credentials, '
                                       'this test cannot work'))
def test_recipe01_esgf_ask(tmpfiles, capsys):
    # Ask:
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 ask -O -s
                            --ask_month=1,2,10,11,12
                            --debug
                            --serial
                            --distrib
                            --openid={0}
                            --password={1}
                            --swap_dir={2}
                            --ask_var=tas:day-atmos-day,orog:fx-atmos-fx
                            --ask_experiment=amip:1979-2004
                            --search_path=https://pcmdi.llnl.gov/esg-search/
                            --institute=NCAR --model=CCSM4 --ensemble=r1i1p1
                            --ensemble=r0i0p0
                            {3}
        '''.format(openid, password, tmpfiles['swap_dir'],
                   tmpfiles['ask'])))
    out1, err1 = capsys.readouterr()

    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 list_fields -f institute
                                    -f model
                                    -f ensemble
                                    {0}
        '''.format(tmpfiles['ask'])))
    out2, err2 = capsys.readouterr()
    assert out2 == "NCAR,CCSM4,r0i0p0\nNCAR,CCSM4,r1i1p1\n"


@pytest.mark.skipif(skip_auth, reason=('Without auth credentials, '
                                       'this test cannot work'))
def test_recipe01_esgf_validate(tmpfiles, capsys):
    # Validate simulations:
    # Exclude data_node http://esgf2.dkrz.de because it is on a
    # tape archive (slow).
    # If you do not exclude it, it will likely be excluded because of its slow
    # response time.
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 validate -O -s
                                --debug
                                --serial
                                --openid={0}
                                --password={1}
                                --swap_dir={2}
                                --Xdata_node=http://esgf2.dkrz.de
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


@pytest.mark.skipif(skip_auth, reason=('Without auth credentials, '
                                       'this test cannot work'))
def test_recipe01_esgf_download_opendap(tmpfiles, capsys):
    # Retrieve to netCDF:
    # Retrieve the first month:
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 download_opendap -O -s
                        --year=1979 --month=1
                        --debug
                        --serial
                        --openid={0}
                        --password={1}
                        --swap_dir={2}
                        {3} {4}
        '''.format(openid, password, tmpfiles['swap_dir'],
                   tmpfiles['validate'],
                   tmpfiles['download_opendap'])))
    # List simulations:
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 list_fields -f institute
                                    -f model
                                    -f ensemble
                                    {0}
        '''.format(tmpfiles['download_opendap'])))
    out2, err2 = capsys.readouterr()
    assert out2 == "NCAR,CCSM4,r0i0p0\nNCAR,CCSM4,r1i1p1\n"


@pytest.mark.skipif(skip_auth, reason=('Without auth credentials, '
                                       'this test cannot work'))
def test_recipe01_esgf_reduce(tmpfiles):
    # Convert hierarchical file to files on filesystem (much faster than ncks):
    # Identity reduction simply copies the data to disk
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 reduce -O -s
                        --debug
                        --serial
                        --swap_dir={0}
                        --out_destination={1}
                        'cp'
                        {2} {3}
        '''.format(tmpfiles['swap_dir'], tmpfiles['outdir'],
                   tmpfiles['download_opendap'],
                   tmpfiles['reduce'])))

    version = ('v' +
               datetime.datetime.now().strftime('%Y%m%d'))
    out_file = tmpfiles['outdir'].join(
                        'NCAR', 'CCSM4', 'amip', 'day', 'atmos',
                        'day', 'r1i1p1', version, 'tas',
                        'tas_day_CCSM4_amip_r1i1p1_19790101-19790131.nc')
    assert out_file.check()

    data = dict()
    with netCDF4.Dataset(out_file) as dataset:
        for var in ['tas']:
            assert var in dataset.variables
            data[var] = dataset.variables[var][:]

    # Check the first slices:
    expected = [[[246.76385498, 247.0161438],
                 [247.44604492, 247.81329346]],
                [[246.76124573, 247.19403076],
                 [247.93475342, 248.31892395]]]
    np.testing.assert_almost_equal(data['tas'][:2, :2, :2], expected)

    out_file = tmpfiles['outdir'].join(
                        'NCAR', 'CCSM4', 'amip', 'fx', 'atmos',
                        'fx', 'r0i0p0', version, 'orog',
                        'orog_fx_CCSM4_amip_r0i0p0.nc')
    assert out_file.check()

    data = dict()
    with netCDF4.Dataset(out_file) as dataset:
        for var in ['orog']:
            assert var in dataset.variables
            data[var] = dataset.variables[var][:]

    # Check the first slices:
    expected = [[2824.92138672, 2824.92138672],
                [2731.1394043, 2733.16772461]]
    np.testing.assert_almost_equal(data['orog'][:2, :2], expected)
