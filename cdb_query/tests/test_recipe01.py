from cdb_query import core
import shlex
import os
import pytest
import netCDF4

openid = os.environ['OPENID_ESGF']
password = os.environ['PASSWORD_ESGF']


@pytest.fixture(scope='module')
def tmpfiles(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp('recipe01')
    return {'ask': tmpdir.join('tas_ONDJF_pointers.nc'),
            'validate': tmpdir.join('tas_ONDJF_pointers.nc'),
            'download_opendap': tmpdir.join('tas_ONDJF_pointers.retrieved.nc'),
            'reduce': tmpdir.join('tas_ONDJF_pointers.retrieved.reduced.nc'),
            'outdir': tmpdir.mkdir('out')}


@pytest.mark.skip()
def test_recipe01_ask(tmpfiles, capsys):
    # Ask:
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 ask -O
                            --ask_month=1,2,10,11,12
                            --debug
                            --serial
                            --openid={0}
                            --password={1}
                            --ask_var=tas:day-atmos-day,orog:fx-atmos-fx
                            --ask_experiment=amip:1979-2004
                            --search_path=https://pcmdi.llnl.gov/esg-search/
                            --institute=NCAR --model=CCSM4 --ensemble=r1i1p1
                            {2}
        '''.format(openid, password, tmpfiles['ask'])))
    out1, err1 = capsys.readouterr()

    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 list_fields -f institute
                                    -f model
                                    -f ensemble
                                    {0}
        '''.format(tmpfiles['ask'])))
    out2, err2 = capsys.readouterr()
    assert out2 == "NCAR,CCSM4,r0i0p0\nNCAR,CCSM4,r1i1p1"


@pytest.mark.skip()
def test_recipe01_validate(tmpfiles, capsys):
    # Validate simulations:
    # Exclude data_node http://esgf2.dkrz.de because it is on a
    # tape archive (slow).
    # If you do not exclude it, it will likely be excluded because of its slow
    # response time.
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 validate -O
                                --debug
                                --serial
                                --openid={0}
                                --password={1}
                                --Xdata_node=http://esgf2.dkrz.de
                                {2} {3}
        '''.format(openid, password, tmpfiles['ask'], tmpfiles['validate'])))
    out, err = capsys.readouterr()

    # List simulations:
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 list_fields -f institute
                                    -f model
                                    -f ensemble
                                    {0}
        '''.format(tmpfiles['validate'])))
    out, err = capsys.readouterr()


@pytest.mark.skip()
def test_recipe01_download_opendap(tmpfiles):
    # Retrieve to netCDF:
    # Retrieve the first month:
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 download_opendap -O
                        --year=1979 --month=1
                        --debug
                        --serial
                        --openid={0}
                        --password={1}
                        --num_dl=3
                        {2} {3}
        '''.format(openid, password, tmpfiles['validate'],
                   tmpfiles['download_opendap'])))


@pytest.mark.skip()
def test_recipe01_reduce(tmpfiles):
    # Convert hierarchical file to files on filesystem (much faster than ncks):
    # Identity reduction simply copies the data to disk
    core.cdb_query_from_list(shlex.split(
        '''
        cdb_query CMIP5 reduce -O
                        --debug
                        --serial
                        '' \
                        --out_destination={0}
                        {1} {2}
        '''.format(tmpfiles['outdir'], tmpfiles['download_opendap'],
                   tmpfiles['reduce'])))
