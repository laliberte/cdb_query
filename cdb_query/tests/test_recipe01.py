import pytest
from cdb_query import core
import shlex
import os

openid = os.environ['OPENID_ESGF']
password = os.environ['PASSWORD_ESGF']
num_procs = 3

def test_recipe01(tmpdir):

    # Ask:
    file_name_ask = tmpdir.join('tas_ONDJF_pointers.nc')
    core.cdb_query_from_list(shlex.split(
    '''
    cdb_query CMIP5 ask --ask_month=1,2,10,11,12
                        --debug
                        --openid={0}
                        --password={1}
                        --ask_var=tas:day-atmos-day,orog:fx-atmos-fx
                        --ask_experiment=amip:1979-2004
                        --model=CanAM4 --model=CCSM4 --model=GISS-E2-R --model=MRI-CGCM3
                        --num_procs={2}
                        {3}
    '''.format(openid, password, num_procs, file_name_ask)
    ))

    core.cdb_query_from_list(shlex.split(
    '''
    cdb_query CMIP5 list_fields -f institute
                                -f model
                                -f ensemble 
                                {0}
    '''.format(file_name_ask)
    ))

    #Validate simulations:
    #Exclude data_node http://esgf2.dkrz.de because it is on a tape archive (slow)
    #If you do not exclude it, it will likely be excluded because of its slow
    #response time.
    file_name_validate = tmpdir.join('tas_validate.nc')
    core.cdb_query_from_list(shlex.split(
    '''
    cdb_query CMIP5 validate
                            --debug
                            --openid={0}
                            --password={1}
                            --num_procs={2}
                            --Xdata_node=http://esgf2.dkrz.de
                            {3} {4}
    '''.format(openid, password, num_procs, file_name_ask, file_name_validate)
    ))

    #List simulations:
    core.cdb_query_from_list(shlex.split(
    '''
    cdb_query CMIP5 list_fields -f institute
                                -f model
                                -f ensemble
                                {0}
    '''.format(file_name_validate)
    ))

    #CHOOSE:
    # *1* Retrieve files:
    #file_name_df = tmpdir.join('tas_df.nc')
    #out_dl_dir = tmpdir.mkdir('in')
    #'''
    #cdb_query CMIP5 download_files
    #                --debug
    #                --year=1979 --month=1
    #                --download_all_files
    #                --openid={0}
    #                --password={1}
    #                --out_download_dir={2}
    #                {3} {4}
    #'''.format(openid, password, out_dl_dir,
    #           file_name_validate, file_name_df)

    # *2* Retrieve to netCDF:
        #Retrieve the first month:
    file_name_do = tmpdir.join('tas_do.nc')
    core.cdb_query_from_list(shlex.split(
    '''
    cdb_query CMIP5 download_opendap 
                    --year=1979 --month=1
                    --debug
                    --openid={0}
                    --password={1}
                    --num_dl=3
                    {2} {3}
    '''.format(openid, password, file_name_validate, file_name_do)
    ))

    #Convert hierarchical file to files on filesystem (much faster than ncks):
    #Identity reduction simply copies the data to disk
    file_name_reduce = tmpdir.join('tas_reduce.nc')
    out_dir = tmpdir.mkdir('out')
    core.cdb_query_from_list(shlex.split(
    '''
    cdb_query CMIP5 reduce
                    --debug
                    '' \
                    --out_destination={0}
                    {1} {2}
    '''.format(out_dir, file_name_do, file_name_reduce)
    ))
