from cdb_query.nc_Database import db_manager
from argparse import Namespace
import numpy as np
import netCDF4


def test_recursive_dates_empty_branch(tmpdir):
    options = Namespace()
    with netCDF4.Dataset(tmpdir.join('test.nc'), 'w',
                         diskless=True, persist=False) as data:
        data.createGroup('test')
        data.groups['test'].setncattr('level_name', 'model')
        time_axis = db_manager.retrieve_dates_recursive(data, options)
    assert time_axis.size == 0
