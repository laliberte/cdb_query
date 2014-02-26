import netCDF4
import numpy as np

class remote_netCDF:
    def __init__(self,netcdf_file_name,semaphores):
        self.file_name=netcdf_file_name
        self.semaphores=semaphores
        return
    def open(self):
        try:
            self.Dataset=netCDF4.Dataset(self.file_name)
        except:
            error=' '.join('''
    The url {0} could not be opened. 
    Copy and paste this url in a browser and try downloading the file.
    If it works, you can stop the download and retry using cdb_query. If
    it still does not work it is likely that your certificates are either
    not available or out of date.
            '''.splitlines())
            raise dodsError(error.format(self.file_name.replace('dodsC','fileServer')))
    
    def close(self):
        try:
            self.Dataset.cose()
        except:
            pass
        del self.Dataset
        return

    def test(self):
        self.Dataset=netCDF4.Dataset(self.file_name)
        self.close() 
        return

    def get_time(self):

        self.open()
        try:
            if 'calendar' in dir(self.Dataset.variables['time']):
                calendar=self.Dataset.variables['time'].calendar
            else:
                calendar='standard'
            time_axis=(netCDF4.num2date(self.Dataset.variables['time'][:],
                                         units=self.Dataset.variables['time'].units,
                                         calendar=calendar)
                            )
        except:
            time_axis=np.empty((0,))
        self.close()
        return time_axis

class dodsError(Exception):
     def __init__(self, value):
         self.value = value
     def __str__(self):
         return repr(self.value)

