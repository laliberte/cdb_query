import netCDF4
import numpy as np
import retrieval_utils
import time

class remote_netCDF:
    def __init__(self,netcdf_file_name,semaphores):
        self.file_name=netcdf_file_name
        self.semaphores=semaphores
        self.remote_data_node=retrieval_utils.get_data_node(self.file_name, 'HTTPServer')
        if isinstance(semaphores,dict):
            self.in_semaphores=(self.remote_data_node in  self.semaphores.keys())
        else:
            self.in_semaphores=False
        return

    def open(self):
        self.acquire_semaphore()
        try:
            self.Dataset=netCDF4.Dataset(self.file_name)
        except:
            self.release_semaphore()
        return
    
    def close(self):
        try:
            self.Dataset.cose()
        except:
            pass
        del self.Dataset
        self.release_semaphore()
        return

    def acquire_semaphore(self):
        if self.in_semaphores:
            self.semaphores[self.remote_data_node].acquire()
        return

    def release_semaphore(self):
        if self.in_semaphores:
            self.semaphores[self.remote_data_node].release()
        return

    def open_with_error(self):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.
            '''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        try:
            self.Dataset=netCDF4.Dataset(self.file_name)
        except:
            raise dodsError(error)

    def is_available(self):
        try:
            self.open()
            self.close()
            return True
        except:
            return False

    def check_if_available_and_find_alternative(self,paths_list,checksums_list):
        if not self.is_available:
            checksum=checksums_list[paths_list.index(self.file_name)]
            for cs_id, cs in enumerate(checksums_list):
                if cs==checksum and paths_list[id]!=path:
                    remote_data=remote_netCDF(paths_list[id],self.semaphores)
                    if remote_data.isavailable():
                        return paths_list[id]
            return self.file_name
        else:
            return self.file_name

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

