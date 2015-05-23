import netCDF4
import numpy as np
import retrieval_utils
import netcdf_utils
import time
import sys
import os

class RedirectStdStreams(object):
    def __init__(self, stdout=None, stderr=None):
        self._stdout = stdout or sys.stdout
        self._stderr = stderr or sys.stderr

    def __enter__(self):
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush(); self.old_stderr.flush()
        sys.stdout, sys.stderr = self._stdout, self._stderr

    def __exit__(self, exc_type, exc_value, traceback):
        self._stdout.flush(); self._stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr


class remote_netCDF:
    def __init__(self,netcdf_file_name,semaphores):
        self.file_name=netcdf_file_name
        self.semaphores=semaphores
        self.remote_data_node=retrieval_utils.get_data_node(self.file_name, 'HTTPServer')
        if isinstance(semaphores,dict):
            self.in_semaphores=(self.remote_data_node in  self.semaphores.keys())
        else:
            self.in_semaphores=False
        self.Dataset=None
        return

    def open(self):
        self.acquire_semaphore()
        try:
            self.Dataset=netCDF4.Dataset(self.file_name)
        except:
            self.close()
        return
    
    def close(self):
        try:
            if isinstance(self.Dataset,netCDF4.Dataset):
                self.Dataset.close()
        except:
            pass
        #del self.Dataset
        self.Dataset=None
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

    def open_with_error(self,num_trials=1):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        self.acquire_semaphore()
        try:
            self.Dataset=netCDF4.Dataset(self.file_name)
        except:
            if num_trials>0:
                time.sleep(30)
                self.open_with_error(num_trials=num_trials-1)
            else:
                self.close()
                if not retrieval_utils.check_file_availability(self.file_name.replace('dodsC','fileServer')):
                    raise dodsError(error_statement)
        return

    def is_available(self):
        try:
            #devnull = open(os.devnull, 'w')
            #with RedirectStdStreams(stdout=devnull, stderr=devnull):
            self.open_with_error()
            self.close()
            return True
        except dodsError as e:
            self.close()
            e_mod=" This is a common error and is not fatal. It could however affect the number of datasets that are kept."
            print e.value+e_mod
            return False

    def check_if_available_and_find_alternative(self,paths_list,checksums_list):
        if not self.is_available():
            checksum=checksums_list[list(paths_list).index(self.file_name)]
            for cs_id, cs in enumerate(checksums_list):
                if cs==checksum and paths_list[cs_id]!=self.file_name:
                    remote_data=remote_netCDF(paths_list[cs_id],self.semaphores)
                    if remote_data.is_available():
                        return paths_list[cs_id]
            return self.file_name
        else:
            return self.file_name

    def retrieve_time(self):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        try:
            try:
                if 'calendar' in self.Dataset.variables['time'].ncattrs():
                    calendar=self.Dataset.variables['time'].calendar
                else:
                    calendar='standard'
                units=self.Dataset.variables['time'].units
                native_time_axis=self.Dataset.variables['time'][:]
            except:
                time.sleep(30)
                if 'calendar' in self.Dataset.variables['time'].ncattrs():
                    calendar=self.Dataset.variables['time'].calendar
                else:
                    calendar='standard'
                units=self.Dataset.variables['time'].units
                native_time_axis=self.Dataset.variables['time'][:]
        except:
            raise dodsError(error_statement)
        if units=='day as %Y%m%d.%f':
            time_axis=np.array(map(netcdf_utils.convert_to_date_absolute,native_time_axis))
        else:
            try:
                #Put cmip5_rewrite_time_axis here:
                time_axis=netCDF4.num2date(native_time_axis,units=units,calendar=calendar)
            except TypeError:
                time_axis=np.array([]) 
        return time_axis

    def get_time(self):
        time_axis=np.zeros((0,))
        try:
            #temp = sys.stdout
            #sys.stdout = NullDevice()
            self.open_with_error()
            #if '/'.join(self.file_name.split('/')[0:3]).split('.')[-1]=='de':
            #    print "opened "+self.file_name
            time_axis=self.retrieve_time()
            self.close()
            #sys.stdout=temp
        except dodsError as e:
            self.close()
            e_mod=" This is a common error and is not fatal. It could however affect the number of datasets that are kept."
            print e.value+e_mod
        return time_axis

class dodsError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
class NullDevice():
    def write(self, s):
        pass
