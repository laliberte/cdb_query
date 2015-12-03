import netCDF4
import numpy as np
import retrieval_utils
import netcdf_utils
import time
import sys
import os
import timeaxis_mod

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


queryable_file_types=['OPENDAP','local_file']

class remote_netCDF:
    def __init__(self,netcdf_file_name,file_type,semaphores):
        self.file_name=netcdf_file_name
        self.semaphores=semaphores
        self.file_type=file_type
        self.remote_data_node=retrieval_utils.get_data_node(self.file_name, self.file_type)
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

    def open_with_error(self,num_trials=2):
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
                time.sleep(15)
                self.open_with_error(num_trials=num_trials-1)
            else:
                self.close()
                #if not retrieval_utils.check_file_availability(self.file_name.replace('dodsC','fileServer')):
                raise dodsError(error_statement)
        return

    def is_available(self):
        if not self.file_type in queryable_file_types: 
            return False

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

    def check_if_available_and_find_alternative(self,paths_list,file_type_list,checksums_list):
        if not self.is_available():
            checksum=checksums_list[list(paths_list).index(self.file_name)]
            for cs_id, cs in enumerate(checksums_list):
                if cs==checksum and paths_list[cs_id]!=self.file_name:
                    remote_data=remote_netCDF(paths_list[cs_id],file_type_list[cs_id],self.semaphores)
                    if remote_data.is_available():
                        return paths_list[cs_id]
            return self.file_name
        else:
            return self.file_name

    def retrieve_dimension(self,dimension,num_trials=2):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        attributes=dict()
        try:
            if dimension in self.Dataset.variables.keys():
                #Retrieve attributes:
                for att in self.Dataset.variables[dimension].ncattrs():
                    attributes[att]=self.Dataset.variables[dimension].getncattr(att)
                #If dimension is avaiable, retrieve
                data = self.Dataset.variables[dimension][:]
            else:
                #If dimension is not avaiable, create a simple indexing dimension
                data = np.arange(len(self.Dataset.dimensions[dimension]))
        except:
            if num_trials>0:
                time.sleep(15)
                data,attributes=self.retrieve_dimension(dimension,num_trials=num_trials-1)
            else:
                raise dodsError(error_statement)
        return data, attributes

    def retrieve_dimension_list(self,var,num_trials=2):
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.file_name.replace('dodsC','fileServer'))
        attributes=dict()
        try:
            dimensions=self.Dataset.variables[var].dimensions
        except:
            if num_trials>0:
                time.sleep(15)
                dimensions=self.retrieve_dimension_list(var,num_trials=num_trials-1)
            else:
                raise dodsError(error_statement)
        return dimensions

    def retrieve_variables(self,output,zlib=False):
        #open and record:
        try:
            self.open_with_error()
            for var_name in self.Dataset.variables.keys():
                output=netcdf_utils.replicate_and_copy_variable(output,self.Dataset,var_name,zlib=zlib,check_empty=False)
                #netcdf_utils.replicate_netcdf_var(output,self.Dataset,var_name)
                #output.variables[var_name][:]=self.Dataset.variables[var_name][:]
            self.close()
        except dodsError as e:
            if not self.file_type in ['local_file']:
                e_mod=" This is an uncommon error. It could be FATAL if it is not part of the validate step."
                self.close()
                print e.value+e_mod
        return output

    def grab_indices(self,var,indices,unsort_indices):
        dimensions=self.retrieve_dimension_list(var)
        return retrieve_slice(self.Dataset.variables[var],indices,unsort_indices,dimensions[0],dimensions[1:],0)

    #def retrieve_time(self):
    #    time_axis, attributes=self.retrieve_dimension('time')
    #    return netcdf_utils.create_date_axis_from_time_axis(time_axis,attributes)

    def get_time(self,time_frequency=None,is_instant=False,calendar='standard'):
        if self.file_type in queryable_file_types:
            date_axis=np.zeros((0,))
            try:
                #temp = sys.stdout
                #sys.stdout = NullDevice()
                self.open_with_error()
                time_axis, attributes=self.retrieve_dimension('time')
                date_axis=netcdf_utils.create_date_axis_from_time_axis(time_axis,attributes)
                self.close()
                #sys.stdout=temp
            except dodsError as e:
                self.close()
                e_mod=" This is a common error and is not fatal. It could however affect the number of datasets that are kept."
                print e.value+e_mod
            return date_axis
        elif time_frequency!=None:
            start_date,end_date=dates_from_filename(self.file_name,calendar)
            units=self.get_time_units(calendar)
            start_id=0

            funits=timeaxis_mod.convert_time_units(units, time_frequency)
            end_id=timeaxis_mod.Date2num(end_date,funits,calendar)

            inc = timeaxis_mod.time_inc(time_frequency)
            length=end_id/inc-2
            
            last_rebuild=start_date
            while last_rebuild < end_date:
                date_axis=rebuild_date_axis(0, length, is_instant, inc, funits,calendar=calendar)
                last_rebuild=date_axis[-1]
                length+=1

            return date_axis
        else:
            raise StandardError('time_frequency not provided for non-queryable file type.')
            return

    def get_calendar(self):
        calendar='standard'
        if self.file_type in queryable_file_types:
            try:
                self.open_with_error()
                calendar=netcdf_utils.netcdf_calendar(self.Dataset)
                self.close()
            except dodsError as e:
                self.close()
        return calendar

    def get_time_units(self,calendar):
        if self.file_type in queryable_file_types:
            try:
                self.open_with_error()
                units=netcdf_utils.netcdf_time_units(self.Dataset)
                self.close()
            except dodsError as e:
                self.close()
        else:
            #Get units from filename:
            start_date,end_date=dates_from_filename(self.file_name,calendar)
            units='days since '+str(start_date)
        return units

class dodsError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
class NullDevice():
    def write(self, s):
        pass

def dates_from_filename(filename, calendar):
    """
    Returns datetime objetcs for start and end dates from the filename.

    :param str filename: The filename
    :param str calendar: The NetCDF calendar attribute

    :returns: ``datetime`` instances for start and end dates from the filename
    :rtype: *datetime.datetime*
    This code is adapted from cmip5-timeaxis.

    """
    dates = []
    for date in filename.replace('.nc','').split('_')[-1].split('-'):
        digits = timeaxis_mod.untroncated_timestamp(date)
        # Convert string digits to %Y-%m-%d %H:%M:%S format
        date_as_since = ''.join([''.join(triple) for triple in zip(digits[::2], digits[1::2], ['', '-', '-', ' ', ':', ':', ':'])])[:-1]
        # Use num2date to create netCDF4 datetime objects
        dates.append(netCDF4.num2date(0.0, units='days since ' + date_as_since, calendar=calendar))
    return dates

def rebuild_date_axis(start, length, instant, inc, units,calendar='standard'):
    """
    Rebuilds date axis from numerical time axis, depending on MIP frequency, calendar and instant status.

    :param float date: The numerical date to start (from ``netCDF4.date2num`` or :func:`Date2num`)
    :param int length: The time axis length (i.e., the timesteps number)
    :param boolean instant: The instant status (from :func:`is_instant_time_axis`)
    :param int inc: The time incrementation (from :func:`time_inc`)

    :returns: The corresponding theoretical date axis
    :rtype: *datetime array*

    """
    num_axis = np.arange(start=start, stop=start + length * inc, step=inc)
    if units.split(' ')[0] in ['years', 'months']:
        last = timeaxis_mod.Num2date(num_axis[-1], units=units, calendar=calendar)[0]
    else:
        last = timeaxis_mod.Num2date(num_axis[-1], units=units, calendar=calendar)
    if not instant and not inc in [3, 6]:  # To solve non-instant [36]hr files
        num_axis += 0.5 * inc
    date_axis = timeaxis_mod.Num2date(num_axis, units=units, calendar=calendar)
    return date_axis

def retrieve_slice(variable,indices,unsort_indices,dim,dimensions,dim_id,getitem_tuple=tuple(),num_trials=2):
    if len(dimensions)>0:
        return np.take(np.concatenate(map(lambda x: retrieve_slice(variable,
                                                 indices,
                                                 unsort_indices,
                                                 dimensions[0],
                                                 dimensions[1:],
                                                 dim_id+1,
                                                 getitem_tuple=getitem_tuple+(x,)),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)
    else:
        try:
            return np.take(np.concatenate(map(lambda x: variable.__getitem__(getitem_tuple+(x,)),
                                                     indices[dim]),
                                  axis=dim_id),unsort_indices[dim],axis=dim_id)
        except RuntimeError:
            time.sleep(15)
            if num_trials>0:
                return retrieve_slice(variable,
                                        indices,
                                            unsort_indices,
                                            dim,dimensions,
                                            dim_id,
                                            getitem_tuple=getitem_tuple,
                                            num_trials=num_trials-1)
            else:
                raise RuntimeError

def getitem_pedantic(shape,getitem_tuple):
    getitem_tuple_fixed=()
    for item_id, item in enumerate(getitem_tuple):
        indices_list=range(shape[item_id])[item]
        if indices_list[-1]+item.step>shape[item_id]:
            #Must fix the slice:
            #getitem_tuple_fixed+=(slice(item.start,shape[item_id],item.step),)
            getitem_tuple_fixed+=(indices_list,)
        else:
            getitem_tuple_fixed+=(item,)
    return getitem_tuple_fixed
        
def retrieve_slice_pedantic(variable,indices,unsort_indices,dim,dimensions,dim_id,getitem_tuple=tuple()):
    if len(dimensions)>0:
        return np.take(np.concatenate(map(lambda x: retrieve_slice_pedantic(variable,
                                                 indices,
                                                 unsort_indices,
                                                 dimensions[0],
                                                 dimensions[1:],
                                                 dim_id+1,
                                                 getitem_tuple=getitem_tuple+(x,)),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)
    else:
        shape=variable.shape
        return np.take(np.concatenate(map(lambda x: variable.__getitem__(getitem_pedantic(variable.shape,getitem_tuple+(x,))),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)


def ensure_zero(indices):
    if indices.start > 0:
        return [0,]+range(0,indices.stop)[indices]
    else:
        return indices

def remove_zero_if_added(arr,indices,dim_id):
    if indices.start > 0:
        return np.take(arr,range(1,arr.shape[dim_id]),axis=dim_id)
    else:
        return arr
