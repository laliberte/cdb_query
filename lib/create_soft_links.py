
import numpy as np
import math

import tempfile

import netCDF4
import datetime

import copy

import os

import cdb_query_archive_class

import json

import retrieval_utils

import netcdf_utils

import remote_netcdf

import indices_utils

import nc_Database

import random


queryable_file_types=['OPeNDAP','local_file']
class create_netCDF_pointers:
    def __init__(self,paths_list,var,time_frequency,years,months,file_type_list,data_node_list,semaphores=[]):
        self.file_type_list=file_type_list
        self.data_node_list=data_node_list
        self.semaphores=semaphores
        self.paths_list=paths_list

        self.sorts_list=['version','file_type_id','data_node_id','path_id']
        self.id_list=['data_node','file_type','path','checksum']

        self.var=var
        self.time_frequency=time_frequency
        self.is_instant=True

        self.months=months
        self.years=years

        self.paths_ordering=self.order_paths_by_preference()
        
        self.calendar=self.obtain_unique_calendar()
        return

    def record_paths(self,output,username=None,user_pass=None):
        self.create(output)
        return

    def record_meta_data(self,output,username=None,user_pass=None):
        if self.time_frequency in ['fx','clim']:
            self.record_fx(output,username=username,user_pass=user_pass)
        else:
            #Retrieve time and meta:
            self.create_variable(output,self.var,self.years,self.months)
            #Put version:
            output.setncattr('netcdf_soft_links_version','1.1')
        return

    def record_fx(self,output,username=None,user_pass=None):
        #Create soft links
        self.create(output)
        output.groups['soft_links'].createVariable(self.var,np.float32,(),zlib=True)

        #Find the most recent version:
        most_recent_version='v'+str(np.max([int(item['version'][1:]) for item in self.paths_list]))
        usable_paths_list=[ item for item in self.paths_list if item['version']==most_recent_version]

        queryable_paths_list=[item for item in usable_paths_list if item['file_type'] in queryable_file_types]
        if len(queryable_paths_list)==0:
            temp_file_handle, temp_file_name=tempfile.mkstemp()

        try:
            if len(queryable_paths_list)==0:
                path=usable_paths_list[0]
                #output.createVariable(self.var,np.float32,(),zlib=True)
                #Download the file to temp
                retrieval_utils.download_secure(path['path'].split('|')[0],
                                temp_file_name,
                                path['file_type'],
                                username=username,user_pass=user_pass)
                remote_data=remote_netcdf.remote_netCDF(temp_file_name,path['file_type'],self.semaphores)
            else:
                #Check if data in available:
                path = queryable_paths_list[0]

                remote_data=remote_netcdf.remote_netCDF(path['path'].split('|')[0],path['file_type'],self.semaphores)
                alt_path_name=remote_data.check_if_available_and_find_alternative([item['path'].split('|')[0] for item in queryable_paths_list],
                                                                         [item['path'].split('|')[1] for item in queryable_paths_list])

                #Use aternative path:
                path=queryable_paths_list[[item['path'].split('|')[0] for item in queryable_paths_list].index(alt_path_name)]
                remote_data=remote_netcdf.remote_netCDF(path['path'].split('|')[0],path['file_type'],self.semaphores)
            remote_data.retrieve_variables(output,zlib=True)

            for att in path.keys():
                if att!='path':      
                    output.setncattr(att,path[att])
            output.setncattr('path',path['path'].split('|')[0])
            output.setncattr('checksum',path['path'].split('|')[1])
            output.sync()
        finally:
            pass
            if len(queryable_paths_list)==0:
                os.remove(temp_file_name)
        return

    def create(self,output):
        if not 'soft_links' in output.groups.keys():
            output_grp=output.createGroup('soft_links')
        else:
            output_grp=output.groups['soft_links']

        #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
        output_grp.createDimension('path',len(self.paths_ordering))
        for id in ['version','path_id']:
            output_grp.createVariable(id,np.int32,('path',))[:]=self.paths_ordering[id]
        for id in self.id_list:
            temp=output_grp.createVariable(id,str,('path',))
            for file_id, file in enumerate(self.paths_ordering['path']):
                temp[file_id]=str(self.paths_ordering[id][file_id])
        return 

    def order_paths_by_preference(self):
        #FIND ORDERING:
        paths_desc=[]
        for id in self.sorts_list:
            paths_desc.append((id,np.int32))
        for id in self.id_list:
            paths_desc.append((id,'a255'))
        paths_ordering=np.empty((len(self.paths_list),), dtype=paths_desc)
        for file_id, file in enumerate(self.paths_list):
            paths_ordering['path'][file_id]=file['path'].split('|')[0]
            #Convert path name to 'unique' integer using hash.
            #The integer will not really be unique but collisions
            #should be extremely rare for similar strings with only small variations.
            paths_ordering['path_id'][file_id]=hash(
                                                    paths_ordering['path'][file_id]
                                                        )
            paths_ordering['checksum'][file_id]=file['path'].split('|')[1]
            paths_ordering['version'][file_id]=np.long(file['version'][1:])

            paths_ordering['file_type'][file_id]=file['file_type']
            paths_ordering['data_node'][file_id]=retrieval_utils.get_data_node(file['path'],paths_ordering['file_type'][file_id])

        #Sort paths from most desired to least desired:
        #First order desiredness for least to most:
        data_node_order=copy.copy(self.data_node_list)[::-1]#list(np.unique(paths_ordering['data_node']))
        file_type_order=copy.copy(self.file_type_list)[::-1]#list(np.unique(paths_ordering['file_type']))
        for file_id, file in enumerate(self.paths_list):
            paths_ordering['data_node_id'][file_id]=data_node_order.index(paths_ordering['data_node'][file_id])
            paths_ordering['file_type_id'][file_id]=file_type_order.index(paths_ordering['file_type'][file_id])
        #'version' is implicitly from least to most

        #sort and reverse order to get from most to least:
        return np.sort(paths_ordering,order=self.sorts_list)[::-1]

    def _recover_time(self,path):
        file_type=path['file_type']
        checksum=path['checksum']
        path_name=str(path['path']).split('|')[0]
        remote_data=remote_netcdf.remote_netCDF(path_name,file_type,self.semaphores)
        time_axis=remote_data.get_time(time_frequency=self.time_frequency,
                                        is_instant=self.is_instant,
                                        calendar=self.calendar)
        table_desc=[
                   ('paths','a255'),
                   ('file_type','a255'),
                   ('checksum','a255'),
                   ('indices','int32')
                   ]
        table=np.empty(time_axis.shape, dtype=table_desc)
        if len(time_axis)>0:
            table['paths']=np.array([str(path_name) for item in time_axis])
            table['file_type']=np.array([str(file_type) for item in time_axis])
            table['checksum']=np.array([str(checksum) for item in time_axis])
            table['indices']=range(0,len(time_axis))
        return time_axis,table
    
    def obtain_table(self):
        #Retrieve time axes from queryable file types or reconstruct time axes from time stamp
        #from non-queryable file types.
        self.time_axis, self.table= map(np.concatenate,
                        zip(*map(self._recover_time,np.nditer(self.paths_ordering))))
        return

    def _recover_calendar(self,path):
        file_type=path['file_type']
        checksum=path['checksum']
        path_name=str(path['path']).split('|')[0]
        remote_data=remote_netcdf.remote_netCDF(path_name,file_type,self.semaphores)
        calendar=remote_data.get_calendar()
        return calendar, file_type 

    def obtain_unique_calendar(self):
        calendar_list,file_type_list=zip(*map(self._recover_calendar,np.nditer(self.paths_ordering)))
        #Find the calendars found from queryable file types:
        calendars = set([item[0] for item in zip(calendar_list,file_type_list) if item[1] in queryable_file_types])
        if len(calendars)==1:
            return calendars.pop()
        return calendar_list[0]

    def reduce_paths_ordering(self):
        #CREATE LOOK-UP TABLE:
        self.paths_indices=np.empty(self.time_axis.shape,dtype=np.int32)
        self.time_indices=np.empty(self.time_axis.shape,dtype=np.int32)

        paths_list=[path for path in self.paths_ordering['path'] ]
        paths_id_list=[path_id for path_id in self.paths_ordering['path_id'] ]
        for path_id, path in zip(paths_id_list,paths_list):
            #find in self.table the path and assign path_id to it:
            self.paths_indices[path==self.table['paths']]=path_id

        #Remove paths that are not necessary over the requested time range:
        #First, list the paths_id used:
        useful_paths_id_list=list(np.unique([np.min(self.paths_indices[time==self.time_axis])  for time_id, time in enumerate(self.time_axis_unique)]))
        #Second, list the path_names corresponding to these paths_id:
        useful_file_name_list=[useful_path.split('/')[-1] for useful_path in 
                                [path for path_id, path in zip(paths_id_list,paths_list)
                                        if path_id in useful_paths_id_list] ]

        #Find the indices to keep:
        useful_file_id_list=[file_id for file_id, file in enumerate(self.paths_ordering)
                                if self.paths_ordering['path_id'][file_id] in useful_paths_id_list]
                                
        #Finally, check if some equivalent indices are worth keeping:
        for file_id, file in enumerate(self.paths_ordering):
            if not self.paths_ordering['path_id'][file_id] in useful_paths_id_list:
                #This file was not kept but it might be the same data, in which
                #case we would like to keep it.
                #Find the file name (remove path):
                file_name=self.paths_ordering['path'][file_id].split('/')[-1]
                if file_name in useful_file_name_list:
                    #If the file name is useful, find its path_id: 
                    equivalent_path_id=useful_paths_id_list[useful_file_name_list.index(file_name)]
                    #Use this to find its file_id:
                    equivalent_file_id=list(self.paths_ordering['path_id']).index(equivalent_path_id)
                    #Then check if the checksum are the same. If yes, keep the file!
                    if self.paths_ordering['checksum'][file_id]==self.paths_ordering['checksum'][equivalent_file_id]:
                        useful_file_id_list.append(file_id)
                
        #Sort paths_ordering:
        if len(useful_file_id_list)>0:
            self.paths_ordering=self.paths_ordering[np.sort(useful_file_id_list)]
            
        #The last lines were commented to allow for collision-free (up to 32-bits hashing
        #algorithm) indexing.

        #Finally, set the path_id field to be following the indices in paths_ordering:
        #self.paths_ordering['path_id']=range(len(self.paths_ordering))

        #Recompute the indices to paths:
        #paths_list=[path for path in self.paths_ordering['path'] ]
        #for path_id, path in enumerate(paths_list):
        #    self.paths_indices[path.replace('fileServer','dodsC')==self.table['paths']]=path_id
        return

    def unique_time_axis(self,data,years,months):
        if data==None:
            units='days since '+str(self.time_axis[0])
        else:
            units=data.variables['time'].units

        time_axis = netCDF4.date2num(self.time_axis,units,calendar=self.calendar)
        time_axis_unique = np.unique(time_axis)

        time_axis_unique_date=netCDF4.num2date(time_axis_unique,units,calendar=self.calendar)

        #Include a filter on years: 
        time_desc={}
        years_range=range(*years)
        years_range.append(years[-1])
        if years[0]<10:
            #This is important for piControl
            years_range=list(np.array(years_range)+np.min([date.year for date in time_axis_unique_date]))
            #min_year=np.min([date.year for date in time_axis_unique_date])

        valid_times=np.array([True  if (date.year in years_range and 
                                     date.month in months) else False for date in  time_axis_unique_date])
        self.time_axis_unique=time_axis_unique[valid_times]
        self.time_axis_unique_date=time_axis_unique_date[valid_times]
        self.time_axis=time_axis
        return

    def create_variable(self,output,var,years,months):
        #Recover time axis for all files:
        self.obtain_table()

        queryable_file_types_available=list(set(self.table['file_type']).intersection(queryable_file_types))
        if len(self.table['paths'])>0:
            if len(queryable_file_types_available)>0:
                #Open the first file and use its metadata to populate container file:
                first_id=list(self.table['file_type']).index(queryable_file_types_available[0])
                remote_data=remote_netcdf.remote_netCDF(self.table['paths'][first_id],self.table['file_type'][first_id],self.semaphores)
                #try:
                remote_data.open_with_error()
                netcdf_utils.replicate_netcdf_file(output,remote_data.Dataset)
            else:
                remote_data=remote_netcdf.remote_netCDF(self.table['paths'][0],self.table['file_type'][0],self.semaphores)

            #Convert time axis to numbers and find the unique time axis:
            self.unique_time_axis(remote_data.Dataset,years,months)

            self.reduce_paths_ordering()
            #Create time axis in ouptut:
            netcdf_utils.create_time_axis_date(output,remote_data.Dataset,self.time_axis_unique_date)

            self.create(output)
            #if len(queryable_file_types_available)>0:
            self.record_indices(output,remote_data.Dataset,var)
            #except dodsError as e:
            #    e_mod=" This is an uncommon error. It is likely to be FATAL."
            #    print e.value+e_mod
            remote_data.close()

            output.sync()
        return

    def record_indices(self,output,data,var):
        if data!=None:
            #Create descriptive vars:
            for other_var in data.variables.keys():
                if ( (not 'time' in data.variables[other_var].dimensions) and 
                     (not other_var in output.variables.keys()) ):
                    netcdf_utils.replicate_and_copy_variable(output,data,other_var)

        #CREATE LOOK-UP TABLE:
        output_grp=output.groups['soft_links']
        output_grp.createDimension('indices',2)
        indices=output_grp.createVariable('indices',np.str,('indices',))
        indices[0]='path'
        indices[1]='time'

        #Create main variable:
        if data!=None:
            netcdf_utils.replicate_netcdf_var(output,data,var,chunksize=-1,zlib=True)
        else:
            output.createVariable(var,np.float32,('time',),zlib=True)

        var_out = output_grp.createVariable(var,np.int32,('time','indices'),zlib=False,fill_value=np.iinfo(np.int32).max)
        #Create soft links:
        for time_id, time in enumerate(self.time_axis_unique):
            var_out[time_id,0]=np.min(self.paths_indices[time==self.time_axis])
            var_out[time_id,1]=self.table['indices'][np.logical_and(self.paths_indices==var_out[time_id,0],time==self.time_axis)][0]

        if data!=None:
            #Create support variables:
            for other_var in data.variables.keys():
                if ( ('time' in data.variables[other_var].dimensions) and (other_var!=var) and
                     (not other_var in output.variables.keys()) ):
                    netcdf_utils.replicate_netcdf_var(output,data,other_var,chunksize=-1,zlib=True)
                    var_out = output_grp.createVariable(other_var,np.int32,('time','indices'),zlib=False,fill_value=np.iinfo(np.int32).max)
                    #Create soft links:
                    for time_id, time in enumerate(self.time_axis_unique):
                        var_out[time_id,0]=np.min(self.paths_indices[time==self.time_axis])
                        var_out[time_id,1]=self.table['indices'][np.logical_and(self.paths_indices==var_out[time_id,0],time==self.time_axis)][0]
        return


#def create_variable_soft_links2(data,output,var,time_axis,time_axis_unique,paths_indices,paths_list,table):
#    #CREATE LOOK-UP TABLE:
#    common_substring=long_substr(paths_list)
#    soft_link_numpy=np.dtype([(path.replace('/','&#47;'),np.byte) for path in paths_list]+[('time',np.int16)])
#    soft_link=output.createCompoundType(soft_link_numpy,'soft_link')
#    netcdf_utils.replicate_netcdf_var(output,data,var,chunksize=-1,datatype=soft_link)
#    for var in data.variables.keys():
#        if not 'time' in data.variables[var].dimensions:
#            output=netcdf_utils.replicate_netcdf_var(output,data,var)
#            output.variables[var][:]=data.variables[var][:]
#    shape = (1,)+output.variables[var].shape[1:]
#
#    #Create soft links:
#    var_out=output.variables[var]
#    for time_id, time in enumerate(time_axis_unique):
#        var_temp=np.zeros(shape,dtype=soft_link_numpy)
#
#        path_id=np.min(paths_indices[time==time_axis])
#        var_temp[paths_list[path_id].replace('/','&#47;')]=1
#        var_temp['time']=table['indices'][np.logical_and(paths_indices==path_id,time==time_axis)][0]
#        var_out[time_id,...]=var_temp
#    return
#
#def long_substr(data):
#    #from http://stackoverflow.com/questions/2892931/longest-common-substring-from-more-than-two-strings-python
#    substr = ''
#    if len(data) > 1 and len(data[0]) > 0:
#        for i in range(len(data[0])):
#            for j in range(len(data[0])-i+1):
#                if j > len(substr) and all(data[0][i:i+j] in x for x in data):
#                    substr = data[0][i:i+j]
#    return substr



