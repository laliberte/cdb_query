
import numpy as np
import math

import netCDF4
import datetime

import hashlib

import copy

import os

import cdb_query_archive_class

import json

import retrieval_utils

import netcdf_utils

import indices_utils

import nc_Database

import random


class netCDF_pointers:
    def __init__(self,header,options,semaphores):
        output_file_name=options.out_diagnostic_netcdf_file
        self.output_root=netCDF4.Dataset(output_file_name+'.pid'+str(os.getpid()),'w',format='NETCDF4',diskless=True,persist=True)
        self.header=header
        self.record_header()
        self.semaphores=semaphores
        return

    def record_header(self):
        for value in self.header.keys():
            self.output_root.setncattr(value,json.dumps(self.header[value]))
        #Put version:
        self.output_root.setncattr('cdb_query_file_spec_version','1.0')
        return

    def create_tree(self,tree):
        return create_tree_recursive(self.output_root,tree)

    def record_paths(tree,paths_list,var,time_frequency,experiment):
        #First order source files by preference:
        self.output_root.setncattr('test','allo')
        self.output_root.sync()
        #soft_links=soft_links_netCDF(self.header,self.semaphores,paths_list)
        #output=self.create_tree(tree)
        #soft_links.create(output)
        return

    def record_meta_data(tree,paths_list,var,time_frequency,experiment):
        if not time_frequency in ['fx','clim']:
            #Retrieve time and meta:
            soft_links=soft_links_netCDF(self.header,self.semaphores,paths_list)
            output=self.create_tree(tree)
            soft_links.create_variable(output,var,experiment)
        else:
            #Retrieve fixed and clim variables:
            self.retrieve_fx(tree,paths_list,var)
        return

    def retrieve_fx(tree,paths_list,var):
        #Create tree:
        output=self.create_tree(tree)

        #Find the most recent version:
        most_recent_version='v'+str(np.max([int(item['version'][1:]) for item in paths_list]))
        path=paths_list[[item['version'] for item in paths_list].index(most_recent_version)]

        for att in path.keys():
            if att!='path':      
                output.setncattr(att,path[att])
        output.setncattr('path',path['path'].split('|')[0])
        output.setncattr('checksum',path['path'].split('|')[1])

        dataset=remote_netcdf.remote_netCDF(path['path'].split('|')[0].replace('fileServer','dodsC'))
        dataset.open()
        remote_data=dataset.Dataset
        for var_name in remote_data.variables.keys():
            netcdf_utils.replicate_netcdf_var(output,remote_data,var_name)
            output.variables[var_name][:]=remote_data.variables[var_name][:]
        dataset.close()
        return

def create_tree_recursive(output_root,tree):
    level_name=tree[0][1]
    if not level_name in output_root.groups.keys(): 
        output=output_top.createGroup(level_name)
        output.level_name=tree[0][0]
    else:
        output=output_root.groups[level_name]
    if len(tree)>1:
        output=create_tree(output,tree[1:])
    return output

def record_to_file(output_root,output):
    netcdf_utils.replicate_netcdf_file(output_root,output)
    netcdf_utils.replicate_full_netcdf_recursive(output_root,output)
    filepath=output.filepath()
    output.close()
    try:
        os.remove(filepath)
    except OSError:
        pass
    return


class soft_links_netCDF:
    def __init__(self,header,semaphores,paths_list):
        self.sorts_list=['version','file_type_id','data_node_id','path_id']
        self.id_list=['data_node','file_type','path','checksum','search']
        self.header=header
        self.semaphores=sempaphores
        self.paths_list=paths_list
        self.paths_ordering=self.order_paths_by_preference()
        return

    def create(self,output):
        if not 'soft_links' in output.groups.keys():
            output_grp=output.createGroup('soft_links')
        else:
            output_grp=output.groups['soft_links']

        #OUTPUT TO NETCDF FILE PATHS DESCRIPTIONS:
        output_grp.createDimension('path',len(self.paths_ordering))
        for id in ['version','path_id']:
            output_grp.createVariable(id,np.uint32,('path',))[:]=self.paths_ordering[id]
        for id in id_list:
            temp=output_grp.createVariable(id,str,('path',))
            for file_id, file in enumerate(self.paths_ordering['path']):
                temp[file_id]=str(self.paths_ordering[id][file_id])
        return 

    def order_paths_by_preference(self):
        #FIND ORDERING:
        paths_desc=[]
        for id in self.sorts_list:
            paths_desc.append((id,np.uint32))
        for id in self.id_list:
            paths_desc.append((id,'a255'))
        paths_ordering=np.empty((len(self.paths_list),), dtype=paths_desc)
        for file_id, file in enumerate(self.paths_list):
            paths_ordering['path'][file_id]=file['path'].split('|')[0]
            paths_ordering['path_id'][file_id]=file_id
            paths_ordering['checksum'][file_id]=file['path'].split('|')[1]
            paths_ordering['search'][file_id]=file['search']
            paths_ordering['version'][file_id]=np.uint32(file['version'][1:])
            paths_ordering['file_type'][file_id]=file['file_type']
            paths_ordering['data_node'][file_id]=get_data_node(file['path'],paths_ordering['file_type'][file_id])

        #Sort paths from most desired to least desired:
        #First order desiredness for least to most:
        data_node_order=copy.copy(self.header['data_node_list'])[::-1]#list(np.unique(paths_ordering['data_node']))
        file_type_order=copy.copy(self.header['file_type_list'])[::-1]#list(np.unique(paths_ordering['file_type']))
        for file_id, file in enumerate(self.paths_list):
            paths_ordering['data_node_id'][file_id]=data_node_order.index(paths_ordering['data_node'][file_id])
            paths_ordering['file_type_id'][file_id]=file_type_order.index(paths_ordering['file_type'][file_id])
        #'version' is implicitly from least to most

        #sort and reverse order to get from most to least:
        return np.sort(paths_ordering,order=self.sorts_list)[::-1]

    def recover_time(self,path):
        path_name=str(path['path']).replace('fileServer','dodsC').split('|')[0]
        dataset=remote_netCDF(path,self.semaphores)
        time_axis=dataset.get_time()
        table_desc=[
                   ('paths','a255'),
                   ('indices','uint32')
                   ]
        table=np.empty(time_axis.shape, dtype=table_desc)
        if len(time_axis)>0:
            table['paths']=np.array([str(path_name) for item in time_axis])
            table['indices']=range(0,len(time_axis))
        return time_axis,table
    
    def obtain_table(self):
        self.time_axis, self.table= map(np.concatenate,
                        zip(*map(recover_time,np.nditer(self.path_ordering))))
        return

    def reduce_paths_ordering(self):
        #CREATE LOOK-UP TABLE:
        self.paths_indices=np.empty(self.time_axis.shape,dtype=np.uint32)
        self.ime_indices=np.empty(self.time_axis.shape,dtype=np.uint32)

        paths_list=[path for path in self.paths_ordering['path'] ]
        for path_id, path in enumerate(paths_list):
            paths_indices[path.replace('fileServer','dodsC')==table['paths']]=path_id

        #Remove paths that are not necessary over the requested time range:
        paths_id_list=list(np.unique([np.min(paths_indices[time==self.time_axis])  for time_id, time in enumerate(self.time_axis_unique)]))
        useful_file_name_list=[path.split('/')[-1] for path in self.paths_ordering['path'][paths_id_list] ]
        for file_id, file in enumerate(self.paths_ordering):
            if not file_id in paths_id_list:
                file_name=self.paths_ordering['path'][file_id].split('/')[-1]
                if file_name in useful_file_name_list:
                    equivalent_file_id=paths_id_list[useful_file_name_list.index(file_name)]
                    if self.paths_ordering['checksum'][file_id]==self.paths_ordering['checksum'][equivalent_file_id]:
                        paths_id_list.append(file_id)
        #Sort paths_ordering:
        self.paths_ordering=self.paths_ordering[np.sort(paths_id_list)]
        #Finally, set the path_id field to be following the indices in paths_ordering:
        self.paths_ordering['path_id']=range(len(paths_ordering))

        #Recompute the indices to paths:
        paths_list=[path for path in self.paths_ordering['path'] ]
        for path_id, path in enumerate(paths_list):
            self.paths_indices[path.replace('fileServer','dodsC')==self.table['paths']]=path_id
        return

    def unique_time_axis(data,experiment):
        time_axis = netCDF4.date2num(self.time_axis,units=data.variables['time'].units,calendar=data.variables['time'].calendar)
        time_axis_unique = np.unique(time_axis)

        time_axis_unique_date=netCDF4.num2date(time_axis_unique,units=data.variables['time'].units,calendar=data.variables['time'].calendar)

        #Include a filter on years: 
        time_desc={}
        years_range=range(*[ int(year) for year in self.header['experiment_list'][experiment].split(',')])
        years_range+=[years_range[-1]+1]
        if int(self.header['experiment_list'][experiment].split(',')[0])<10:
            #This is important for piControl
            years_range=list(np.array(years_range)+np.min([date.year for date in time_axis_unique_date]))
            #min_year=np.min([date.year for date in time_axis_unique_date])
        if 'months_list' in self.header.keys():
            months_range=self.header['months_list']
        else:
            months_range=range(1,13)

        valid_times=np.array([True  if (date.year in years_range and 
                                     date.month in months_range) else False for date in  time_axis_unique_date])
        self.time_axis_unique=time_axis_unique[valid_times]
        self.time_axis=time_axis
        return

    def create_variable(self,output,var,experiment):
        #Recover time axis for all files:
        self.obtain_table()

        #Open the first file and use its metadata to populate container file:
        remote_data=remote_netcdf.remote_netCDF(self.table['paths'][0],self.semaphores)
        remote_data.open()
        data=remote_data.Dataset
        netcdf_utils.replicate_netcdf_file(output,data)

        #Convert time axis to numbers and find the unique time axis:
        self.unique_time_axis(data,experiment)

        self.reduce_paths_ordering()
        #Create time axis in ouptut:
        netcdf_utils.create_time_axis(output,data,self.time_axis_unique)

        self.create(output)
        self.record_indices(output,data,var)
        remote_data.close()
        return

    def record_indices(self,output,var):
        netcdf_utils.replicate_netcdf_var(output,data,var)

        for other_var in data.variables.keys():
            if not 'time' in data.variables[other_var].dimensions:
                output=netcdf_utils.replicate_netcdf_var(output,data,other_var)
                output.variables[other_var][:]=data.variables[other_var][:]

        #CREATE LOOK-UP TABLE:
        output_grp=output.groups['soft_links']
        output_grp.createDimension('indices',2)
        indices=output_grp.createVariable('indices',np.str,('indices',))
        indices[0]='path'
        indices[1]='time'

        var_out = output_grp.createVariable(var,np.uint32,('time','indices'),zlib=False,fill_value=np.iinfo(np.uint32).max)

        #Create soft links:
        for time_id, time in enumerate(self.time_axis_unique):
            var_out[time_id,0]=np.min(self.paths_indices[time==self.time_axis])
            var_out[time_id,1]=self.table['indices'][np.logical_and(self.paths_indices==var_out[time_id,0],time==self.time_axis)][0]
        return


#def create_variable_soft_links2(data,output,var,time_axis,time_axis_unique,paths_indices,paths_list,table):
#    #CREATE LOOK-UP TABLE:
#    common_substring=long_substr(paths_list)
#    soft_link_numpy=np.dtype([(path.replace('/','&#47;'),np.byte) for path in paths_list]+[('time',np.uint16)])
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



