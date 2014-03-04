
import numpy as np

import netCDF4
import os

import json

import netcdf_utils

import remote_netcdf

import create_soft_links

import indices_utils

import retrieval_utils

import copy

class create_netCDF_pointers:
    def __init__(self,file_type_list,data_node_list,semaphores=[]):
        self.file_type_list=file_type_list
        self.data_node_list=data_node_list
        self.semaphores=semaphores
        return


    def record_paths(self,output,paths_list,var,years,months):
        #First order source files by preference:
        soft_links=create_soft_links.soft_links(paths_list,self.file_type_list,self.data_node_list,semaphores=self.semaphores)
        soft_links.create(output)
        return

    def record_meta_data(self,output,paths_list,var,years,months):
        #Retrieve time and meta:
        soft_links=create_soft_links.soft_links(paths_list,self.file_type_list,self.data_node_list,semaphores=self.semaphores)
        soft_links.create_variable(output,var,years,months)
        #Put version:
        output.setncattr('netcdf_soft_links_version','1.0')
        return

    def record_fx(self,output,paths_list,var):
        #Find the most recent version:
        most_recent_version='v'+str(np.max([int(item['version'][1:]) for item in paths_list]))
        path=paths_list[[item['version'] for item in paths_list].index(most_recent_version)]

        #Check if data in available:
        remote_data=remote_netcdf.remote_netCDF(path['path'],self.semaphores)
        alt_path_name=remote_data.check_if_available_and_find_alternative([item['path'] for item in paths_list],
                                                                 [item['checksum'] for item in paths_list])

        #Use aternative path:
        path=paths_list[[item['path'] for item in paths_list].index(alt_path_name)]

        for att in path.keys():
            if att!='path':      
                output.setncattr(att,path[att])
        output.setncattr('path',path['path'].split('|')[0])
        output.setncattr('checksum',path['path'].split('|')[1])

        remote_data=remote_netcdf.remote_netCDF(path['path'].split('|')[0].replace('fileServer','dodsC'),self.semaphores)
        remote_data.open()
        remote_data=remote_data.Dataset
        for var_name in remote_data.variables.keys():
            netcdf_utils.replicate_netcdf_var(output,remote_data,var_name)
            output.variables[var_name][:]=remote_data.variables[var_name][:]
        remote_data.close()
        return

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

class read_netCDF_pointers:
    def __init__(self,data_root,data_node=None,queues=dict()):
        self.data_root=data_root
        self.queues=queues
        self.data_node=data_node
        return

    def replicate(self,output):
        #replicate attributes
        netcdf_utils.replicate_netcdf_file(output,self.data_root)
        #replicate and copy variables:
        for var_name in self.data_root.variables.keys():
            netcdf_utils.replicate_and_copy_variable(output,self.data_root,var_name)
        if 'soft_links' in self.data_root.groups.keys():
            output_grp=netcdf_utils.replicate_group(output,self.data_root,'soft_links')
            netcdf_utils.replicate_netcdf_file(output_grp,self.data_root.groups['soft_links'])
            for var_name in self.data_root.groups['soft_links'].variables.keys():
                netcdf_utils.replicate_and_copy_variable(output_grp,self.data_root.groups['soft_links'],var_name)
        return

    def retrieve_time_axis(self,year=None,month=None,min_year=None):
        time_axis=self.data_root.variables['time'][:]
        time_restriction=np.ones(time_axis.shape,dtype=np.bool)
        if year!=None or month!=None:
            date_axis=netcdf_utils.get_date_axis(self.data_root.variables['time'])
            if year!=None:
                year_axis=np.array([date.year for date in date_axis])
                if min_year!=None:
                    #Important for piControl:
                    time_restriction=np.logical_and(time_restriction,year_axis-year_axis.min()+min_year== year)
                else:
                    time_restriction=np.logical_and(time_restriction,year_axis== year)
            if month!=None:
                month_axis=np.array([date.month for date in date_axis])
                time_restriction=np.logical_and(time_restriction,month_axis== month)
        return time_axis,time_restriction

    def retrieve(self,output,retrieval_function,year=None,month=None,min_year=None,source_dir=None,semaphores=dict()):
        #First find time axis, time restriction and which variables to retrieve:
        time_axis, time_restriction=self.retrieve_time_axis(year=year,month=month,min_year=min_year)
        vars_to_retrieve=[var for var in self.data_root.variables.keys() 
                                if  var in self.data_root.groups['soft_links'].variables.keys()]

        #Record to output if output is a netCDF4 Dataset:
        if (isinstance(output,netCDF4.Dataset) or
            isinstance(output,netCDF4.Group)):

            if not 'time' in output.dimensions.keys():
                netcdf_utils.create_time_axis(output,self.data_root,time_axis[time_restriction])

            #Replicate all the other variables:
            for var in set(self.data_root.variables.keys()).difference(vars_to_retrieve):
                if not var in output.variables.keys():
                    output=netcdf_utils.replicate_netcdf_var(output,self.data_root,var)
                    output.variables[var][:]=self.data_root.variables[var][:]

        #Get list of paths:
        paths_list=self.data_root.groups['soft_links'].variables['path'][:]
        checksums_list=self.data_root.groups['soft_links'].variables['checksum'][:]
        paths_id_list=self.data_root.groups['soft_links'].variables['path_id'][:]
        file_type_list=self.data_root.groups['soft_links'].variables['file_type'][:]
        version_list=self.data_root.groups['soft_links'].variables['version'][:]
        if source_dir!=None:
            #Check if the file has already been retrieved:
            paths_list,file_type_list=retrieval_utils.find_local_file(source_dir,self.data_root.groups['soft_links'])

        for var_to_retrieve in vars_to_retrieve:
            self.retrieve_variables(retrieval_function,var_to_retrieve,time_restriction,
                                        paths_list,file_type_list,paths_id_list,checksums_list,version_list,
                                        output,semaphores=semaphores)
        return

    def retrieve_variables(self,retrieval_function,var_to_retrieve,time_restriction,
                                paths_list,file_type_list,paths_id_list,checksums_list,version_list,
                                            output,semaphores=dict()):
        paths_link=self.data_root.groups['soft_links'].variables[var_to_retrieve][time_restriction,0]
        indices_link=self.data_root.groups['soft_links'].variables[var_to_retrieve][time_restriction,1]

        #Convert paths_link to id in path dimension:
        paths_link=np.array([list(paths_id_list).index(path_id) for path_id in paths_link])

        #Sort the paths so that we query each only once:
        sorting_paths=np.argsort(paths_link)
        unique_paths_list_id=np.unique(paths_link[sorting_paths])
        sorted_paths_link=paths_link[sorting_paths]
        sorted_indices_link=indices_link[sorting_paths]
        
        #Replicate variable to output:
        if (isinstance(output,netCDF4.Dataset) or
            isinstance(output,netCDF4.Group)):
            output=netcdf_utils.replicate_netcdf_var(output,self.data_root,var_to_retrieve,chunksize=-1,zlib=True)
            file_path=output.filepath()
        else:
            file_path=output

        dimensions=dict()
        unsort_dimensions=dict()
        dims_length=[]
        for dim in self.data_root.variables[var_to_retrieve].dimensions:
            if dim != 'time':
                dimensions[dim] = self.data_root.variables[dim][:]
                unsort_dimensions[dim] = None
                dims_length.append(len(dimensions[dim]))
        #Maximum number of time step per request:
        max_request=450 #maximum request in Mb
        max_time_steps=np.floor(max_request*1024*1024/(32*np.prod(dims_length)))
        for path_id in unique_paths_list_id:
            path=paths_list[paths_id_list[path_id]]

            #Next, we check if the file is available. If it is not we replace it
            #with another file with the same checksum, if there is one!
            remote_data=remote_netcdf.remote_netCDF(path,semaphores)
            path=remote_data.check_if_available_and_find_alternative(paths_list,checksums_list)
            file_type=file_type_list[list(paths_list).index(path)]
            version='v'+str(version_list[list(paths_list).index(path)])
            checksum=checksums_list[list(paths_list).index(path)]

            #Append the checksum:
            path+='|'+checksum

            time_indices=sorted_indices_link[sorted_paths_link==path_id]
            num_time_chunk=int(np.ceil(len(time_indices)/float(max_time_steps)))
            for time_chunk in range(num_time_chunk):
                dimensions['time'], unsort_dimensions['time'] = indices_utils.prepare_indices(time_indices[time_chunk*max_time_steps:(time_chunk+1)*max_time_steps])
                
                #Get the file tree:
                tree=self.data_root.path.split('/')[1:]+[var_to_retrieve]
                args = (path,
                        var_to_retrieve,
                        dimensions,
                        unsort_dimensions,
                        np.argsort(sorting_paths)[sorted_paths_link==path_id],
                        version,
                        file_path,
                        tree)

                #Retrieve only if it is from the requested data node:
                data_node=retrieval_utils.get_data_node(path,file_type)
                if (self.data_node==None or data_node==self.data_node):
                    self.queues[retrieval_utils.get_data_node(path,file_type)].put((retrieval_function,)+copy.deepcopy(args))
        return 

