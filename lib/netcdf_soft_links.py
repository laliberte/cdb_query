
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

import nc_Database

class create_netCDF_pointers:
    def __init__(self,file_type_list,data_node_list,semaphores=None):
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
        remote_data=remote_netcdf.remote_netCDF(path['path'].split('|')[0].replace('fileServer','dodsC'),self.semaphores)
        alt_path_name=remote_data.check_if_available_and_find_alternative([item['path'].split('|')[0].replace('fileServer','dodsC') for item in paths_list],
                                                                 [item['path'].split('|')[1] for item in paths_list]).replace('dodsC','fileServer')

        #Use aternative path:
        path=paths_list[[item['path'].split('|')[0] for item in paths_list].index(alt_path_name)]

        for att in path.keys():
            if att!='path':      
                output.setncattr(att,path[att])
        output.setncattr('path',path['path'].split('|')[0])
        output.setncattr('checksum',path['path'].split('|')[1])

        remote_data=remote_netcdf.remote_netCDF(path['path'].split('|')[0].replace('fileServer','dodsC'),self.semaphores)
        #open and record:
        try:
            remote_data.open_with_error()
            for var_name in remote_data.Dataset.variables.keys():
                netcdf_utils.replicate_and_copy_variable(output,remote_data.Dataset,var_name,zlib=True)
                #netcdf_utils.replicate_netcdf_var(output,remote_data.Dataset,var_name)
                #output.variables[var_name][:]=remote_data.Dataset.variables[var_name][:]
            output.sync()
        except remote_netcdf.dodsError as e:
            e_mod=" This is an uncommon error. It is likely to be FATAL."
            print e.value+e_mod
        remote_data.close()
        return


class read_netCDF_pointers:
    def __init__(self,data_root,Xdata_node=None,data_node=None,queues=dict()):
        self.data_root=data_root
        self.queues=queues
        self.data_node=data_node
        self.Xdata_node=Xdata_node
        return

    def initialize_retrieval(self):

        if 'soft_links' in self.data_root.groups.keys():
            #Initialize variables:
            self.retrievable_vars=[var for var in self.data_root.variables.keys() 
                                if  var in self.data_root.groups['soft_links'].variables.keys()]

            #Get list of paths:
            self.paths_list=self.data_root.groups['soft_links'].variables['path'][:]
            self.checksums_list=self.data_root.groups['soft_links'].variables['checksum'][:]
            self.paths_id_list=self.data_root.groups['soft_links'].variables['path_id'][:]
            self.file_type_list=self.data_root.groups['soft_links'].variables['file_type'][:]
            self.version_list=self.data_root.groups['soft_links'].variables['version'][:]
        else:
            self.retrievable_vars=[var for var in self.data_root.variables.keys()]

        self.variables=dict()
        return

    def replicate(self,output,hdf5=None,check_empty=False):
        #replicate attributes
        netcdf_utils.replicate_netcdf_file(output,self.data_root)
        #replicate and copy variables:
        for var_name in self.data_root.variables.keys():
            netcdf_utils.replicate_and_copy_variable(output,self.data_root,var_name,hdf5=hdf5,check_empty=check_empty)
        if 'soft_links' in self.data_root.groups.keys():
            output_grp=netcdf_utils.replicate_group(output,self.data_root,'soft_links')
            netcdf_utils.replicate_netcdf_file(output_grp,self.data_root.groups['soft_links'])
            for var_name in self.data_root.groups['soft_links'].variables.keys():
                netcdf_utils.replicate_and_copy_variable(output_grp,self.data_root.groups['soft_links'],var_name,hdf5=hdf5['soft_links'],check_empty=check_empty)
        return

    def retrieve_time_axis(self,years=None,months=None,days=None,min_year=None,previous=False,next=False):
        time_axis=self.data_root.variables['time'][:]
        time_restriction=np.ones(time_axis.shape,dtype=np.bool)
        if years!=None or months!=None or days!=None:
            date_axis=netcdf_utils.get_date_axis(self.data_root.variables['time'])
            if years!=None:
                years_axis=np.array([date.year for date in date_axis])
                if min_year!=None:
                    #Important for piControl:
                    #time_restriction=np.logical_and(time_restriction,years_axis-years_axis.min()+min_year== years)
                    time_restriction=np.logical_and(time_restriction,[True if year in years else False for year in years_axis-years_axis.min()+min_year])
                else:
                    #time_restriction=np.logical_and(time_restriction,years_axis== years)
                    time_restriction=np.logical_and(time_restriction,[True if year in years else False for year in years_axis])
            if months!=None:
                months_axis=np.array([date.month for date in date_axis])
                #time_restriction=np.logical_and(time_restriction,months_axis==month)
                time_restriction=np.logical_and(time_restriction,[True if month in months else False for month in months_axis])
                #Check that months are continuous:
                if months==[item for item in months if (item % 12 +1 in months or item-2% 12+1 in months)]:
                    time_restriction_copy=copy.copy(time_restriction)
                    #Months are continuous further restrict time_restriction to preserve continuity:
                    if time_restriction[0] and months_axis[0]-2 % 12 +1 in months:
                        time_restriction[0]=False
                    if time_restriction[-1] and months_axis[-1] % 12 +1 in months:
                        time_restriction[-1]=False

                    for id in range(len(time_restriction))[1:-1]:
                        if time_restriction[id]:
                            #print date_axis[id], months_axis[id-1],months_axis[id], months_axis[id]-2 % 12 +1, time_restriction[id-1]
                            if (( ((months_axis[id-1]-1)-(months_axis[id]-1)) % 12 ==11 or
                                 months_axis[id-1] == months_axis[id] ) and
                                not time_restriction[id-1]):
                                time_restriction[id]=False

                    for id in reversed(range(len(time_restriction))[1:-1]):
                        if time_restriction[id]:
                            if (( ((months_axis[id+1]-1)-(months_axis[id]-1)) % 12 ==1 or
                                 months_axis[id+1] == months_axis[id] ) and
                                not time_restriction[id+1]):
                                time_restriction[id]=False
                    #If all values were eliminated, do not ensure continuity:
                    if not np.any(time_restriction):
                        time_restriction=time_restriction_copy

            if days!=None:
                days_axis=np.array([date.day for date in date_axis])
                time_restriction=np.logical_and(time_restriction,[True if month in days else False for month in days_axis])
                    
                
        if previous and next:
            return time_axis,add_next(add_previous(time_restriction))
        elif previous and not next:
            return time_axis,add_previous(time_restriction)
        elif not previous and next:
            return time_axis,add_next(time_restriction)
        else:
            return time_axis,time_restriction

    def retrieve(self,output,retrieval_function,year=None,month=None,day=None,min_year=None,previous=False,next=False,source_dir=None,semaphores=[]):
        self.initialize_retrieval()
        if source_dir!=None:
            #Check if the file has already been retrieved:
            self.paths_list,self.file_type_list=retrieval_utils.find_local_file(source_dir,self.data_root.groups['soft_links'])

        #Define tree:
        self.tree=self.data_root.path.split('/')[1:]

        if 'time' in self.data_root.variables.keys():
            #Then find time axis, time restriction and which variables to retrieve:
            time_axis, time_restriction=self.retrieve_time_axis(years=year,months=month,days=day,min_year=min_year,previous=previous,next=next)

            #Record to output if output is a netCDF4 Dataset:
            if (isinstance(output,netCDF4.Dataset) or
                isinstance(output,netCDF4.Group)):

                if not 'time' in output.dimensions.keys():
                    netcdf_utils.create_time_axis(output,self.data_root,time_axis[time_restriction])

                #Replicate all the other variables:
                for var in set(self.data_root.variables.keys()).difference(self.retrievable_vars):
                    if not var in output.variables.keys():
                        output=netcdf_utils.replicate_and_copy_variable(output,self.data_root,var)
                        #output=netcdf_utils.replicate_netcdf_var(output,self.data_root,var)
                        #output.variables[var][:]=self.data_root.variables[var][:]

            for var_to_retrieve in self.retrievable_vars:
                self.retrieve_variables(retrieval_function,var_to_retrieve,time_restriction,
                                            output,semaphores=semaphores)
                                            #paths_list,file_type_list,paths_id_list,checksums_list,version_list,
        else:
            #Downloading before a complete validate has been performed:
            self.retrieve_without_time(retrieval_function,output,semaphores=semaphores)
        return

    def open(self):
        self.initialize_retrieval()
        self.tree=[]
        self.output_root=netCDF4.Dataset('temp_file.pid'+str(os.getpid()),
                                      'w',format='NETCDF4',diskless=True,persist=False)
        return

    def assign(self,var_to_retrieve,time_restriction):
        time_axis, time_bool=self.retrieve_time_axis()

        self.output_root.createGroup(var_to_retrieve)
        netcdf_utils.create_time_axis(self.output_root.groups[var_to_retrieve],self.data_root,time_axis[np.array(time_restriction)])
        self.retrieve_variables('retrieve_path_data',var_to_retrieve,np.array(time_restriction),self.output_root.groups[var_to_retrieve])
        for var in self.output_root.groups[var_to_retrieve].variables.keys():
            self.variables[var]=self.output_root.groups[var_to_retrieve].variables[var]
        return

    def close(self):
        self.output_root.close()
        return

    def retrieve_without_time(self,retrieval_function,output,semaphores=None):
        #This function simply retrieves all the files:
        file_path=output
        for path_to_retrieve in self.paths_list:
            file_type=self.file_type_list[list(self.paths_list).index(path_to_retrieve)]
            version='v'+str(self.version_list[list(self.paths_list).index(path_to_retrieve)])
            checksum=self.checksums_list[list(self.paths_list).index(path_to_retrieve)]
            #Get the file tree:
            args = ({'path':path_to_retrieve+'|'+checksum,
                    'var':self.tree[-1],
                    'file_path':file_path,
                    'version':version},
                    copy.deepcopy(self.tree))
                    #'sort_table':np.argsort(sorting_paths)[sorted_paths_link==path_id][time_slice],

            #Retrieve only if it is from the requested data node:
            data_node=retrieval_utils.get_data_node(path_to_retrieve,file_type)
            if nc_Database.is_level_name_included_and_not_excluded('data_node',self,data_node):
                if data_node in self.queues.keys():
                    #print 'Recovering '+var_to_retrieve+' in '+path_to_retrieve
                    print 'Recovering '+'/'.join(self.tree)
                    self.queues[data_node].put((retrieval_function,)+copy.deepcopy(args))
        return

    def retrieve_variables(self,retrieval_function,var_to_retrieve,time_restriction,
                                            output,semaphores=None):
        #Replicate variable to output:
        if (isinstance(output,netCDF4.Dataset) or
            isinstance(output,netCDF4.Group)):
            output=netcdf_utils.replicate_netcdf_var(output,self.data_root,var_to_retrieve,chunksize=-1,zlib=True)
            #file_path=output.filepath()
            file_path=None
            if not 'soft_links' in self.data_root.groups.keys():
                #Variable is stored here and simply retrieve it:
                output.variables[var_to_retrieve][:]=self.data_root.variables[var_to_retrieve][time_restriction]
                return
        else:
            file_path=output

        dimensions=dict()
        unsort_dimensions=dict()
        dims_length=[]
        for dim in self.data_root.variables[var_to_retrieve].dimensions:
            if dim != 'time':
                if dim in self.data_root.variables.keys():
                    dimensions[dim] = self.data_root.variables[dim][:]
                else:
                    dimensions[dim] = np.arange(len(self.data_root.dimensions[dim]))
                unsort_dimensions[dim] = None
                dims_length.append(len(dimensions[dim]))

        # Determine the paths_ids for soft links:
        paths_link=self.data_root.groups['soft_links'].variables[var_to_retrieve][time_restriction,0]
        indices_link=self.data_root.groups['soft_links'].variables[var_to_retrieve][time_restriction,1]

        #Convert paths_link to id in path dimension:
        paths_link=np.array([list(self.paths_id_list).index(path_id) for path_id in paths_link])

        #Sort the paths so that we query each only once:
        unique_paths_list_id, sorting_paths=np.unique(paths_link,return_inverse=True)

        #Maximum number of time step per request:
        max_request=450 #maximum request in Mb
        max_time_steps=max(int(np.floor(max_request*1024*1024/(32*np.prod(dims_length)))),1)
        for unique_path_id, path_id in enumerate(unique_paths_list_id):
            path_to_retrieve=self.paths_list[self.paths_id_list[path_id]]

            #Next, we check if the file is available. If it is not we replace it
            #with another file with the same checksum, if there is one!
            remote_data=remote_netcdf.remote_netCDF(path_to_retrieve.replace('fileServer','dodsC'),semaphores)
            path_to_retrieve=remote_data.check_if_available_and_find_alternative([path.replace('fileServer','dodsC') for path in self.paths_list],
                                                                      self.checksums_list).replace('dodsC','fileServer')
            file_type=self.file_type_list[list(self.paths_list).index(path_to_retrieve)]
            version='v'+str(self.version_list[list(self.paths_list).index(path_to_retrieve)])
            checksum=self.checksums_list[list(self.paths_list).index(path_to_retrieve)]

            #Append the checksum:
            path_to_retrieve+='|'+checksum

            #time_indices=sorted_indices_link[sorted_paths_link==path_id]
            time_indices=indices_link[sorting_paths==unique_path_id]
            num_time_chunk=int(np.ceil(len(time_indices)/float(max_time_steps)))
            for time_chunk in range(num_time_chunk):
                time_slice=slice(time_chunk*max_time_steps,(time_chunk+1)*max_time_steps,1)
                dimensions['time'], unsort_dimensions['time'] = indices_utils.prepare_indices(time_indices[time_slice])
                
                #Get the file tree:
                args = ({'path':path_to_retrieve,
                        'var':var_to_retrieve,
                        'indices':dimensions,
                        'unsort_indices':unsort_dimensions,
                        'sort_table':np.arange(len(sorting_paths))[sorting_paths==unique_path_id][time_slice],
                        'file_path':file_path,
                        'version':version},
                        copy.deepcopy(self.tree))
                        #'sort_table':np.argsort(sorting_paths)[sorted_paths_link==path_id][time_slice],

                #Retrieve only if it is from the requested data node:
                data_node=retrieval_utils.get_data_node(path_to_retrieve,file_type)
                if nc_Database.is_level_name_included_and_not_excluded('data_node',self,data_node):
                    if data_node in self.queues.keys():
                        if ( (isinstance(output,netCDF4.Dataset) or
                             isinstance(output,netCDF4.Group)) or
                             time_chunk==0 ):
                            #If it is download: retrieve
                            #If it is download_raw: retrieve only first time_chunk
                            if var_to_retrieve==self.tree[-1]:
                                #print 'Recovering '+var_to_retrieve+' in '+path_to_retrieve
                                print 'Recovering '+'/'.join(self.tree)
                            self.queues[data_node].put((retrieval_function,)+copy.deepcopy(args))
                    else:
                        if (isinstance(output,netCDF4.Dataset) or
                            isinstance(output,netCDF4.Group)):
                            #netcdf_utils.assign_tree(output,*getattr(netcdf_utils,retrieval_function)(args[0],args[1]))
                            netcdf_utils.assign_tree(output,*getattr(retrieval_utils,retrieval_function)(args[0],args[1]))
        return 

def add_previous(time_restriction):
    return np.logical_or(time_restriction,np.append(time_restriction[1:],False))

def add_next(time_restriction):
    return np.logical_or(time_restriction,np.insert(time_restriction[:-1],0,False))
