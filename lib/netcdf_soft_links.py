
import numpy as np

import netCDF4
import os

import json

import netcdf_utils

import remote_netcdf

import create_soft_links

class netCDF_pointers:
    def __init__(self,out_netcdf_file,file_type_list,data_node_list,semaphores=[]):
        self.output_root=netCDF4.Dataset(out_netcdf_file+'.pid'+str(os.getpid()),
                                          'w',format='NETCDF4',diskless=True,persist=True)
        self.file_type_list=file_type_list
        self.data_node_list=data_node_list
        self.semaphores=semaphores
        return

    def record_header(self,header):
        for value in header.keys():
            self.output_root.setncattr(value,json.dumps(header[value]))
        #Put version:
        self.output_root.setncattr('cdb_query_file_spec_version','1.0')
        return

    def create_tree(self,tree):
        return create_tree_recursive(self.output_root,tree)

    def record_paths(self,tree,paths_list,var,years,months):
        #First order source files by preference:
        soft_links=create_soft_links.soft_links(paths_list,self.file_type_list,self.data_node_list,semaphores=self.semaphores)
        output=self.create_tree(tree)
        soft_links.create(output)
        return

    def record_meta_data(self,tree,paths_list,var,years,months):
        #Retrieve time and meta:
        soft_links=create_soft_links.soft_links(paths_list,self.file_type_list,self.data_node_list,semaphores=self.semaphores)
        output=self.create_tree(tree)
        soft_links.create_variable(output,var,years,months)
        return

    def record_fx(self,tree,paths_list,var):
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

        dataset=remote_netcdf.remote_netCDF(path['path'].split('|')[0].replace('fileServer','dodsC'),self.semaphores)
        dataset.open()
        remote_data=dataset.Dataset
        for var_name in remote_data.variables.keys():
            netcdf_utils.replicate_netcdf_var(output,remote_data,var_name)
            output.variables[var_name][:]=remote_data.variables[var_name][:]
        dataset.close()
        return

def create_tree_recursive(output_top,tree):
    level_name=tree[0][1]
    if not level_name in output_top.groups.keys(): 
        output=output_top.createGroup(level_name)
        output.level_name=tree[0][0]
    else:
        output=output_top.groups[level_name]
    if len(tree)>1:
        output=create_tree_recursive(output,tree[1:])
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


