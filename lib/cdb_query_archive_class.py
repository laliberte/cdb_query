import os
import hashlib

import filesystem_query
import esgf_query

import valid_experiments_path
import valid_experiments_time

import copy

import tree_utils
from tree_utils import File_Expt

import cdb_query_archive_parsers
import netcdf_utils
import netCDF4
import retrieval_utils


import json

import datetime
import sqlalchemy

import numpy as np

import warnings

class SimpleTree:
    def __init__(self,tree,options,project_drs):

        if 'header' in tree.keys():
            self.header =  tree['header']
        #print { val:getattr(project_drs,val) for val in dir(project_drs) if val[0]!='_'}

        self.drs=project_drs

        #Create tree
        self.pointers=tree_utils.Tree(self.drs.discovered_drs,options)

        if 'pointers' in tree.keys():
            self.pointers.tree=tree['pointers']
        elif 'in_diagnostic_netcdf_file' in dir(options):
            self.pointers.populate_pointers_from_netcdf(options)
            #print json.dumps(self.pointers.tree,sort_keys=True, indent=4)
        return

    def union_header(self):
        #This method creates a simplified header

        #Create the diagnostic description dictionary:
        self.header_simple={}
        #Find all the requested realms, frequencies and variables:

        variable_list=['var_list']+[field+'_list' for field in self.drs.var_specs]
        for list_name in variable_list: self.header_simple[list_name]=[]
        for var_name in self.header['variable_list'].keys():
            self.header_simple['var_list'].append(var_name)
            for list_id, list_name in enumerate(list(variable_list[1:])):
                self.header_simple[list_name].append(self.header['variable_list'][var_name][list_id])

        #Find all the requested experiments and years:
        experiment_list=['experiment_list','years_list']
        for list_name in experiment_list: self.header_simple[list_name]=[]
        for experiment_name in self.header['experiment_list'].keys():
            self.header_simple['experiment_list'].append(experiment_name)
            for list_name in list(experiment_list[1:]):
                self.header_simple[list_name].append(self.header['experiment_list'][experiment_name])
                
        #Find the unique members:
        for list_name in self.header_simple.keys(): self.header_simple[list_name]=list(set(self.header_simple[list_name]))
        return

    def list_subset(self,subset):
        subset_list=self.pointers.session.query(*subset
                                                        ).distinct().all()
        return subset_list

    def discover(self,options):
        #First simplify the header
        self.union_header()

        if options.list_only_field!=None:
            only_list=[]

        #Local filesystem archive
        local_paths=[search_path for search_path in 
                        self.header['search_list']
                        if os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
        for search_path in local_paths:
            if options.list_only_field!=None:
                only_list.append(filesystem_query.descend_tree(self.pointers,self.header,self.header_simple,search_path,list_level=options.list_only_field))
            else:
                filesystem_query.descend_tree(self.pointers,self.header,self.header_simple,search_path)


        #ESGF search
        remote_paths=[search_path for search_path in 
                        self.header['search_list']
                        if not os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
        for search_path in remote_paths:
            if options.list_only_field!=None:
                only_list.append(esgf_query.descend_tree(self.pointers,self.drs,self.header,search_path,options,list_level=options.list_only_field))
            else:
                esgf_query.descend_tree(self.pointers,self.drs,self.header,search_path,options)
        #print json.dumps(self.pointers.tree,sort_keys=True, indent=4)

        if options.list_only_field!=None:
            for field_name in set([item for sublist in only_list for item in sublist]):
                print field_name
        else:
            valid_experiments_path.intersection(self,self.drs)
            #print json.dumps(self.pointers.tree,sort_keys=True, indent=4)
            #List data_nodes:
            self.header['data_node_list']=self.list_data_nodes()
            self.create_netcdf_container(options,'record_paths')
        return

    def simulations_list(self):
        ensemble_index=self.drs.simulations_desc.index('ensemble')
        subset_desc=(getattr(File_Expt,item) for item in self.drs.simulations_desc)
        simulations_list=self.list_subset(subset_desc)
        return [simulation for simulation in simulations_list if simulation[ensemble_index]!='r0i0p0']

    def simulations(self,options):
        self.pointers.slice(options)
        self.pointers.create_database(find_simple)

        simulations_list=self.simulations_list()
        for simulation in simulations_list:
            #if simulation[2]!='r0i0p0':
            print '_'.join(simulation)
        return

    def list_data_nodes(self):
        return sorted(
                    list(
                        set(
                            [netcdf_utils.get_data_node(*path) for 
                                path in self.list_subset((File_Expt.path,File_Expt.file_type))
                              ]
                            )
                         )
                      )

    def get_data_nodes_list(self,options):
        self.pointers.create_database(find_simple)
        return self.list_data_nodes()

    def get_paths_list(self,options):
        self.pointers.create_database(find_simple)
        subset=tuple([File_Expt.path,File_Expt.file_type]+[getattr(File_Expt,item) for item in self.drs.official_drs])
        return sorted(list(set(self.list_subset(subset))))

    def list_fields(self,options):
        #slice with options:
        self.pointers.slice(options)
        self.pointers.create_database(find_simple)
        fields_list=sorted(list(set(self.list_subset((getattr(File_Expt,field) for field in options.field)))))
        for field in fields_list:
            print ','.join(field)
        return

    def optimset(self,options):
        #First slice the input:
        self.pointers.slice(options)

        #Find the list of institute / model with all the months for all the years / experiments and variables requested:
        valid_experiments_time.intersection(self,self.drs)
        #print json.dumps(self.pointers.tree,sort_keys=True, indent=4)
        
        self.create_netcdf_container(options,'record_meta_data')
        return

    def create_netcdf_container(self,options,record_function_handle):
        #List all the trees:
        drs_list=self.drs.base_drs

        drs_to_remove=['search','path','file_type','version','time']
        for drs in drs_to_remove: drs_list.remove(drs)
        #Remove the time:
        drs_to_remove.remove('time')

        #Find the unique tuples:
        trees_list=self.list_subset([getattr(File_Expt,level) for level in drs_list])

        #Create output:
        output_file_name=options.out_diagnostic_netcdf_file
        output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')
        
        for tree in trees_list:
            time_frequency=tree[drs_list.index('time_frequency')]
            experiment=tree[drs_list.index('experiment')]
            var=tree[drs_list.index('var')]
            output=netcdf_utils.create_tree(output_root,zip(drs_list,tree))
            output_root.sync()
            conditions=[ getattr(File_Expt,level)==value for level,value in zip(drs_list,tree)]
            out_tuples=[ getattr(File_Expt,level) for level in drs_to_remove]
            paths_list=[{drs_name:path[drs_id] for drs_id, drs_name in enumerate(drs_to_remove)} for path in self.pointers.session.query(*out_tuples
                                    ).filter(sqlalchemy.and_(*conditions)).distinct().all()]
            getattr(netcdf_utils,record_function_handle)(self.header,output,paths_list,var,time_frequency,experiment)

        #if record_function_handle=='record_meta_data':
        #    del self.header['data_node_list']
        self.record_header(output_root)
        output_root.close()
        return

    def record_header(self,output):
        #output_hdr=output.createGroup('headers')
        for value in self.header.keys():
            output.setncattr(value,json.dumps(self.header[value]))
        #Put version:
        output.setncattr('cdb_query_file_spec_version','1.0')
        output.sync()
        return

    def slice(self,options):
        self.pointers.slice(options)
        self.pointers.create_database(find_simple)
        self.create_netcdf_container(options,'record_paths')
        return

    def simplify(self,options):
        self.pointers.slice(options)
        self.pointers.simplify(self.header)
        self.pointers.create_database(find_simple)
        self.create_netcdf_container(options,'record_paths')
        return


#def remote_retrieve(options):
#    data=netCDF4.Dataset(options.in_diagnostic_netcdf_file,'r')
#    paths_list=[]
#    for timestamp in options.timestamp:
#        paths_list.append(netcdf_utils.descend_tree(options,data,timestamp=timestamp))
#    data.close()
#    identical_paths,unique_paths=organize_identical_paths(paths_list)
#    if options.nco: print 'unset NCO_RETRIEVE;'
#    if options.list_data_nodes: print 'unset DOMAINS;'
#    for path_num, path in enumerate(unique_paths):
#        if options.cdo:
#            sel_string=' -seltimestep,'+','.join([str(item+1) for item in identical_paths[path]['indices']])
#            #print sel_string+' '+path.replace('fileServer','dodsC')
#            print 'CDO_RETRIEVE="'+' '.join([sel_string,path.replace('fileServer','dodsC')])+'"'
#        elif options.nco:
#            for slice_indices in convert_indices_to_slices(identical_paths[path]['indices']):
#                if len(slice_indices)==2:
#                    sel_string='-d time,'+','.join([str(item) for item in slice_indices])
#                else:
#                    sel_string='-d time,'+str(slice_indices[0])+','+str(slice_indices[0])
#                print 'NCO_RETRIEVE['+str(path_num+1)+']="'+' '.join([sel_string,path.replace('fileServer','dodsC')])+'";'
#        elif options.list_data_nodes:
#            print 'DOMAINS['+str(path_num+1)+']="'+' '.join(identical_paths[path]['data_nodes'])+'";'
#        #else:
#        #    netcdf_utils.create_local_netcdf(options,options.out_netcdf_file,tuple_list)
#
#    return

#def organize_identical_paths(paths_list):
#    #FIND UNIQUE PATHS AND ENSURE THAT THEY ARE SORTED:
#    sort_timestamp=np.argsort([item['timestamp'] for item in paths_list])
#    unique_paths, unique_indices=np.unique(np.array([item['path'] for item in paths_list])[sort_timestamp],return_index=True)
#    unique_paths=np.array([item['path'] for item in paths_list])[sort_timestamp][unique_indices]
#    paths_organized={}
#    for path in unique_paths:
#        indices=[item['index'] for item in paths_list if item['path']==path]
#        indices_sort=np.argsort(indices)
#        if len(indices)>1: indices=np.array(indices)[indices_sort]
#        paths_organized[path]={
#                                'indices': indices,
#                                'timestamps':np.array([item['timestamp'] for item in paths_list if item['path']==path])[indices_sort],
#                                'data_nodes':[item['data_nodes'] for item in paths_list if item['path']==path][0]
#                              }
#    return paths_organized, unique_paths
                             

def find_simple(pointers,file_expt):
    #for item in dir(file_expt):
    #    if item[0]!='_':
    #        print getattr(file_expt,item)
    pointers.session.add(file_expt)
    pointers.session.commit()
    return
