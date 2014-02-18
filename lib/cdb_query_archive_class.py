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

import nc_Database


import json

import datetime
import sqlalchemy

import numpy as np

import warnings

class SimpleTree:
    def __init__(self,options,project_drs):

        self.drs=project_drs
        self.nc_Database=nc_Database.nc_Database(self.drs)

        if 'in_diagnostic_netcdf_file' in dir(options):
            self.Dataset=netCDF4.Dataset(netcdf4_file,'r')
            #Load header:
            self.header=dict()
            for att in set(self.drs.header_desc).intersection(infile.ncattrs()):
                self.header[att]=json.loads(self.Dataset.getncattr(att))
        elif 'in_diagnostic_headers_file' in dir(options):
            #Load header:
            self.header=json.load(open(options.in_diagnostic_headers_file,'r'))['header']
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
        subset_list=self.nc_Database.session.query(*subset).distinct().all()
        return subset_list

    def discover(self,options):
        #First simplify the header
        self.union_header()

        only_list=[]

        #Local filesystem archive
        local_paths=[search_path for search_path in 
                        self.header['search_list']
                        if os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
        for search_path in local_paths:
            only_list.append(filesystem_query.descend_tree(self,search_path,list_level=options.list_only_field))

        #ESGF search
        remote_paths=[search_path for search_path in 
                        self.header['search_list']
                        if not os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
        for search_path in remote_paths:
            only_list.append(esgf_query.descend_tree(self,search_path,options,list_level=options.list_only_field))

        if options.list_only_field!=None:
            for field_name in set([item for sublist in only_list for item in sublist]):
                print field_name
        else:
            valid_experiments_path.intersection(self)
            #List data_nodes:
            self.header['data_node_list']=self.list_data_nodes()
            self.create_netcdf_container(options,'record_paths')
        return

    def simulations_list(self):
        subset_desc=(getattr(File_Expt,item) for item in self.drs.simulations_desc)
        simulations_list=self.list_subset(subset_desc)
        return simulations_list
        #return [simulation for simulation in simulations_list if simulation[ensemble_index]!='r0i0p0']

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
            #Find list of paths:
            paths_list=[{drs_name:path[drs_id] for drs_id, drs_name in enumerate(drs_to_remove)} for path in self.pointers.session.query(*out_tuples
                                    ).filter(sqlalchemy.and_(*conditions)).distinct().all()]
            #Record data:
            getattr(netcdf_utils,record_function_handle)(self.header,output,paths_list,var,time_frequency,experiment)
            #Remove recorded data from database:
            self.pointers.session.query(*out_tuples).filter(sqlalchemy.and_(*conditions)).delete()

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


def find_simple(pointers,file_expt):
    #for item in dir(file_expt):
    #    if item[0]!='_':
    #        print getattr(file_expt,item)
    pointers.session.add(file_expt)
    pointers.session.commit()
    return
