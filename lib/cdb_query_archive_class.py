import os
import hashlib

import filesystem_query
import esgf_query

import discover
import optimset

import copy

import tree_utils
from nc_Database import File_Expt

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

    def discover(self,options):
        #Load header:
        self.header=json.load(open(options.in_diagnostic_headers_file,'r'))['header']
        #Simplify the header:
        self.union_header()

        discover.discover(self,options)
        return

    def optimset(self,options):
        optimset.optimset(self,options)
        return

    def list_fields(self,options):
        #slice with options:
        self.load_nc_file(options.in_diagnostic_netcdf_file)
        self.nc_Database.populate_database(self.Dataset,find_simple)
        fields_list=sorted(list(set(self.nc_Database.list_subset((getattr(File_Expt,field) for field in options.field)))))
        for field in fields_list:
            print ','.join(field)
        return

    def load_nc_file(self,netcdf4_file):
        self.Dataset=netCDF4.Dataset(netcdf4_file,'r')
        #Load header:
        self.header=dict()
        for att in set(self.drs.header_desc).intersection(self.Dataset.ncattrs()):
            self.header[att]=json.loads(self.Dataset.getncattr(att))
        self.nc_Database=nc_Database.nc_Database(self.drs)
        return



def find_simple(pointers,file_expt):
    #for item in dir(file_expt):
    #    if item[0]!='_':
    #        print getattr(file_expt,item)
    pointers.session.add(file_expt)
    pointers.session.commit()
    return
