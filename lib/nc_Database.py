import sqlalchemy
import sqlalchemy.orm
import numpy as np
import datetime
import os

import netcdf_utils
import netCDF4

import copy

class nc_Database:
    def __init__(self,project_drs):
        #Defines the tree structure:
        self.drs=project_drs

        self._setup_database()

        return

    def _setup_database(self):
        #Create an in-memory sqlite database, for easy subselecting.
        #Uses sqlalchemy
        self.engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=False)
        self.metadata = sqlalchemy.MetaData(bind=self.engine)

        self.time_db = sqlalchemy.Table('time_db',self.metadata,
                sqlalchemy.Column('case_id',sqlalchemy.Integer,primary_key=True),
                *(sqlalchemy.Column(level_name, sqlalchemy.String(255)) for level_name in self.drs.discovered_drs)
                )
        self.metadata.create_all()
        sqlalchemy.orm.clear_mappers()
        sqlalchemy.orm.mapper(File_Expt,self.time_db)

        self.session = sqlalchemy.orm.create_session(bind=self.engine, autocommit=False, autoflush=True)
        self.file_expt = File_Expt(self.drs.discovered_drs)
        return

    def populate_database(self,find_function):
        self.file_expt.time='0'
        populate_database_recursive(self,self.Dataset,find_function)
        return

#####################################################################
#####################################################################
#########################  DATABASE CONVERSION
#####################################################################
#####################################################################

def populate_database_recursive(nc_Database,nc_Dataset,find_function):
    if len(nc_Dataset.groups.keys())>0:
        for group in nc_Dataset.groups.keys():
            setattr(nc_Database.file_expt,nc_Dataset.groups[group].getncattr('level_name'),group)
            populate_nc_Database_recursive(nc_Database,nc_Dataset.groups[group],find_function)
    else:
        if 'path' in nc_Dataset.variables.keys():
            paths=nc_Dataset.variables['path'][:]
            for path_id, path in enumerate(paths):
                id_list=['file_type','search']
                for id in id_list:
                    setattr(nc_Database.file_expt,id,nc_Dataset.variables[id][path_id])
                setattr(nc_Database.file_expt,'path','|'.join([nc_Dataset.variables['path'][path_id],
                                                       nc_Dataset.variables['checksum'][path_id]]))
                setattr(nc_Database.file_expt,'version','v'+str(nc_Dataset.variables['version'][path_id]))
                find_function(nc_Database,copy.deepcopy(nc_Database.file_expt))
        elif 'path' in nc_Dataset.ncattrs():
            #for fx variables:
            id_list=['file_type','search']
            for id in id_list:
                setattr(nc_Database.file_expt,id,nc_Dataset.getncattr(id))
            setattr(nc_Database.file_expt,'path','|'.join([nc_Dataset.getncattr('path'),
                                                   nc_Dataset.getncattr('checksum')]))
            setattr(nc_Database.file_expt,'version',str(nc_Dataset.getncattr('version')))
            find_function(nc_Database,copy.deepcopy(nc_Database.file_expt))
    return


class File_Expt(object):
    #Create a class that can be used with sqlachemy:
    def __init__(self,diag_tree_desc):
        for tree_desc in diag_tree_desc:
            setattr(self,tree_desc,'')
