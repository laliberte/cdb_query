import sqlalchemy
import sqlalchemy.orm
import json

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

    def close_database(self):
        self.session.close()
        self.engine.dispose()
        return

    def populate_database(self,Dataset,find_function):
        self.file_expt.time='0'
        populate_database_recursive(self,Dataset,find_function)
        return

    def simulations_list(self):
        subset_desc=(getattr(File_Expt,item) for item in self.drs.simulations_desc)
        simulations_list=self.list_subset(subset_desc)
        return simulations_list

    def list_subset(self,subset):
        subset_list=self.session.query(*subset).distinct().all()
        return subset_list

    def list_fields(self,fields_to_list):
        fields_list=sorted(list(set(self.list_subset((getattr(File_Expt,field) for field in fields_to_list)))))
        return fields_list

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

    def list_paths(self):
        subset=tuple([File_Expt.path,File_Expt.file_type]+[getattr(File_Expt,item) for item in self.drs.official_drs])
        return sorted(list(set(self.list_subset(subset))))

    def create_netcdf_container(self,header,options,record_function_handle):
        #List all the trees:
        drs_list=copy.copy(self.drs.base_drs)

        drs_to_remove=['search','path','file_type','version','time']
        for drs in drs_to_remove: drs_list.remove(drs)
        #Remove the time:
        drs_to_remove.remove('time')

        #Find the unique tuples:
        trees_list=self.list_subset([getattr(File_Expt,level) for level in drs_list])

        #Create output:
        output_file_name=options.out_diagnostic_netcdf_file
        output_root=netCDF4.Dataset(output_file_name+'.pid'+str(os.getpid()),'w',format='NETCDF4',diskless=True,persist=True)
        #output_root=netCDF4.Dataset(output_file_name+'.pid'+str(os.getpid()),'w',format='NETCDF4',diskless=True)
        #output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')
        
        for tree in trees_list:
            time_frequency=tree[drs_list.index('time_frequency')]
            experiment=tree[drs_list.index('experiment')]
            var=tree[drs_list.index('var')]
            output=netcdf_utils.create_tree(output_root,zip(drs_list,tree))
            output_root.sync()
            conditions=[ getattr(File_Expt,level)==value for level,value in zip(drs_list,tree)]
            out_tuples=[ getattr(File_Expt,level) for level in drs_to_remove]
            #Find list of paths:
            paths_list=[{drs_name:path[drs_id] for drs_id, drs_name in enumerate(drs_to_remove)} for path in self.session.query(*out_tuples
                                    ).filter(sqlalchemy.and_(*conditions)).distinct().all()]
            #Record data:
            getattr(netcdf_utils,record_function_handle)(header,output,paths_list,var,time_frequency,experiment)
            #Remove recorded data from database:
            self.session.query(*out_tuples).filter(sqlalchemy.and_(*conditions)).delete()

        self.record_header(header,output_root)
        return output_root

    def record_header(self,header,output):
        #output_hdr=output.createGroup('headers')
        for value in header.keys():
            output.setncattr(value,json.dumps(header[value]))
        #Put version:
        output.setncattr('cdb_query_file_spec_version','1.0')
        output.sync()
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
            populate_database_recursive(nc_Database,nc_Dataset.groups[group],find_function)
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
