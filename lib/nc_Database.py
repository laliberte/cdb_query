#External:
import sqlalchemy
import sqlalchemy.orm
import json
import os
import netCDF4
import copy

#External but related:
import netcdf4_soft_links.read_soft_links as read_soft_links
import netcdf4_soft_links.create_soft_links as create_soft_links
import netcdf4_soft_links.remote_netcdf as remote_netcdf

#Internal:
import nc_Database_utils

class nc_Database:
    def __init__(self,project_drs,database_file=None):
        #Defines the tree structure:
        self.drs=project_drs
        self.database_file=database_file

        self._setup_database()

        return

    def _setup_database(self):
        #Create an in-memory sqlite database, for easy subselecting.
        #Uses sqlalchemy
        self.engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=False)
        self.metadata = sqlalchemy.MetaData(bind=self.engine)

        self.time_db = sqlalchemy.Table('time_db',self.metadata,
                sqlalchemy.Column('case_id',sqlalchemy.Integer,primary_key=True),
                *(sqlalchemy.Column(level_name, sqlalchemy.String(255)) for level_name in self.drs.base_drs)
                )
        self.metadata.create_all()
        sqlalchemy.orm.clear_mappers()
        sqlalchemy.orm.mapper(File_Expt,self.time_db)

        self.file_expt = File_Expt(self.drs.base_drs)
        self.session = sqlalchemy.orm.create_session(bind=self.engine, autocommit=False, autoflush=True)
        return

    def close_database(self):
        self.session.close()
        self.engine.dispose()
        self._setup_database()
        return

    def load_nc_file(self):
        self.Dataset=netCDF4.Dataset(self.database_file,'r')
        return

    def close_nc_file(self):
        self.Dataset.close()
        del self.Dataset
        return

    def load_header(self):
        self.load_nc_file()
        #Load header:
        header=dict()
        for att in set(self.drs.header_desc).intersection(self.Dataset.ncattrs()):
            header[att]=json.loads(self.Dataset.getncattr(att))
        self.close_nc_file()
        return header

    def record_header(self,output_root,header):
        for value in header.keys():
            output_root.setncattr(value,json.dumps(header[value]))
        return

    def populate_database(self,options,find_function,semaphores=None):
        self.load_nc_file()
        self.file_expt.time='0'
        populate_database_recursive(self,self.Dataset,options,find_function,semaphores=semaphores)
        self.close_nc_file()

        #Allow complex queries:
        if ('field' in dir(options) and options.field!=[] and options.field!=None):
            if ( 'complex_query' in dir(options) and options.complex_query!=[] or
                 'Xcomplex_query' in dir(options) and options.Xcomplex_query!=[]  ):
                list_query=self.list_fields(options.field)
                for query in list_query:
                    if ( (options.complex_query!=[] and not query in options.complex_query) or
                         (options.Xcomplex_query!=[] and query in options.Xcomplex_query) ):
                        conditions=[ getattr(File_Expt,field)==query[field_id] for field_id, field in enumerate(options.field)]
                        self.session.query(File_Expt).filter(*conditions).delete()
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

    def list_data_nodes(self,options):
        data_node_list=self.list_subset((File_Expt.data_node,))
        return  [data_node[0] for data_node in data_node_list  
                if is_level_name_included_and_not_excluded('data_node',options,data_node)]
                
    def list_paths_by_data_node(self,data_node):
        return self.session.query(File_Expt.path).filter(File_Expt.data_node==data_node).first()[0]

    def list_paths(self):
        #subset=tuple([File_Expt.path,File_Expt.file_type]+[getattr(File_Expt,item) for item in self.drs.official_drs])
        subset=tuple([File_Expt.path,]+[getattr(File_Expt,item) for item in self.drs.official_drs])
        return sorted(list(set(self.list_subset(subset))))

    def write_database(self,header,options,record_function_handle,semaphores=dict()):
        #List all the trees:
        drs_list=copy.copy(self.drs.base_drs)

        #drs_to_remove=['search','path','file_type','version','time']
        drs_to_remove=['path','data_node','file_type','version','time']
        for drs in drs_to_remove: drs_list.remove(drs)
        #Remove the time:
        drs_to_remove.remove('time')

        #Find the unique tuples:
        trees_list=self.list_subset([getattr(File_Expt,level) for level in drs_list])

        #Create output:
        filepath=options.out_netcdf_file+'.pid'+str(os.getpid())
        output_root=netCDF4.Dataset(filepath,
                                      'w',format='NETCDF4')
                                      #'w',format='NETCDF4',diskless=True,persist=True)
        if 'no_check_availability' in dir(options) and options.no_check_availability:
            self.record_header(output_root,{val:header[val] for val in header.keys() if val!='data_node_list'})
        else:
            self.record_header(output_root,header)
        #temp_string=''
        #for att in self.drs.simulations_desc:
        #    if ( att in dir(options) and
        #         getattr(options,att)!=None):
        #        temp_string+=att+': '+str(getattr(options,att))
        #if len(temp_string)>0:
        #    output_root.setncattr('cdb_query_temp',temp_string)
                    

        #Define time subset:
        if 'months_list' in header.keys():
            months=header['months_list']
        else:
            months=range(1,13)
        
        for tree in trees_list:
            time_frequency=tree[drs_list.index('time_frequency')]
            experiment=tree[drs_list.index('experiment')]
            var=tree[drs_list.index('var')]
            conditions=[ getattr(File_Expt,level)==value for level,value in zip(drs_list,tree)]
            out_tuples=[ getattr(File_Expt,level) for level in drs_to_remove]
            #Find list of paths:
            paths_list=[{drs_name:path[drs_id] for drs_id, drs_name in enumerate(drs_to_remove)} for path in self.session.query(*out_tuples
                                    ).filter(sqlalchemy.and_(*conditions)).distinct().all()]

            output=create_tree(output_root,zip(drs_list,tree))
            #Record data:
            if 'missing_years' in dir(options) and options.missing_years:
                years=None
            else:
                years_range=[ int(year) for year in header['experiment_list'][experiment].split(',')]
                years=range(years_range[0],years_range[1]+1)

            netcdf_pointers=create_soft_links.create_netCDF_pointers(
                                                              paths_list,time_frequency,years, months,
                                                              header['file_type_list'],
                                                              header['data_node_list'],
                                                              semaphores=semaphores)

            getattr(netcdf_pointers,record_function_handle)(output,var,username=options.username,user_pass=options.password)

            #Remove recorded data from database:
            self.session.query(*out_tuples).filter(sqlalchemy.and_(*conditions)).delete()

        return output_root, filepath

    def retrieve_database(self,options,output,queues):
        ##Recover the database meta data:

        #Find the trees. Here, we want all of them with the full DRS:
        drs_to_eliminate=self.drs.official_drs_no_version
        #trees_list=[zip(self.drs.official_drs_no_version,[ None
        #                    for field in self.drs.official_drs_no_version]) for var in 
        #                    self.list_fields(drs_to_eliminate) ]
        trees_list=[zip(self.drs.official_drs_no_version,[ None
                            for field in self.drs.official_drs_no_version]),]
        self.load_nc_file()
        map(lambda x: nc_Database_utils.extract_netcdf_variable(output,self.Dataset,x,options,queues=queues),trees_list)
        self.close_nc_file()
        return


#####################################################################
#####################################################################
#########################  DATABASE CONVERSION
#####################################################################
#####################################################################
file_unique_id_list=['checksum_type','checksum','tracking_id']

def populate_database_recursive(nc_Database,data,options,find_function,semaphores=None):
    if 'soft_links' in data.groups.keys():
        soft_links=data.groups['soft_links']
        paths=soft_links.variables['path'][:]
        for path_id, path in enumerate(paths):
            #id_list=['file_type','search']
            id_list=['file_type']
            for id in id_list:
                setattr(nc_Database.file_expt,id,soft_links.variables[id][path_id])

            #Check if data_node was included:
            data_node=remote_netcdf.get_data_node(soft_links.variables['path'][path_id],
                                                    soft_links.variables['file_type'][path_id])

            if is_level_name_included_and_not_excluded('data_node',options,data_node):
                file_path='|'.join([soft_links.variables['path'][path_id],] +
                                                       [soft_links.variables[unique_file_id][path_id] for unique_file_id in file_unique_id_list])
                setattr(nc_Database.file_expt,'path',file_path)
                setattr(nc_Database.file_expt,'version','v'+str(soft_links.variables['version'][path_id]))
                setattr(nc_Database.file_expt,'data_node',data_node)
                find_function(nc_Database,copy.deepcopy(nc_Database.file_expt),semaphores=semaphores)
    elif len(data.groups.keys())>0:
        for group in data.groups.keys():
            level_name=data.groups[group].getncattr('level_name')
            if is_level_name_included_and_not_excluded(level_name,options,group):
                setattr(nc_Database.file_expt,data.groups[group].getncattr('level_name'),group)
                populate_database_recursive(nc_Database,data.groups[group],options,find_function,semaphores=semaphores)
    elif 'path' in data.ncattrs():
        #for fx variables:
        #id_list=['file_type','search']
        id_list=['file_type']
        for id in id_list:
            setattr(nc_Database.file_expt,id,data.getncattr(id))

        #Check if data_node was included:
        data_node=remote_netcdf.get_data_node(data.getncattr('path'),
                                                data.getncattr('file_type'))
        if is_level_name_included_and_not_excluded('data_node',options,data_node):
            file_path='|'.join([data.getncattr('path'),] +
                                                   [ data.getncattr(file_unique_id) if file_unique_id in data.ncattrs() else '' for file_unique_id in file_unique_id_list])
            setattr(nc_Database.file_expt,'path',file_path)
            setattr(nc_Database.file_expt,'version',str(data.getncattr('version')))

            setattr(nc_Database.file_expt,'data_node',
                        remote_netcdf.get_data_node(nc_Database.file_expt.path,
                                                      nc_Database.file_expt.file_type))
            find_function(nc_Database,copy.deepcopy(nc_Database.file_expt))
    else:
        #for retrieved datasets:
        #id_list=['file_type','search','path','version']
        id_list=['file_type','path','version']
        for id in id_list:
            setattr(nc_Database.file_expt,id,'')
        if len(data.variables.keys())>0:
            setattr(nc_Database.file_expt,'data_node',
                        remote_netcdf.get_data_node(nc_Database.file_expt.path,
                                                      nc_Database.file_expt.file_type))
            find_function(nc_Database,copy.deepcopy(nc_Database.file_expt))
    return

def create_tree(output_root,tree):
    return create_tree_recursive(output_root,tree)

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

#def retrieve_tree_recursive(options,data,output,queues,retrieval_function_name):
#    if 'soft_links' in data.groups.keys():
#        netcdf_pointers=read_soft_links.read_netCDF_pointers(data,options=options,queues=queues)
#        netcdf_pointers.retrieve(output,
#                                 retrieval_function_name,
#                                 options,
#                                 username=options.username,
#                                 user_pass=options.user_pass
#                                 )
#    elif len(data.groups.keys())>0:
#        for group in data.groups.keys():
#            level_name=data.groups[group].getncattr('level_name')
#            if ( is_level_name_included_and_not_excluded(level_name,options,group) and
#                 tree_recursive_check_not_empty(options,data.groups[group])):
#                if (isinstance(output,netCDF4.Dataset) or
#                    isinstance(output,netCDF4.Group)):
#                    if not group in output.groups.keys():
#                        output_grp=output.createGroup(group)
#                    else:
#                        output_grp=output.groups[group]
#                    for att in data.groups[group].ncattrs():
#                        if not att in output_grp.ncattrs():
#                            output_grp.setncattr(att,data.groups[group].getncattr(att))
#                else:
#                    output_grp=output
#                retrieve_tree_recursive(options,data.groups[group],output_grp,queues,retrieval_function_name)
#            
#    return

def tree_recursive_check_not_empty(options,data):
    if 'soft_links' in data.groups.keys():
        return True
    elif len(data.groups.keys())>0:
        empty_list=[]
        for group in data.groups.keys():
            level_name=data.groups[group].getncattr('level_name')
            if is_level_name_included_and_not_excluded(level_name,options,group):
                empty_list.append(tree_recursive_check_not_empty(options,data.groups[group]))
        return any(empty_list)
    else:
        if len(data.variables.keys())>0:
            return True
        else:
            return False

def is_level_name_included_and_not_excluded(level_name,options,group):
    if level_name in dir(options):
        if isinstance(getattr(options,level_name),list):
            included=((getattr(options,level_name)==[]) or
                     (group in getattr(options,level_name)))
        else:
            included=((getattr(options,level_name)==None) or 
                       (getattr(options,level_name)==group)) 
    else:
        included=True

    if 'X'+level_name in dir(options):
        if isinstance(getattr(options,'X'+level_name),list):
            not_excluded=((getattr(options,'X'+level_name)==[]) or
                     (not group in getattr(options,'X'+level_name)))
        else:
            not_excluded=((getattr(options,'X'+level_name)==None) or 
                           (getattr(options,'X'+level_name)!=group)) 
    else:
        not_excluded=True
    return included and not_excluded


class File_Expt(object):
    #Create a class that can be used with sqlachemy:
    def __init__(self,diag_tree_desc):
        for tree_desc in diag_tree_desc:
            setattr(self,tree_desc,'')
