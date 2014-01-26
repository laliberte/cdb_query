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

import datetime
import sqlalchemy

import numpy as np
from itertools import groupby, count

class SimpleTree:
    def __init__(self,tree,options):

        if 'header' in tree.keys():
            self.header =  tree['header']

        #Create tree
        self.pointers=tree_utils.Tree(self.header['drs'],options)

        if 'pointers' in tree.keys():
            self.pointers.tree=tree['pointers']
        return

    def union_header(self):
        #This method creates a simplified header

        #Create the diagnostic description dictionary:
        self.header_simple={}
        #Find all the requested realms, frequencies and variables:
        variable_list=['var_list','frequency_list','realm_list','mip_list']
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

    def discover_centers(self,options):
        self.discover_both(options,True)
        return

    def discover(self,options):
        self.discover_both(options,False)
        return

    def discover_both(self,options,discover_centers):
        #First simplify the header
        self.union_header()
        centers_list=[]

        #Local filesystem archive
        local_paths=[search_path for search_path in 
                        self.header['search_list']
                        if os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
                        #[ os.path.abspath(os.path.expanduser(os.path.expandvars(path))) for path in self.header['search_list']]
        for search_path in local_paths:
            if discover_centers:
                centers_list.append(filesystem_query.descend_tree(self.pointers,self.header,self.header_simple,search_path,list_level='center'))
            else:
                filesystem_query.descend_tree(self.pointers,self.header,self.header_simple,search_path)


        #ESGF search
        remote_paths=[search_path for search_path in 
                        self.header['search_list']
                        if not os.path.exists(os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))]
                        #[ os.path.expanduser(os.path.expandvars(path)) for path in self.header['search_list']]
        for search_path in remote_paths:
            if discover_centers:
                centers_list.append(esgf_query.descend_tree(self.pointers,self.header,search_path,options,list_level='center'))
            else:
                esgf_query.descend_tree(self.pointers,self.header,search_path,options)

        if discover_centers:
            for center in set([item for sublist in centers_list for item in sublist]):
                print center
        else:
            valid_experiments_path.intersection(self)
            #List domains:
            self.header['domain_list']=sorted(list(set([netcdf_utils.get_domain(*path) for path in self.list_subset((File_Expt.path,File_Expt.file_type))])))
        return

    def simulations_list(self):
        simulations_list=self.list_subset((File_Expt.center,File_Expt.model,File_Expt.rip))
        return [simulation for simulation in simulations_list if simulation[2]!='r0i0p0']

    def simulations(self,options):
        self.pointers.slice(options)
        self.pointers.create_database(find_simple)

        simulations_list=self.simulations_list()
        for simulation in simulations_list:
            #if simulation[2]!='r0i0p0':
            print '_'.join(simulation)
        return

    def list_domains(self,options):
        #slice with options:
        self.pointers.slice(options)
        self.pointers.create_database(find_simple)
        domains_list=sorted(list(set([netcdf_utils.get_domain(*path) for path in self.list_subset((File_Expt.path,File_Expt.file_type))])))
        for path in domains_list:
            #print '\''+path+'\','
            print path
        return

    def list_paths(self,options):
        #slice with options:
        self.pointers.slice(options)
        self.pointers.create_database(find_simple)
        paths_list=sorted(list(set([path[0] for path in self.list_subset((File_Expt.path,))])))
        for path in paths_list:
            if 'wget' in dir(options) and options.wget:
                decomposition=path.split('|')
                if isinstance(decomposition,list) and len(decomposition)>1:
                    root_path=decomposition[0]
                    print('\''+
                          '/'.join(root_path.split('/')[-10:])+
                          '\' \''+root_path+
                          '\' \'MD5\' \''+decomposition[1]+'\'')
            else:
                print path
        return

    def optimset(self,options):
        #First slice the input:
        self.pointers.slice(options)

        self.header['drs']=[
                                'experiment','center','model','rip','frequency','realm','mip',
                                'var','time','version','file_type','search','path'
                             ]

        #Find the list of center / model with all the months for all the years / experiments and variables requested:
        valid_experiments_time.intersection(self)

        return


    def netcdf_paths(self,options):
        import netCDF4
        #First slice the input:
        self.pointers.slice(options)
        self.pointers.create_database(find_simple)

        #List all the trees:
        drs_list=cdb_query_archive_parsers.base_drs()
        drs_to_remove=['search','path','file_type','version','time']
        for drs in drs_to_remove: drs_list.remove(drs)
        #Remove the time:
        drs_to_remove.remove('time')

        #Find the unique tuples:
        trees_list=self.list_subset([getattr(File_Expt,level) for level in drs_list])
        #version_name='v'+str(datetime.date.today()).replace('-','')
        file_name_drs=['var','mip','model','experiment','rip']

        time_dict={'mon':('%Y','%m')}
        time_dict['day']=time_dict['mon']+('%d',)
        time_dict['6hr']=time_dict['day']+('%H','%M',)
        time_dict['3hr']=time_dict['6hr']

        #Create output:
        output_file_name=options.out_diagnostic_netcdf_file
        output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')
        soft_link_desc={'numpy':np.dtype([('path','uint32'),('time','uint32')])}
        #soft_link_desc['netcdf4']=output_root.createCompoundType(soft_link_desc['numpy'],'soft_link')
        
        for tree in trees_list:
            frequency=tree[3]
            var=tree[-1]
            output=netcdf_utils.create_tree(output_root,zip(drs_list,tree))
            output_root.sync()
            conditions=[ getattr(File_Expt,level)==value for level,value in zip(drs_list,tree)]
            out_tuples=[ getattr(File_Expt,level) for level in drs_to_remove]
            paths_list=[{drs_name:path[drs_id] for drs_id, drs_name in enumerate(drs_to_remove)} for path in self.pointers.session.query(*out_tuples
                                    ).filter(sqlalchemy.and_(*conditions)).distinct().all()]
            if not frequency in ['fx','clim']:
                netcdf_utils.concatenate_paths(self.header,output_root,output,soft_link_desc,paths_list,var)
            else:
                #Retrieve fixed and clim variables:
                netcdf_utils.retrieve_fx(output,paths_list,var)
        return

    def slice(self,options):
        self.pointers.slice(options)
        return

    def simplify(self,options):
        self.pointers.slice(options)
        self.pointers.simplify(self.header)
        return

    def find_local(self,options):
        import database_utils
        self.pointers.slice(options)

        #In the search paths, find which one are local:
        local_search_path=[ top_path for top_path in 
                                [os.path.expanduser(os.path.expandvars(search_path)) for search_path in self.header['search_list']]
                                if os.path.exists(top_path)]
        local_search_path=[ os.path.abspath(top_path) for top_path in local_search_path]

        #For each remote file type, find the paths and see if a local copy exists:
        path_equivalence=dict()
        options.file_type=['HTTPServer','GridFTP']
        pointers_copy=copy.deepcopy(self.pointers.tree)
        self.pointers.slice(options)

        #for path in sorted(list(set(self.pointers.level_list_last()))):
        self.pointers.create_database(find_simple)
        paths_list=sorted(list(set([path[0] for path in self.list_subset((File_Expt.path,))])))

        for path in paths_list:
            for search_path in local_search_path:
                md5checksum=path.split('|')[1]
                path_to_file=search_path+'/'+'/'.join(path.split('|')[0].split('/')[-10:])
                try:
                    f=open(path_to_file)
                    if netcdf_utils.md5_for_file(f)==md5checksum:
                        path_equivalence[path]=path_to_file
                        break
                except:
                    pass

        #Replace original tree:
        options.file_type=None
        self.pointers.tree=pointers_copy

        self.pointers.replace_last(path_equivalence)
        return

def remote_retrieve(options):
    data=netCDF4.Dataset(options.in_diagnostic_netcdf_file,'r')
    paths_list=[]
    for timestamp in options.timestamp:
        paths_list.append(netcdf_utils.descend_tree(options,data,timestamp))
    data.close()
    identical_paths=organize_identical_paths(paths_list)
    for path in identical_paths.keys():
        if options.cdo:
            sel_string=' -seltimestep,'+','.join([str(item+1) for item in identical_paths[path]['indices']])
            print sel_string+' '+path.replace('fileServer','dodsC')
        elif options.nco:
            for slice_indices in convert_indices_to_slices(identical_paths[path]['indices']):
                if len(slice_indices)==2:
                    sel_string='-d time,'+','.join([str(item) for item in slice_indices])
                else:
                    sel_string='-d time,'+str(slice_indices[0])+','+str(slice_indices[0])
                print ' '.join([sel_string,path.replace('fileServer','dodsC')])
        elif options.list_domains:
            print ' '.join(identical_paths[path]['domains'])
        #else:
        #    netcdf_utils.create_local_netcdf(options,options.out_netcdf_file,tuple_list)

    return

def convert_indices_to_slices(indices):
    return [[next(v)] + list(v)[-1:] for k,v in groupby(indices, lambda x,c=count(): x-next(c))]

def organize_identical_paths(paths_list):
    unique_paths=np.unique([item['path'] for item in paths_list])
    paths_organized={}
    for path in unique_paths:
        indices=[item['index'] for item in paths_list if item['path']==path]
        indices_sort=np.argsort(indices)
        if len(indices)>1: indices=np.array(indices)[indices_sort]
        paths_organized[path]={
                                'indices': indices,
                                'timestamps':np.array([item['timestamp'] for item in paths_list if item['path']==path])[indices_sort],
                                'domains':[item['domains'] for item in paths_list if item['path']==path][0]
                              }
    return paths_organized
                             

def find_simple(pointers,file_expt):
    #for item in dir(file_expt):
    #    if item[0]!='_':
    #        print getattr(file_expt,item)
    pointers.session.add(file_expt)
    pointers.session.commit()
    return
