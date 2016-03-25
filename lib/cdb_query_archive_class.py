#External:
import netCDF4
import copy
import os
import json
import timeit
import numpy as np
import multiprocessing
import random
import sys
import getpass

#External but related:
import netcdf4_soft_links.certificates as certificates
import netcdf4_soft_links.retrieval_manager as retrieval_manager

#Internal:
import ask
import nc_Database
import nc_Database_apply
#import nc_Database_convert
import recovery_manager
import apply_manager

class SimpleTree:
    def __init__(self,project_drs):
        self.drs=project_drs
        self.queues=dict()
        return

    def ask(self,options):
        #Load header:
        try:
            self.header=json.load(open(options.in_headers_file,'r'))['header']
        except ValueError as e:
            print 'The input diagnostic file '+options.in_headers_file+' does not conform to JSON standard. Make sure to check its syntax'
            raise

        #Simplify the header:
        self.union_header()

        #Only a listing of a few fields was requested.
        if ('list_only_field' in dir(options) and options.list_only_field!=None):
            for field_name in ask.ask(self.drs,options):
                print field_name
            return

        if ('catalogue_missing_simulations_desc' in dir(self.drs)
           and self.drs.catalogue_missing_simulations_desc):
            #This allows some projects to be inconsistent in their publications:
            dataset, temp_output_file_name=ask.ask(self.drs,options)
            dataset.close()
            try:
                os.rename(temp_output_file_name,options.out_netcdf_file)
            except OSError:
                pass
            return

        #Check if this is an update and if this an update find the previous simulations list:
        #prev_simulations_list=self.load_previous_simulations(options)
        prev_simulations_list=[]

        #Find the simulation list:
        simulations_list=ask.ask_simulations_recursive(self,options,self.drs.simulations_desc)
        simulations_list=sorted(list(set(simulations_list).difference(prev_simulations_list)))
        #Remove fixed variable:
        simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                    simulation[self.drs.simulations_desc.index('ensemble')]!='r0i0p0']
        if not ('silent' in dir(options) and options.silent):
            print "This is a list of simulations that COULD satisfy the query:"
            for simulation in simulations_list_no_fx:
                print ','.join(simulation)
            print "cdb_query will now attempt to confirm that these simulations have all the requested variables."
            print "This can take some time. Please abort if there are not enough simulations for your needs."

        vars_list=[[ var[self.drs.simulations_desc.index(field)] if field in self.drs.simulations_desc else None
                            for field in self.drs.official_drs_no_version] for var in 
                            simulations_list_no_fx ]

        #Randomize to minimize strain on index nodes:
        import random
        random.shuffle(vars_list)

        options.ask=True
        options.script=''
        options.download=False
        output=apply_manager.distributed_apply(nc_Database_apply.apply_to_variable,
                                               nc_Database_apply.extract_single_tree_and_file,
                                                 self.drs,options,vars_list)
        #Close dataset
        if not ('convert' in dir(options) and options.convert):
            nc_Database.record_header(output,self.header)
            output.close()
        return


    def validate(self,options):
        self.load_header(options)

        if 'username' in dir(options) and options.username!=None:
            certificates.retrieve_certificates(options.username,options.service,user_pass=options.password,trustroots=options.no_trustroots)

        if not 'data_node_list' in self.header.keys():
            data_node_list, url_list, simulations_list =self.find_data_nodes_and_simulations(options)
            if len(data_node_list)>1 and not options.no_check_availability:
                #self.header['data_node_list']=self.rank_data_nodes(options,data_node_list,url_list)
                data_node_list=self.rank_data_nodes(options,data_node_list,url_list)
        else:
            simulations_list=[]
        self.drs.data_node_list=data_node_list

        #Find the atomic simulations:
        if simulations_list==[]:
            simulations_list=self.list_fields_local(options,self.drs.simulations_desc)
        #Remove fixed variable:
        simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                    simulation[self.drs.simulations_desc.index('ensemble')]!='r0i0p0']
        vars_list=[[ var[self.drs.simulations_desc.index(field)] if field in self.drs.simulations_desc else None
                            for field in self.drs.official_drs_no_version] for var in 
                            simulations_list_no_fx ]

        #Randomize to minimize strain on index nodes:
        import random
        random.shuffle(vars_list)

        #manager=multiprocessing.Manager()
        #semaphores=dict()
        #for data_node in  self.header['data_node_list']:
        #    semaphores[data_node]=manager.Semaphore(5)
        options.validate=True
        options.script=''
        options.download=False
        output=apply_manager.distributed_apply(nc_Database_apply.apply_to_variable,
                                               nc_Database_apply.extract_single_tree_and_file,
                                                 self.drs,options,vars_list)
        #Close dataset
        if not ('convert' in dir(options) and options.convert):
            self.record_header(options,output)
            output.close()
        return

    def download(self,options):
        options.download=True
        output=netCDF4.Dataset(options.out_netcdf_file,'w')
        processes=self.download_and_download_raw_start_queues(options)

        try:
            self.download_and_download_raw(options,output)
        finally:
            for item in processes.keys():
                processes[item].terminate()
        return

    def download_raw(self,options):
        options.download=True
        output=options.out_destination
        #Describe the tree pattern:
        if self.drs.official_drs.index('var')>self.drs.official_drs.index('version'):
            output+='/tree/version/var/'
        else:
            output+='/tree/var/version/'
        processes=self.download_and_download_raw_start_queues(options)
        
        try:
            self.download_and_download_raw(options,output)
        finally:
            for item in processes.keys():
                processes[item].terminate()
        return

    def download_and_download_raw_start_queues(self,options,manager=None):
        if 'username' in dir(options) and options.username!=None:
            certificates.retrieve_certificates(options.username,options.service,user_pass=options.password,trustroots=options.no_trustroots)

        #Recover the database meta data:
        self.load_header(options)
        #Check if years should be relative, eg for piControl:
        options.min_year=None
        if 'experiment_list' in self.header.keys():
            for experiment in self.header['experiment_list']:
                min_year=int(self.header['experiment_list'][experiment].split(',')[0])
                if min_year<10:
                    options.min_year=min_year
                    if not ('silent' in dir(options) and options.silent):
                        print 'Using min year {0} for experiment {1}'.format(str(min_year),experiment)

        #Find data node list:
        self.load_database(options,find_simple)
        data_node_list=[item[0] for item in self.nc_Database.list_fields(['data_node'])]
        self.close_database()

        #manager=multiprocessing.Manager()
        self.queues, self.data_node_list, processes=retrieval_manager.start_processes(options,data_node_list,manager=manager)
        return processes

    def download_and_download_raw(self,options,output):
        #Find the data that needs to be recovered:
        self.load_database(options,find_simple)
        self.nc_Database.retrieve_database(options,output,self.queues)
        self.close_database()

        #Launch the retrieval/monitoring:
        retrieval_manager.launch_download_and_remote_retrieve(output,self.data_node_list,self.queues,options)
        return

    def apply(self,options,manager=None):
        
        if (options.script=='' and 
            ('in_extra_netcdf_files' in dir(options) and 
              len(options.in_extra_netcdf_files)>0) ):
            raise InputErrorr('The identity script \'\' can only be used when no extra netcdf files are specified.')

        #Recover the database meta data:
        if ('keep_field' in dir(options) and options.keep_field!=None):
            drs_to_eliminate=[field for field in self.drs.official_drs_no_version if
                                                 not field in options.keep_field]
        else:
            drs_to_eliminate=self.drs.official_drs_no_version
        vars_list=[[ var[drs_to_eliminate.index(field)] if field in drs_to_eliminate else None
                            for field in self.drs.official_drs_no_version] for var in 
                            self.list_fields_local(options,drs_to_eliminate) ]

        #Randonmize the list:
        import random
        random.shuffle(vars_list)

        output=apply_manager.distributed_apply(nc_Database_apply.apply_to_variable,
                                               nc_Database_apply.extract_single_tree_and_file,
                                                 self.drs,options,vars_list,retrieval_queues=self.queues,manager=manager)
        if not ('convert' in dir(options) and options.convert):
            self.record_header(options,output)
            output.close()
        return

    def download_and_apply(self,options):
        options.download=True
        manager=multiprocessing.Manager()
        processes=self.download_and_download_raw_start_queues(options,manager=manager)
        try:
            self.apply(options,manager=manager)
        finally:
            for item in processes.keys():
                processes[item].terminate()
        return

    def convert(self,options,manager=None):
        options.convert=True
        options.script=''
        self.apply(options)
        return

    def find_data_nodes_and_simulations(self,options):
        #We have to time the response of the data node.
        self.load_database(options,find_simple)
        simulations_list=self.nc_Database.list_fields(self.drs.simulations_desc)

        data_node_list=[item[0] for item in self.nc_Database.list_fields(['data_node'])]
        url_list=[self.nc_Database.list_paths_by_data_node(data_node).split('|')[0].replace('fileServer','dodsC')
                    for data_node in data_node_list ]
        self.close_database()
        return data_node_list,url_list, simulations_list

    def rank_data_nodes(self,options,data_node_list,url_list):
        data_node_list_timed=[]
        data_node_timing=[]
        for data_node_id, data_node in enumerate(data_node_list):
            url=url_list[data_node_id]
            print 'Querying '+url+' to measure response time of data node... '
            #Try opening a link on the data node. If it does not work put this data node at the end.
            number_of_trials=5
            try:
                import_string='import cdb_query.remote_netcdf;import time;'
                load_string='remote_data=cdb_query.remote_netcdf.remote_netCDF(\''+url+'\',[]);remote_data.is_available();time.sleep(2);'
                timing=timeit.timeit(import_string+load_string,number=number_of_trials)
                data_node_timing.append(timing)
                data_node_list_timed.append(data_node)
            except:
                pass
            print 'Done!'
        return list(np.array(data_node_list_timed)[np.argsort(data_node_timing)])+list(set(data_node_list).difference(data_node_list_timed))

    def load_previous_simulations(self,options):
        prev_simulations_list=[]
        if ('update' in dir(options) and
             getattr(options,'update')!=None):
             for update_file in options.update:
                 #There is an update file:
                 options_copy=copy.copy(options)
                 options_copy.in_netcdf_file=update_file
                 #Load the header to update:
                 self.define_database(options_copy)
                 old_header=self.nc_Database.load_header()
                 self.close_database()
                 if remove_entry_from_dictionary(self.header,'search_list')==remove_entry_from_dictionary(old_header,'search_list'):
                    prev_simulations_list.extend(self.list_fields_local(options_copy,self.drs.simulations_desc))
        if len(prev_simulations_list)>0:
            if not ('silent' in dir(options) and options.silent):
                print 'Updating, not considering the following simulations:'
                for simulation in prev_simulations_list:
                    print ','.join(simulation)
        return sorted(prev_simulations_list)

    def list_fields(self,options):
        fields_list=self.list_fields_local(options,options.field)
        for field in fields_list:
            print ','.join(field)
        return

    def list_fields_local(self,options,fields_to_list):
        self.load_database(options,find_simple)
        fields_list=self.nc_Database.list_fields(fields_to_list)
        self.close_database()
        return fields_list

    def define_database(self,options,Dataset=None):
        if (isinstance(Dataset,netCDF4.Dataset) or
            isinstance(Dataset,netCDF4.Group)):
            self.nc_Database=nc_Database.nc_Database(self.drs,database_Dataset=Dataset)
            return
            
        if 'in_netcdf_file' in dir(options):
            self.nc_Database=nc_Database.nc_Database(self.drs,database_file=options.in_netcdf_file)
            return

        self.nc_Database=nc_Database.nc_Database(self.drs)
        return

    def load_header(self,options):
        if ('in_headers_file' in dir(options) and 
           options.in_headers_file!=None):
            try:
                self.header=json.load(open(options.in_headers_file,'r'))['header']
            except ValueError as e:
                print 'The input diagnostic file '+options.in_headers_file+' does not conform to JSON standard. Make sure to check its syntax'
            #for field_to_limit in ['experiment_list','variable_list']:
            #    if (field_to_limit in dir(options) and
            #        getattr(options,field_to_limit)!=None):
            #        setattr(options,field_to_limit,
            #                list(set(getattr(option,field_to_limit)).intersection(self.header[field_to_limit].keys())))
            #    else:
            #        setattr(options,field_to_limit,
            #                self.header[field_to_limit].keys())
        else:
            self.define_database(options)
            self.header=self.nc_Database.load_header()
            self.close_database()
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

    def record_header(self,options,output):
        self.define_database(options)
        self.header=self.nc_Database.load_header()
        nc_Database.record_header(output,self.header)
        self.close_database()
        return

    def load_database(self,options,find_function,Dataset=None,semaphores=dict()):
        self.define_database(options,Dataset=Dataset)
        if 'header' in dir(self):
            self.nc_Database.header=self.header
        self.nc_Database.populate_database(options,find_function,semaphores=semaphores)
        if 'ensemble' in dir(options) and options.ensemble!=None:
            #Always include r0i0p0 when ensemble was sliced:
            options_copy=copy.copy(options)
            options_copy.ensemble='r0i0p0'
            self.nc_Database.populate_database(options_copy,find_function,semaphores=semaphores)
        return

    def close_database(self):
        self.nc_Database.close_database()
        del self.nc_Database
        return

        
def find_simple(pointers,file_expt,semaphores=None):
    pointers.session.add(file_expt)
    pointers.session.commit()
    return

def remove_entry_from_dictionary(dictio,entry):
    return {name:dictio[name] for name in dictio.keys() if name!=entry}

