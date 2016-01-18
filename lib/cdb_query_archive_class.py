import discover
import optimset

import netCDF4

import nc_Database

import copy
import os

import json
import timeit

import numpy as np

import multiprocessing

import random

import sys

import certificates
import getpass

import retrieval_manager
import recovery_manager

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

    def ask(self,options):
        #Load header:
        try:
            self.header=json.load(open(options.in_diagnostic_headers_file,'r'))['header']
        except ValueError as e:
            print 'The input diagnostic file '+options.in_diagnostic_headers_file+' does not conform to JSON standard. Make sure to check its syntax'
            raise

        if 'username' in dir(options) and options.username!=None:
            if not options.password_from_pipe:
                user_pass=getpass.getpass('Enter Credential phrase:')
            else:
                user_pass=sys.stdin.readline()
        else:
            user_pass=None
        options.password=user_pass

        #Simplify the header:
        self.union_header()

        if options.list_only_field!=None:
            #Only a listing of a few fields was requested.
            for field_name in discover.discover(self,options):
                print field_name
            return
        else:
            if ('catalogue_missing_simulations_desc' in dir(self.drs)
               and self.drs.catalogue_missing_simulations_desc):
                #This allows some projects to be inconsistent in their publications:
                filepath=discover.discover(self,options)
                try:
                    os.rename(filepath,filepath.replace('.pid'+str(os.getpid()),''))
                except OSError:
                    pass
            else:
                #Check if this is an update and if this an update find the previous simulations list:
                prev_simulations_list=self.load_previous_simulations(options)

                simulations_list=discover.discover_simulations_recursive(self,options,self.drs.simulations_desc)
                simulations_list=sorted(list(set(simulations_list).difference(prev_simulations_list)))
                print "This is a list of simulations that COULD satisfy the query:"
                for simulation in simulations_list:
                    print ','.join(simulation)
                print "cdb_query will now attempt to confirm that these simulations have all the requested variables."
                print "This can take some time. Please abort if there are not enough simulations for your needs."

                import random
                random.shuffle(simulations_list)

                manager=multiprocessing.Manager()
                output=recovery_manager.distributed_recovery(discover.discover,self,options,simulations_list,manager)

                #Close dataset
                output.close()
        return

    def load_previous_simulations(self,options):
        prev_simulations_list=[]
        if ('update' in dir(options) and
             getattr(options,'update')!=None):
             for update_file in options.update:
                 #There is an update file:
                 options_copy=copy.copy(options)
                 options_copy.in_diagnostic_netcdf_file=update_file
                 #Load the header to update:
                 self.define_database(options_copy)
                 old_header=self.nc_Database.load_header()
                 self.close_database()
                 if remove_entry_from_dictionary(self.header,'search_list')==remove_entry_from_dictionary(old_header,'search_list'):
                    prev_simulations_list.extend(self.list_fields_local(options_copy,self.drs.simulations_desc))
        if len(prev_simulations_list)>0:
            print 'Updating, not considering the following simulations:'
            for simulation in prev_simulations_list:
                print ','.join(simulation)
        return sorted(prev_simulations_list)

    def validate(self,options):
        self.load_header(options)
        #if options.data_nodes!=None:
        #    self.header['data_node_list']=options.data_nodes

        if 'username' in dir(options) and options.username!=None:
            if not options.password_from_pipe:
                user_pass=getpass.getpass('Enter Credential phrase:')
                #Get certificates if requested by user:
            else:
                user_pass=sys.stdin.readline()

            certificates.retrieve_certificates(options.username,options.service,user_pass=user_pass,trustroots=options.no_trustroots)
        else:
            user_pass=None
        options.password=user_pass

        if not 'data_node_list' in self.header.keys():
            data_node_list, url_list, simulations_list =self.find_data_nodes_and_simulations(options)
            if len(data_node_list)>1 and not options.no_check_availability:
                self.header['data_node_list']=self.rank_data_nodes(options,data_node_list,url_list)
            else:
                self.header['data_node_list']=data_node_list
        else:
            simulations_list=[]

        if options.num_procs==1:
            filepath=optimset.optimset(self,options)
            try:
                os.rename(filepath,filepath.replace('.pid'+str(os.getpid()),''))
            except OSError:
                pass
        else:
            #Find the atomic simulations:
            if simulations_list==[]:
                simulations_list=self.list_fields_local(options,self.drs.simulations_desc)

            #Randomize the list:
            import random
            random.shuffle(simulations_list)

            #for simulation in simulations_list:
            #    #if simulation[-1]!='r0i0p0':
            #    print '_'.join(simulation)
            #print "Validating"

            manager=multiprocessing.Manager()
            semaphores=dict()
            for data_node in  self.header['data_node_list']:
                semaphores[data_node]=manager.Semaphore(5)
            #semaphores=[]
            #original_stderr = sys.stderr
            #sys.stderr = NullDevice()
            output=recovery_manager.distributed_recovery(optimset.optimset_distributed,self,options,simulations_list,manager,args=(semaphores,),user_pass=user_pass)
            #sys.stderr = original_stderr
            #Close datasets:
            output.close()
        return

    def download(self,options):

        output=netCDF4.Dataset(options.out_diagnostic_netcdf_file,'w')
        retrieval_function='retrieve_path_data'
        self.remote_retrieve_and_download(options,output,retrieval_function)
        return

    def download_raw(self,options):
        output=options.out_destination

        #Describe the tree pattern:
        if self.drs.official_drs.index('var')>self.drs.official_drs.index('version'):
            output+='/tree/version/var/'
        else:
            output+='/tree/var/version/'
            
        retrieval_function='retrieve_path'
        self.remote_retrieve_and_download(options,output,retrieval_function)
        return

    def remote_retrieve_and_download(self,options,output,retrieval_function):
        if 'username' in dir(options) and options.username!=None:
            if not options.password_from_pipe:
                user_pass=getpass.getpass('Enter Credential phrase:')
            else:
                user_pass=sys.stdin.readline().rstrip()
            #Get certificates if requested by user:
            certificates.retrieve_certificates(options.username,options.service,user_pass=user_pass,trustroots=options.no_trustroots)
        else:
            user_pass=None

        options.user_pass=user_pass

        #Recover the database meta data:
        self.load_header(options)
        self.load_database(options,find_simple)

        #Check if years should be relative, eg for piControl:
        options.min_year=None
        if 'experiment_list' in self.header.keys():
            for experiment in self.header['experiment_list']:
                min_year=int(self.header['experiment_list'][experiment].split(',')[0])
                if min_year<10:
                    options.min_year=min_year
                    print 'Using min year {0} for experiment {1}'.format(str(min_year),experiment)

        #Find data node list:
        data_node_list=[item[0] for item in self.nc_Database.list_fields(['data_node'])]
        paths_list=self.nc_Database.list_paths()
        self.close_database()

        queues, processes, data_node_list=retrieval_manager.start_processes(options,data_node_list)

        #Find the data that needs to be recovered:
        self.define_database(options)
        self.nc_Database.retrieve_database(options,output,queues,retrieval_function)
        self.close_database()

        #Launch the retrieval/monitoring:
        retrieval_manager.launch_download_and_remote_retrieve(output,data_node_list,queues,retrieval_function,options,user_pass=user_pass)
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

    def define_database(self,options):
        if 'in_diagnostic_netcdf_file' in dir(options):
            self.nc_Database=nc_Database.nc_Database(self.drs,database_file=options.in_diagnostic_netcdf_file)
        else:
            self.nc_Database=nc_Database.nc_Database(self.drs)
        return

    def load_header(self,options):
        if ('in_diagnostic_headers_file' in dir(options) and 
           options.in_diagnostic_headers_file!=None):
            try:
                self.header=json.load(open(options.in_diagnostic_headers_file,'r'))['header']
            except ValueError as e:
                print 'The input diagnostic file '+options.in_diagnostic_headers_file+' does not conform to JSON standard. Make sure to check its syntax'
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

    def record_header(self,options,output):
        self.define_database(options)
        self.header=self.nc_Database.load_header()
        self.nc_Database.record_header(output,self.header)
        self.close_database()
        return

    def load_database(self,options,find_function,semaphores=None):
        self.define_database(options)
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
