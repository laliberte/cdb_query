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
import validate
import nc_Database
import nc_Database_reduce
import downloads
#import nc_Database_convert
import recovery_manager

class SimpleTree:
    def __init__(self,project_drs,queues_manager=None):
        self.drs=project_drs
        self.queues_manager=queues_manager
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

        #Find the simulation list:
        simulations_list=ask.ask_simulations_recursive(self,options,self.drs.simulations_desc)

        #Remove fixed variable:
        simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                    simulation[self.drs.simulations_desc.index('ensemble')]!='r0i0p0']
        if not ('silent' in dir(options) and options.silent) and len(simulations_list_no_fx)>1:
            print "This is a list of simulations that COULD satisfy the query:"
            for simulation in simulations_list_no_fx:
                print ','.join(simulation)
            print "cdb_query will now attempt to confirm that these simulations have all the requested variables."
            print "This can take some time. Please abort if there are not enough simulations for your needs."

        #Do it by simulation, except if one simulation field should be kept for further operations:
        if ('keep_field' in dir(options) and options.keep_field!=None):
            drs_to_eliminate=[field for field in self.drs.simulations_desc if
                                                 not field in options.keep_field]
        else:
            drs_to_eliminate=self.drs.simulations_desc
        vars_list=[[ var[self.drs.simulations_desc.index(field)] if field in drs_to_eliminate else None
                            for field in self.drs.official_drs_no_version] for var in 
                            simulations_list_no_fx ]

        self.put_or_process(ask.ask,'ask',var_list,options)
        return

    def validate(self,options):
        self.load_header(options)

        if not 'data_node_list' in self.header.keys():
            data_node_list, url_list, simulations_list =self.find_data_nodes_and_simulations(options)
            if len(data_node_list)>1 and not options.no_check_availability:
                #self.header['data_node_list']=rank_data_nodes(options,data_node_list,url_list)
                data_node_list=rank_data_nodes(options,data_node_list,url_list)
        else:
            simulations_list=[]
        self.drs.data_node_list=data_node_list

        #Find the atomic simulations:
        if simulations_list==[]:
            simulations_list=self.list_fields_local(options,self.drs.simulations_desc)
        #Remove fixed variable:
        simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                    simulation[self.drs.simulations_desc.index('ensemble')]!='r0i0p0']

        #Do it by simulation, except if one simulation field should be kept for further operations:
        if ('keep_field' in dir(options) and options.keep_field!=None):
            drs_to_eliminate=[field for field in self.drs.simulations_desc if
                                                 not field in options.keep_field]
        else:
            drs_to_eliminate=self.drs.simulations_desc
        vars_list=[[ var[self.drs.simulations_desc.index(field)] if field in drs_to_eliminate else None
                            for field in self.drs.official_drs_no_version] for var in 
                            simulations_list_no_fx ]

        self.put_or_process(validate.validate,'validate',vars_list,options)
        return

    def download_raw(self,options):
        #Recover the database meta data:
        if ('keep_field' in dir(options) and options.keep_field!=None):
            drs_to_eliminate=[field for field in self.drs.official_drs_no_version if
                                                 not field in options.keep_field]
        else:
            drs_to_eliminate=self.drs.official_drs_no_version
        vars_list=[[ var[drs_to_eliminate.index(field)] if field in drs_to_eliminate else None
                            for field in self.drs.official_drs_no_version] for var in 
                            self.list_fields_local(options,drs_to_eliminate) ]

        self.put_or_process(downloads.download_raw,'download_raw',vars_list,options)
        return

    def time_split(self,options):
        if ('keep_field' in dir(options) and options.keep_field!=None):
            drs_to_eliminate=[field for field in self.drs.official_drs_no_version if
                                                 not field in options.keep_field]
        else:
            drs_to_eliminate=self.drs.official_drs_no_version
        vars_list=[[ var[drs_to_eliminate.index(field)] if field in drs_to_eliminate else None
                            for field in self.drs.official_drs_no_version] for var in 
                            self.list_fields_local(options,drs_to_eliminate) ]

        self.put_or_process(time_split.time_split,'time_split',vars_list,options)
        return

    def download(self,options):
        #Recover the database meta data:
        if ('keep_field' in dir(options) and options.keep_field!=None):
            drs_to_eliminate=[field for field in self.drs.official_drs_no_version if
                                                 not field in options.keep_field]
        else:
            drs_to_eliminate=self.drs.official_drs_no_version
        vars_list=[[ var[drs_to_eliminate.index(field)] if field in drs_to_eliminate else None
                            for field in self.drs.official_drs_no_version] for var in 
                            self.list_fields_local(options,drs_to_eliminate) ]

        self.put_or_process(downloads.download,'download',vars_list,options)
        return

    def reduce(self,options):
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

        self.put_or_process(reduce_engine.reduce_application,'reduce',vars_list,options)
        return

    def convert(self,options):
        self.download(options)
        return

    def list_fields(self,options):
        fields_list=self.list_fields_local(options,options.field)
        for field in fields_list:
            print ','.join(field)
        return

    def put_or_process(self,function_handle,function_name,vars_list,options):
        if (len(vars_list)==1 or
            self.qeues_manager==None or
           ('serial' in dir(options) and options.serial)):
            ouput_file_name=function_handle(self,options)
            options.in_netcdf_file=output_file_name
            next_function=self.queues_manager.queues_names[self.queues_manager.queues_names.index(function_name)+1]
            self.queues_manager.put((next_function,self.drs,options))
        else:
            #Randomize to minimize strain on index nodes:
            random.shuffle(vars_list)
            for var in enumerate(vars_list):
                options_copy=copy.copy(options)
                for opt_id, opt in enumerate(project_drs.official_drs_no_version):
                    if var[opt_id]!=None:
                        setattr(options_copy,opt,var[opt_id])
                    self.queues_manager.put((function_name,self.drs,options_copy))
        return

    def list_fields_local(self,options,fields_to_list):
        self.load_database(options,find_simple)
        fields_list=self.nc_Database.list_fields(fields_to_list)
        self.close_database()
        return fields_list

    def find_data_nodes_and_simulations(self,options):
        #We have to time the response of the data node.
        self.load_database(options,find_simple)
        simulations_list=self.nc_Database.list_fields(self.drs.simulations_desc)

        data_node_list=[item[0] for item in self.nc_Database.list_fields(['data_node'])]
        url_list=[self.nc_Database.list_paths_by_data_node(data_node).split('|')[0].replace('fileServer','dodsC')
                    for data_node in data_node_list ]
        self.close_database()
        return data_node_list,url_list, simulations_list

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

    def load_header(self,options):
        if ('in_headers_file' in dir(options) and 
           options.in_headers_file!=None):
            try:
                self.header=json.load(open(options.in_headers_file,'r'))['header']
            except ValueError as e:
                print 'The input diagnostic file '+options.in_headers_file+' does not conform to JSON standard. Make sure to check its syntax'
        else:
            self.define_database(options)
            self.header=self.nc_Database.load_header()
            self.close_database()
        return
        
    #def load_database(self,options,find_function,Dataset=None,semaphores=dict()):
    def load_database(self,options,find_function,semaphores=dict()):
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

    def define_database(self,options):
        if 'in_netcdf_file' in dir(options):
            self.nc_Database=nc_Database.nc_Database(self.drs,database_file=options.in_netcdf_file)
            return

        self.nc_Database=nc_Database.nc_Database(self.drs)
        return

    def close_database(self):
        self.nc_Database.close_database()
        del self.nc_Database
        return

    #def record_header(self,options,output):
    #    self.define_database(options)
    #    self.header=self.nc_Database.load_header()
    #    nc_Database.record_header(output,self.header)
    #    self.close_database()
    #    return


        
def find_simple(pointers,file_expt,semaphores=None):
    pointers.session.add(file_expt)
    pointers.session.commit()
    return

def remove_entry_from_dictionary(dictio,entry):
    return {name:dictio[name] for name in dictio.keys() if name!=entry}

def rank_data_nodes(options,data_node_list,url_list):
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
