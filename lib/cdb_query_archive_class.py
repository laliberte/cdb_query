import discover
import optimset

import netCDF4

import nc_Database
import netcdf_utils

import copy
import os

import json

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

        if options.list_only_field!=None:
            for field_name in discover.discover(self,options):
                print field_name
            return
        else:
            if options.num_procs==1:
                filepath=discover.discover(self,options)
                try:
                    os.rename(filepath,filepath.replace('.pid'+str(os.getpid()),''))
                except OSError:
                    pass
            else:
                #Find the atomic simulations:
                simulations_list=discover.discover_simulations_recursive(self,options,self.drs.simulations_desc)

                output=distributed_recovery(discover.discover,self,options,simulations_list)

                #Finally, find the list of data_nodes and record it in the header
                #self.nc_Database=nc_Database.nc_Database(self.drs)
                #self.nc_Database.populate_database(output,options,find_simple)
                #data_node_list=self.nc_Database.list_data_nodes()
                #self.nc_Database.close_database()
                #del self.nc_Database
                #output.setncattr('data_node_list', json.dumps(data_node_list))

                #Close dataset
                output.close()
        return

    def optimset(self,options):
        if options.num_procs==1:
            filepath=optimset.optimset(self,options)
            try:
                os.rename(filepath,filepath.replace('.pid'+str(os.getpid()),''))
            except OSError:
                pass
        else:
            #Find the atomic simulations:
            simulations_list=self.list_fields_local(options.in_diagnostic_netcdf_file,options,self.drs.simulations_desc)

            output=distributed_recovery(optimset.optimset,self,options,simulations_list)
            #Close dataset:
            output.close()
        return

    def list_fields(self,options):
        #slice with options:
        fields_list=self.list_fields_local(options.in_diagnostic_netcdf_file,options,options.field)
        for field in fields_list:
            print ','.join(field)
        return

    def list_fields_local(self,netcdf4_file,options,fields_to_list):
        self.load_nc_file(netcdf4_file)
        self.nc_Database.populate_database(self.Dataset,options,find_simple)
        self.Dataset.close()
        fields_list=self.nc_Database.list_fields(fields_to_list)
        self.nc_Database.close_database()
        del self.nc_Database
        return fields_list

    def load_nc_file(self,netcdf4_file):
        self.Dataset=netCDF4.Dataset(netcdf4_file,'r')
        #Load header:
        self.header=dict()
        for att in set(self.drs.header_desc).intersection(self.Dataset.ncattrs()):
            self.header[att]=json.loads(self.Dataset.getncattr(att))
        self.nc_Database=nc_Database.nc_Database(self.drs)
        return



def worker(function,database,options,queue,*args):
    queue.put(function(database,options,*args))
    return

def distributed_recovery(function_handle,database,options,simulations_list,args=tuple()):

        #Open output file:
        output_file_name=options.out_diagnostic_netcdf_file
        output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')

        simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                    simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']

        from multiprocessing import Queue
        record_to_file_queue=Queue()

        from multiprocessing import Process, current_process

        #Perform the discovery simulation per simulation:
        for simulation_id,simulation in enumerate(simulations_list_no_fx):
            options_copy=copy.copy(options)
            for desc_id, desc in enumerate(database.drs.simulations_desc):
                setattr(options_copy,desc,simulation[desc_id])
            Process(target=worker, args=(function_handle,copy.copy(database),options_copy,record_to_file_queue)+args).start()
            if simulation_id>options.num_procs:
                netcdf_utils.record_to_file(output_root,netCDF4.Dataset(record_to_file_queue.get(),'r'))

        #Record the output to single file:
        for simulation_id in range(options.num_procs+1):
            netcdf_utils.record_to_file(output_root,netCDF4.Dataset(record_to_file_queue.get(),'r'))

        return output_root
        
def find_simple(pointers,file_expt):
    #for item in dir(file_expt):
    #    if item[0]!='_':
    #        print getattr(file_expt,item)
    pointers.session.add(file_expt)
    pointers.session.commit()
    return
