import discover
import optimset

import netCDF4

import nc_Database

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
            simulations_list=discover_simulations_recursive(self,options,self.drs.simulations_desc)
            simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                        simulation[self.drs.simulations_desc.index('ensemble')]!='r0i0p0']
            for simulation in simulations_list_no_fx:
                print simulation
                for desc_id, desc in enumerate(self.drs.simulations_desc):
                    setattr(options,desc,simulation[desc_id])
                output=discover.discover(self,options)
                output.close()
                for desc_id, desc in enumerate(self.drs.simulations_desc):
                    setattr(options,desc,None)
        return

    def optimset(self,options):
        simulations_list=self.list_fields_local(options.in_diagnostic_netcdf_file,self.drs.simulations_desc)
        simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                    simulation[self.drs.simulations_desc.index('ensemble')]!='r0i0p0']
        output=optimset.optimset(self,options)
        output.close()
        return

    def list_fields(self,options):
        #slice with options:
        fields_list=self.list_fields_local(options.in_diagnostic_netcdf_file,options.field)
        for field in fields_list:
            print ','.join(field)
        return

    def list_fields_local(self,netcdf4_file,fields_to_list):
        self.load_nc_file(netcdf4_file)
        self.nc_Database.populate_database(self.Dataset,find_simple)
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

def discover_simulations_recursive(database,options,simulations_desc):
    if isinstance(simulations_desc,list) and len(simulations_desc)>1:
        options.list_only_field=simulations_desc[0]
        output=discover.discover(database,options)
        options.list_only_field=None
        simulations_list=[]
        for val in output:
            setattr(options,simulations_desc[0],val)
            simulations_list.extend([(val,)+item for item in discover_simulations_recursive(database,options,simulations_desc[1:])])
            setattr(options,simulations_desc[0],None)
    else:
        options.list_only_field=simulations_desc[0]
        simulations_list=[(item,) for item in discover.discover(database,options)]
        options.list_only_field=None
    return simulations_list
        
def find_simple(pointers,file_expt):
    #for item in dir(file_expt):
    #    if item[0]!='_':
    #        print getattr(file_expt,item)
    pointers.session.add(file_expt)
    pointers.session.commit()
    return
