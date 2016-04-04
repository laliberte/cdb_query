#External:
import netCDF4
import h5py
import copy
import os
import multiprocessing
import Queue
import numpy as np

#Internal:
import nc_Database_utils
import nc_Database
import cdb_query_archive_class
import netcdf4_soft_links.queues_manager as NC4SL_queues_manager

class CDB_queues_manager:
    def __init__(self,options):
        self.manager=multiprocessing.Manager()

        #Create queues:
        self.queues_names=[]
        #for name in ['ask','validate','download_raw','time_split']:
        #for name in ['ask','validate','time_split']:
        authorized_functions=['ask','validate',
                               'download_raw','reduce_soft_links',
                                'time_split','download_remote',
                                'download','reduce']
        for name in authorized_functions:
            if (name in dir(options) and getattr(options,name)):
                self.queues_names.append(name)
        self.queues_names.append('record')
        
        for queue_name in self.queues_names:
            setattr(self,queue_name,self.manager.Queue())
            setattr(self,queue_name+'_expected',NC4SL_queues_manager.Shared_Counter(self.manager))
        
        #Create a shared counter to prevent file collisions:
        self.counter=NC4SL_queues_manager.Shared_Counter(self.manager)

        #Create semaphores for validate:
        if 'validate' in self.queues_names:
            self.validate_semaphores=NC4SL_queues_manager.Semaphores_data_node(self.manager,num_concurrent=5)

        if 'download' in self.queues_names:
            #Create queues and semaphores for download:
            self.download=NC4SL_queues_manager.NC4SL_queues_manager(options,processes_names,manager=self.manager)
                
    def put(self,item):
        if item[0]==self.queues_names[0]:
            getattr(self,item[0]+'_expected').increment()
        #Put the item in the right queue and give it a number:
        getattr(self,item[0]).put((self.counter.increment(),)+item)
        return

    def get_no_record(self):
        return self.get()

    def get_record(self):
        return self.get(record=True)

    def get(self,record=False):
        #Simple get that goes through the queues sequentially
        timeout_first=0.01
        timeout_subsequent=0.1
        timeout=timeout_first

        while self.expected_queue_size()>0:
            #Get an element from one queue, starting from the last:
            for queue_name in self.queues_names[::-1]:
                #Record workers can pick from the record queue
                if not (not record and queue_name == 'record'):
                    try:
                        return self.get_queue(queue_name,timeout)
                    except Queue.Empty:
                        pass
                #First pass, short timeout. Subsequent pass, longer:
                if timeout==timeout_first: timeout=timeout_subsequent
        return 'STOP'

    def get_queue(self,queue_name,timeout):
        #Get queue with locks:
        with getattr(self,queue_name+'_expected').lock:
            if getattr(self,queue_name+'_expected').value_no_lock > 0:
                #Will fail with Queue.Empty if the item had not been put in the queue:
                item=getattr(self,queue_name).get(True,timeout)
                #Increment future actions:
                if item[1]!='record':
                    next_queue_name=self.queues_names[self.queues_names.index(item[1])+1]
                    getattr(self,next_queue_name+'_expected').increment()
                #Decrement current acction:
                getattr(self,queue_name+'_expected').decrement_no_lock()
                return item
            else:
                raise Queue.Empty

    def expected_queue_size(self):
        return np.max([getattr(self,queue_name+'_expected').value for queue_name in self.queues_names])

def recorder(queue_manager,project_drs,options):
    if not ('convert' in dir(options) and options.convert):
        output=netCDF4.Dataset(options.out_netcdf_file,'w')

    for item in iter(queue_manager.get_record,'STOP'):
        if item[1]!='record':
            consume_one_item(item[0],item[1],item[2],queue_manager,project_drs)
        elif not ('convert' in dir(options) and options.convert):
            record_to_netcdf_file(item[2],output,project_drs)
    return

def consumer(queue_manager,project_drs):
    for item in iter(queue_manager.get_no_record,'STOP'):
        consume_one_item(item[0],item[1],item[2],queue_manager,project_drs)
    return

def consume_one_item(counter,function_name,options,queue_manager,project_drs):
    #Create unique file id:
    options.out_netcdf_file+='.'+str(counter)

    #Recursively apply commands:
    apps_class=cdb_query_archive_class.SimpleTree(project_drs,queues_manager=queue_manager)
    #Run the command:
    getattr(apps_class,function_name)(options)
    return

def record_to_netcdf_file(options,output,project_drs):
    apps_class=cdb_query_archive_class.SimpleTree(project_drs)
    apps_class.load_header(options)
    nc_Database.record_header(output,apps_class.header)

    temp_file_name=options.in_netcdf_file
    #import subprocess
    #subprocess.call('ncdump -h '+temp_file_name,shell=True)
    data=netCDF4.Dataset(temp_file_name,'r')
    data_hdf5=None
    for item in h5py.h5f.get_obj_ids():
        if 'name' in dir(item) and item.name==temp_file_name:
            data_hdf5=h5py.File(item)

    var=[ getattr(options,opt) for opt in project_drs.official_drs_no_version]
    tree=zip(project_drs.official_drs_no_version,var)

    if ('applying_to_soft_links' in dir(options) and
        options.applying_to_soft_links):
        #Do not check empty:
        nc_Database_utils.replace_netcdf_variable_recursive(output,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=False)
    else:
        nc_Database_utils.replace_netcdf_variable_recursive(output,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=True)
        data.close()

    if data_hdf5!=None:
        data_hdf5.close()
    try:
        os.remove(temp_file_name)
    except OSError:
        pass
    return
