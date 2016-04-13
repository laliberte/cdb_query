#External:
import netCDF4
import h5py
import copy
import os
import multiprocessing
import Queue
import numpy as np
import datetime

#Internal:
import cdb_query_archive_class
import nc_Database
import nc_Database_utils
import netcdf4_soft_links.queues_manager as NC4SL_queues_manager
import netcdf4_soft_links.retrieval_manager as retrieval_manager

class CDB_queues_manager:
    def __init__(self,options):
        self.manager=multiprocessing.Manager()

        #Create queues:
        self.queues_names=[]
        if ('serial' in dir(options) and options.serial):
            self.serial=True
        else:
            self.serial=False
        if ('num_dl' in dir(options) and options.num_dl):
            self.num_dl=options.num_dl
        else:
            self.num_dl=1

        #for name in ['ask','validate','time_split']:
        authorized_functions=['ask','validate',
                               'download_files','reduce_soft_links',
                                'download_opendap','reduce']
        for name in authorized_functions:
            if (name in dir(options) and getattr(options,name)):
                self.queues_names.append(name)
        self.queues_names.append('record')

        #If reduce_soft_links_script is identity, do
        #not pipe results through reduce_soft_links:
        if (('reduce_soft_links_script' in dir(options) and 
            options.reduce_soft_links_script=='') and
            'reduce_soft_links' in self.queues_names):
            self.queues_names.remove('reduce_soft_links')
        
        for queue_name in self.queues_names:
            setattr(self,queue_name,self.manager.Queue())
            setattr(self,queue_name+'_expected',NC4SL_queues_manager.Shared_Counter(self.manager))
        
        #Create a shared counter to prevent file collisions:
        self.counter=NC4SL_queues_manager.Shared_Counter(self.manager)

        #Create an event that prevents consumers from terminating:
        self.do_not_keep_consumers_alive=self.manager.Event()

        #Create semaphores for validate:
        if 'validate' in self.queues_names:
            self.validate_semaphores=NC4SL_queues_manager.Semaphores_data_node(self.manager,num_concurrent=5)

        if len(set(['download_files','download_opendap']).intersection(self.queues_names))>0:
            #Create queues and semaphores for download:
            self.download=NC4SL_queues_manager.NC4SL_queues_manager(options,consumer_processes_names(options),manager=self.manager)
            self.download_processes=dict()

    def start_download_processes(self):
        if (len(set(['download_files','download_opendap']).intersection(self.queues_names))>0
            and not self.serial):
            self.download_processes=retrieval_manager.start_download_processes_no_serial(self.download,self.num_dl,self.download_processes)
        return

    def stop_download_processes(self):
        if (len(set(['download_files','download_opendap']).intersection(self.queues_names))>0
            and not self.serial):
            for proc_name in self.download_processes.keys(): self.download_processes[proc_name].terminate()
        return
                
    def set_closed(self):
        self.do_not_keep_consumers_alive.set()
        return

    def put(self,item):
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

        while not (self.do_not_keep_consumers_alive.is_set() and 
                    self.expected_queue_size()==0):
            #Get an element from one queue, starting from the last:
            for queue_name in self.queues_names[::-1]:
                if record:
                    #The record worker tries to start download processes whenever it can
                    self.start_download_processes()

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
                #Decrement current action:
                getattr(self,queue_name+'_expected').decrement_no_lock()
                return item
            else:
                raise Queue.Empty

    def expected_queue_size(self):
        return np.max([getattr(self,queue_name+'_expected').value for queue_name in self.queues_names])

def recorder(q_manager,project_drs,options):
    renewal_time=datetime.datetime.now()

    #Start downloads
    q_manager.start_download_processes()
    #The consumers can now terminate:
    q_manager.set_closed()

    #Set number of processors to 1 for recoder process.
    options.num_procs=1

    output=netCDF4.Dataset(options.out_netcdf_file,'w')
    output.set_fill_off()

    for item in iter(q_manager.get_record,'STOP'):
        if item[1]!='record':
            consume_one_item(item[0],item[1],item[2],q_manager,project_drs)
        else:
            record_to_netcdf_file(item[2],output,project_drs)
        output.sync()

        if ('username' in dir(options) and 
            options.username!=None and
            options.password!=None and
            datetime.datetime.now() - renewal_time > datetime.timedelta(hours=1)):
            #Reactivate certificates every hours:
            certificates.retrieve_certificates(options.username,options.service,user_pass=options.password)
            renewal_time=datetime.datetime.now()
    output.close()
    return

def record_to_netcdf_file(options,output,project_drs):
    database=cdb_query_archive_class.Database_Manager(project_drs)
    database.load_header(options)
    nc_Database.record_header(output,database.header)

    temp_file_name=options.in_netcdf_file
    #import subprocess; subprocess.Popen('ncdump -v path '+temp_file_name,shell=True)
    nc_Database_utils.record_to_netcdf_file_from_file_name(options,temp_file_name,output,project_drs)
    output.sync()
    os.remove(temp_file_name)
    return

def consumer(q_manager,project_drs):
    for item in iter(q_manager.get_no_record,'STOP'):
        consume_one_item(item[0],item[1],item[2],q_manager,project_drs)
    return

def consume_one_item(counter,function_name,options,q_manager,project_drs):
    #First copy options:
    options_copy=copy.copy(options)
    #Create unique file id:
    options_copy.out_netcdf_file+='.'+str(counter)

    #Recursively apply commands:
    database=cdb_query_archive_class.Database_Manager(project_drs)
    #Run the command:
    #getattr(database,function_name)(options_copy,q_manager=q_manager)
    try:
        getattr(cdb_query_archive_class,function_name)(database,options_copy,q_manager=q_manager)
    except:
        print function_name+' failed with the following options:',options_copy
        raise
    return

def start_consumer_processes(q_manager,project_drs,options):
    processes_names=consumer_processes_names(options)
    processes=dict()
    for process_name in processes_names:
        if process_name!=multiprocessing.current_process().name:
           processes[process_name]=multiprocessing.Process(target=consumer,
                                                           name=process_name,
                                                           args=(q_manager,project_drs))
           processes[process_name].start()
        else:
            processes[process_name]=multiprocessing.current_process()
    return processes

def consumer_processes_names(options):
    processes_names=[multiprocessing.current_process().name,]
    if (not ( 'serial' in dir(options) and options.serial) and
         ( 'num_procs' in dir(options) and options.num_procs>1) ):
        for proc_id in range(options.num_procs-1):
           processes_names.append('consumer_'+str(proc_id))
    return processes_names

