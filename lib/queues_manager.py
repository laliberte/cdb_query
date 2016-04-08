#External:
import netCDF4
import h5py
import copy
import os
import multiprocessing
import Queue
import numpy as np

#Internal:
import cdb_query_archive_class
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
                #Decrement current acction:
                getattr(self,queue_name+'_expected').decrement_no_lock()
                return item
            else:
                raise Queue.Empty

    def expected_queue_size(self):
        return np.max([getattr(self,queue_name+'_expected').value for queue_name in self.queues_names])

def recorder(q_manager,project_drs,options):
    output=netCDF4.Dataset(options.out_netcdf_file,'w')

    for item in iter(q_manager.get_record,'STOP'):
        if item[1]!='record':
            cdb_query_archive_class.consume_one_item(item[0],item[1],item[2],q_manager,project_drs)
        else:
            cdb_query_archive_class.record_to_netcdf_file(item[2],output,project_drs)
    return

def consumer(q_manager,project_drs):
    for item in iter(q_manager.get_no_record,'STOP'):
        cdb_query_archive_class.consume_one_item(item[0],item[1],item[2],q_manager,project_drs)
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

