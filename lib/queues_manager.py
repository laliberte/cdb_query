#External:
import netCDF4
import h5py
import copy
import os
import sys
import multiprocessing
import multiprocessing.managers as managers
import Queue
import threading
import numpy as np
import datetime
import glob

#Internal:
import cdb_query_archive_class
import nc_Database
import nc_Database_utils
import netcdf4_soft_links.queues_manager as NC4SL_queues_manager
import netcdf4_soft_links.retrieval_manager as retrieval_manager
import netcdf4_soft_links.certificates as certificates

class SimpleSyncManager(managers.BaseManager):
    '''
    Subclass of `BaseManager'' it includes a subset of the 
    shared objects provided by multiprocessing.manager
    '''
SimpleSyncManager.register('Queue', Queue.Queue)
SimpleSyncManager.register('PriorityQueue', Queue.PriorityQueue)
SimpleSyncManager.register('Event', threading.Event, managers.EventProxy)
SimpleSyncManager.register('Lock', threading.Lock, managers.AcquirerProxy)
#SimpleSyncManager.register('RLock', threading.RLock, AcquirerProxy)
SimpleSyncManager.register('Semaphore', threading.Semaphore, managers.AcquirerProxy)
#SimpleSyncManager.register('BoundedSemaphore', threading.BoundedSemaphore,
#                    managers.AcquirerProxy)
#SimpleSyncManager.register('Condition', threading.Condition, managers.ConditionProxy)
#SimpleSyncManager.register('Barrier', threading.Barrier, managers.BarrierProxy)
#SimpleSyncManager.register('Pool', pool.Pool, managers.PoolProxy)
SimpleSyncManager.register('list', list, managers.ListProxy)
SimpleSyncManager.register('dict', dict, managers.DictProxy)
SimpleSyncManager.register('Value', managers.Value, managers.ValueProxy)
#SimpleSyncManager.register('Array', Array, managers.ArrayProxy)
#SimpleSyncManager.register('Namespace', Namespace, managers.NamespaceProxy)
    

class CDB_queues_manager:
    def __init__(self,options):
        #self.manager=multiprocessing.Manager()
        self.manager=SimpleSyncManager()
        self.manager.start()

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
            setattr(self,queue_name,self.manager.PriorityQueue())
            setattr(self,queue_name+'_expected',NC4SL_queues_manager.Shared_Counter(self.manager))
        
        #Create a shared counter to prevent file collisions:
        self.counter=NC4SL_queues_manager.Shared_Counter(self.manager)

        #Create an event that prevents consumers from terminating:
        self.do_not_keep_consumers_alive=self.manager.Event()

        #Create semaphores for validate:
        if 'validate' in self.queues_names:
            self.validate_semaphores=NC4SL_queues_manager.Semaphores_data_node(self.manager,num_concurrent=1)

        if len(set(['download_files','download_opendap']).intersection(self.queues_names))>0:
            #Create queues and semaphores for download:
            self.download=NC4SL_queues_manager.NC4SL_queues_manager(options,consumer_processes_names(self,options),manager=self.manager)
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
        getattr(self,item[0]).put((item[-1].priority,(self.counter.increment(),)+item))
        return

    def get_dr_no_record(self):
        return self.get(queues_names=[queue for queue in ['download_opendap','reduce']
                                        if queue in self.queues_names])

    def get_avdr_no_record(self):
        return self.get(queues_names=[queue for queue in ['ask','validate','download_files','reduce_soft_links']
                                        if queue in self.queues_names])

    def get_all_record(self):
        return self.get(record=True,queues_names=self.queues_names)

    def get(self,record=False,queues_names=[]):
        #Simple get that goes through the queues sequentially
        timeout_first=0.01
        timeout_subsequent=0.1
        timeout=timeout_first

        while not (self.do_not_keep_consumers_alive.is_set() and 
                    self.expected_queue_size(self.queues_names)==0):
            #Get an element from one queue, starting from the last:
            for queue_name in queues_names[::-1]:
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
                item=getattr(self,queue_name).get(True,timeout)[1]
                #reset priority to 0:
                item[-1].priority=0
                #Increment future actions:
                if item[1]!='record':
                    next_queue_name=self.queues_names[self.queues_names.index(item[1])+1]
                    getattr(self,next_queue_name+'_expected').increment()
                #Decrement current action:
                getattr(self,queue_name+'_expected').decrement_no_lock()
                return item
            else:
                raise Queue.Empty

    def expected_queue_size(self,restricted_queues_names):
        return np.max([getattr(self,queue_name+'_expected').value for queue_name in restricted_queues_names])

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

    database=cdb_query_archive_class.Database_Manager(project_drs)
    database.load_header(options)
    nc_Database.record_header(output,database.header)

    output.close()

    if ( 'log_files' in options and options.log_files ):
        log_file_name=multiprocessing.current_process().name + ".out"
        with open(log_file_name, "w") as logger:
            sys.stdout=logger
            old_stdout=sys.stdout
            old_stdout.flush()
            try:
                sys.stdout=logger
                recorder_queue_consume(q_manager,project_drs,options)
            finally:
                sys.stdout.flush()
                sys.stdout=old_stdout
    else:
        recorder_queue_consume(q_manager,project_drs,options)
    return
        
def recorder_queue_consume(q_manager,project_drs,options):
    renewal_time=datetime.datetime.now()
    for item in iter(q_manager.get_all_record,'STOP'):
        if item[1]!='record':
            consume_one_item(item[0],item[1],item[2],q_manager,project_drs)
        else:
            record_to_netcdf_file(item[2],options.out_netcdf_file,project_drs)

        if ('username' in dir(options) and 
            options.username!=None and
            options.password!=None and
            datetime.datetime.now() - renewal_time > datetime.timedelta(hours=1)):
            #Reactivate certificates every hours:
            certificates.retrieve_certificates(options.username,options.service,user_pass=options.password)
            renewal_time=datetime.datetime.now()
    return

def record_to_netcdf_file(options,file_name,project_drs):
    temp_file_name=options.in_netcdf_file
    #import subprocess; subprocess.Popen('ncdump -v path '+temp_file_name,shell=True)
    if ( 'log_files' in options and options.log_files ):
        print('Recording: ',datetime.datetime.now(),options)
    output=netCDF4.Dataset(file_name,'a')
    nc_Database_utils.record_to_netcdf_file_from_file_name(options,temp_file_name,output,project_drs)
    output.close()
    if ( 'log_files' in options and options.log_files ):
        print('DONE Recording: ',datetime.datetime.now(),options)
    #Make sure the file is gone:
    for id in range(2):
        try:
            os.remove(temp_file_name)
        except:
            pass
    return

def consumer(q_manager,project_drs,options):
    if ( 'log_files' in options and options.log_files ):
        log_file_name=multiprocessing.current_process().name + ".out"
        with open(log_file_name, "w") as logger:
            old_stdout=sys.stdout
            old_stdout.flush()
            try:
                sys.stdout=logger
                consumer_queue_consume(q_manager,project_drs)
            finally:
                sys.stdout.flush()
                sys.stdout=old_stdout
    else:
        consumer_queue_consume(q_manager,project_drs)
    return

def consumer_queue_consume(q_manager,project_drs):
    get_type=getattr(q_manager,'get_'+multiprocessing.current_process().name.split('_')[0]+'_no_record')
    for item in iter(get_type,'STOP'):
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
        if ( 'log_files' in options and options.log_files ):
            print('Process: ',datetime.datetime.now(),function_name,[(queue_name,getattr(q_manager,queue_name+'_expected').value) for queue_name in q_manager.queues_names],options_copy)
        getattr(cdb_query_archive_class,function_name)(database,options_copy,q_manager=q_manager)
        if ( 'log_files' in options and options.log_files ):
            print('DONE Process: ',datetime.datetime.now(),function_name,[(queue_name,getattr(q_manager,queue_name+'_expected').value) for queue_name in q_manager.queues_names],options_copy)
        #Reset trial counter:
        options_copy.trial=0
    except:
        if options_copy.trial<options.max_trial:
            #Put it back in the queue, increasing its trial number:
            options_copy.trial+=1
            next_function_name=q_manager.queues_names[q_manager.queues_names.index(function_name)+1]
            #Decrement expectation in next function:
            getattr(q_manager,next_function_name+'_expected').decrement()
            #Increment expectation in current function:
            getattr(q_manager,function_name+'_expected').increment()
            #Delete output from previous attempt files:
            try:
                map(os.remove,glob.glob(options_copy.out_netcdf_file+'.*'))
                os.remove(options_copy.out_netcdf_file)
            except:
                pass
            #Reset output file:
            options_copy.out_netcdf_file=options.out_netcdf_file
            #Resubmit:
            q_manager.put((function_name,options_copy))
        else:
            print function_name+' failed with the following options:',options_copy
            raise
    return

def start_consumer_processes(q_manager,project_drs,options):
    processes_names=consumer_processes_names(q_manager,options)
    processes=dict()
    for process_name in processes_names:
        if process_name!=multiprocessing.current_process().name:
           processes[process_name]=multiprocessing.Process(target=consumer,
                                                           name=process_name,
                                                           args=(q_manager,project_drs,options))
           processes[process_name].start()
        else:
           processes[process_name]=multiprocessing.current_process()
    return processes

def consumer_processes_names(q_manager,options):
    processes_names=[multiprocessing.current_process().name,]
    if (not ( 'serial' in dir(options) and options.serial) and
         ( 'num_procs' in dir(options) and options.num_procs>1) ):
        avdr_types=(len(set(['ask','validate','download_files','reduce_soft_links']).intersection(q_manager.queues_names))>0)
        dr_types=(len(set(['download_opendap','reduce']).intersection(q_manager.queues_names))>0)

        if avdr_types and not dr_types:
            for proc_id in range(options.num_procs-1):
               processes_names.append('avdr_'+str(proc_id))
        elif dr_types and not avdr_types:
            for proc_id in range(options.num_procs-1):
               processes_names.append('dr_'+str(proc_id))
        else:
            for proc_id in range(options.num_procs-1):
                #One third of processes go to avdr:
                if proc_id < (options.num_procs-1) // 3:
                   processes_names.append('avdr_'+str(proc_id))
                else:
                   processes_names.append('dr_'+str(proc_id))
    return processes_names

