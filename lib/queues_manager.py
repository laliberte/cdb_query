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
import shutil
from sqlite3 import DatabaseError
import requests

#Internal:
import cdb_query_archive_class
import nc_Database
import nc_Database_utils
import netcdf4_soft_links.queues_manager as NC4SL_queues_manager
import netcdf4_soft_links.retrieval_manager as retrieval_manager
import netcdf4_soft_links.certificates as certificates
import netcdf4_soft_links.requests_sessions as requests_sessions

class SimpleSyncManager(managers.BaseManager):
    '''
    Subclass of `BaseManager'' it includes a subset of the 
    shared objects provided by multiprocessing.manager
    '''
    pass
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

class ReduceManager(managers.BaseManager):
    pass

class CDB_queues_manager:
    def __init__(self,options):
        #This vanilla manager seems more robust bu use the custom manager to have the PriorityQueue
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

        if ('num_procs' in dir(options) and options.num_procs):
            self.num_procs=options.num_procs
        else:
            self.num_procs=1

        #for name in ['ask','validate','time_split']:
        #authorized_functions=['ask','validate',
        authorized_functions=['ask','validate','record_validate',
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

        self.download_remote_netcdf_kwargs=dict()
        if 'download_cache' in dir(options) and options.download_cache:
            self.download_remote_netcdf_kwargs['cache']=options.download_cache.split(',')[0]
            if len(options.download_cache.split(','))>1:
                self.download_remote_netcdf_kwargs['expire_after']=datetime.timedelta(hours=float(options.download_cache.split(',')[1]))
        
        for queue_name in self.queues_names:
            setattr(self,queue_name,self.manager.PriorityQueue())
            #setattr(self,queue_name,self.manager.Queue())
            setattr(self,queue_name+'_expected',NC4SL_queues_manager.Shared_Counter(self.manager))
        
        #Create a shared counter to prevent file collisions:
        self.counter=NC4SL_queues_manager.Shared_Counter(self.manager)

        #Create an event that prevents consumers from terminating:
        self.do_not_keep_consumers_alive=self.manager.Event()

        #Create semaphores for validate:
        if 'validate' in self.queues_names:
            self.validate_semaphores=NC4SL_queues_manager.Semaphores_data_node(self.manager,num_concurrent=self.num_dl)

        #Create sessions:
        self.sessions=create_sessions(options,q_manager=self)
        if 'ask' in self.queues_names:
            adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=5)
            self.sessions['ask'].mount('http://',adapter)
            self.sessions['ask'].mount('https://',adapter)

        if 'validate' in self.queues_names:
            adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=self.num_dl)
            self.sessions['validate'].mount('http://',adapter)
            self.sessions['validate'].mount('https://',adapter)

        if len(set(['download_files','download_opendap']).intersection(self.queues_names))>0:
            #Create queues and semaphores for download:
            self.download=NC4SL_queues_manager.NC4SL_queues_manager(options,consumer_processes_names(self,options),
                                                                     manager=self.manager,
                                                                     remote_netcdf_kwargs=self.download_remote_netcdf_kwargs)
            self.download_processes=dict()

    def start_download_processes(self):
        if (len(set(['download_files','download_opendap']).intersection(self.queues_names))>0
            and not self.serial):
            self.download_processes=retrieval_manager.start_download_processes_no_serial(
                                                            self.download,
                                                            self.num_dl,
                                                            self.download_processes,
                                                            remote_netcdf_kwargs=self.download_remote_netcdf_kwargs)
        return

    def stop_download_processes(self):
        if (len(set(['download_files','download_opendap']).intersection(self.queues_names))>0
            and not self.serial):
            for proc_name in self.download_processes.keys(): self.download_processes[proc_name].terminate()
        return
                
    def set_closed(self):
        self.do_not_keep_consumers_alive.set()
        return

    def increment_expected_and_put(self,item):
        #Put the item in the right queue and give it a number:
        if ('in_netcdf_file' in dir(item[-1]) and
            self.queues_names.index(item[0])>0):
            #Copy input files to prevent garbage from accumulating:
            counter=self.counter.increment()
            item[-1].in_netcdf_file+='.'+str(counter)
            new_file_name=item[-1].in_netcdf_file
        else:
            new_file_name=''
        getattr(self,item[0]+'_expected').increment()
        getattr(self,item[0]).put((item[-1].priority,(self.counter.increment(),)+item))
        return new_file_name

    def put_to_next(self,item):
        #Put the item in the next function queue and give it a number:
        next_function_name=self.queues_names[self.queues_names.index(item[0])+1]
        getattr(self,next_function_name).put((item[-1].priority,(self.counter.increment(),next_function_name)+item[1:]))
        if ('in_netcdf_file' in dir(item[-1]) and
            self.queues_names.index(item[0])>0):
            return True
        else:
            return False

    def remove(self,item):
        next_function_name=self.queues_names[self.queues_names.index(item[0])+1]
        getattr(self,next_function_name+'_expected').decrement()
        if ('in_netcdf_file' in dir(item[-1]) and
            self.queues_names.index(item[0])>0):
            return True
        else:
            return False

    def get_do_no_record(self):
        return self.get(queues_names=[queue for queue in ['download_opendap']
                                        if queue in self.queues_names])

    def get_reduce_no_record(self):
        return self.get(queues_names=[queue for queue in ['reduce']
                                        if queue in self.queues_names])

    def get_dr_no_record(self):
        return self.get(queues_names=[queue for queue in ['download_opendap','reduce']
                                        if queue in self.queues_names])

    def get_avdr_no_record(self):
        return self.get(queues_names=[queue for queue in ['ask','validate','download_files','reduce_soft_links']
                                        if queue in self.queues_names])
    def get_avdrdr_no_record(self):
        return self.get(queues_names=[queue for queue in ['ask','validate','download_files','reduce_soft_links','download_opendap','reduce']
                                        if queue in self.queues_names])

    def get_avdrd_no_record(self):
        return self.get(queues_names=[queue for queue in ['ask','validate','download_files','reduce_soft_links','download_opendap']
                                        if queue in self.queues_names])

    def get_limited_record(self):
        return self.get(record=True,queues_names=['record_validate','record'])

    def get_all_record(self):
        return self.get(record=True,queues_names=self.queues_names)

    def get_all_server_record(self):
        return self.get(record=True,queues_names=['ask','validate','record_validate','download_files','reduce_soft_links','download_opendap','record'])
        #return self.get(record=True,queues_names=['ask','validate','download_files','reduce_soft_links','download_opendap','record'])

    def get(self,record=False,queues_names=[]):
        #Simple get that goes through the queues sequentially
        timeout_first=0.01
        timeout_subsequent=0.1
        timeout=timeout_first

        while not (self.do_not_keep_consumers_alive.is_set() and 
                    self.expected_queue_size(queues_names)==0):
                    #and
                    #getattr(self,'record_expected').value==getattr(self,'record').qsize()):
            #Get an element from one queue, starting from the last:
            for queue_name in queues_names[::-1]:
                if queue_name in self.queues_names:
                    if record:
                        #if getattr(self,'record_expected').value==getattr(self,queue_name).qsize():
                        #    #When all that is left to do is record, terminate download processes:
                        #    self.stop_download_processes()
                        #else:
                        #The record worker tries to start download processes whenever it can:
                        self.start_download_processes()

                    #Record workers can pick from the record queue
                    if not (not record and 'record' in queue_name.split('_')):
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
            if  ( ( getattr(self,queue_name+'_expected').value_no_lock == 0) or
                  (queue_name == 'download_opendap' and
                   'reduce' in self.queues_names and
                   getattr(self,'reduce_expected').value>2*self.num_procs)):
                #If nothing is expected or there is already downloaded to
                #feed reduce, skip!
                raise Queue.Empty
            else:
                #Will fail with Queue.Empty if the item had not been put in the queue:
                item=getattr(self,queue_name).get(True,timeout)[1]
                #reset priority to 0:
                item[-1].priority=0
                #Increment future actions:
                if 'record'!=item[1]:
                    next_queue_name=self.queues_names[self.queues_names.index(item[1])+1]
                    getattr(self,next_queue_name+'_expected').increment()
                #Decrement current action:
                getattr(self,queue_name+'_expected').decrement_no_lock()
                return item

    def expected_queue_size(self,restricted_queues_names):
        return np.max([getattr(self,queue_name+'_expected').value for queue_name in self.queues_names])

def recorder(q_manager,project_drs,options):
    #Start downloads
    q_manager.start_download_processes()
    #The consumers can now terminate:
    q_manager.set_closed()


    if ( 'log_files' in options and options.log_files ):
        log_file_name=multiprocessing.current_process().name + ".out"
        with open(log_file_name, "w") as logger:
            sys.stdout=logger
            old_stdout=sys.stdout
            old_stdout.flush()
            try:
                sys.stdout=logger
                print(multiprocessing.current_process().name+' with pid '+str(os.getpid()))
                recorder_queue_consume(q_manager,project_drs,options)
            finally:
                sys.stdout.flush()
                sys.stdout=old_stdout
    else:
        recorder_queue_consume(q_manager,project_drs,options)
    return
        
def recorder_queue_consume(q_manager,project_drs,original_options):
    renewal_time=datetime.datetime.now()
    sessions=create_sessions(original_options,q_manager=q_manager)

    if original_options.num_procs>1:
        #get_type=getattr(q_manager,'get_all_record')
        get_type=getattr(q_manager,'get_limited_record')
    elif ('start_server' in dir(original_options) and original_options.start_server):
        get_type=getattr(q_manager,'get_all_server_record')
    else:
        get_type=getattr(q_manager,'get_all_record')

    output=dict()
    try:
        #Output diskless for perfomance. File will be created on closing:
        output['record']=netCDF4.Dataset(original_options.out_netcdf_file,'w',diskless=True,persist=True)
        output['record'].set_fill_off()
        database=cdb_query_archive_class.Database_Manager(project_drs)
        database.load_header(original_options)
        nc_Database.record_header(output['record'],database.header)

        if 'record_validate' in q_manager.queues_names:
            #Output diskless for perfomance. File will be created on closing:
            output['record_validate']=netCDF4.Dataset(original_options.out_netcdf_file+'.validate','w',diskless=True,persist=True)
            output['record_validate'].set_fill_off()
            database=cdb_query_archive_class.Database_Manager(project_drs)
            database.load_header(original_options)
            nc_Database.record_header(output['record_validate'],database.header)

        for item in iter(get_type,'STOP'):
            if not 'record' in item[1].split('_'):
                consume_one_item(item[0],item[1],item[2],q_manager,project_drs,original_options,sessions=sessions)
            else:
                record_to_netcdf_file(item[0],item[1],item[2],output,q_manager,project_drs)

            if ('username' in dir(original_options) and 
                original_options.username!=None and
                original_options.password!=None and
                datetime.datetime.now() - renewal_time > datetime.timedelta(hours=1)):
                #Reactivate certificates every hours:
                certificates.retrieve_certificates(original_options.username,
                                                   original_options.service,
                                                   user_pass=original_options.password,
                                                   timeout=original_options.timeout)
                renewal_time=datetime.datetime.now()
    finally:
        #Clean exit:
        for session_name in sessions.keys():
            sessions[session_name].close()
        for record_name in output.keys():
            output[record_name].close()
    return

def record_to_netcdf_file(counter,function_name,options,output,q_manager,project_drs):

    temp_file_name=options.in_netcdf_file
    if ((function_name in dir(options) and
        getattr(options,function_name) and
        function_name in output.keys()) or
        function_name=='record'):
        #Only record this function if it was requested:
        #import subprocess; subprocess.Popen('ncdump -v path '+temp_file_name,shell=True)
        if ( 'log_files' in options and options.log_files ):
            print('Recording: '+function_name,datetime.datetime.now(),options)
        #with netCDF4.Dataset(record_file_name,'a') as output:
        nc_Database_utils.record_to_netcdf_file_from_file_name(options,temp_file_name,output[function_name],project_drs)
        if ( 'log_files' in options and options.log_files ):
            print('DONE Recording: '+function_name,datetime.datetime.now(),options)

    if len(function_name.split('_'))>1:
        options_copy=copy.copy(options)
        options_copy.out_netcdf_file+='.'+str(counter)
        q_manager.put_to_next((function_name,options_copy))
    else:
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
                print(multiprocessing.current_process().name+' with pid '+str(os.getpid()))
                consumer_queue_consume(q_manager,project_drs,options)
            finally:
                sys.stdout.flush()
                sys.stdout=old_stdout
    else:
        consumer_queue_consume(q_manager,project_drs,options)
    return

def consumer_queue_consume(q_manager,project_drs,original_options):
    sessions=create_sessions(original_options,q_manager=q_manager)
    get_type=getattr(q_manager,'get_'+multiprocessing.current_process().name.split('_')[0]+'_no_record')
    for item in iter(get_type,'STOP'):
        consume_one_item(item[0],item[1],item[2],q_manager,project_drs,original_options,sessions=sessions)
    for session_name in sessions.keys():
        sessions[session_name].close()
    return

def reducer(q_manager,project_drs,options):
    if ( 'log_files' in options and options.log_files ):
        log_file_name=multiprocessing.current_process().name + ".out"
        with open(log_file_name, "w") as logger:
            old_stdout=sys.stdout
            old_stdout.flush()
            try:
                sys.stdout=logger
                print(multiprocessing.current_process().name+' with pid '+str(os.getpid()))
                reducer_queue_consume(q_manager,project_drs,original_options)
            finally:
                sys.stdout.flush()
                sys.stdout=old_stdout
    else:
        reducer_queue_consume(q_manager,project_drs,original_options)
    return

def reducer_queue_consume(q_manager,project_drs,original_options):
    for item in iter(q_manager.get_reduce_no_record,'STOP'):
        consume_one_item(item[0],item[1],item[2],q_manager,project_drs,original_options)
    return

def consume_one_item(counter,function_name,options,q_manager,project_drs,original_options,sessions=dict()):
    #First copy options:
    options_copy=copy.copy(options)
    options_save=copy.copy(options)

    #Create unique file id:
    options_copy.out_netcdf_file+='.'+str(counter)

    #Recursively apply commands:
    database=cdb_query_archive_class.Database_Manager(project_drs)
    #Run the command:
    try:
        if ( 'log_files' in options and options.log_files ):
            print('Process: ',datetime.datetime.now(),function_name,[(queue_name,getattr(q_manager,queue_name+'_expected').value) for queue_name in q_manager.queues_names],options_copy)
        getattr(cdb_query_archive_class,function_name)(database,options_copy,q_manager=q_manager,sessions=sessions)
        if ( 'log_files' in options and options.log_files ):
            print('DONE Process: ',datetime.datetime.now(),function_name,[(queue_name,getattr(q_manager,queue_name+'_expected').value) for queue_name in q_manager.queues_names],options_copy)
    except:
        if options.trial<options.max_trial:
            #Decrement expectation in next function:
            q_manager.remove((function_name,options_copy))
            #Delete output from previous attempt files:
            try:
                map(os.remove,glob.glob(options_save.out_netcdf_file+'.*'))
                os.remove(options_save.out_netcdf_file)
            except:
                pass

            #Put it back in the queue, increasing its trial number:
            options_save.trial+=1
            #Increment expectation in current function and resubmit:
            new_file_name=q_manager.increment_expected_and_put((function_name,options_save))
            if new_file_name!='':
                shutil.copyfile(options.in_netcdf_file,new_file_name)
        else:
            print(function_name+' failed with the following options:',options_save)
            if ('not_failsafe' in dir(original_options) and
                original_options.not_failsafe):
                raise
            options_save.trial=0
            if 'in_netcdf_file' in dir(original_options):
                options_save.in_netcdf_file=original_options.in_netcdf_file
            elif 'in_netcdf_file' in dir(options_save):
                del options_save.in_netcdf_file
            options_save.out_netcdf_ile=original_options.out_netcdf_file
            if ('record_validate' in dir(options_save) and
                'record_validate' in q_manager.queues_names and
                 q_manager.queues_names.index(function_name) > q_manager.queues_names.index('record_validate')):
                #If validate was already recorded:
                options_save.record_validate=False

            q_manager.remove((function_name,options_copy))
            #Delete output from previous attempt files:
            try:
                map(os.remove,glob.glob(options_save.out_netcdf_file+'.*'))
                os.remove(options_save.out_netcdf_file)
            except:
                pass

            #Put back to first function:
            new_file_name=q_manager.increment_expected_and_put((q_manager.queues_names[0],options_save))
    return

def create_sessions(original_options,q_manager=None):
    sessions=dict()
    for type in ['validate','ask']:
        if ( not 'queues_names' in dir(q_manager) or
            ( 'queues_names' in dir(q_manager) and
              type in q_manager.queues_names ) ):
            remote_netcdf_kwargs=dict()
            if type+'_cache' in dir(original_options) and getattr(original_options,type+'_cache'):
                remote_netcdf_kwargs['cache']=getattr(original_options,type+'_cache').split(',')[0]
                if len(getattr(original_options,type+'_cache').split(','))>1:
                    remote_netcdf_kwargs['expire_after']=datetime.timedelta(hours=float(getattr(original_options,type+'_cache').split(',')[1]))
            if not ('sessions' in dir(q_manager) and
                    type in getattr(q_manager,'sessions').keys()):
                #Create if it does not exist:
                sessions[type]=requests_sessions.create_single_session(**remote_netcdf_kwargs)
            else:
                sessions[type]=q_manager.sessions[type]
    return sessions

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
        if ('start_server' in dir(options) and options.start_server):
            for proc_id in range(options.num_procs-1):
               processes_names.append('avdrd_'+str(proc_id))
        else:
            avdr_types=(len(set(['ask','validate','download_files','reduce_soft_links']).intersection(q_manager.queues_names))>0)
            if avdr_types:
                for proc_id in range(options.num_procs-1):
                   processes_names.append('avdr_'+str(proc_id))
            #dr_types=(len(set(['download_opendap','reduce']).intersection(q_manager.queues_names))>0)
            #if dr_types:
            #    for proc_id in range(options.num_procs-1):
            #       processes_names.append('dr_'+str(proc_id))
            do_types=(len(set(['download_opendap']).intersection(q_manager.queues_names))>0)
            if do_types:
                for proc_id in range(options.num_procs-1):
                   processes_names.append('do_'+str(proc_id))

            reduce_types=(len(set(['reduce']).intersection(q_manager.queues_names))>0)
            if reduce_types:
                for proc_id in range(options.num_procs-1):
                   processes_names.append('reduce_'+str(proc_id))

    return processes_names
