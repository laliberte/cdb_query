#External:
import netCDF4
import h5py
import copy
import os
import os.path
import sys
import multiprocessing
import multiprocessing.managers as managers
import Queue
import threading
import numpy as np
import datetime
import glob
import shutil
import tempfile
from sqlite3 import DatabaseError
import requests
import logging
_logger = logging.getLogger(__name__)

#External but related:
import netcdf4_soft_links.queues_manager as NC4SL_queues_manager
import netcdf4_soft_links.retrieval_manager as retrieval_manager
import netcdf4_soft_links.certificates.certificates as certificates
import netcdf4_soft_links.requests_sessions as requests_sessions

#Internal:
from . import parsers, commands, commands_parser
from .nc_Database import db_manager, db_utils

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

        #Create queues:
        self.queues_names = commands_parser._get_command_names(options)

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

        #Add credentials:
        self.download_remote_netcdf_kwargs.update({opt: getattr(options,opt) for opt in ['openid','username','password','use_certifiates'
                                                                     ] if opt in dir(options)})
        
        for queue_name in self.queues_names:
            setattr(self,queue_name,self.manager.PriorityQueue())
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

    def increment_expected_and_put(self, options, copyfile=False):
        options_copy = copy.copy(options)
        function_name = commands_parser._get_command_name(options)
        #Put the item in the right queue and give it a number:
        if ( 'in_netcdf_file' in dir(options_copy) and
             options_copy.command_number > 0):
            #Copy input files to prevent garbage from accumulating:
            counter = self.counter.increment()

            fileno, options_copy.in_netcdf_file = tempfile.mkstemp(dir=options_copy.swap_dir, suffix='.'+str(counter))
            #must close file number:
            os.close(fileno)
            if copyfile:
                parsers._copyfile(options, 'in_netcdf_file', options_copy, 'in_netcdf_file')
        getattr(self, function_name + '_expected').increment()
        getattr(self, function_name).put((options_copy.priority, self.counter.increment(), options_copy))
        return

    def put_to_next(self, options, removefile=True):
        options_copy = copy.copy(options)

        #Set to output_file
        options_copy.in_netcdf_file = options.out_netcdf_file

        #Put the item in the next function queue and give it a number:
        next_function_name = commands_parser._get_command_name(options_copy, next=True)

        #Increment to next function:
        options_copy.command_number += 1

        getattr(self,next_function_name).put((options_copy.priority, self.counter.increment(), options_copy))

        # Remove temporary input files if not the first function:
        if ( removefile and
             'in_netcdf_file' in dir(options) and
             options.command_number > 0 ):
            parsers._remove(options,'in_netcdf_file')
        return

    def remove(self, options):
        next_function_name = commands_parser._get_command_name(options, next=True)
        getattr(self, next_function_name+'_expected').decrement()
        if ('in_netcdf_file' in dir(options) and
            options.command_number > 0 ):
            parsers._remove(options,'in_netcdf_file')
        return

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
        return self.get(record=True,queues_names=[ queue for queue in self.queues_names if 'record' in queue.split('_')])

    def get_all_record(self):
        return self.get(record=True,queues_names=self.queues_names)

    def get_all_server_record(self):
        return self.get(record=True,queues_names=['ask','validate','download_files','reduce_soft_links','download_opendap']+
                                                 [ queue for queue in self.queues_names if 'record' in queue.split('_')])

    def get(self,record=False,queues_names=[]):
        #Simple get that goes through the queues sequentially
        timeout_first = 0.01
        timeout_subsequent = 0.1
        timeout = timeout_first

        while not (self.do_not_keep_consumers_alive.is_set() and 
                    self.expected_queue_size()==0):
            #Get an element from one queue, starting from the last:
            for queue_name in queues_names[::-1]:
                if queue_name in self.queues_names:
                    if record:
                        #The record worker tries to start download processes whenever it can:
                        self.start_download_processes()

                    #Record workers can pick from the record queue
                    if not ( not record and 
                             'record' in queue_name.split('_') ):
                        try:
                            return self.get_queue(queue_name, timeout)
                        except Queue.Empty:
                            pass
                    #First pass, short timeout. Subsequent pass, longer:
                    if timeout == timeout_first: timeout = timeout_subsequent
        return 'STOP'

    def get_queue(self, queue_name, timeout):
        # Get queue with locks:
        with getattr(self, queue_name+'_expected').lock:
            if  getattr(self, queue_name+'_expected').value_no_lock == 0:
                # If nothing is expected, skip:
                raise Queue.Empty
            else:
                # Will fail with Queue.Empty if the item had not been put in the queue:
                priority, counter, options = getattr(self, queue_name).get(True, timeout)
                future_queue_name = commands_parser._get_command_name(options, next=True)

                if ( future_queue_name is not None and
                     getattr(self, future_queue_name + '_expected').value > 2 * self.num_procs ):
                    getattr(self, queue_name).put((priority, counter, options))
                    # Prevent piling up:
                    raise Queue.Empty
                else:
                    # reset priority to 0:
                    options.priority = 0
                    # Increment future actions:
                    if future_queue_name is not None:
                        getattr(self, future_queue_name + '_expected').increment()
                    # Decrement current action:
                    getattr(self, queue_name + '_expected').decrement_no_lock()
                    return counter, options

    def expected_queue_size(self):
        return np.max([ getattr(self, queue_name+'_expected').value for queue_name in self.queues_names ])

def recorder(q_manager, project_drs, options):
    #Start downloads
    q_manager.start_download_processes()
    #The consumers can now terminate:
    q_manager.set_closed()

    _logger.debug(multiprocessing.current_process().name+' with pid '+str(os.getpid()))
    recorder_queue_consume(q_manager, project_drs, options)
    _logger.debug(multiprocessing.current_process().name+' with pid '+str(os.getpid())+
                  ' finished cleanly.')
    return
        
def recorder_queue_consume(q_manager, project_drs, cproc_options):
    renewal_time = datetime.datetime.now()
    sessions = create_sessions(cproc_options, q_manager=q_manager)

    if ( 'num_procs' in dir(cproc_options) 
         and cproc_options.num_procs > 1):
        get_type = getattr(q_manager, 'get_limited_record')
    elif ('start_server' in dir(cproc_options) 
           and cproc_options.start_server):
        get_type = getattr(q_manager, 'get_all_server_record')
    else:
        get_type = getattr(q_manager, 'get_all_record')

    output = dict()
    try:
        #Output diskless for perfomance. File will be created on closing:
        command_names = commands_parser._get_command_names(cproc_options)
        for command_id, command_name in enumerate(command_names):
            if 'record' in command_name.split('_'):
                if command_id == len(command_names)-1:
                    file_mod = ''
                else:
                    file_mod = '.'+command_name.split('_')[1]
                output[command_name] = netCDF4.Dataset(cproc_options.out_netcdf_file+file_mod, 'w', diskless=True, persist=True)
                output[command_name].set_fill_off()
                database = commands.Database_Manager(project_drs)
                database.load_header(cproc_options)
                db_manager.record_header(output[command_name], database.header)

        for counter, options in iter(get_type,'STOP'):
            command_name = commands_parser._get_command_name(options)
            if 'record' in command_name.split('_'):
                record_to_netcdf_file(counter, options, output, q_manager, project_drs)
            else:
                consume_one_item(counter, options, q_manager, project_drs, cproc_options, sessions=sessions)

            if ('username' in dir(cproc_options) and 
                cproc_options.username != None and
                cproc_options.password != None and
                cproc_options.use_certificates and
                datetime.datetime.now() - renewal_time > datetime.timedelta(hours=1)):
                #Reactivate certificates every hours:
                certificates.retrieve_certificates(cproc_options.username,
                                                   'ceda',
                                                   user_pass=cproc_options.password,
                                                   timeout=cproc_options.timeout)
                renewal_time=datetime.datetime.now()
    finally:
        #Clean exit:
        for session_name in sessions.keys():
            sessions[session_name].close()
        for record_name in output.keys():
            if output[record_name]._isopen:
                output[record_name].close()
    return

def record_to_netcdf_file(counter, options, output, q_manager, project_drs):
    command_name = commands_parser._get_command_name(options)

    if ( counter == 2 and 
         q_manager.expected_queue_size() == 0 and
         commands_parser._number_of_commands(options) == 2 ):
        #Only one function was computed and it is already structured
        #Can simply copy instead of recording:
        out_file_name = output[command_name].filepath()
        output[command_name].close()
        try:
            os.remove(out_file_name)
        except Exception:
            pass
        shutil.move(options.in_netcdf_file, out_file_name)
    elif command_name in output.keys():
        #Only record this function if it was requested:
        #import subprocess; subprocess.Popen('ncdump -v path '+temp_file_name,shell=True)
        _logger.debug('Recording: '+command_name+', with options: '+str(options))
        db_utils.record_to_netcdf_file_from_file_name(options, options.in_netcdf_file, output[command_name], project_drs)
        _logger.debug('DONE Recording: '+command_name)

    if command_name == commands_parser._get_command_names(options)[-1]:
        # Final recording:
        # Make sure the file is gone:
        for id in range(2):
            try:
                os.remove(options.in_netcdf_file)
            except Exception:
                pass
    else:
        options_copy = copy.copy(options)
        options_copy.out_netcdf_file = options_copy.in_netcdf_file 
        q_manager.put_to_next(options_copy, removefile=False)
    return

def consumer(q_manager,project_drs,options):
    _logger.debug(multiprocessing.current_process().name+' with pid '+str(os.getpid()))
    consumer_queue_consume(q_manager,project_drs,options)
    _logger.debug(multiprocessing.current_process().name+' with pid '+str(os.getpid())+
                  ' finished cleanly.')
    return

def consumer_queue_consume(q_manager,project_drs,cproc_options):
    sessions=create_sessions(cproc_options,q_manager=q_manager)
    get_type=getattr(q_manager, 'get_'+multiprocessing.current_process().name.split('_')[0]+'_no_record')
    for counter, options in iter(get_type, 'STOP'):
        consume_one_item(counter, options, q_manager, project_drs, cproc_options, sessions=sessions)
    for session_name in sessions.keys():
        sessions[session_name].close()
    return

def reducer(q_manager, project_drs, options):
    _logger.debug(multiprocessing.current_process().name+' with pid '+str(os.getpid()))
    reducer_queue_consume(q_manager, project_drs, options)
    _logger.debug(multiprocessing.current_process().name+' with pid '+str(os.getpid())+
                  ' finished cleanly.')
    return

def reducer_queue_consume(q_manager, project_drs, cproc_options):
    for counter, options in iter(q_manager.get_reduce_no_record, 'STOP'):
        consume_one_item(counter, options, q_manager, project_drs, cproc_options)
    return

def consume_one_item(counter, options, q_manager, project_drs, cproc_options, sessions=dict()):
    #First copy options:
    options_copy=copy.copy(options)
    options_save=copy.copy(options)

    #Create unique file id:
    fileno, options_copy.out_netcdf_file = tempfile.mkstemp(dir=options_copy.swap_dir,suffix='.'+str(counter))
    #must close file number:
    os.close(fileno)

    #Recursively apply commands:
    database = commands.Database_Manager(project_drs)
    #Run the command:
    function_name = commands_parser._get_command_name(options_copy)
    try:
        if not cproc_options.command == 'reduce_server':
            _logger.debug('Process: '+
                        function_name+
                        ', current queues: '+
                        str([ (queue_name, getattr(q_manager, queue_name+'_expected').value) 
                              for queue_name in q_manager.queues_names])+
                        ', with options: '+
                        str(options_copy))
        else:
            _logger.debug('Process: '+
                        function_name+
                        ', with options: '+
                        str(options_copy))
        getattr(commands, function_name)(database, options_copy, q_manager=q_manager, sessions=sessions)
        if not cproc_options.command == 'reduce_server':
            _logger.debug('DONE Process: '+
                        function_name+
                        ', current queues: '+
                        str([ (queue_name,getattr(q_manager,queue_name+'_expected').value)
                              for queue_name in q_manager.queues_names ])
                            )
    except Exception as e:
        if str(e).startswith('The kind of user must be selected'):
            raise

        if ( 'debug' in dir(cproc_options) and
             cproc_options.debug ):
            _logger.error(function_name+
                          ' failed with the following options: '+
                          str(options_save))
            raise

        if options.trial > 0:
            #Retry this function.

            #Decrement expectation in next function:
            q_manager.remove(options_copy)
            #Delete output from previous attempt files:
            try:
                map(os.remove, glob.glob(options_save.out_netcdf_file+'.*'))
                os.remove(options_save.out_netcdf_file)
            except Exception:
                pass

            #Put it back in the queue, increasing its trial number:
            options_save.trial -= 1
            #Increment expectation in current function and resubmit:
            q_manager.increment_expected_and_put(options_save, copyfile=True)
        elif options.failsafe_attempt > 0:

            #Reset branch:
            for field in ['in_netcdf_file','out_netcdf_file','trial']:
                if 'original_'+field in dir(options_save):
                    setattr(options_save,field,getattr(options_save,'original_'+field))
                elif field in dir(options_save):
                    delattr(options_save, field)

            #if ('record_validate' in dir(options_save) and
            #    'record_validate' in q_manager.queues_names and
            #     q_manager.queues_names.index(function_name) > q_manager.queues_names.index('record_validate')):
            #    #If validate was already recorded:
            #    options_save.record_validate = False

            q_manager.remove(options_copy)
            #Delete output from previous attempt files:
            try:
                map(os.remove, glob.glob(options_save.out_netcdf_file+'.*'))
                os.remove(options_save.out_netcdf_file)
            except Exception:
                pass
            
            #Decrement failsafe attempt:
            options_save.failsafe_attempt -= 1

            #Reset to first command:
            options_save.command_number = 0

            #Put back to first function:
            q_manager.increment_expected_and_put(options_save)
        else:
            #If it keeps on failing, ignore this whole branch!
            logging.error(function_name + 
                          ' failed with the following options: ' +
                          str(options_save))
    return


def create_sessions(cprocs_options,q_manager=None):
    sessions=dict()
    for type in ['validate','ask']:
        if ( not 'queues_names' in dir(q_manager) or
            ( 'queues_names' in dir(q_manager) and
              type in q_manager.queues_names ) ):
            remote_netcdf_kwargs=dict()
            if type+'_cache' in dir(cprocs_options) and getattr(cprocs_options,type+'_cache'):
                remote_netcdf_kwargs['cache']=getattr(cprocs_options,type+'_cache').split(',')[0]
                if len(getattr(cprocs_options,type+'_cache').split(','))>1:
                    remote_netcdf_kwargs['expire_after']=datetime.timedelta(hours=float(getattr(cprocs_options,type+'_cache').split(',')[1]))
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
            do_types=(len(set(['download_opendap']).intersection(q_manager.queues_names))>0)
            if do_types:
                for proc_id in range(options.num_procs-1):
                   processes_names.append('do_'+str(proc_id))

            reduce_types=(len(set(['reduce']).intersection(q_manager.queues_names))>0)
            if reduce_types:
                for proc_id in range(options.num_procs-1):
                   processes_names.append('reduce_'+str(proc_id))

    return processes_names
