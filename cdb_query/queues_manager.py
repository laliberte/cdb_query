# External:
import netCDF4
import copy
import os
import os.path
import multiprocessing
import multiprocessing.managers as managers
try:
    import Queue as queue
except ImportError:
    import queue
import threading
import numpy as np
import datetime
import glob
import shutil
import tempfile
import requests
import logging

# External but related:
from .netcdf4_soft_links import queues_manager as NC4SL_queues_manager
from .netcdf4_soft_links import (retrieval_manager, requests_sessions)

# Internal:
from . import parsers, commands, commands_parser
from .nc_Database import db_manager, db_utils

_logger = logging.getLogger(__name__)


class SimpleSyncManager(managers.BaseManager):
    '''
    Subclass of `BaseManager'' it includes a subset of the
    shared objects provided by multiprocessing.manager
    '''
    pass


SimpleSyncManager.register('Queue', queue.Queue)
SimpleSyncManager.register('PriorityQueue', queue.PriorityQueue)
SimpleSyncManager.register('Event', threading.Event, managers.EventProxy)
SimpleSyncManager.register('Lock', threading.Lock, managers.AcquirerProxy)
# SimpleSyncManager.register('RLock', threading.RLock, AcquirerProxy)
SimpleSyncManager.register('Semaphore', threading.Semaphore,
                           managers.AcquirerProxy)
# SimpleSyncManager.register('BoundedSemaphore', threading.BoundedSemaphore,
#                            managers.AcquirerProxy)
# SimpleSyncManager.register('Condition', threading.Condition,
#                            managers.ConditionProxy)
# SimpleSyncManager.register('Barrier', threading.Barrier,
#                            managers.BarrierProxy)
# SimpleSyncManager.register('Pool', pool.Pool, managers.PoolProxy)
SimpleSyncManager.register('list', list, managers.ListProxy)
SimpleSyncManager.register('dict', dict, managers.DictProxy)
SimpleSyncManager.register('Value', managers.Value, managers.ValueProxy)
# SimpleSyncManager.register('Array', Array, managers.ArrayProxy)
# SimpleSyncManager.register('Namespace', Namespace, managers.NamespaceProxy)


class ReduceManager(managers.BaseManager):
    pass


class CDB_queues_manager:
    def __init__(self, options):
        # This vanilla manager seems more robust but use the custom manager
        # to have the PriorityQueue
        self.manager = SimpleSyncManager()
        self.manager.start()

        if (hasattr(options, 'serial') and options.serial):
            self.serial = True
        else:
            self.serial = False
        if (hasattr(options, 'num_dl') and options.num_dl):
            self.num_dl = options.num_dl
        else:
            self.num_dl = 1

        if (hasattr(options, 'num_procs') and options.num_procs):
            self.num_procs = options.num_procs
        else:
            self.num_procs = 1

        # Create queues:
        self.queues_names = commands_parser._get_command_names(options)

        # If reduce_soft_links_script is identity, do
        # not pipe results through reduce_soft_links:
        if (hasattr(options, 'reduce_soft_links_script') and
            options.reduce_soft_links_script == '' and
           'reduce_soft_links' in self.queues_names):
            self.queues_names.remove('reduce_soft_links')

        self.dl_nc_kws = dict()
        if hasattr(options, 'download_cache') and options.download_cache:
            self.dl_nc_kws['cache'] = (options.download_cache.split(',')[0])
            if len(options.download_cache.split(',')) > 1:
                td = datetime.timedelta(hours=float(options.download_cache
                                                    .split(',')[1]))
                self.dl_nc_kws['expire_after'] = td

        # Add credentials:
        (self.dl_nc_kws.update({opt: getattr(options, opt) for opt
                                in ['openid', 'username', 'password',
                                    'use_certificates', 'timeout']
                                if hasattr(options, opt)}))

        for queue_name in self.queues_names:
            setattr(self, queue_name, self.manager.PriorityQueue())
            setattr(self, queue_name + '_expected',
                    NC4SL_queues_manager.Shared_Counter(self.manager))

        # Create a shared counter to prevent file collisions:
        self.counter = NC4SL_queues_manager.Shared_Counter(self.manager)

        # Create an event that prevents consumers from terminating:
        self.do_not_keep_consumers_alive = self.manager.Event()

        # Create semaphores for validate:
        if 'validate' in self.queues_names:
            self.validate_semaphores = (NC4SL_queues_manager
                                        .Semaphores_data_node(
                                                self.manager,
                                                num_concurrent=self.num_dl))

        # Create sessions:
        self.sessions = create_sessions(options, q_manager=self)
        if 'ask' in self.queues_names:
            adapter = requests.adapters.HTTPAdapter(pool_connections=20,
                                                    pool_maxsize=5)
            self.sessions['ask'].mount('http://', adapter)
            self.sessions['ask'].mount('https://', adapter)

        if 'validate' in self.queues_names:
            adapter = requests.adapters.HTTPAdapter(pool_connections=20,
                                                    pool_maxsize=self.num_dl)
            self.sessions['validate'].mount('http://', adapter)
            self.sessions['validate'].mount('https://', adapter)

        if len(set(['download_files', 'download_opendap'])
               .intersection(self.queues_names)) > 0:
            # Create queues and semaphores for download:
            self.download = (NC4SL_queues_manager
                             .NC4SL_queues_manager(
                                options,
                                consumer_processes_names(self, options),
                                manager=self.manager,
                                remote_netcdf_kwargs=self.dl_nc_kws))
            self.download_processes = dict()

    def start_download_processes(self):
        if (len(set(['download_files', 'download_opendap'])
                .intersection(self.queues_names)) > 0 and
           not self.serial):
            self.download_processes = (retrieval_manager
                                       .start_download_processes_no_serial(
                                        self.download,
                                        self.num_dl,
                                        self.download_processes,
                                        remote_netcdf_kwargs=self.dl_nc_kws))
        return

    def stop_download_processes(self):
        if (len(set(['download_files', 'download_opendap'])
                .intersection(self.queues_names)) > 0 and
           not self.serial):
            retrieval_manager.stop_download_processes(self.download_processes)
        return

    def set_closed(self):
        self.do_not_keep_consumers_alive.set()
        return

    def increment_expected_and_put(self, options, copyfile=False):
        options_cp = copy.copy(options)
        function_name = commands_parser._get_command_name(options)
        # Put the item in the right queue and give it a number:
        if (hasattr(options_cp, 'in_netcdf_file') and
           options_cp.command_number > 0):
            # Copy input files to prevent garbage from accumulating:
            counter = self.counter.increment()

            (fileno,
             options_cp.in_netcdf_file) = (tempfile
                                           .mkstemp(dir=options_cp.swap_dir,
                                                    suffix='.'+str(counter)))
            # must close file number:
            os.close(fileno)
            if copyfile:
                parsers._copyfile(options, 'in_netcdf_file',
                                  options_cp, 'in_netcdf_file')
        getattr(self, function_name + '_expected').increment()
        getattr(self, function_name).put((options_cp.priority,
                                          self.counter.increment(),
                                          options_cp))
        return

    def put_to_next(self, options, removefile=True):
        options_cp = copy.copy(options)

        # Set to output_file
        options_cp.in_netcdf_file = options.out_netcdf_file

        # Put the item in the next function queue and give it a number:
        next_function_name = commands_parser._get_command_name(options_cp,
                                                               nxt=True)

        # Increment to next function:
        options_cp.command_number += 1
        if options_cp.max_command_number < options_cp.command_number:
            options_cp.max_command_number += 1

        getattr(self, next_function_name).put((options_cp.priority,
                                               self.counter.increment(),
                                               options_cp))

        # Remove temporary input files if not the first function:
        if (removefile and
            hasattr(options, 'in_netcdf_file') and
           options.command_number > 0):
            parsers._remove(options, 'in_netcdf_file')
        return

    def remove(self, options):
        next_function_name = commands_parser._get_command_name(options,
                                                               nxt=True)
        getattr(self, next_function_name+'_expected').decrement()
        if (hasattr(options, 'in_netcdf_file') and
           options.command_number > 0):
            parsers._remove(options, 'in_netcdf_file')
        return

    def get_do_no_record(self):
        return self.get(queues_names=[queue for queue in ['download_opendap']
                                      if queue in self.queues_names])

    def get_reduce_no_record(self):
        return self.get(queues_names=[queue for queue in ['reduce']
                                      if queue in self.queues_names])

    def get_dr_no_record(self):
        return self.get(queues_names=[queue for queue in ['download_opendap',
                                                          'reduce']
                                      if queue in self.queues_names])

    def get_avdr_no_record(self):
        return self.get(queues_names=[queue for queue in ['ask', 'validate',
                                                          'download_files',
                                                          'reduce_soft_links']
                                      if queue in self.queues_names])

    def get_avdrdr_no_record(self):
        return self.get(queues_names=[queue for queue
                                      in ['ask', 'validate', 'download_files',
                                          'reduce_soft_links',
                                          'download_opendap', 'reduce']
                                      if queue in self.queues_names])

    def get_avdrd_no_record(self):
        return self.get(queues_names=[queue for queue
                                      in ['ask', 'validate', 'download_files',
                                          'reduce_soft_links',
                                          'download_opendap']
                                      if queue in self.queues_names])

    def get_limited_record(self):
        return self.get(record=True,
                        queues_names=[queue for queue in self.queues_names
                                      if 'record' in queue.split('_')])

    def get_all_record(self):
        return self.get(record=True, queues_names=self.queues_names)

    def get_all_server_record(self):
        return self.get(record=True,
                        queues_names=['ask', 'validate', 'download_files',
                                      'reduce_soft_links',
                                      'download_opendap'] +
                                     [queue for queue in self.queues_names
                                      if 'record' in queue.split('_')])

    def get(self, record=False, queues_names=[]):
        # Simple get that goes through the queues sequentially
        timeout_first = 0.01
        timeout_subsequent = 0.1
        timeout = timeout_first

        while not (self.do_not_keep_consumers_alive.is_set() and
                   self.expected_queue_size() == 0):
            # Get an element from one queue, starting from the last:
            for queue_name in queues_names[::-1]:
                if queue_name in self.queues_names:
                    if record:
                        # The record worker tries to start download processes
                        # whenever it can:
                        self.start_download_processes()

                    # Record workers can pick from the record queue
                    if not (not record and
                            'record' in queue_name.split('_')):
                        try:
                            return self.get_queue(queue_name, timeout)
                        except queue.Empty:
                            pass
                    # First pass, short timeout. Subsequent pass, longer:
                    if timeout == timeout_first:
                        timeout = timeout_subsequent
        return 'STOP'

    def get_queue(self, queue_name, timeout):
        # Get queue with locks:
        with getattr(self, queue_name + '_expected').lock:
            if getattr(self, queue_name + '_expected').value_no_lock == 0:
                # If nothing is expected, skip:
                raise queue.Empty
            else:
                # Will fail with queue.Empty if the item had not
                # been put in the queue:
                (priority,
                 counter,
                 options) = getattr(self, queue_name).get(True, timeout)
                future_queue_name = (commands_parser
                                     ._get_command_name(options,
                                                        nxt=True))

                future_expected = 0
                if future_queue_name is not None:
                    future_expected = getattr(self, (future_queue_name +
                                                     '_expected')).value
                if ((future_queue_name == 'reduce' and
                     future_expected > 2 * self.num_procs) or
                   (future_expected > 50 * self.num_procs)):
                    getattr(self, queue_name).put((priority, counter, options))
                    # Prevent piling up:
                    raise queue.Empty
                else:
                    # reset priority to 0:
                    options.priority = 0
                    # Increment future actions:
                    if future_queue_name is not None:
                        (getattr(self, future_queue_name + '_expected')
                         .increment())
                    # Decrement current action:
                    getattr(self, queue_name + '_expected').decrement_no_lock()
                    return counter, options

    def expected_queue_size(self):
        return np.max([getattr(self, queue_name+'_expected').value
                       for queue_name in self.queues_names])


def recorder(q_manager, project_drs, options):
    # Start downloads
    q_manager.start_download_processes()
    # The consumers can now terminate:
    q_manager.set_closed()

    _logger.info(multiprocessing.current_process().name + ' with pid ' +
                 str(os.getpid()))
    recorder_queue_consume(q_manager, project_drs, options)
    _logger.info(multiprocessing.current_process().name + ' with pid ' +
                 str(os.getpid()) + ' finished cleanly.')
    return


def recorder_queue_consume(q_manager, project_drs, cproc_options):
    sessions = create_sessions(cproc_options, q_manager=q_manager)

    if (hasattr(cproc_options, 'num_procs') and
       cproc_options.num_procs > 1):
        get_type = getattr(q_manager, 'get_limited_record')
    elif (hasattr(cproc_options, 'start_server') and
          cproc_options.start_server):
        get_type = getattr(q_manager, 'get_all_server_record')
    else:
        get_type = getattr(q_manager, 'get_all_record')

    output = dict()
    try:
        command_names = commands_parser._get_command_names(cproc_options)
        for command_id, command_name in enumerate(command_names):
            if 'record' in command_name.split('_'):
                if command_id == len(command_names)-1:
                    file_mod = ''
                else:
                    file_mod = '.'+'_'.join(command_name.split('_')[1:])
                write_kwargs = {'mode': 'w', 'diskless': True, 'persist': True}
                if (hasattr(cproc_options, 'A') and cproc_options.A):
                    kwargs = {'mode': 'a'}
                else:
                    kwargs = write_kwargs

                file_name = (cproc_options.original_out_netcdf_file +
                             file_mod)
                try:
                    output[command_name] = netCDF4.Dataset(file_name, **kwargs)
                except OSError:
                    # i.e. file might not exists with append
                    # so force overwrite:
                    output[command_name] = netCDF4.Dataset(file_name,
                                                           **write_kwargs)
                output[command_name].set_fill_off()
                database = commands.Database_Manager(project_drs)
                database.load_header(cproc_options)
                db_manager.record_header(output[command_name], database.header)

        for counter, options in iter(get_type, 'STOP'):
            command_name = commands_parser._get_command_name(options)
            if 'record' in command_name.split('_'):
                record_to_netcdf_file(counter, options, output, q_manager,
                                      project_drs)
            else:
                consume_one_item(counter, options, q_manager, project_drs,
                                 cproc_options, sessions=sessions)
    finally:
        # Clean exit:
        for session_name in sessions:
            sessions[session_name].close()
        for record_name in output:
            if output[record_name]._isopen:
                output[record_name].close()
    return


def record_to_netcdf_file(counter, options, output, q_manager, project_drs):
    command_name = commands_parser._get_command_name(options)

    if (counter == 2 and
        q_manager.expected_queue_size() == 0 and
       commands_parser._number_of_commands(options) == 2):
        # Only one function was computed and it is already structured
        # Can simply copy instead of recording:
        out_file_name = output[command_name].filepath()
        output[command_name].close()
        try:
            os.remove(out_file_name)
        except Exception:
            pass
        shutil.move(options.in_netcdf_file, out_file_name)
    elif command_name in output:
        # Only record this function if it was requested::
        _logger.info('Recording: ' + command_name + ', with options: ' +
                     opts_to_str(options))
        db_utils.record_to_netcdf_file_from_file_name(
                                options, options.in_netcdf_file,
                                output[command_name], project_drs)
        _logger.info('DONE Recording: ' + command_name)

    if command_name == commands_parser._get_command_names(options)[-1]:
        # Final recording:
        # Make sure the file is gone:
        for id in range(2):
            try:
                os.remove(options.in_netcdf_file)
            except Exception:
                pass
    else:
        options_cp = copy.copy(options)
        options_cp.out_netcdf_file = options_cp.in_netcdf_file
        q_manager.put_to_next(options_cp, removefile=False)
    return


def opts_to_str(options):
    fields_to_hide = ['password', 'username', 'openid']
    options_cp = copy.copy(options)
    for field in fields_to_hide:
        setattr(options_cp, field, 'secure')
    return str(options_cp)


def consumer(q_manager, project_drs, options):
    _logger.info(multiprocessing.current_process().name + ' with pid ' +
                 str(os.getpid()))
    consumer_queue_consume(q_manager, project_drs, options)
    _logger.info(multiprocessing.current_process().name + ' with pid ' +
                 str(os.getpid()) + ' finished cleanly.')
    return


def consumer_queue_consume(q_manager, project_drs, cproc_options):
    sessions = create_sessions(cproc_options, q_manager=q_manager)
    get_type = getattr(q_manager,
                       'get_' + (multiprocessing.current_process()
                                 .name.split('_')[0]) +
                       '_no_record')
    for counter, options in iter(get_type, 'STOP'):
        consume_one_item(counter, options, q_manager, project_drs,
                         cproc_options, sessions=sessions)
    for session_name in sessions:
        sessions[session_name].close()
    return


def reducer(q_manager, project_drs, options):
    _logger.info(multiprocessing.current_process().name + ' with pid ' +
                 str(os.getpid()))
    reducer_queue_consume(q_manager, project_drs, options)
    _logger.info(multiprocessing.current_process().name + ' with pid ' +
                 str(os.getpid()) + ' finished cleanly.')
    return


def reducer_queue_consume(q_manager, project_drs, cproc_options):
    for counter, options in iter(q_manager.get_reduce_no_record, 'STOP'):
        consume_one_item(counter, options, q_manager, project_drs,
                         cproc_options)
    return


def consume_one_item(counter, options, q_manager, project_drs, cproc_options,
                     sessions=dict()):
    # First copy options:
    options_cp = copy.copy(options)
    options_save = copy.copy(options)

    # Create unique file id:
    (fileno,
     options_cp.out_netcdf_file) = tempfile.mkstemp(dir=options_cp.swap_dir,
                                                    suffix='.'+str(counter))
    # must close file number:
    os.close(fileno)

    # Recursively apply commands:
    database = commands.Database_Manager(project_drs)
    # Run the command:
    function_name = commands_parser._get_command_name(options_cp)
    try:
        if cproc_options.command != 'reduce_server':
            _logger.info('Process: ' + function_name + ', current queues: ' +
                         str([(queue_name, getattr(q_manager,
                                                   queue_name +
                                                   '_expected').value)
                              for queue_name in q_manager.queues_names]) +
                         ', with options: ' + opts_to_str(options_cp))
        else:
            _logger.info('Process: ' + function_name + ', with options: ' +
                         opts_to_str(options_cp))
        getattr(commands, function_name)(database, options_cp,
                                         q_manager=q_manager,
                                         sessions=sessions)
        if cproc_options.command != 'reduce_server':
            _logger.info('DONE Process: ' + function_name +
                         ', current queues: ' +
                         str([(queue_name, getattr(q_manager,
                                                   queue_name +
                                                   '_expected').value)
                              for queue_name in q_manager.queues_names]))
    except Exception as e:
        if str(e).startswith('The kind of user must be selected'):
            raise

        if ((hasattr(cproc_options, 'debug') and
             cproc_options.debug) or
            (hasattr(cproc_options, 'log_files') and
             cproc_options.log_files)):
            _logger.exception(function_name +
                              ' failed with the following options: ' +
                              opts_to_str(options_save))

        if (hasattr(cproc_options, 'debug') and
           cproc_options.debug):
            raise

        # Remove from queue:
        q_manager.remove(options_cp)
        # Delete output from previous attempt files:
        if (hasattr(options_cp, 'original_out_netcdf_file') and
           options_cp.out_netcdf_file != options_cp.original_out_netcdf_file):
            try:
                map(os.remove, glob.glob(options_cp.out_netcdf_file + '.*'))
                os.remove(options_cp.out_netcdf_file)
            except Exception:
                pass

        if options_save.trial > 0:
            # Retry this function.

            # Put it back in the queue, increasing its trial number:
            options_save.trial -= 1

            # Increment expectation in current function and resubmit:
            q_manager.increment_expected_and_put(options_save, copyfile=True)
        elif options_save.failsafe_attempt > 0:
            # Reset branch:
            for field in ['in_netcdf_file', 'out_netcdf_file', 'trial']:
                if hasattr(options_save, 'original_' + field):
                    setattr(options_save, field, getattr(options_save,
                                                         'original_' + field))
                elif hasattr(options_save, field):
                    delattr(options_save, field)

            # Reset to first command:
            options_save.command_number = 0

            # Decrement failsafe attempt:
            options_save.failsafe_attempt -= 1

            # Put back to first function:
            q_manager.increment_expected_and_put(options_save)
        else:
            # If it keeps on failing, ignore this whole branch!
            _logger.exception(function_name +
                              ' failed with the following options: ' +
                              opts_to_str(options_save) +
                              '. Skipping this simulation(s) for good')
    return


def create_sessions(cprocs_options, q_manager=None):
    sessions = dict()
    for command in ['validate', 'ask']:
        if (not hasattr(q_manager, 'queues_names') or
            (hasattr(q_manager, 'queues_names') and
             command in q_manager.queues_names)):
            remote_netcdf_kwargs = dict()
            if (hasattr(cprocs_options, command + '_cache') and
               getattr(cprocs_options, command + '_cache')):
                remote_netcdf_kwargs['cache'] = (getattr(cprocs_options,
                                                         command + '_cache')
                                                 .split(',')[0])
                if len(getattr(cprocs_options,
                               command + '_cache').split(',')) > 1:
                    td = datetime.timedelta(hours=float(
                                                    getattr(cprocs_options,
                                                            command + '_cache')
                                                    .split(',')[1]))
                    remote_netcdf_kwargs['expire_after'] = td
            if not (hasattr(q_manager, 'sessions') and
                    command in getattr(q_manager, 'sessions')):
                # Create if it does not exist:
                sessions[command] = requests_sessions.create_single_session(
                                                        **remote_netcdf_kwargs)
            else:
                sessions[command] = q_manager.sessions[command]
    return sessions


def start_consumer_processes(q_manager, project_drs, options):
    processes_names = consumer_processes_names(q_manager, options)
    processes = dict()
    for process_name in processes_names:
        if process_name != multiprocessing.current_process().name:
            processes[process_name] = multiprocessing.Process(
                                                        target=consumer,
                                                        name=process_name,
                                                        args=(q_manager,
                                                              project_drs,
                                                              options))
            processes[process_name].start()
        else:
            processes[process_name] = multiprocessing.current_process()
    return processes


def consumer_processes_names(q_manager, options):
    processes_names = [multiprocessing.current_process().name]
    if (not (hasattr(options, 'serial') and options.serial) and
       (hasattr(options, 'num_procs') and options.num_procs > 1)):
        if (hasattr(options, 'start_server') and options.start_server):
            for proc_id in range(options.num_procs - 1):
                processes_names.append('avdrd_' + str(proc_id))
        else:
            avdr_types = (len(set(['ask', 'validate', 'download_files',
                                   'reduce_soft_links'])
                              .intersection(q_manager.queues_names)) > 0)
            if avdr_types:
                for proc_id in range(options.num_procs - 1):
                    processes_names.append('avdr_' + str(proc_id))
            do_types = (len(set(['download_opendap'])
                            .intersection(q_manager.queues_names)) > 0)
            if do_types:
                for proc_id in range(options.num_procs-1):
                    processes_names.append('do_' + str(proc_id))

            reduce_types = (len(set(['reduce'])
                                .intersection(q_manager.queues_names)) > 0)
            if reduce_types:
                for proc_id in range(options.num_procs - 1):
                    processes_names.append('reduce_' + str(proc_id))
    return processes_names
