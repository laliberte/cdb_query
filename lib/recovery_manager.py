#External:
import netCDF4
import h5py
import copy
import os
import multiprocessing
import datetime
#from StringIO import StringIO
#import sys

#External but related:
import netcdf4_soft_links.netcdf_utils as netcdf_utils
import netcdf4_soft_links.certificates as certificates

def worker_recovery(in_queue,out_queue,semaphores):
    for tuple in iter(in_queue.get,'STOP'):
        result=recovery_func(tuple,semaphores)
        out_queue.put(result,out_queue)
    out_queue.put('STOP')
    return

def recovery_func(tuple,semaphores):
    return tuple[0](*tuple[1:],semaphores=semaphores)

def distributed_recovery(function_handle,database,options,simulations_list,manager=None,semaphores=dict()):
    if manager==None:
        manager=multiprocessing.Manager()

    renewal_time = datetime.datetime.now()
    #Queue for submission:
    submit_queue=manager.Queue()
    #Queue to record:
    record_queue=manager.Queue()

    #Set up the discovery simulation per simulation:
    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']
    for simulation_id,simulation in enumerate(simulations_list_no_fx):
        options_copy=copy.copy(options)
        for desc_id, desc in enumerate(database.drs.simulations_desc):
            setattr(options_copy,desc,simulation[desc_id])
        submit_queue.put((function_handle,copy.copy(database),options_copy))
    
    #Span up to options.num_procs processes and each child process analyzes only one simulation
    if options.num_procs==1:
        submit_queue.put('STOP')
        output_root=netCDF4.Dataset(options.out_netcdf_file,'w',format='NETCDF4')
        for tuple in iter(submit_queue.get,'STOP'):
            renewal_time=record_in_output(recovery_func(tuple),renewal_time,output_root,options)
            output_root.sync()
    else:
        processes=[]
        try:
            processes.extend([multiprocessing.Process(target=worker_recovery, 
                                            args=(submit_queue,record_queue,semaphores)) for proc in range(options.num_procs)])
            for process in processes:
                process.start()
                submit_queue.put('STOP')
            output_root=worker_record(record_queue,renewal_time,options)
        finally:
            for process in processes: process.terminate()
    return output_root

def worker_record(out_queue,renewal_time,options):
    #Open output file:
    output_root=netCDF4.Dataset(options.out_netcdf_file,'w',format='NETCDF4')
    for process in range(options.num_procs):
        for filename in iter(out_queue.get,'STOP'):
            record_in_output(filename,renewal_time,output_root,options)
            output_root.sync()
    return output_root

def record_in_output(filename,renewal_time,output_root,options):
    source_data=netCDF4.Dataset(filename,'r')
    source_data_hdf5=None
    for item in h5py.h5f.get_obj_ids():
        if 'name' in dir(item) and item.name==filename:
            source_data_hdf5=h5py.File(item)
    record_to_file(output_root,source_data,source_data_hdf5)
    source_data.close()
    if source_data_hdf5!=None:
        source_data_hdf5.close()
    try:
        os.remove(filename)
    except OSError:
        pass

    renewal_elapsed_time=datetime.datetime.now() - renewal_time
    if ('username' in dir(options) and 
        options.username!=None and
        options.password!=None and
        renewal_elapsed_time > datetime.timedelta(hours=1)):
        #Reactivate certificates:
        certificates.retrieve_certificates(options.username,options.service,user_pass=options.password)
        renewal_time=datetime.datetime.now()
    return renewal_time

def record_to_file(output_root,output,output_hdf5):
    netcdf_utils.replicate_netcdf_file(output_root,output)
    #netcdf_utils.replicate_full_netcdf_recursive(output_root,output,check_empty=True)
    #netcdf_utils.replicate_full_netcdf_recursive(output_root,output,check_empty=False,hdf5=output_hdf5)
    netcdf_utils.replicate_full_netcdf_recursive(output_root,output,check_empty=True,hdf5=output_hdf5)
    return

#def worker_query(tuple):
#    result=tuple[0](*tuple[1:-1])
#    sys.stdout.flush()
#    sys.stderr.flush()
#    tuple[-1].put(result)
#    return
#
#class MyStringIO(StringIO):
#    def __init__(self, queue, *args, **kwargs):
#        StringIO.__init__(self, *args, **kwargs)
#        self.queue = queue
#    def flush(self):
#        self.queue.put((multiprocessing.current_process().name, self.getvalue()))
#        self.truncate(0)

#def initializer(queue):
#     sys.stderr = sys.stdout = MyStringIO(queue)
