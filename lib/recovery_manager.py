#External:
import netCDF4
import h5py
import copy
import os
import multiprocessing
import datetime
from StringIO import StringIO
import sys

#External but related:
import netcdf4_soft_links.netcdf_utils as netcdf_utils
import netcdf4_soft_links.certificates as certificates

def distributed_recovery(function_handle,database,options,simulations_list,manager,args=tuple()):
    renewal_time = datetime.datetime.now()
    #Open output file:
    output_file_name=options.out_netcdf_file
    output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')

    simulations_list_no_fx=[simulation for simulation in simulations_list if 
                                simulation[database.drs.simulations_desc.index('ensemble')]!='r0i0p0']

    queue_result=manager.Queue()
    #queue_output=manager.Queue()
    #Set up the discovery simulation per simulation:
    args_list=[]
    for simulation_id,simulation in enumerate(simulations_list_no_fx):
        options_copy=copy.copy(options)
        for desc_id, desc in enumerate(database.drs.simulations_desc):
            setattr(options_copy,desc,simulation[desc_id])
        args_list.append((function_handle,copy.copy(database),options_copy)+args+(queue_result,))
    
    #Span up to options.num_procs processes and each child process analyzes only one simulation
    #pool=multiprocessing.Pool(processes=options.num_procs,initializer=initializer,initargs=[queue_output],maxtasksperchild=1)
    if options.num_procs>1:
        pool=multiprocessing.Pool(processes=options.num_procs,maxtasksperchild=1)

    #try:
    if options.num_procs>1:
        #result=pool.map_async(worker_query,args_list,chunksize=1)
        result=pool.map(worker_query,args_list,chunksize=1)
    for arg in args_list:
        renewal_time=record_in_output(renewal_time,arg,queue_result,output_root,database,options)
    #finally:
    if options.num_procs>1:
        pool.terminate()
        pool.join()
    return output_root

def record_in_output(renewal_time,arg,queue,output_root,database,options):
    if options.num_procs==1:
        result=worker_query(arg)
    filename=queue.get(1e20)
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
    output_root.sync()

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

def worker_query(tuple):
    result=tuple[0](*tuple[1:-1])
    sys.stdout.flush()
    sys.stderr.flush()
    tuple[-1].put(result)
    return

class MyStringIO(StringIO):
    def __init__(self, queue, *args, **kwargs):
        StringIO.__init__(self, *args, **kwargs)
        self.queue = queue
    def flush(self):
        self.queue.put((multiprocessing.current_process().name, self.getvalue()))
        self.truncate(0)

def initializer(queue):
     sys.stderr = sys.stdout = MyStringIO(queue)
