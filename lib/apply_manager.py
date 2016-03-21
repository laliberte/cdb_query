#External:
import netCDF4
import h5py
import copy
import os
import multiprocessing

#Internal:
import nc_Database_utils

def worker_apply(in_queue,out_queue):
    for tuple in iter(in_queue.get,'STOP'):
        result=apply_func(tuple)
        out_queue.put(result,out_queue)
    out_queue.put('STOP')
    return

def apply_func(tuple):
    return tuple[0](*tuple[1:])

def distributed_apply(function_handle,database,options,vars_list,args=tuple(),manager=None):
    if manager==None:
        manager=multiprocessing.Manager()
    #This is the gathering queue:
    record_queue=manager.Queue()
    #This is the apply queue:
    apply_queue=manager.Queue()

    if 'download' in dir(options) and options.download:
        #This starts a process to handle the recovery:
        recovery_queue=manager.Queue()
        retrieved_file_list=manager.list()
        recovery_process=multiprocessing.Process(target=worker_recovery, 
                                        name=worker_recovery,
                                        args=(recovery_queue, database.queues, retrieved_file_list))
        recovery_process.start()
    else:
        retrieved_file_list=[]
        database.queues=dict()
        recovery_queue=None

    #Set up the discovery var per var:
    for var_id,var in enumerate(vars_list):
        options_copy=copy.copy(options)
        for opt_id, opt in enumerate(database.drs.official_drs_no_version):
            if var[opt_id]!=None:
                setattr(options_copy,opt,var[opt_id])
        apply_queue.put((function_handle,copy.copy(database),retrieved_file_list,recovery_queue,options_copy)+args)
    apply_queue.put('STOP')
    
    #Span up to options.num_procs processes and each child process analyzes only one simulation
    if options.num_procs==1:
        output_root=netCDF4.Dataset(options.out_netcdf_file,'w',format='NETCDF4')
        for tuple in iter(submit_queue.get,'STOP'):
            record_in_output(apply_func(tuple),output_root,project_drs,options)
            output_root.sync()
    else:
        try:
            processes=[multiprocessing.Process(target=worker_apply, 
                                            args=(submit_queue,record_queue)) for proc in range(options.num_procs)]
            for process in processes: process.start()
            output_root=worker_record(record_queue,database.drs,options)
        finally:
            for process in processes: process.terminate()
            if 'terminate' in dir(recovery_process): recover_process.terminate()

    if 'download' in dir(options) and options.download:
        recovery_queue.put('STOP')
    return output_root

def worker_record(out_queue,project_drs,options):
    #Open output file:
    output_root=netCDF4.Dataset(options.out_netcdf_file,'w',format='NETCDF4')
    for description in iter(out_queue.get,'STOP'):
        record_in_output(description,output_root,project_drs,options)
        output_root.sync()
    return output_root

def record_in_output(description,output_root,project_drs,options):
    temp_file_name=description[0]
    var=description[1]
    data=netCDF4.Dataset(temp_file_name,'r')
    data_hdf5=None
    for item in h5py.h5f.get_obj_ids():
        if 'name' in dir(item) and item.name==temp_file_name:
            data_hdf5=h5py.File(item)
    tree=zip(project_drs.official_drs_no_version,var)

    if ('applying_to_soft_links' in dir(options) and
        options.applying_to_soft_links):
        #Do not check empty:
        nc_Database_utils.replace_netcdf_variable_recursive(output_root,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=False)
    else:
        nc_Database_utils.replace_netcdf_variable_recursive(output_root,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=True)
        data.close()

    if data_hdf5!=None:
        data_hdf5.close()
    try:
        os.remove(temp_file_name)
    except OSError:
        pass
    return
