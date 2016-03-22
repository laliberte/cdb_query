#External:
import netCDF4
import h5py
import copy
import os
import multiprocessing

#Internal:
import nc_Database_utils

def worker_apply(function_handle,in_queue,out_queue,recovered_file_list,recovery_queue):
    for tuple in iter(in_queue.get,'STOP'):
        result=apply_func(function_handle,tuple,recovered_file_list=recovered_file_list,recovery_queue=recovery_queue)
        out_queue.put(result,out_queue)
    out_queue.put('STOP')
    return

def apply_func(function_handle,tuple,recovered_file_list=[],recovery_queue=None):
    return function_handle(*tuple,recovered_file_list=recovered_file_list,recovery_queue=recovery_queue)

def worker_recovery(recovery_function_handle, input_queue, retrieval_queues, output_list):
    for tuple in iter(input_queue.get, 'STOP'):
        recovery_function_handle(*tuple,check_empty=True,queues=retrieval_queues)
        output_list.append(tuple[0])
    return

def distributed_apply(function_handle,recovery_function_handle,project_drs,options,vars_list,manager=None,retrieval_queues=dict()):
    if manager==None:
        manager=multiprocessing.Manager()
    #This is the gathering queue:
    record_queue=manager.Queue()
    #This is the apply queue:
    apply_queue=manager.Queue()

    if 'download' in dir(options) and options.download:
        #This starts a process to handle the recovery:
        recovery_queue=manager.Queue()
        recovered_file_list=manager.list()
        recovery_process=multiprocessing.Process(target=worker_recovery, 
                                        name='worker_recovery',
                                        args=(recovery_function_handle,recovery_queue,retrieval_queues,recovered_file_list))
        recovery_process.start()
    else:
        recovered_file_list=[]
        retrieval_queues=dict()
        recovery_queue=None
        recovery_process=None

    #Set up the discovery var per var:
    for var_id,var in enumerate(vars_list):
        options_copy=copy.copy(options)
        for opt_id, opt in enumerate(project_drs.official_drs_no_version):
            if var[opt_id]!=None:
                setattr(options_copy,opt,var[opt_id])
        apply_queue.put((project_drs,options_copy))
    
    #Span up to options.num_procs processes and each child process analyzes only one simulation
    if options.num_procs==1:
        apply_queue.put('STOP')
        if not ('convert' in dir(options) and options.convert):
            output=netCDF4.Dataset(options.out_netcdf_file,'w',format='NETCDF4')
        else:
            output=None

        for tuple in iter(apply_queue.get,'STOP'):
            record_in_output(apply_func(function_handle,tuple,recovered_file_list=recovered_file_list,recovery_queue=recovery_queue),
                            output,project_drs,options)
            if not ('convert' in dir(options) and options.convert): output.sync()
    else:
        try:
            processes=[multiprocessing.Process(target=worker_apply, 
                                            args=(function_handle,apply_queue,record_queue,recovered_file_list,recovery_queue)) for proc in range(options.num_procs)]
            for process in processes:
                process.start()
                apply_queue.put('STOP')
            output=worker_record(record_queue,project_drs,options)
        finally:
            for process in processes: process.terminate()
            if 'terminate' in dir(recovery_process): recovery_process.terminate()

    if 'download' in dir(options) and options.download:
        recovery_queue.put('STOP')
    return output

def worker_record(out_queue,project_drs,options):
    #Open output file:
    if not ('convert' in dir(options) and options.convert):
        output=netCDF4.Dataset(options.out_netcdf_file,'w',format='NETCDF4')
    else:
        output=None
    for process in range(options.num_procs):
        for description in iter(out_queue.get,'STOP'):
            record_in_output(description,output,project_drs,options)
            if not ('convert' in dir(options) and options.convert): output.sync()
    return output

def record_in_output(description,output,project_drs,options):
    if (isinstance(output,netCDF4.Dataset) or
        isinstance(output,netCDF4.Group)):
        return record_in_output_database(description,output,project_drs,options)
    else:
        return record_in_output_directory(description,project_drs,options)

def record_in_output_directory(description,project_drs,options):
    temp_file_name=description[0]
    var=description[1]

    if ('out_destination' in dir(options)):
        output_file_name=options.out_destination+'/'+'/'.join(var)+'/'+os.path.basename(temp_file_name)
    else:
        output_file_name=options.out_netcdf_file+'/'+'/'.join(var)+'/'+os.path.basename(temp_file_name)

    #Create directory:
    try:
        directory=os.path.dirname(output_file_name)
        if not os.path.exists(directory):
            os.makedirs(directory)
    except:
        pass

    time_frequency=var[project_drs.official_drs_no_version.index('time_frequency')]
    #Get the time:
    output_tmp=netCDF4.Dataset(temp_file_name,'r')
    timestamp=nc_Database_utils.convert_dates_to_timestamps(output_tmp,time_frequency)
    output_tmp.close()

    if timestamp=='':
        os.remove(temp_file_name)
    else:
        os.rename(temp_file_name,'.'.join(output_file_name.split('.')[:-1])+timestamp+'.nc')
    return

def record_in_output_database(description,output,project_drs,options):
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
