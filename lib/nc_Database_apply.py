#External:
import netCDF4
import h5py
import copy
import os
import multiprocessing
import subprocess

#External but related:
import netcdf4_soft_links.retrieval_manager as retrieval_manager

#Internal:
import cdb_query_archive_class
import nc_Database_utils

def apply(options,database,manager=None):
    if options.script=='': return
    #database=cdb_query_archive_class.SimpleTree(project_drs)
    #Recover the database meta data:
    if options.keep_field!=None:
        drs_to_eliminate=[field for field in database.drs.official_drs_no_version if
                                             not field in options.keep_field]
    else:
        drs_to_eliminate=database.drs.official_drs_no_version
    vars_list=[[ var[drs_to_eliminate.index(field)] if field in drs_to_eliminate else None
                        for field in database.drs.official_drs_no_version] for var in 
                        database.list_fields_local(options,drs_to_eliminate) ]

    #Always add the fixed variables to apply:
    #vars_list=[ var for var in vars_list if var[database.drs.official_drs_no_version.index('ensemble')] !='r0i0p0' ]

    #Randonmize the list:
    import random
    random.shuffle(vars_list)

    output_root=distributed_apply(apply_to_variable,database,options,vars_list,manager=manager)
    database.record_header(options,output_root)
    output_root.close()
    return

def apply_to_variable(database,retrieved_file_list,recovery_queue,options):
    input_file_name=options.in_netcdf_file
    files_list=[input_file_name,]+options.in_extra_netcdf_files

    output_file_name=options.out_netcdf_file+'.pid'+str(os.getpid())
    #Put temp files in the swap dir:
    temp_output_file_name= options.swap_dir+'/'+os.path.basename(output_file_name)+'.tmp'

    var=[getattr(options,opt) for opt in database.drs.official_drs_no_version]
    tree=zip(database.drs.official_drs_no_version,var)

    #Decide whether to add fixed variables:
    if options.add_fixed:
        #Specification for fixed vars:
        var_fx=[ getattr(options,opt) if not opt in database.drs.var_specs+['var',] else None for opt in database.drs.official_drs_no_version]
        var_fx=copy.copy(var)
        var_fx[database.drs.official_drs_no_version.index('ensemble')]='r0i0p0'
        var_fx[database.drs.official_drs_no_version.index('var')]=None
        for opt  in database.drs.var_specs+['var',]:
            if ( opt in ['time_frequency','cmor_table'] and
                 not var[database.drs.official_drs_no_version.index(opt)]==None):
                var_fx[database.drs.official_drs_no_version.index(opt)]='fx'
        tree_fx=zip(database.drs.official_drs_no_version,var_fx)
        options_fx=copy.copy(options)
        for opt_id,opt in enumerate(tree_fx):
            if opt!=tree[opt_id]:
                setattr(options_fx,opt[0],opt[1])
                if ('X'+opt[0] in dir(options_fx) and
                     isinstance(getattr(options_fx,'X'+opt[0]),list) and
                     opt[1] in getattr(options_fx,'X'+opt[0])):
                     getattr(options_fx,'X'+opt[0]).remove(opt[1])
    else:
        tree_fx=None
        options_fx=None

    temp_files_list=[]
    for file in files_list:
        temp_file=options.swap_dir+'/'+os.path.basename(file)+'.pid'+str(os.getpid())
        if 'download' in dir(options) and options.download:
            recovery_queue.put((temp_file,file,tree,tree_fx,options,options_fx))
        else:
            extract_single_tree_and_file(temp_file,file,tree,tree_fx,options,options_fx,queues=database.queues,check_empty=True)
        temp_files_list.append(temp_file)

    if 'download' in dir(options) and options.download:
        #Wait until the file has been retrieved:
        for file in temp_files_list:
            while not file in retrieved_file_list:
                pass
            retrieved_file_list.remove(file)

    temp_files_list.append(temp_output_file_name)

    script_to_call=options.script
    for file_id, file in enumerate(temp_files_list):
        if not '{'+str(file_id)+'}' in options.script:
            script_to_call+=' {'+str(file_id)+'}'

    out=subprocess.call(script_to_call.format(*temp_files_list),shell=True)
    try:
        for file in temp_files_list[:-1]:
            os.remove(file)
    except OSError:
        pass
    return (temp_output_file_name, var)

def extract_single_tree_and_file(temp_file,file,tree,tree_fx,options,options_fx,queues=dict(),check_empty=False):
    data=netCDF4.Dataset(file,'r')
    #Load the hdf5 api:
    data_hdf5=None
    for item in h5py.h5f.get_obj_ids():
        if 'name' in dir(item) and item.name==file:
            data_hdf5=h5py.File(item)
    #Put temp files in the swap dir:
    output_tmp=netCDF4.Dataset(temp_file,'w',format='NETCDF4',diskless=True,persist=True)
    if options.add_fixed:
        #nc_Database_utils.extract_netcdf_variable(output_tmp,data,tree_fx,options_fx,check_empty=True)
        nc_Database_utils.extract_netcdf_variable(output_tmp,data,tree_fx,options_fx,check_empty=True,hdf5=data_hdf5)

    #nc_Database_utils.extract_netcdf_variable(output_tmp,data,tree,options,check_empty=True)
    nc_Database_utils.extract_netcdf_variable(output_tmp,data,tree,options,check_empty=True,hdf5=data_hdf5,queues=queues)
    if 'download' in dir(options) and options.download:
        data_node_list=queues.keys()
        data_node_list.remove('end')
        retrieval_manager.launch_download_and_remote_retrieve(output_tmp,data_node_list,queues,options)
        #output_tmp is implictly closed by the retrieval manager
    else:
        output_tmp.close()
    data.close()
    if data_hdf5!=None:
        data_hdf5.close()
    return

def worker_recovery(input_queue, retrieval_queues, output_list):
    for tuple in iter(input_queue.get, 'STOP'):
        extract_single_tree_and_file(*tuple,check_empty=True,queues=retrieval_queues)
        output_list.append(tuple[0])
    return

def worker_apply(tuple):
    result=tuple[0](*tuple[1:-1])
    tuple[-1].put(result)
    return
    
def distributed_apply(function_handle,database,options,vars_list,args=tuple(),manager=None):
        #Open output file:
        output_file_name=options.out_netcdf_file
        output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')


        if manager==None:
            manager=multiprocessing.Manager()
        #This is the gathering queue:
        gathering_queue=manager.Queue()

        if 'download' in dir(options) and options.download:
            #This starts a process to handle the recovery:
            recovery_queue=manager.Queue()
            retrieved_file_list=manager.list()
            process=multiprocessing.Process(target=worker_recovery, 
                                            name=worker_recovery,
                                            args=(recovery_queue, database.queues, retrieved_file_list))
            process.start()
        else:
            retrieved_file_list=[]
            database.queues=dict()
            recovery_queue=None

        #Set up the discovery var per var:
        args_list=[]
        for var_id,var in enumerate(vars_list):
            options_copy=copy.copy(options)
            for opt_id, opt in enumerate(database.drs.official_drs_no_version):
                if var[opt_id]!=None:
                    setattr(options_copy,opt,var[opt_id])
            args_list.append((function_handle,copy.copy(database),retrieved_file_list,recovery_queue,options_copy)+args+(gathering_queue,))
        
        #Span up to options.num_procs processes and each child process analyzes only one simulation
        if options.num_procs>1:
            pool=multiprocessing.Pool(processes=options.num_procs,maxtasksperchild=1)
        try:
            if options.num_procs>1:
                #result=pool.map_async(worker_apply,args_list,chunksize=1)
                result=pool.map(worker_apply,args_list,chunksize=1)
                #result=[pool.apply_async(worker_apply,arg) for arg in args_list]
            #Record files to main file:
            for arg in args_list:
                record_in_output(arg,gathering_queue,output_root,database,options)
                output_root.sync()
        finally:
            recovery_queue.put('STOP')
            if options.num_procs>1:
                pool.terminate()
                pool.join()
        return output_root

def record_in_output(arg,gathering_queue,output_root,database,options):
    if options.num_procs==1:
        worker_apply(arg)
    description=gathering_queue.get(1e20)
    temp_file_name=description[0]
    var=description[1]
    data=netCDF4.Dataset(temp_file_name,'r')
    #data_hdf5=h5py.File(temp_file_name,'r')
    data_hdf5=None
    for item in h5py.h5f.get_obj_ids():
        if 'name' in dir(item) and item.name==temp_file_name:
            data_hdf5=h5py.File(item)
    tree=zip(database.drs.official_drs_no_version,var)
    #if ('applying_to_soft_links' in dir(options) and
    #    options.applying_to_soft_links):
    #    #Do not check empty:
    #    nc_Database_utils.replace_netcdf_variable_recursive(output_root,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=False)
    #else:
    nc_Database_utils.replace_netcdf_variable_recursive(output_root,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=True)
    data.close()
    if data_hdf5!=None:
        data_hdf5.close()
    try:
        os.remove(temp_file_name)
    except OSError:
        pass
    return
