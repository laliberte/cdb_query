
import netCDF4
import h5py

import copy

import os

import cdb_query_archive_class

import multiprocessing
import subprocess

import nc_Database_utils

def apply(options,project_drs):
    if options.script=='': return
    database=cdb_query_archive_class.SimpleTree(options,project_drs)
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

    output_root=distributed_apply(apply_to_variable,database,options,vars_list)
    database.record_header(options,output_root)
    output_root.close()
    return

def apply_to_variable(database,options):
    input_file_name=options.in_diagnostic_netcdf_file
    files_list=[input_file_name,]+options.in_extra_netcdf_files

    output_file_name=options.out_netcdf_file+'.pid'+str(os.getpid())
    temp_output_file_name= output_file_name+'.tmp'

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

    temp_files_list=[]
    for file in files_list:
        data=netCDF4.Dataset(file,'r')
        #Load the hdf5 api:
        data_hdf5=None
        for item in h5py.h5f.get_obj_ids():
            if 'name' in dir(item) and item.name==file:
                data_hdf5=h5py.File(item)
        temp_file=file+'.pid'+str(os.getpid())
        output_tmp=netCDF4.Dataset(temp_file,'w',format='NETCDF4',diskless=True,persist=True)

        #nc_Database_utils.extract_netcdf_variable_recursive(output_tmp,data,tree[0],tree[1:],options,check_empty=True)
        nc_Database_utils.extract_netcdf_variable_recursive(output_tmp,data,tree[0],tree[1:],options,check_empty=False,hdf5=data_hdf5)
        if options.add_fixed:
            #nc_Database_utils.extract_netcdf_variable_recursive(output_tmp,data,tree_fx[0],tree_fx[1:],options_fx,check_empty=True)
            nc_Database_utils.extract_netcdf_variable_recursive(output_tmp,data,tree_fx[0],tree_fx[1:],options_fx,check_empty=False,hdf5=data_hdf5)
        temp_files_list.append(temp_file)
        output_tmp.close()
        data.close()
        if data_hdf5!=None:
            data_hdf5.close()

    temp_files_list.append(temp_output_file_name)

    script_to_call=options.script
    for file_id, file in enumerate(temp_files_list):
        if not '{'+str(file_id)+'}' in options.script:
            script_to_call+=' {'+str(file_id)+'}'

    #script_to_call+=' '+temp_output_file_name

    #print '/'.join(var)
    #print script_to_call.format(*temp_files_list)
    out=subprocess.call(script_to_call.format(*temp_files_list),shell=True)

    #import shlex
    #args = shlex.split(script_to_call.format(*temp_files_list))
    #out=subprocess.call(args,shell=True)
    #out=subprocess.Popen(script_to_call.format(*temp_files_list),shell=True,
    #                     stdout=subprocess.PIPE,
    #                     stderr=subprocess.PIPE)
    #out.wait()

    try:
        for file in temp_files_list[:-1]:
            os.remove(file)
    except OSError:
        pass
    return (temp_output_file_name, var)

def worker(tuple):
    result=tuple[0](*tuple[1:-1])
    tuple[-1].put(result)
    return
    
def distributed_apply(function_handle,database,options,vars_list,args=tuple()):

        #Open output file:
        output_file_name=options.out_netcdf_file
        output_root=netCDF4.Dataset(output_file_name,'w',format='NETCDF4')

        manager=multiprocessing.Manager()
        queue=manager.Queue()
        #queue=multiprocessing.Queue()
        #Set up the discovery var per var:
        args_list=[]
        for var_id,var in enumerate(vars_list):
            options_copy=copy.copy(options)
            for opt_id, opt in enumerate(database.drs.official_drs_no_version):
                if var[opt_id]!=None:
                    setattr(options_copy,opt,var[opt_id])
            args_list.append((function_handle,copy.copy(database),options_copy)+args+(queue,))
        
        #Span up to options.num_procs processes and each child process analyzes only one simulation
        if options.num_procs>1:
            pool=multiprocessing.Pool(processes=options.num_procs,maxtasksperchild=1)
        try:
            if options.num_procs>1:
                #result=pool.map_async(worker,args_list,chunksize=1)
                result=pool.map(worker,args_list,chunksize=1)
                #result=[pool.apply_async(worker,arg) for arg in args_list]
            #Record files to main file:
            for arg in args_list:
                record_in_output(arg,queue,output_root,database,options)
                output_root.sync()
        finally:
            if options.num_procs>1:
                pool.terminate()
                pool.join()
        return output_root

def record_in_output(arg,queue,output_root,database,options):
    if options.num_procs==1:
        worker(arg)
    description=queue.get(1e20)
    temp_file_name=description[0]
    var=description[1]
    data=netCDF4.Dataset(temp_file_name,'r')
    #data_hdf5=h5py.File(temp_file_name,'r')
    data_hdf5=None
    for item in h5py.h5f.get_obj_ids():
        if 'name' in dir(item) and item.name==temp_file_name:
            data_hdf5=h5py.File(item)
    tree=zip(database.drs.official_drs_no_version,var)
    if ('applying_to_soft_links' in dir(options) and
        options.applying_to_soft_links):
        nc_Database_utils.replace_netcdf_variable_recursive(output_root,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=True)
    else:
        nc_Database_utils.replace_netcdf_variable_recursive(output_root,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=False)
    data.close()
    if data_hdf5!=None:
        data_hdf5.close()
    try:
        os.remove(temp_file_name)
    except OSError:
        pass
    return
