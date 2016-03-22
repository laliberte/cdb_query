#External:
import netCDF4
import h5py
import copy
import subprocess
import os

#External but related:
import netcdf4_soft_links.retrieval_manager as retrieval_manager

#Internal:
import nc_Database_utils

def apply_to_variable(project_drs,options,recovered_file_list=[],recovery_queue=None):
    input_file_name=options.in_netcdf_file
    files_list=[input_file_name,]
    if 'in_extra_netcdf_files' in dir(options): file_list+=options.in_extra_netcdf_files

    if (options.script=='' and 
        ('in_extra_netcdf_files' in dir(options) and 
              len(options.in_extra_netcdf_files)>0) ):
        raise InputErrorr('The identity script \'\' can only be used when no extra netcdf files are specified.')

    #The leaf (ves) considered here:
    var=[getattr(options,opt) for opt in project_drs.official_drs_no_version]
    tree=zip(project_drs.official_drs_no_version,var)

    output_file_name=get_output_name(project_drs,options,var)

    #Put temp files in the swap dir:
    if ('swap_dir' in dir(options) and options.swap_dir!='.'):
        temp_output_file_name=options.swap_dir+'/'+os.path.basename(output_file_name)+'.pid'+str(os.getpid())
    else:
        temp_output_file_name=output_file_name+'.pid'+str(os.getpid())

    #Create directory:
    try:
        directory=os.path.dirname(temp_output_file_name)
        if not os.path.exists(directory):
            os.makedirs(directory)
    except:
        pass

    #Decide whether to add fixed variables:
    if ('add_fixed' in dir(options) and options.add_fixed):
        #Specification for fixed vars:
        var_fx=[ getattr(options,opt) if not opt in project_drs.var_specs+['var',] else None for opt in project_drs.official_drs_no_version]
        var_fx=copy.copy(var)
        var_fx[project_drs.official_drs_no_version.index('ensemble')]='r0i0p0'
        var_fx[project_drs.official_drs_no_version.index('var')]=None
        for opt  in project_drs.var_specs+['var',]:
            if ( opt in ['time_frequency','cmor_table'] and
                 not var[project_drs.official_drs_no_version.index(opt)]==None):
                var_fx[project_drs.official_drs_no_version.index(opt)]='fx'
        tree_fx=zip(project_drs.official_drs_no_version,var_fx)
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
        if options.script=='':
            #If the script is identity, output to output_file name directly:
            temp_file=temp_output_file_name
        else:
            temp_file=options.swap_dir+'/'+os.path.basename(file)+'.pid'+str(os.getpid())

        if 'download' in dir(options) and options.download:
            recovery_queue.put((temp_file,file,tree,tree_fx,options,options_fx))
        else:
            extract_single_tree_and_file(temp_file,file,tree,tree_fx,options,options_fx,check_empty=True)
        temp_files_list.append(temp_file)

    if 'download' in dir(options) and options.download:
        #Wait until the file has been retrieved:
        for file in temp_files_list:
            while not file in recovered_file_list:
                pass
            recovered_file_list.remove(file)

    if options.script=='':
        return (temp_output_file_name, var)

    #Otherwise call script:
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
    if ('add_fixed' in dir(options) and options.add_fixed):
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

def get_output_name(project_drs,options,var):
    if ('convert' in dir(options) and options.convert):
        file_name=[getattr(options,opt) if not opt=='version' else 'v'+datetime.datetime.now().strftime('%Y%m%d')
                                    for opt in project_drs.filename_drs]
        if ('out_destination' in dir(options)):
            output_file_name=options.out_destination+'/'+'/'.join(var)+'/'+'_'.join(file_name)
        else:
            output_file_name=options.out_netcdf_file+'/'+'/'.join(var)+'/'+'_'.join(file_name)
    else:
        output_file_name=options.out_netcdf_file
    return output_file_name

