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
import discover
import validate

def ask_to_variable(project_drs,options):
    dataset, query_output_file_name=discover.discover(project_drs,options)

    var=[getattr(options,opt) for opt in project_drs.official_drs_no_version]
    tree=zip(project_drs.official_drs_no_version,var)

    #Decide whether to add fixed variables:
    tree_fx,options_fx=get_fixed_var_tree(project_drs,options,var)

    if not ('validate' in dir(options) and options.validate):
        make_ask_result_compatible_with_apply(temp_output_file_name, dataset, tree,tree_fx,options,options_fx,hdf5_file=query_output_file_name)
    dataset.close()
    return query_output_file_name, var

def validate_to_variable(project_drs,options):
    dataset, query_output_file_name=validate.validate(project_drs,options,Dataset=dataset,semaphores=validate_semaphores)
    dataset.close()
    var=[getattr(options,opt) for opt in project_drs.official_drs_no_version]
    return query_output_file_name, var

def download_raw_to_variable(project_drs,options):
    return query_output_file_name, var

def find_local_to_variable(project_drs,options):
    return query_output_file_name, var

def time_split_to_variable(project_drs,options):
    return query_output_file_name, var

def download_and_apply_to_variable(project_drs,options):
    return query_output_file_name, var

def download_to_variable(project_drs,options):
    return query_output_file_name, var

def apply_to_variable(project_drs,options,downloaded_file_list=[],download_queue=None,validate_semaphores=dict()):
    #The leaf(ves) considered here:
    var=[getattr(options,opt) for opt in project_drs.official_drs_no_version]
    tree=zip(project_drs.official_drs_no_version,var)

    #Decide whether to add fixed variables:
    tree_fx,options_fx=get_fixed_var_tree(project_drs,options,var)

    #Define output_file_name:
    output_file_name=get_output_name(project_drs,options,var)
    temp_output_file_name=get_temp_output_file_name(options,output_file_name)

    if ('ask' in dir(options) and options.ask):
        dataset, query_output_file_name=discover.discover(project_drs,options)
        if not ('validate' in dir(options) and options.validate):
            make_ask_result_compatible_with_apply(temp_output_file_name, dataset, tree,tree_fx,options,options_fx,hdf5_file=query_output_file_name)
            return (temp_output_file_name, var)
    else:
        dataset, query_out_file_name=(None,None)

    file_name_list=get_input_file_names(project_drs,options)
    temp_file_name_list=[]

    #If validate, download / extract the first input file using its dataset:
    if ('validate' in dir(options) and options.validate):
        dataset, query_output_file_name=validate.validate(project_drs,options,Dataset=dataset,semaphores=validate_semaphores)
        temp_file_name=get_temp_output_file_name(options,file_name_list[0])
        if 'download' in dir(options) and options.download:
            #close the dataset (which creates a persistent file) and move it to a temp input file:
            dataset.close()
            os.rename(query_output_file_name,temp_file_name)
            download_queue.put((temp_file_name,file_name,tree,tree_fx,options,options_fx))
        else:
            extract_single_tree_and_data(temp_file_name,dataset,tree,tree_fx,options,options_fx,check_empty=True,hdf5_file=temp_output_file_name)
            os.remove(temp_output_file_name)
        temp_file_name_list.append(temp_file_name)
        file_name_list.remove(file_name_list[0])

    #Download / extract remaining files:
    if 'download' in dir(options) and options.download:
        for file_name in file_name_list:
            temp_file_name=get_temp_output_file_name(options,file_name)
            download_queue.put((temp_file_name,file_name,tree,tree_fx,options,options_fx))
            temp_file_name_list.append(temp_file_name)
        #Wait until the file has been retrieved:
        for file in temp_file_name_list:
            while not file in downloaded_file_list:
                pass
            downloaded_file_list.remove(file)
    else:
        for file_name in file_name_list:
            temp_file_name=get_temp_output_file_name(options,file_name)
            extract_single_tree_and_file(temp_file_name,file_name,tree,tree_fx,options,options_fx,check_empty=True)
            temp_file_name_list.append(temp_file_name)

    if options.script=='':
        os.rename(temp_file_name_list[0],temp_output_file_name)
    else:
        #If script is not empty, call script:
        temp_file_name_list.append(temp_output_file_name)
        script_to_call=options.script
        for file_id, file in enumerate(temp_file_name_list):
            if not '{'+str(file_id)+'}' in options.script:
                script_to_call+=' {'+str(file_id)+'}'

        out=subprocess.call(script_to_call.format(*temp_file_name_list),shell=True)

    try:
        for file in temp_file_name_list[:-1]:
            os.remove(file)
    except OSError:
        pass
    return (temp_output_file_name, var)

def extract_single_tree_and_file(temp_file,file,tree,tree_fx,options,options_fx,queues=dict(),check_empty=False):
    data=netCDF4.Dataset(file,'r')
    extract_single_tree_and_data(temp_file,data,tree,tree_fx,options,options_fx,queues=queues,check_empty=check_empty,hdf5_file=file)
    return

def extract_single_tree_and_data(temp_file,data,tree,tree_fx,options,options_fx,queues=dict(),check_empty=False,hdf5_file=None):
    data_hdf5=None
    if hdf5_file!=None:
        for item in h5py.h5f.get_obj_ids():
            if 'name' in dir(item) and item.name==hdf5_file:
                data_hdf5=h5py.File(item)

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

def make_ask_result_compatible_with_apply(output_file,data, tree,tree_fx,options,options_fx,hdf5_file=file):
    #This call prunes the output datast from ask and validate to make them conform with the expected apply format:
    #Do not download:
    options_copy=copy.copy(options)
    options_copy.download=False
    options_fx_copy=copy.copy(options_fx)
    if ('add_fixed' in dir(options) and options.add_fixed):
        options_fx_copy.download=False

    #Extract, making sure not to inflate data size with empty arrays from soft links:
    extract_single_tree_and_data(output_file+'.tmp',data,tree,tree_fx,options_copy,options_fx_copy,check_empty=True,hdf5_file=file)
    os.rename(output_file+'.tmp',output_file)
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

def get_input_file_names(project_drs,options):
        
    input_file_name=options.in_netcdf_file
    file_name_list=[input_file_name,]
    if 'in_extra_netcdf_files' in dir(options): file_name_list+=options.in_extra_netcdf_files
    
    if (options.script=='' and 
        ('in_extra_netcdf_files' in dir(options) and 
              len(options.in_extra_netcdf_files)>0) ):
        raise InputErrorr('The identity script \'\' can only be used when no extra netcdf files are specified.')

    return file_name_list

def get_fixed_var_tree(project_drs,options,var):
    if not ('add_fixed' in dir(options) and options.add_fixed):
        return None, None

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
    return tree_fx, options_fx
    
def get_temp_output_file_name(options,output_file_name):
    #if options.script=='':
    #    #If the script is identity, output to output_file name directly:
    #    temp_output_file_name=output_file_name
    #else:
    temp_output_file_name=output_file_name+'.pid'+str(os.getpid())

    #Put temp files in the swap dir:
    if ('swap_dir' in dir(options) and options.swap_dir!='.'):
        temp_output_file_name=options.swap_dir+'/'+os.path.basename(temp_output_file_name)

    #Create directory:
    try:
        directory=os.path.dirname(temp_output_file_name)
        if not os.path.exists(directory):
            os.makedirs(directory)
    except:
        pass
    return temp_output_file_name
