#External:
import netCDF4
import h5py
import copy
import subprocess
import os
import sys

#Internal:
import nc_Database_utils

def reduce_soft_links(database,options,q_manager=None,sessions=dict()):
    return reduce_sl_or_var(database,options,q_manager=q_manager,sessions=sessions,retrieval_type='reduce_soft_links',script=options.reduce_soft_links_script)

def reduce_variable(database,options,q_manager=None,sessions=dict(),retrieval_type='reduce'):
    return reduce_sl_or_var(database,options,q_manager=q_manager,sessions=sessions,retrieval_type='reduce',script=options.script)

def reduce_sl_or_var(database,options,q_manager=None,sessions=dict(),retrieval_type='reduce',script=''):
    #The leaf(ves) considered here:
    fix_list=(lambda x: x[0] if len(x)==1 else x)

    #Warning! Does not allow --serial option:
    var=[fix_list(getattr(options,opt)) if getattr(options,opt)!=None
                                 else None for opt in database.drs.official_drs_no_version]
    tree=zip(database.drs.official_drs_no_version,var)

    #Decide whether to add fixed variables:
    tree_fx,options_fx=get_fixed_var_tree(database.drs,options,var)

    #Define output_file_name:
    output_file_name=get_output_name(database.drs,options,var)
    temp_output_file_name=get_temp_output_file_name(options,output_file_name)

    file_name_list=get_input_file_names(database.drs,options,script)
    temp_file_name_list=[]

    if 'validate' in sessions.keys():
        session=sessions['validate']
    else:
        session=None

    for file_name in file_name_list:
        temp_file_name=get_temp_input_file_name(options,file_name)
        extract_single_tree(temp_file_name,file_name,
                                    tree,tree_fx,
                                    options,options_fx,
                                    retrieval_type=retrieval_type,
                                    session=session,
                                    check_empty=(retrieval_type=='reduce'))
        temp_file_name_list.append(temp_file_name)

    if script=='':
        os.rename(temp_file_name_list[0],temp_output_file_name)
    else:
        #If script is not empty, call script:
        temp_file_name_list.append(temp_output_file_name)
        script_to_call=script
        for file_id, file in enumerate(temp_file_name_list):
            if not '{'+str(file_id)+'}' in script:
                script_to_call+=' {'+str(file_id)+'}'

        out=subprocess.call(script_to_call.format(*temp_file_name_list),shell=True,stdout=sys.stdout,stderr=sys.stderr)
    try:
        for file in temp_file_name_list[:-1]:
            os.remove(file)
    except OSError:
        pass

    if retrieval_type=='reduce_soft_links':
        output_file_name=temp_output_file_name+'.tmp'
        with netCDF4.Dataset(output_file_name,'w') as output:
            nc_Database_utils.record_to_netcdf_file_from_file_name(options,temp_output_file_name,output,database.drs)
    else:
        #This is the last function in the chain. Convert and create soft links:
        output_file_name=nc_Database_utils.record_to_output_directory(temp_output_file_name,database.drs,options)
    try:
        os.remove(temp_output_file_name)
        os.rename(output_file_name,temp_output_file_name)
    except OSError:
        pass
    return temp_output_file_name

def extract_single_tree(temp_file,file,tree,tree_fx,options,options_fx,session=None,retrieval_type='reduce',check_empty=False):
    with netCDF4.Dataset(file,'r') as data:
        hdf5=None
        try:
            for item in h5py.h5f.get_obj_ids(types=h5py.h5f.OBJ_FILE):
                if 'name' in dir(item) and item.name==file:
                    hdf5=h5py.File(item)
        except ValueError:
            pass
        except RuntimeError:
            pass

        with netCDF4.Dataset(temp_file,'w',format='NETCDF4',diskless=True,persist=True) as output_tmp:
            if ('add_fixed' in dir(options) and options.add_fixed):
                nc_Database_utils.extract_netcdf_variable(output_tmp,data,tree_fx,options_fx,session=session,retrieval_type=retrieval_type,check_empty=True,hdf5=hdf5)

            nc_Database_utils.extract_netcdf_variable(output_tmp,data,tree,options,session=session,retrieval_type=retrieval_type,check_empty=check_empty,hdf5=hdf5)

    if hdf5!=None:
        hdf5.close()
    return

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
    #Put temp files in the swap dir:
    if ('swap_dir' in dir(options) and options.swap_dir!='.'):
        temp_output_file_name=options.swap_dir+'/'+os.path.basename(output_file_name)
    else:
        temp_output_file_name=output_file_name

    #Create directory:
    try:
        directory=os.path.dirname(temp_output_file_name)
        if not os.path.exists(directory):
            os.makedirs(directory)
    except:
        pass
    return temp_output_file_name

def get_temp_input_file_name(options,input_file_name):
    #Put temp files in the swap dir:
    if ('swap_dir' in dir(options) and options.swap_dir!='.'):
        temp_input_file_name=options.swap_dir+'/'+os.path.basename(input_file_name)
    else:
        temp_input_file_name=input_file_name

    temp_input_file_name+='.pid'+str(os.getpid())

    #Create directory:
    try:
        directory=os.path.dirname(temp_input_file_name)
        if not os.path.exists(directory):
            os.makedirs(directory)
    except:
        pass
    return temp_input_file_name

def get_output_name(project_drs,options,var):
    output_file_name=options.out_netcdf_file
    return output_file_name

def get_input_file_names(project_drs,options,script):
    input_file_name=options.in_netcdf_file
    file_name_list=[input_file_name,]
    if 'in_extra_netcdf_files' in dir(options): file_name_list+=options.in_extra_netcdf_files
    
    if (script=='' and 
        ('in_extra_netcdf_files' in dir(options) and 
              len(options.in_extra_netcdf_files)>0) ):
        raise InputErrorr('The identity script \'\' can only be used when no extra netcdf files are specified.')

    return file_name_list
