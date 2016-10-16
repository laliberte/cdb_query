#External:
import netCDF4
import copy
import subprocess
import os
import sys
import tempfile
import numpy as np
import shutil
import errno
from StringIO import StringIO
import logging
#_logger = logging.getLogger(__name__)
#_logger.addHandler(logging.NullHandler())
#_logger.setLevel(logging.CRITICAL)

#Internal:
from ..nc_Database import db_utils
from . import downloads_utils

def _fix_list(x):
    if len(x)==1:
        return x[0]
    else:
        return x

def make_list(item):
    if isinstance(item,list):
        return item
    elif (isinstance(item,set) or isinstance(item,tuple)):
        return list(item)
    else:
        if item!=None:
            return [item,]
        else:
            return None

def set_new_var_options(options_copy, var_item, official_drs_no_version):
    for opt_id, opt in enumerate(official_drs_no_version):
        if var_item[opt_id]!=None:
            setattr(options_copy,opt,make_list(var_item[opt_id]))
    return

def set_new_time_options(options_copy, time_item):
    for opt_id, opt in enumerate(['year','month','day','hour']):
        if time_item[opt_id]!=None and opt in dir(options_copy):
            setattr(options_copy,opt,make_list(time_item[opt_id]))
    return

#Do it by simulation, except if one simulation field should be kept for further operations:
def reduce_var_list(database,options):
    if ('keep_field' in dir(options) and options.keep_field!=None):
        drs_to_eliminate=[field for field in database.drs.official_drs_no_version if
                                             not field in options.keep_field]
    else:
        drs_to_eliminate=database.drs.official_drs_no_version
    var_list=[ [ make_list(item) for item in var_list] for var_list in 
                set([
                    tuple([ 
                        tuple(sorted(set(make_list(var[drs_to_eliminate.index(field)]))))
                        if field in drs_to_eliminate else None
                        for field in database.drs.official_drs_no_version]) for var in 
                        database.list_fields_local(options,drs_to_eliminate, soft_links=False) ])]
    if len(var_list)>1:
        #This is a fix necessary for MOHC models. 
        if 'var' in drs_to_eliminate:
            var_index = database.drs.official_drs_no_version.index('var')
            var_names = set(map(lambda x: tuple(x[var_index]),var_list))
            if len(var_names) == 1:
                ensemble_index = database.drs.official_drs_no_version.index('ensemble')
                ensemble_names = np.unique(np.concatenate(map(lambda x: tuple(x[ensemble_index]),var_list)))
                if 'r0i0p0' in ensemble_names:
                    for var in var_list:
                        if 'r0i0p0' in var[ensemble_index]:
                            return [var,]
    return var_list

def reduce_soft_links(database, options, q_manager=None, sessions=dict()):

    vars_list = reduce_var_list(database, options)
    with netCDF4.Dataset(options.out_netcdf_file,'w',diskless=True,persist=True) as output:
        for var in vars_list:
            options_copy = copy.copy(options)
            set_new_var_options(options_copy,var,database.drs.official_drs_no_version)
            logging.debug('Reducing soft_links '+str(var))
            temp_output_file_name_one_var = reduce_sl_or_var(database, options_copy, q_manager=q_manager,
                                                             sessions=sessions, retrieval_type='reduce_soft_links',
                                                             script=options.reduce_soft_links_script)
            db_utils.record_to_netcdf_file_from_file_name(options_copy, temp_output_file_name_one_var, output ,database.drs)
            logging.debug('Done reducing soft_links '+str(var))

            try:
                os.remove(temp_output_file_name_one_var)
            except Exception:
                pass
    return

def reduce_variable(database,options,q_manager=None,sessions=dict(),retrieval_type='reduce'):
    vars_list = reduce_var_list(database, options)
    with netCDF4.Dataset(options.out_netcdf_file,'w',diskless=True,persist=True) as output:
        for var in vars_list:
            options_copy = copy.copy(options)
            set_new_var_options(options_copy,var,database.drs.official_drs_no_version)
            times_list=downloads_utils.time_split(database,options_copy)
            for time in times_list:
                options_copy_time = copy.copy(options_copy)
                set_new_time_options(options_copy_time,time)
                logging.debug('Reducing variables '+str(var)+' '+str(time))
                temp_output_file_name_one_var = reduce_sl_or_var(database,options_copy_time,q_manager=q_manager,
                                                               sessions=sessions,retrieval_type='reduce',
                                                               script=options.script)
                db_utils.record_to_netcdf_file_from_file_name(options_copy_time,temp_output_file_name_one_var,output,database.drs)
                logging.debug('Done Reducing variables '+str(var)+' '+str(time))

                try:
                    os.remove(temp_output_file_name_one_var)
                except Exception:
                    pass
    return

def reduce_sl_or_var(database,options,q_manager=None,sessions=dict(),retrieval_type='reduce',script=''):
    #The leaf(ves) considered here:
    #Warning! Does not allow --serial option:
    var=[_fix_list(getattr(options,opt)) if getattr(options,opt)!=None
                                 else None for opt in database.drs.official_drs_no_version]
    tree=zip(database.drs.official_drs_no_version,var)

    #Decide whether to add fixed variables:
    tree_fx,options_fx=get_fixed_var_tree(database.drs,options,var)

    #Define temp_output_file_name:
    fileno, temp_output_file_name = tempfile.mkstemp(dir=options.swap_dir)
    #must close fileno
    os.close(fileno)

    file_name_list = get_input_file_names(database.drs,options,script)
    temp_file_name_list=[]

    if 'validate' in sessions.keys():
        session=sessions['validate']
    else:
        session=None

    for file_name in file_name_list:
        fileno, temp_file_name = tempfile.mkstemp(dir=options.swap_dir)
        #must close fileno
        os.close(fileno)

        extract_single_tree(temp_file_name,file_name,
                                    tree,tree_fx,
                                    options,options_fx,
                                    retrieval_type=retrieval_type,
                                    session=session,
                                    check_empty=(retrieval_type=='reduce'))
        temp_file_name_list.append(temp_file_name)

    if 'sample' in dir(options) and options.sample:
        try:
            os.makedirs(options.out_destination)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        for file in temp_file_name_list:
            shutil.copy(file,options.out_destination+'/'+os.path.basename(file))

    if script.strip() == '':
        os.rename(temp_file_name_list[0],temp_output_file_name)
    else:
        #If script is not empty, call script:
        temp_file_name_list.append(temp_output_file_name)
        script_to_call=script
        for file_id, file in enumerate(temp_file_name_list):
            if not '{'+str(file_id)+'}' in script:
                script_to_call+=' {'+str(file_id)+'}'

        #Remove temp_output_file_name to avoid programs that request before overwriting:
        os.remove(temp_output_file_name)
        try:
            output = subprocess.check_output(script_to_call.format(*temp_file_name_list),
                                             shell=True, stderr=subprocess.STDOUT)
            #Capture subprocess errors to print output:
            for line in iter(output.splitlines()):
                logging.info(line)
        except subprocess.CalledProcessError as e:
            #Capture subprocess errors to print output:
            for line in iter(e.output.splitlines()):
                logging.info(line)
            raise
        #out=subprocess.call(script_to_call.format(*temp_file_name_list),shell=True)

    try:
        for file in temp_file_name_list[:-1]:
            os.remove(file)
    except OSError:
        pass

    if retrieval_type=='reduce_soft_links':
        processed_output_file_name = temp_output_file_name+'.tmp'
        with netCDF4.Dataset(processed_output_file_name,'w') as output:
            db_utils.record_to_netcdf_file_from_file_name(options,temp_output_file_name,output,database.drs)
    else:
        #This is the last function in the chain. Convert and create soft links:
        processed_output_file_name = db_utils.record_to_output_directory(temp_output_file_name,database.drs,options)
    try:
        os.remove(temp_output_file_name)
        os.rename(processed_output_file_name, temp_output_file_name)
    except OSError:
        pass
    return temp_output_file_name

def extract_single_tree(temp_file,file,tree,tree_fx,options,options_fx,session=None,retrieval_type='reduce',check_empty=False):
    with db_utils._read_Dataset(file)(file,'r') as data:
        with netCDF4.Dataset(temp_file,'w',format='NETCDF4',diskless=True,persist=True) as output_tmp:
            if ('add_fixed' in dir(options) and options.add_fixed):
                db_utils.extract_netcdf_variable(output_tmp,data,tree_fx,options_fx,session=session,retrieval_type=retrieval_type,check_empty=True)

            db_utils.extract_netcdf_variable(output_tmp,data,tree,options,session=session,retrieval_type=retrieval_type,check_empty=check_empty)
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
        if opt != tree_fx[opt_id]:
            setattr(options_fx,opt[0],opt[1])
            if ('X'+opt[0] in dir(options_fx) and
                 isinstance(getattr(options_fx,'X'+opt[0]),list) and
                 opt[1] in getattr(options_fx,'X'+opt[0])):
                     getattr(options_fx,'X'+opt[0]).remove(opt[1])
    return tree_fx, options_fx
    
def get_input_file_names(project_drs,options,script):
    if (script.strip() == '' and 
        ('in_extra_netcdf_files' in dir(options) and 
              len(options.in_extra_netcdf_files) > 0) ):
        raise InputErrorr('The identity script \'\' can only be used when no extra netcdf files are specified.')

    input_file_name = options.in_netcdf_file
    file_name_list = [input_file_name,]
    if 'in_extra_netcdf_files' in dir(options): file_name_list += options.in_extra_netcdf_files
    
    return file_name_list
