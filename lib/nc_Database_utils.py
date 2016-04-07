#External:
import netCDF4
import h5py
import copy
import datetime
import os

#External but related:
import netcdf4_soft_links.create_soft_links as create_soft_links
import netcdf4_soft_links.read_soft_links as read_soft_links
import netcdf4_soft_links.netcdf_utils as netcdf_utils
import netcdf4_soft_links.remote_netcdf as remote_netcdf

#Internal:
import nc_Database

#EXTRACTIONS:
def extract_netcdf_variable(output,data,tree,options,check_empty=False,hdf5=None,download_semaphores=dict(),download_queues_manager=None,retrieval_type='reduce'):
    return extract_netcdf_variable_recursive(output,data,tree[0],tree[1:],retrieval_type,options,check_empty,hdf5,download_semaphores,download_queues_manager)

def extract_netcdf_variable_recursive(output,data,level_desc,tree,retrieval_type,options,check_empty,hdf5,download_semaphores,download_queues_manager):
    level_name=level_desc[0]
    group_name=level_desc[1]
    if group_name==None:
        for group in data.groups.keys():
            if ( nc_Database.is_level_name_included_and_not_excluded(level_name,options,group) and
                 nc_Database.tree_recursive_check_not_empty(options,data.groups[group])):
                output_grp=netcdf_utils.replicate_group(output,data,group)
                extract_retrieve_or_replicate(group,output_grp,data,tree,retrieval_type,download_semaphores,download_queues_manager,hdf5,check_empty,options)
    else:
        if (len(tree)>0 and group_name==''):
            extract_netcdf_variable_recursive(output,data,tree[0],tree[1:],retrieval_type,options,check_empty,hdf5,download_semaphores,download_queues_manager)
        else:
            extract_retrieve_or_replicate(group_name,output,data,tree,retrieval_type,download_semaphores,download_queues_manager,hdf5,check_empty,options)
    return

def extract_retrieve_or_replicate(group_name,output,data,tree,retrieval_type,download_semaphores,download_queues_manager,hdf5,check_empty,options):
    if len(tree)>0:
        if group_name in data.groups.keys():
            if hdf5!=None:
                hdf5_grp=hdf5[group_name]
            else:
                hdf5_grp=hdf5
            extract_netcdf_variable_recursive(output,data.groups[group_name],tree[0],tree[1:],retrieval_type,options,check_empty,hdf5_grp,download_semaphores,download_queues_manager)
    else:
        retrieve_or_replicate(output,data,group_name,retrieval_type,options,check_empty,hdf5,download_semaphores,download_queues_manager)
    return

def retrieve_or_replicate(output_grp,data,group,retrieval_type,options,check_empty,hdf5,download_semaphores,download_queues_manager):
    netcdf_pointers=read_soft_links.read_netCDF_pointers(data.groups[group],options=options,semaphores=download_semaphores,queues=download_queues_manager)
    if retrieval_type=='reduce':
        if ( ('reducing_soft_links_script' in dir(options) and
              options.reducing_soft_links_script!='') or
             (not 'soft_links' in data.groups[group].groups.keys())):
            #If applying to soft links, replicate.
            #If there is no soft links, replicate.
            if hdf5!=None:
                hdf5_grp=hdf5[group]
            else:
                hdf5_grp=hdf5
            netcdf_pointers.replicate(output_grp,check_empty=check_empty,hdf5=hdf5_grp)
        else:
            #There are soft links and they are supposed to be loaded:
            netcdf_pointers.retrieve(output_grp,'load',filepath=options.out_netcdf_file)
    else:
        if 'out_destination' in dir(options):
            netcdf_pointers.retrieve(output_grp,retrieval_type,filepath=options.out_destination)
        else:
            netcdf_pointers.retrieve(output_grp,retrieval_type,filepath=options.out_netcdf_file)
    return

#PUT BACK IN DATABASE:
def record_to_netcdf_file_from_file_name(options,temp_file_name,output,project_drs):
    data=netCDF4.Dataset(temp_file_name,'r')
    data_hdf5=None
    for item in h5py.h5f.get_obj_ids():
        if 'name' in dir(item) and item.name==temp_file_name:
            data_hdf5=h5py.File(item)

    var=[ getattr(options,opt)[0] if getattr(options,opt)!=None else None for opt in project_drs.official_drs_no_version]
    tree=zip(project_drs.official_drs_no_version,var)

    if ('reducing_soft_links_script' in dir(options) and
        options.reducing_soft_links_script==''):
        #Do not check empty:
        replace_netcdf_variable_recursive(output,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=False)
    else:
        replace_netcdf_variable_recursive(output,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=True)

    data.close()
    if data_hdf5!=None:
        data_hdf5.close()
    return

def replace_netcdf_variable_recursive(output,data,level_desc,tree,hdf5=None,check_empty=False):
    level_name=level_desc[0]
    group_name=level_desc[1]
    if group_name==None:
        for group in data.groups.keys():
            output_grp=netcdf_utils.create_group(output,data,group)
            if hdf5!=None:
                hdf5_grp=hdf5[group]
            else:
                hdf5_grp=hdf5
            replace_netcdf_variable_recursive_replicate(output_grp,data.groups[group],level_name,group,tree,hdf5=hdf5_grp,check_empty=check_empty)
    else:
        if hdf5!=None:
            hdf5_grp=hdf5[group_name]
        else:
            hdf5_grp=hdf5
        output_grp=netcdf_utils.create_group(output,data,group_name)
        replace_netcdf_variable_recursive_replicate(output_grp,data.groups[group_name],level_name,group_name,tree,hdf5=hdf5_grp,check_empty=check_empty)
        #replace_netcdf_variable_recursive_replicate(output_grp,data,level_name,group_name,tree,hdf5=hdf5,check_empty=check_empty)
    return

def replace_netcdf_variable_recursive_replicate(output_grp,data_grp,level_name,group_name,tree,hdf5=None,check_empty=False):
    if len(tree)>0 or (not group_name in output_grp.groups.keys()):
        try:
            setattr(output_grp,'level_name',level_name)
        except:
            output_grp.setncattr('level_name',level_name)
    if len(tree)>0:
        replace_netcdf_variable_recursive(output_grp,data_grp,tree[0],tree[1:],hdf5=hdf5,check_empty=check_empty)
    else:
        netcdf_pointers=read_soft_links.read_netCDF_pointers(data_grp)
        netcdf_pointers.replicate(output_grp,hdf5=hdf5,check_empty=check_empty)
        #netcdf_pointers.append(output_grp,hdf5=hdf5,check_empty=check_empty)
    return

#PUT INTO FILESYSTEM DATABASE
def record_to_output_directory(output_file_name,project_drs,options):
    data=netCDF4.Dataset(output_file_name,'r')
    data_hdf5=None
    for item in h5py.h5f.get_obj_ids():
        if 'name' in dir(item) and item.name==output_file_name:
            data_hdf5=h5py.File(item)

    output=netCDF4.Dataset(output_file_name+'.tmp','w')
    if ('reducing_soft_links_script' in dir(options) and
        options.reducing_soft_links_script==''):
        #Do not check empty:
        write_netcdf_variable_recursive(output,options.out_destination,data,project_drs.official_drs,project_drs,options,hdf5=data_hdf5,check_empty=False)
    else:
        write_netcdf_variable_recursive(output,options.out_destination,data,project_drs.official_drs,project_drs,options,hdf5=data_hdf5,check_empty=True)
    output.close()

    data.close()
    if data_hdf5!=None:
        data_hdf5.close()
    return output_file_name+'.tmp'

def write_netcdf_variable_recursive(output,out_dir,data,tree,project_drs,options,hdf5=None,check_empty=False):
    level_name=tree[0]
    if level_name=='version':
        version='v'+datetime.datetime.now().strftime('%Y%m%d')
        sub_out_dir=make_sub_dir(out_dir,version)
        options_copy=copy.copy(options)
        setattr(options_copy,level_name,[version,])
        write_netcdf_variable_recursive_replicate(output,sub_out_dir,data,version,tree,project_drs,options_copy,hdf5=hdf5,check_empty=check_empty)
    else:
        group_name=getattr(options,level_name)[0]
        if group_name==None:
            for group in data.groups.keys():
                sub_out_dir=make_sub_dir(out_dir,group)
                output_grp=netcdf_utils.create_group(output,data,group)
                options_copy=copy.copy(options)
                setattr(options_copy,level_name,[group,])
                if hdf5!=None:
                    hdf5_grp=hdf5[group]
                else:
                    hdf5_grp=hdf5
                write_netcdf_variable_recursive_replicate(output_grp,sub_out_dir,data.groups[group],group,tree,project_drs,options_copy,hdf5_grp,check_empty=check_empty)
                        
        else:
            #if hdf5!=None:
            #    print hdf5
            #    hdf5_grp=hdf5[group_name]
            #else:
            #    hdf5_grp=hdf5
            sub_out_dir=make_sub_dir(out_dir,group_name)
            output_grp=netcdf_utils.create_group(output,data,group_name)
            options_copy=copy.copy(options)
            setattr(options_copy,level_name,[group_name,])
            write_netcdf_variable_recursive_replicate(output_grp,sub_out_dir,data,group_name,tree,project_drs,options_copy,hdf5=hdf5,check_empty=check_empty)
    return

def write_netcdf_variable_recursive_replicate(output,sub_out_dir,data_grp,group_name,tree,project_drs,options,hdf5=None,check_empty=False):
    if len(tree)>1:
        write_netcdf_variable_recursive(output,sub_out_dir,data_grp,tree[1:],project_drs,options,hdf5=hdf5,check_empty=check_empty)
    else:
        #Must propagate infos down to this level
        var_with_version=[getattr(options,opt)[0] for opt in project_drs.official_drs]
        output_file_name='_'.join([getattr(options,opt)[0] for opt in project_drs.filename_drs])
        time_frequency=getattr(options,'time_frequency')[0]
        #Get the time:
        timestamp=convert_dates_to_timestamps(data_grp,time_frequency)

        if timestamp=='':
            output_file_name=None
            return output_file_name
        else:
            output_file_name+=timestamp+'.nc'

        output_file_name=sub_out_dir+'/'+output_file_name
        output_data=netCDF4.Dataset(output_file_name,'w')

        netcdf_pointers=read_soft_links.read_netCDF_pointers(data_grp)
        netcdf_pointers.replicate(output_data,hdf5=hdf5,check_empty=check_empty)
        output_data.close()

        unique_file_id_list=['checksum_type','checksum','tracking_id']
        path=os.path.abspath(os.path.expanduser(os.path.expandvars(output_file_name)))
        paths_list=[{'path': '|'.join([path,]+['' for id in unique_file_id_list]),
                     'version':options.version[0],
                     'file_type':'local_file',
                     'data_node': remote_netcdf.get_data_node(path,'local_file')},]

        netcdf_pointers=create_soft_links.create_netCDF_pointers(
                                                          paths_list,
                                                          time_frequency,options.year,options.month,
                                                          ['local_file',],
                                                          [paths_list[0]['data_node'],],
                                                          record_other_vars=True)
        netcdf_pointers.record_meta_data(output,options.var)
        output.sync()
    return

def convert_dates_to_timestamps(output_tmp,time_frequency):
    conversion=dict()
    conversion['year']=(lambda x: str(x.year).zfill(4))
    conversion['mon']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2))
    conversion['day']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2)+str(x.day).zfill(2))
    conversion['6hr']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2)+str(x.day).zfill(2)+str(x.hour).zfill(2)+str(x.minute).zfill(2)+str(x.second).zfill(2))
    conversion['3hr']=(lambda x: str(x.year).zfill(4)+str(x.month).zfill(2)+str(x.day).zfill(2)+str(x.hour).zfill(2)+str(x.minute).zfill(2)+str(x.second).zfill(2))
    if time_frequency!='fx' and len(output_tmp.variables['time'])>0:
        date_axis=netcdf_utils.get_date_axis(output_tmp.variables['time'])[[0,-1]]
        return '_'+'-'.join([conversion[time_frequency](date) for date in date_axis])
    else:
        return ''

def make_sub_dir(out_dir,group):
    #try:
    sub_out_dir=out_dir+'/'+group
    if not os.path.exists(sub_out_dir):
        os.makedirs(sub_out_dir)
    #except:
    #    pass
    return sub_out_dir

