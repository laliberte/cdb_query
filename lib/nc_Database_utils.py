#External:
import netCDF4
import h5py

#External but related:
import netcdf4_soft_links.read_soft_links as read_soft_links
import netcdf4_soft_links.netcdf_utils as netcdf_utils

#Internal:
import nc_Database

def assign_tree(output,val,sort_table,tree):
    if len(tree)>1:
        if tree[0]!='':
            assign_tree(output.groups[tree[0]],val,sort_table,tree[1:])
        else:
            assign_tree(output,val,sort_table,tree[1:])
    else:
        output.variables[tree[0]][sort_table]=val
    return

def extract_netcdf_variable(output,data,tree,options,check_empty=False,hdf5=None,download_semaphores=dict(),download_queues_manager=None,retrieval_type='reduce'):
    return extract_netcdf_variable_recursive(output,data,tree[0],tree[1:],retrieval_type,options,check_empty,hdf5,download_semaphores,download_queues_manager)

def extract_netcdf_variable_recursive(output,data,level_desc,tree,retrieval_type,options,check_empty,hdf5,download_semaphores,download_queues_manager):
    level_name=level_desc[0]
    group_name=level_desc[1]
    if group_name==None or isinstance(group_name,list):
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
                extract_netcdf_variable_recursive(output,data.groups[group_name],tree[0],tree[1:],retrieval_type,options,check_empty,hdf5[group_name],download_semaphores,download_queues_manager)
            else:
                extract_netcdf_variable_recursive(output,data.groups[group_name],tree[0],tree[1:],retrieval_type,options,check_empty,hdf5,download_semaphores,download_queues_manager)
    else:
        retrieve_or_replicate(output,data,group_name,retrieval_type,options,check_empty,hdf5,download_semaphores,download_queues_manager)
    return

def retrieve_or_replicate(output_grp,data,group,retrieval_type,options,check_empty,hdf5,download_semaphores,download_queues_manager):
    netcdf_pointers=read_soft_links.read_netCDF_pointers(data.groups[group],options=options,semaphores=download_semaphores,queues=download_queues_manager)
    if retrieval_type=='reduce':
        if ( ('applying_to_soft_links' in dir(options) and
              options.applying_to_soft_links) or
             (not 'soft_links' in data.groups[group].groups.keys())):
            #If applying to soft links, replicate.
            #If there is no soft links, replicate.
            if hdf5!=None:
                netcdf_pointers.replicate(output_grp,check_empty=check_empty,hdf5=hdf5[group])
            else:
                netcdf_pointers.replicate(output_grp,check_empty=check_empty)
        else:
            #There are soft links and they are supposed to be loaded:
            netcdf_pointers.retrieve(output_grp,'load',filepath=options.out_netcdf_file)
    else:
        if 'out_destination' in dir(options):
            netcdf_pointers.retrieve(output_grp,retrieval_type,filepath=options.out_destination)
        else:
            netcdf_pointers.retrieve(output_grp,retrieval_type,filepath=options.out_netcdf_file)
    return

def replace_netcdf_variable_recursive(output,data,level_desc,tree,hdf5=None,check_empty=False):
    level_name=level_desc[0]
    group_name=level_desc[1]
    if group_name==None or isinstance(group_name,list):
        for group in data.groups.keys():
            if len(tree)>0:
                output_grp=netcdf_utils.create_group(output,data,group)
                try:
                    setattr(output_grp,'level_name',level_name)
                except:
                    output_grp.setncattr('level_name',level_name)
                if hdf5!=None:
                    replace_netcdf_variable_recursive(output_grp,data.groups[group],tree[0],tree[1:],hdf5=hdf5[group],check_empty=check_empty)
                else:
                    replace_netcdf_variable_recursive(output_grp,data.groups[group],tree[0],tree[1:],check_empty=check_empty)
            elif not group in output.groups.keys():
                #Prevent collisions during merge. Replicate only when group does not exist.
                output_grp=netcdf_utils.create_group(output,data,group)
                try:
                    setattr(output_grp,'level_name',level_name)
                except:
                    output_grp.setncattr('level_name',level_name)
                netcdf_pointers=read_soft_links.read_netCDF_pointers(data.groups[group])
                if hdf5!=None:
                    netcdf_pointers.replicate(output_grp,hdf5=hdf5[group],check_empty=check_empty)
                else:
                    netcdf_pointers.replicate(output_grp,hdf5=hdf5,check_empty=check_empty)
                    
    else:
        if len(tree)>0:
            output_grp=netcdf_utils.create_group(output,data,group_name)
            try:
                setattr(output_grp,'level_name',level_name)
            except:
                output_grp.setncattr('level_name',level_name)
            replace_netcdf_variable_recursive(output_grp,data,tree[0],tree[1:],hdf5=hdf5,check_empty=check_empty)
        elif not group_name in output.groups.keys():
            #Prevent collisions during merge. Replicate only when group does not exist.
            output_grp=netcdf_utils.create_group(output,data,group_name)
            try:
                setattr(output_grp,'level_name',level_name)
            except:
                output_grp.setncattr('level_name',level_name)
            netcdf_pointers=read_soft_links.read_netCDF_pointers(data)
            netcdf_pointers.replicate(output_grp,hdf5=hdf5,check_empty=check_empty)
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


def record_to_netcdf_file_from_file_name(options,temp_file_name,output,project_drs):
    data=netCDF4.Dataset(temp_file_name,'r')
    data_hdf5=None
    for item in h5py.h5f.get_obj_ids():
        if 'name' in dir(item) and item.name==temp_file_name:
            data_hdf5=h5py.File(item)

    #var=[ getattr(options,opt) for opt in project_drs.official_drs_no_version]
    var=[ None for opt in project_drs.official_drs_no_version]
    tree=zip(project_drs.official_drs_no_version,var)

    if ('applying_to_soft_links' in dir(options) and
        options.applying_to_soft_links):
        #Do not check empty:
        replace_netcdf_variable_recursive(output,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=False)
    else:
        replace_netcdf_variable_recursive(output,data,tree[0],tree[1:],hdf5=data_hdf5,check_empty=True)
        data.close()

    if data_hdf5!=None:
        data_hdf5.close()
    return
