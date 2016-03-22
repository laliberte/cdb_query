#External:
import netCDF4

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

def extract_netcdf_variable(output,data,tree,options,check_empty=False,hdf5=None,semaphores=dict(),queues=dict()):
    return extract_netcdf_variable_recursive(output,data,tree[0],tree[1:],options,check_empty,hdf5,semaphores,queues)

def extract_netcdf_variable_recursive(output,data,level_desc,tree,options,check_empty,hdf5,semaphores,queues):
    level_name=level_desc[0]
    group_name=level_desc[1]
    if group_name==None or isinstance(group_name,list):
        for group in data.groups.keys():
            if ( nc_Database.is_level_name_included_and_not_excluded(level_name,options,group) and
                 nc_Database.tree_recursive_check_not_empty(options,data.groups[group])):
                if (isinstance(output,netCDF4.Dataset) or
                    isinstance(output,netCDF4.Group)):
                    output_grp=netcdf_utils.replicate_group(output,data,group)
                else:
                    output_grp=output
                if len(tree)>0:
                    if hdf5!=None:
                        extract_netcdf_variable_recursive(output_grp,data.groups[group],tree[0],tree[1:],options,check_empty,hdf5[group],semaphores,queues)
                    else:
                        extract_netcdf_variable_recursive(output_grp,data.groups[group],tree[0],tree[1:],options,check_empty,hdf5,semaphores,queues)
                else:
                    retrieve_or_replicate(output_grp,data,group,options,check_empty,hdf5,semaphores,queues)
    else:
        if len(tree)>0:
            if group_name=='':
                extract_netcdf_variable_recursive(output,data,tree[0],tree[1:],options,check_empty,hdf5,semaphores,queues)
            elif group_name in data.groups.keys():
                if hdf5!=None:
                    extract_netcdf_variable_recursive(output,data.groups[group_name],tree[0],tree[1:],options,check_empty,hdf5[group_name],semaphores,queues)
                else:
                    extract_netcdf_variable_recursive(output,data.groups[group_name],tree[0],tree[1:],options,check_empty,hdf5,semaphores,queues)
        else:
            retrieve_or_replicate(output,data,group_name,options,check_empty,hdf5,semaphores,queues)
    return

def retrieve_or_replicate(output_grp,data,group,options,check_empty,hdf5,semaphores,queues):
    netcdf_pointers=read_soft_links.read_netCDF_pointers(data.groups[group],options=options,semaphores=semaphores,queues=queues)
    if 'download' in dir(options) and options.download:
        if (isinstance(output_grp,netCDF4.Dataset) or
            isinstance(output_grp,netCDF4.Group)):
            netcdf_pointers.retrieve(output_grp,'retrieve_path_data',options,username=options.username,user_pass=options.password)
        else:
            netcdf_pointers.retrieve(output_grp,'retrieve_path',options,username=options.username,user_pass=options.password)
    else:
        if hdf5!=None:
            netcdf_pointers.replicate(output_grp,check_empty=check_empty,hdf5=hdf5[group])
        else:
            netcdf_pointers.replicate(output_grp,check_empty=check_empty)
    return

def replace_netcdf_variable_recursive(output,data,level_desc,tree,hdf5=None,check_empty=False):
    level_name=level_desc[0]
    group_name=level_desc[1]
    if group_name==None or isinstance(group_name,list):
        for group in data.groups.keys():
            output_grp=netcdf_utils.create_group(output,data,group)
            try:
                setattr(output_grp,'level_name',level_name)
            except:
                output_grp.setncattr('level_name',level_name)
            if len(tree)>0:
                if hdf5!=None:
                    replace_netcdf_variable_recursive(output_grp,data.groups[group],tree[0],tree[1:],hdf5=hdf5[group],check_empty=check_empty)
                else:
                    replace_netcdf_variable_recursive(output_grp,data.groups[group],tree[0],tree[1:],check_empty=check_empty)
            else:
                netcdf_pointers=read_soft_links.read_netCDF_pointers(data.groups[group])
                if hdf5!=None:
                    netcdf_pointers.replicate(output_grp,hdf5=hdf5[group],check_empty=check_empty)
                else:
                    netcdf_pointers.replicate(output_grp,hdf5=hdf5,check_empty=check_empty)
                    
    else:
        output_grp=netcdf_utils.create_group(output,data,group_name)
        try:
            setattr(output_grp,'level_name',level_name)
        except:
            output_grp.setncattr('level_name',level_name)
        if len(tree)>0:
            replace_netcdf_variable_recursive(output_grp,data,tree[0],tree[1:],hdf5=hdf5,check_empty=check_empty)
        else:
            netcdf_pointers=read_soft_links.read_netCDF_pointers(data)
            netcdf_pointers.replicate(output_grp,hdf5=hdf5,check_empty=check_empty)
        #    netcdf_pointers=read_soft_links.read_netCDF_pointers(data)
        #    netcdf_pointers.replicate(output,hdf5=hdf5,check_empty=check_empty)
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
