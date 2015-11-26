
import read_soft_links

import nc_Database
import netcdf_utils

def assign_tree(output,val,sort_table,tree):
    if len(tree)>1:
        if tree[0]!='':
            assign_tree(output.groups[tree[0]],val,sort_table,tree[1:])
        else:
            assign_tree(output,val,sort_table,tree[1:])
    else:
        output.variables[tree[0]][sort_table]=val
    return

def extract_netcdf_variable_recursive(output,data,level_desc,tree,options,check_empty=False,hdf5=None):
    level_name=level_desc[0]
    group_name=level_desc[1]
    if group_name==None or isinstance(group_name,list):
        for group in data.groups.keys():
            if ( nc_Database.is_level_name_included_and_not_excluded(level_name,options,group) and
                 nc_Database.retrieve_tree_recursive_check_not_empty(options,data.groups[group])):
                output_grp=netcdf_utils.replicate_group(output,data,group)
                if len(tree)>0:
                    if hdf5!=None:
                        extract_netcdf_variable_recursive(output_grp,data.groups[group],tree[0],tree[1:],options,check_empty=check_empty,hdf5=hdf5[group])
                    else:
                        extract_netcdf_variable_recursive(output_grp,data.groups[group],tree[0],tree[1:],options,check_empty=check_empty)
                else:
                    netcdf_pointers=read_soft_links.read_netCDF_pointers(data.groups[group])
                    if hdf5!=None:
                        netcdf_pointers.replicate(output_grp,check_empty=check_empty,hdf5=hdf5[group])
                    else:
                        netcdf_pointers.replicate(output_grp,check_empty=check_empty)
    else:
        if len(tree)>0:
            if group_name=='':
                extract_netcdf_variable_recursive(output,data,tree[0],tree[1:],options,check_empty=check_empty,hdf5=hdf5)
            elif group_name in data.groups.keys():
                if hdf5!=None:
                    extract_netcdf_variable_recursive(output,data.groups[group_name],tree[0],tree[1:],options,check_empty=check_empty,hdf5=hdf5[group_name])
                else:
                    extract_netcdf_variable_recursive(output,data.groups[group_name],tree[0],tree[1:],options,check_empty=check_empty)
        else:
            netcdf_pointers=read_soft_links.read_netCDF_pointers(data.groups[group_name])
            if hdf5!=None:
                netcdf_pointers.replicate(output,check_empty=check_empty,hdf5=hdf5[group_name])
            else:
                netcdf_pointers.replicate(output,check_empty=check_empty)
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

    
