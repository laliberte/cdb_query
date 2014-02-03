import os
import glob
import copy
import netcdf_utils

def get_immediate_subdirectories(path):
    return [name for name in os.listdir(path)
                if os.path.isdir(os.path.join(path, name))]

def descend_tree(pointers,header,header_simple,search_path,list_level=''):
    filesystem_file_type='local_file'
    institutes_list=[]
    if filesystem_file_type in header['file_type_list']:
        description={'search':search_path,
                   'file_type':filesystem_file_type,
                   'time':'0'}
        for att in description.keys():
            setattr(pointers.file_expt,att,description[att])
        if list_level:
            institutes_list.append(descend_tree_recursive(header_simple,pointers,[item for item in pointers.tree_desc if not item in description.keys()],
                                    os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))),list_level=list_level))
        else:
            descend_tree_recursive(header_simple,pointers,[item for item in pointers.tree_desc if not item in description.keys()],
                                    os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))
    if list_level:
        return institutes_list
    else:
        return

def descend_tree_recursive(header_simple,pointers,tree_desc,top_path,list_level=None):
    if not isinstance(tree_desc,list):
        return

    if len(tree_desc)==1:
        file_list=glob.glob(top_path+'/*.nc') 
        if len(file_list)>0:
            for file in file_list:
                pointers.file_expt.path='|'.join([file,netcdf_utils.md5_for_file(open(file,'r'))])
                pointers.add_item()
        return
    else:
        local_tree_desc=tree_desc[0]
        next_tree_desc=tree_desc[1:]

    subdir_list=[]
    #Loop through subdirectories:
    for subdir in get_immediate_subdirectories(top_path):
        if local_tree_desc+'_list' in header_simple.keys():
            #We keep only the subdirectories that were requested
            if subdir in header_simple[local_tree_desc+'_list']:
                subdir_list.append(subdir)
        else:
            #We also keep the subdirectories if they represent versions
            if not (local_tree_desc=='version' and subdir=='latest'):
                subdir_list.append(subdir)

    if list_level!=None:
        if local_tree_desc==list_level:
            return subdir_list
        else:
            only_list=[]
            for subdir in subdir_list:
                setattr(pointers.file_expt,local_tree_desc,subdir)
                only_list=descend_tree_recursive(header_simple,pointers,next_tree_desc,top_path+'/'+subdir)
            return [item for sublist in only_list for item in sublist]
    else:
        for subdir in subdir_list:
            setattr(pointers.file_expt,local_tree_desc,subdir)
            descend_tree_recursive(header_simple,pointers,next_tree_desc,top_path+'/'+subdir)
        return
