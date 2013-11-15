import os
import glob
import copy

def get_immediate_subdirectories(path):
    return [name for name in os.listdir(path)
                if os.path.isdir(os.path.join(path, name))]

def descend_tree(pointers,header,header_simple,search_path):
    filesystem_file_type='local_file'
    if filesystem_file_type in header['file_type_list']:
        description={'search':search_path,
                   'file_type':filesystem_file_type,
                   'time':0}
        for att in description.keys():
            setattr(pointers.file_expt,att,description[att])

        descend_tree_recursive(header_simple,pointers,[item for item in pointers.tree_desc if not item in description.keys()],
                                os.path.abspath(os.path.expanduser(os.path.expandvars(search_path))))
    return

def descend_tree_recursive(header_simple,pointers,tree_desc,top_path):
    if not isinstance(tree_desc,list):
        return

    if len(tree_desc)==1:
        file_list=glob.glob(top_path+'/*.nc') 
        if len(file_list)>0:
            for file in file_list:
                pointers.file_expt.path='|'.join(file,netcdf_utils.md5_for_file(file))
                pointers.add_item()
        return
    else:
        local_tree_desc=tree_desc[0]
        next_tree_desc=tree_desc[1:]

    subdir_list=[]
    for subdir in get_immediate_subdirectories(top_path):
        if local_tree_desc+'_list' in header_simple.keys():
            if subdir in header_simple[local_tree_desc+'_list']:
                subdir_list.append(subdir)
        else:
            if not (local_tree_desc=='version' and subdir=='latest'):
                subdir_list.append(subdir)

    for subdir in subdir_list:
        setattr(pointers.file_expt,local_tree_desc,subdir)
        descend_tree_recursive(
                              header_simple,
                              pointers,
                              next_tree_desc,
                              top_path+'/'+subdir)
    return
