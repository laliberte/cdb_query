import os
import glob

def get_immediate_subdirectories(path):
    return [name for name in os.listdir(path)
                if os.path.isdir(os.path.join(path, name))]

def descend_tree(diag_desc,diag_tree_desc,top_path):
    paths_dict={}

    if isinstance(diag_tree_desc,list):
        if len(diag_tree_desc)==0:
            file_list=glob.glob(top_path+'/*.nc') 
            if len(file_list)>0:
                paths_dict={}
                paths_dict['_name']='file_type'
                paths_dict['local_file']=file_list
            return paths_dict
        else:
            local_tree_desc=diag_tree_desc[0]
            next_diag_tree_desc=diag_tree_desc[1:]
    else:
        local_tree_desc=diag_tree_desc
        next_diag_tree_desc=[]
    paths_dict['_name']=diag_tree_desc[0]

    subdir_list=[]
    for subdir in get_immediate_subdirectories(top_path):
        if local_tree_desc+'_list' in diag_desc.keys():
            if subdir in diag_desc[local_tree_desc+'_list']:
                subdir_list.append(subdir)
        else:
            if not (paths_dict['_name']=='version' and subdir=='latest'):
                subdir_list.append(subdir)

    for subdir in subdir_list:
        paths_dict[subdir] = descend_tree(
                                          diag_desc,
                                          next_diag_tree_desc,
                                          top_path+'/'+subdir)
    return paths_dict
