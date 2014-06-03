import os
import glob
import copy
import retrieval_utils
import nc_Database

def get_immediate_subdirectories(path):
    return [name for name in os.listdir(path)
                if os.path.isdir(os.path.join(path, name))]

def descend_tree(database,search_path,options,list_level=None):
    filesystem_file_type='local_file'
    only_list=[]
    expanded_search_path=os.path.abspath(os.path.expanduser(os.path.expandvars(search_path)))
    if filesystem_file_type in database.header['file_type_list']:
        description={
                   'file_type':filesystem_file_type,
                   'data_node':retrieval_utils.get_data_node(expanded_search_path,filesystem_file_type),
                   'time':'0'}
        file_expt_copy=copy.deepcopy(database.nc_Database.file_expt)
        for att in description.keys():
            setattr(file_expt_copy,att,description[att])

        only_list.append(descend_tree_recursive(database,file_expt_copy,
                                [item for item in database.drs.base_drs if not item in description.keys()],
                                expanded_search_path,
                                options,list_level=list_level))

        if 'alt_base_drs' in dir(database.drs):
            only_list.append(descend_tree_recursive(database,file_expt_copy,
                                    [item for item in database.drs.alt_base_drs if not item in description.keys()],
                                    expanded_search_path,
                                    options,list_level=list_level,alt=True))
    return [item for sublist in only_list for item in sublist]
    #if len(only_list)>0:
    #    return set(only_list[0]).intersection(*only_list)
    #else:
    #    return []

def descend_tree_recursive(database,file_expt,tree_desc,top_path,options,list_level=None,alt=False):
    if not isinstance(tree_desc,list):
        return

    if len(tree_desc)==1:
        file_list=glob.glob(top_path+'/*.nc') 
        if len(file_list)>0:
            for file in file_list:
                file_expt_copy=copy.deepcopy(file_expt)
                #file_expt_copy.path='|'.join([file,retrieval_utils.md5_for_file(open(file,'r'))])
                file_expt_copy.path=file+'|'
                if alt: 
                    file_expt_copy.model_version=file_expt_copy.model.split('-')[1]
                    file_expt_copy.model='-'.join([file_expt_copy.institute,file_expt_copy.model.split('-')[0]])
                database.nc_Database.session.add(file_expt_copy)
                database.nc_Database.session.commit()
        return file_list

    local_tree_desc=tree_desc[0]
    next_tree_desc=tree_desc[1:]


    subdir_list=[]
    #Loop through subdirectories:
    for subdir in get_immediate_subdirectories(top_path):
        if local_tree_desc+'_list' in database.header_simple.keys():
            #We keep only the subdirectories that were requested
            if subdir in database.header_simple[local_tree_desc+'_list']:
                subdir_list.append(subdir)
        else:
            #We also keep the subdirectories if they represent versions
            if not (local_tree_desc=='version' and subdir=='latest'):
                subdir_list.append(subdir)

    if list_level!=None and local_tree_desc==list_level:
        return subdir_list
    else:
        only_list=[]
        for subdir in subdir_list:
            file_expt_copy=copy.deepcopy(file_expt)
            setattr(file_expt_copy,local_tree_desc,subdir)
            #Include only subdirectories that were specified if this level was specified:
            if nc_Database.is_level_name_included_and_not_excluded(local_tree_desc,options,subdir):
                only_list.append(descend_tree_recursive(database,file_expt_copy,
                                            next_tree_desc,top_path+'/'+subdir,
                                            options,list_level=list_level,alt=alt))
        return [item for sublist in only_list for item in sublist]
