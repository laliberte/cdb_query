#External:
import os
import glob
import copy

#External but related:
import netcdf4_soft_links.remote_netcdf.remote_netcdf as remote_netcdf

#Internal:
from ..nc_Database import db_utils

class browser:
    def __init__(self,search_path,options):
        self.file_type='local_file'
        self.options=options
        self.search_path=os.path.abspath(os.path.expanduser(os.path.expandvars(search_path)))

    def close(self):
        return

    def test_valid(self):
        return True

    def descend_tree(self,database,list_level=None):
        only_list=[]
        if self.file_type in database.header['file_type_list']:
            description={
                       'file_type':self.file_type,
                       'data_node':remote_netcdf.get_data_node(self.search_path,self.file_type),
                       'time':'0'}
            if not 'version' in database.drs.official_drs:
                description.update({'version':'v1'})
            file_expt_copy=copy.deepcopy(database.nc_Database.file_expt)
            for att in description.keys():
                setattr(file_expt_copy,att,description[att])

            only_list.append(descend_tree_recursive(database,file_expt_copy,
                                    [item for item in database.drs.base_drs if not item in description.keys()],
                                    self.search_path,
                                    self.options,list_level=list_level))

            if 'alt_base_drs' in dir(database.drs):
                only_list.append(descend_tree_recursive(database,file_expt_copy,
                                        [item for item in database.drs.alt_base_drs if not item in description.keys()],
                                        self.search_path,
                                        self.options,list_level=list_level,alt=True))
        return [item for sublist in only_list for item in sublist]

unique_file_id_list=['checksum_type','checksum','tracking_id']
def descend_tree_recursive(database,file_expt,tree_desc,top_path,options,list_level=None,alt=False):
    if not isinstance(tree_desc,list):
        return
    
    if len(tree_desc)==1:
        file_list=glob.glob(top_path+'/*.nc') 
        if len(file_list)>0:
            for file in file_list:
                file_expt_copy=copy.deepcopy(file_expt)
                #Add the file identifier to the path:
                file_expt_copy.path=file
                for unique_file_id in unique_file_id_list:
                    #Add empty identifiers:
                    file_expt_copy.path+='|'
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
        #Include only subdirectories that were specified if this level was specified:
        if db_utils.is_level_name_included_and_not_excluded(local_tree_desc,options,subdir):
            if local_tree_desc+'_list' in database.header_simple.keys():
                #We keep only the subdirectories that were requested
                if subdir in database.header_simple[local_tree_desc+'_list']:
                    subdir_list.append(subdir)
            else:
                #Keep all other subdirs as long as they are 
                #1) not latest version
                #2) of the form v{int}
                if not (local_tree_desc=='version' and 
                         (subdir=='latest' or (not RepresentsInt(subdir[1:])))):
                    subdir_list.append(subdir)

    if list_level!=None and local_tree_desc==list_level:
        return subdir_list
    else:
        only_list=[]
        for subdir in subdir_list:
            file_expt_copy=copy.deepcopy(file_expt)
            setattr(file_expt_copy,local_tree_desc,subdir)
            #if db_utils.is_level_name_included_and_not_excluded(local_tree_desc,options,subdir):
            only_list.append(descend_tree_recursive(database,file_expt_copy,
                                        next_tree_desc,top_path+'/'+subdir,
                                        options,list_level=list_level,alt=alt))
        return [item for sublist in only_list for item in sublist]

def get_immediate_subdirectories(path):
    return [name for name in os.listdir(path)
            if os.path.isdir(os.path.join(path, name))]

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
