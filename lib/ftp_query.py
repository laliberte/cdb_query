import os
import glob
import copy
import retrieval_utils
import nc_Database

import ftplib

class browser:
    def __init__(self,search_path,options):
        self.file_type='FTPServer'
        self.options=options
        self.search_path=search_path.rstrip('/')
        self.data_node=retrieval_utils.get_data_node(self.search_path,self.file_type)
        if (self.options.username!=None and 
            'password' in dir(self.options) and
            self.options.password!=None):
            #Use credentials:
            self.ftp=ftplib.FTP(self.data_node.split('/')[2],self.options.username,self.options.password)

        else:
            #Do not use credentials and hope for anonymous:
            self.ftp=ftplib.FTP(self.data_node.split('/')[2])
        return

    def close(self):
        self.ftp.close()
        return

    def descend_tree(self,database,list_level=None):
        only_list=[]
        if self.file_type in database.header['file_type_list']:
            description={
                       'file_type':self.file_type,
                       'data_node':self.data_node,
                       'time':'0'}
            file_expt_copy=copy.deepcopy(database.nc_Database.file_expt)
            for att in description.keys():
                setattr(file_expt_copy,att,description[att])

            only_list.append(descend_tree_recursive(database,file_expt_copy,
                                    [item for item in database.drs.base_drs if not item in description.keys()],
                                    self.search_path,
                                    self.options,self.ftp,list_level=list_level))

            if 'alt_base_drs' in dir(database.drs):
                only_list.append(descend_tree_recursive(database,file_expt_copy,
                                        [item for item in database.drs.alt_base_drs if not item in description.keys()],
                                        self.search_path,
                                        self.options, self.ftp,list_level=list_level,alt=True))
        return [item for sublist in only_list for item in sublist]

unique_file_id_list=['checksum_type','checksum','tracking_id']
def descend_tree_recursive(database,file_expt,tree_desc,top_path,options,ftp,list_level=None,alt=False):
    if not isinstance(tree_desc,list):
        return

    #Make sure we're at the top_path:
    try:
        ftp.cwd('/'+'/'.join(top_path.split('/')[3:]))
    except ftplib.error_perm:
        return []

    if len(tree_desc)==1:
        #If we're at the end of the tree, we should expect files:
        file_list_raw=ftp.nlst()
        #ftp.voidcmd("TYPE I")
        file_list=[file_name for file_name in file_list_raw
                        if (len(file_name)>3 and file_name[-3:]=='.nc' )]
                        #if (len(file_name)>3 and file_name[-3:]=='.nc' and ftp.size(file_name)>0)]
        #ftp.voidcmd("TYPE A")

        if len(file_list)>0:
            for file in file_list:
                file_expt_copy=copy.deepcopy(file_expt)
                #Add the file identifier to the path:
                file_expt_copy.path=top_path+'/'+file
                for unique_file_id in unique_file_id_list:
                    #file_expt_copy.path='|'.join([file,retrieval_utils.md5_for_file(open(file,'r'))])
                    file_expt_copy.path+='|'
                if alt: 
                    file_expt_copy.model_version=file_expt_copy.model.split('-')[1]
                    file_expt_copy.model='-'.join([file_expt_copy.institute,file_expt_copy.model.split('-')[0]])
                database.nc_Database.session.add(file_expt_copy)
                database.nc_Database.session.commit()
        return file_list

    #We're not at the end of the tree, we should expect directories:
    local_tree_desc=tree_desc[0]
    next_tree_desc=tree_desc[1:]

    subdir_list=[]
    #Loop through subdirectories:
    for subdir in ftp.nlst():
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
            #Include only subdirectories that were specified if this level was specified:
            if nc_Database.is_level_name_included_and_not_excluded(local_tree_desc,options,subdir):
                only_list.append(descend_tree_recursive(database,file_expt_copy,
                                            next_tree_desc,top_path+'/'+subdir,
                                            options,ftp,list_level=list_level,alt=alt))
        return [item for sublist in only_list for item in sublist]

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
