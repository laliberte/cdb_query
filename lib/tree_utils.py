import sqlalchemy
import sqlalchemy.orm
import numpy as np
import datetime
import os

import netcdf_utils

import copy

#Taken from https://gist.github.com/hrldcpr/2012250
from collections import defaultdict
def tree_type(): return defaultdict(tree_type)

class Tree:
    def __init__(self,tree_desc):
        #Defines the tree structure:
        self.clear_tree(tree_desc)

        #Create an in-memory sqlite database, for easy subselecting.
        #Uses sqlalchemy

        engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=False)
        metadata = sqlalchemy.MetaData(bind=engine)

        self.time_db = sqlalchemy.Table('time_db',metadata,
                sqlalchemy.Column('case_id',sqlalchemy.Integer,primary_key=True),
                *(sqlalchemy.Column(level_name, sqlalchemy.String(255)) for level_name in self.tree_desc)
                )
        metadata.create_all()
        sqlalchemy.orm.clear_mappers()
        sqlalchemy.orm.mapper(File_Expt,self.time_db)

        self.session = sqlalchemy.orm.create_session(bind=engine, autocommit=False, autoflush=True)
        self.file_expt = File_Expt(self.tree_desc)
        self._database_created=False
        return

    def create_database(self,find_function):
        if not self._database_created:
            create_database_from_tree(self,
                                      self.file_expt,
                                      self.tree,
                                      find_function)
            self._database_created=True
        return

    def clear_tree(self,tree_desc):
        self.tree_desc=tree_desc
        self.tree=tree_type()
        return

    def add_item(self):
        item_desc=[getattr(self.file_expt,level) for level in self.tree_desc]
        add_item_recursive(self.tree,self.tree_desc,item_desc)
        return

    def attribute_item(self,item):
        for val in dir(item):
            if val[0]!='_':
                setattr(self.file_expt,val,getattr(item,val))
        return

    def slice(self,options):
        #Removes branches of a tree
        num_remaining_values=slice_recursive(self.tree,options)
        if num_remaining_values==0:
            del self.tree
            self.tree=tree_type()
        return

    def level_list(self,level_to_list):
        return level_list_recursive(self.tree,level_to_list)

    def level_list_last(self):
        return level_list_recursive(self.tree,'last')

    def simplify(self,header):
        simplify_recursive(self.tree,header)
        return 

    def replace_last(self,level_equivalence):
        branch_desc=dict()
        replace_last_level(self.tree,level_equivalence,branch_desc)
        return 

def add_item_recursive(tree,tree_desc,item_desc):
    #This function recursively creates the output dictionary from one database entry:
    if len(tree_desc)>1 and isinstance(tree_desc,list):
        #At each call, tree_desc is reduced by one element.
        #If it is a list of more than one element, we continue the recursion.
        if '_name' not in tree.keys():
            #This branch has not been created yet, so create it:
            tree['_name']=tree_desc[0]
        
        level_name=item_desc[0]
        if level_name not in tree.keys() and tree_desc[1]=='path':
            #This branch name has not been created, so create it:
            tree[level_name]=[]

        #Recursively create tree:
        add_item_recursive(tree[level_name],tree_desc[1:],item_desc[1:])
    else:
        #The end of the recursion has been reached. This is 
        #to ensure a robust implementation.
        if item_desc[0] not in tree:
            tree.append(item_desc[0])
    return

def slice_recursive(tree,options):
    if isinstance(tree,dict) and tree:
        if '_name' not in tree.keys():
            raise IOError('The tree in slice must have _name entry at each level except the last')

        level_name=tree['_name']

        #Delete the values that were not mentioned:
        if level_name in [opt for opt in dir(options) if getattr(options,opt)]:
            for value in [item for item in tree.keys() if item[0]!='_']:
                if level_name!='time':
                    value_list=getattr(options,level_name)
                    if not isinstance(value_list,list): value_list=[value_list]
                    if (not value in [str(item) for item in value_list]):del tree[value]
                else:
                    #special treatment for time:
                    date_obj=datetime.datetime(int(value[:-2]),int(value[-2:]),1)
                    for unit in ['year','month','day','hour','minute','second']:
                        if unit in dir(options) and getattr(options,unit) and getattr(options,unit)!=getattr(date_obj,unit):
                            if value in tree.keys(): del tree[value]

        list_of_remaining_values=[item for item in tree.keys() if item[0]!='_']
        #Loop over the remaining values:
        for value in list_of_remaining_values:
            remaining_entries=slice_recursive(tree[value],options)
            if remaining_entries==0: del tree[value]

        #Return the number of remaining values:
        return len([item for item in tree.keys() if item[0]!='_'])
    else:
        return 1

def level_list_recursive(tree,level_to_list):
    if isinstance(tree,dict) and tree:
        if '_name' not in tree.keys():
            raise IOError('Dictionnary passed to list_level_recursive must have a _name entry at each level except the last')

        level_name=tree['_name']
        if level_to_list==level_name:
            list_of_names=[item for item in tree.keys() if item[0]!='_']
        else:
            list_of_names=list(sorted(set(
                               [ name for sublist in [level_list_recursive(tree[value],level_to_list) for value 
                                in [item for item in tree.keys() if item[0]!='_'] ] for name in sublist ]
                                )))
    elif level_to_list=='last' and tree:
        list_of_names=[tree[0]]
    else:
        list_of_names=[]
    return list_of_names

def simplify_recursive(tree,header):
    #Simplifies the output tree to make it unique:

    if isinstance(tree,dict) and tree:
        if '_name' not in tree.keys():
            raise IOError('Dictionnary passed to simplify must have a _name entry at each level except the last')

        level_name=tree['_name']
        if level_name=='version':
            #the 'version' field is peculiar. Here, we use the most recent, or largest version number:
            version_list=[int(version[1:]) for version in tree.keys() if str(version)[0]!='_']

            #Keep only the last version:
            for version in [ ver for ver in version_list if ver!=max(version_list) ]:
                del tree['v'+str(version)]

            #Find unique tree by recurrence:
            simplify_recursive(tree['v'+str(max(version_list))],header)

        elif level_name+'_list' in header.keys() and isinstance(header[level_name+'_list'],list):
            #The level was not specified but an ordered list was provided in the diagnostic header.
            #Go through the list and pick the first avilable one:
            level_ordering=[level for level in header[level_name+'_list'] if level in tree.keys()]

            #Keep only the first:
            if len(level_ordering)==0:
                for level in tree.keys(): del tree[level]
            else:
                for level in level_ordering[1:]: del tree[level]
                simplify_recursive(tree[level_ordering[0]],header)
                if not tree[level_ordering[0]]:
                    for level in tree.keys(): del tree[level]
        else:
            for level in [ name for name in tree.keys() if str(name)[0]!='_' ]:
                simplify_recursive(tree[level],header)
    elif isinstance(tree,list) and tree:
        tree=[tree[0]]
    return

def replace_last_level(tree,level_equivalence,branch_desc):
    if isinstance(tree,dict):
        if '_name' not in tree.keys():
            raise IOError('Dictionnary passed to replace_last_level must have a _name entry at each level except the last')

        level_name=tree['_name']
        for value in [item for item in tree.keys() if item[0]!='_']:
            branch_desc[level_name]=value
            replace_last_level(tree[value],level_equivalence,branch_desc)
            if branch_desc[level_name] != value:
                #Change the level name if specified:
                tree[branch_desc[level_name]]=tree.pop(value)
    else:
        if tree[0] in level_equivalence.keys(): 
            tree[0]=replace_path(level_equivalence[tree[0]],branch_desc)
    return

def replace_path(path_name,branch_desc):
    #This function assumes the new path is local_file
    
    #A local copy was found. One just needs to find the indices:
    new_path_name=path_name
    if not branch_desc['frequency'] in ['fx','clim']:
        year_axis, month_axis = netcdf_utils.get_year_axis(path_name)
        if year_axis is None or month_axis is None:
            raise IOError('replace_path netcdf file could not be opened')

        year_id = np.where(year_axis==int(branch_desc['time'][:4]))[0]
        if branch_desc['frequency'] in ['yr']:
            month_id=year_id
        else:
            month_id = year_id[np.where(int(branch_desc['time'][-2:])==month_axis[year_id])[0]]

        new_path_name=path_name+'|'+str(np.min(month_id))+'|'+str(np.max(month_id))

    #Change the file_type to "local_file"
    branch_desc['file_type']='local_file'
    return new_path_name

#####################################################################
#####################################################################
#########################  DATABASE CONVERSION
#####################################################################
#####################################################################

def create_database_from_tree(pointers,file_expt,tree,find_function):
    #Recursively creates a database from tree:
    if isinstance(tree,dict):
        for value in [key for key in tree.keys() if key]:
            if value[0] != '_':
                file_expt_copy=copy.deepcopy(file_expt)
                setattr(file_expt_copy,tree['_name'],value)
                create_database_from_tree(pointers,file_expt_copy,tree[value],find_function)
    else:
        for value in tree:
            file_expt_copy=copy.deepcopy(file_expt)
            file_expt_copy.path=value
            find_function(pointers,file_expt_copy)
    return

class File_Expt(object):
    #Create a class that can be used with sqlachemy:
    def __init__(self,diag_tree_desc):
        for tree_desc in diag_tree_desc:
            setattr(self,tree_desc,'')
