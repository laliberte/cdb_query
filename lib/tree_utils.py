import sqlalchemy
import sqlalchemy.orm
import numpy as np

import copy

#Taken from https://gist.github.com/hrldcpr/2012250
from collections import defaultdict
def tree_type(): return defaultdict(tree)

class Tree:

    def __init__(self,tree_desc):
        #Defines the tree structure:
        self.tree_desc=tree_desc
        self.tree=tree_type()
        return

    def add_item(self,item):
        #Add a Namespace item
        item_ordering=[getattr(item,level) for level in self.tree_desc]

        add_item_data(self.tree,zip(self.tree_desc,item_ordering))
        return

    def slice(self,options):
        #Removes branches of a tree
        num_remaining_values=slice_data(self,options)
        if num_remaining_values==0:
            del self.tree
            self.tree=tree_type()
        return

    def level_list(self,level_to_list):
        return level_list_data(self.tree,level_to_list)

    def simplify(self,diag_desc):
            simplify_data(self.tree,diag_desc)
        return 

    def replace_last(self,level_equivalence):
            branch_desc=dict()
            replace_last_level(self.tree,level_equivalence,branch_desc)
        return 

def add_item_data(tree,tree_desc):
    #This function recursively creates the output dictionary from one database entry:
    if len(tree_desc)>1 and isinstance(tree_desc,list):
        #At each call, tree_desc is reduced by one element.
        #If it is a list of more than one element, we continue the recursion.
        if '_name' not in tree.keys():
            #This branch has not been created yet, so create it:
            ['_name']=tree_desc[0][0]
        
        level_name=tree_desc[0][1]
        if level_name not in tree.keys() and len(tree_desc)==2::
            #This branch name has not been created, so create it:
            tree[level_name]=[]

        #Recursively create tree:
        add_item_data(tree[level_name],tree_desc)
    else:
        #The end of the recursion has been reached. This is 
        #to ensure a robust implementation.
        if tree_desc[1] not in tree:
            tree.append(tree[1]
    return

def slice_data(tree,options):
    if isinstance(tree,dict):
        if '_name' not in tree.keys():
            raise IOError('The tree in slice must have _name entry at each level except the last')

        level_name=tree['_name']

        #Delete the values that were not mentioned:
        if level_name in [opt for opt in dir(options) if getattr(options,opt)]:
            for value in [item for item in tree.keys() if item[0]!='_']:
                if (value!=str(getattr(options,level_name))):del tree[value]

        list_of_remaining_values=[item for item in tree.keys() if item[0]!='_']
        #Loop over the remaining values:
        for value in list_of_remaining_values:
            remaining_entries=slice_data(tree[value],options)
            if len(remaining_entries)=0: del tree[value]

    #Return the number of remaining values:
    return len([item for item in tree.keys() if item[0]!='_'])

def level_list_data(tree,level_to_list):
    if isinstance(tree,dict):
        if '_name' not in tree.keys():
            raise IOError('Dictionnary passed to list_level must have a _name entry at each level except the last')

        level_name=tree['_name']
        if level_to_list==level_name:
            list_of_names=[item for item in tree.keys() if item[0]!='_']
        else:
            list_of_names=[ name for sublist in [level_list_data(tree[value],level_to_list) for value 
                                in [item for item in tree.keys() if item[0]!='_'] ] for name in sublist ]
    return list(sorted(set(list_of_names)))

def simplify_data(tree,diag_desc):
    #Simplifies the output tree to make it unique:
    if not isinstance(tree,dict):
        return

    if '_name' not in tree.keys():
        raise IOError('Dictionnary passed to unique_tree must have a _name entry at each level except the last')

    level_name=tree['_name']
    if level_name=='version':
        #the 'version' field is peculiar. Here, we use the most recent, or largest version number:
        version_list=[int(version[1:]) for version in tree.keys() if str(version)[0]!='_']

        #Keep only the last version:
        for version in [ ver for ver in version_list if ver!=max(version_list) ]:
            del tree['v'+str(version)]

        #Find unique tree by recurrence:
        unique_tree(tree['v'+str(max(version_list))],diag_desc)

    elif level_name+'_list' in diag_desc.keys() and isinstance(diag_desc[level_name+'_list'],list):
        #The level was not specified but an ordered list was provided in the diagnostic header.
        #Go through the list and pick the first avilable one:
        level_ordering=[level for level in diag_desc[level_name+'_list'] if level in tree.keys()]

        #Keep only the first:
        if len(level_ordering)>1:
            for level in level_ordering[1:]: del tree[level]

        unique_tree(tree[level_ordering[0]],diag_desc)
    else:
        for level in [ name for name in tree.keys() if str(name)[0]!='_' ]:
            unique_tree(tree[level],diag_desc)
    return

def replace_last_level(tree,level_equivalence,branch_desc):
    if isinstance(tree,dict):
        if '_name' not in tree.keys():
            raise IOError('Dictionnary passed to replace_last_level must have a _name entry at each level except the last')

        level_name=tree['_name']
        for value in [item for item in tree.keys() if item[0]!='_']:
            branch_desc[level_name]=value
            replace_level(tree[value],level_equivalence,branch_desc)
            if branch_desc[level_name] != value:
                #Change the level name if specified:
                tree[branch_desc[level_name]]=tree.pop(value)
    else:
        if tree[0] in level_equivalence.keys(): 
            tree=[replace_path(level_equivalence[tree[0]],branch_desc)[
    return

def replace_path(path_name,branch_desc)
    import valid_experiments_months
    #This function assumes the new path is local_file
    
    #A local copy was found. One just needs to find the indices:
    new_path_name=path_name
    if not branch_desc['frequency'] in ['fx','clim']:
        year_axis, month_axis = valid_experiments_months.get_year_axis(path_name)
        if year_axis is None or month_axis is None:
            raise IOError('replace_path netcdf file could not be opened')

        year_id = np.where(year_axis==int(temp_options.year))[0]
        if temp_options.frequency in ['yr']:
            month_id=year_id
        else:
            month_id = year_id[np.where(int(temp_options.month)==month_axis[year_id])[0]]
        new_path_name=[path_name+'|'+str(np.min(month_id))+'|'+str(np.max(month_id))

    #Change the file_type to "local_file"
    branch_desc['file_type']='local_file'
    return new_path_name

#####################################################################
#####################################################################
#########################  DATABASE
#####################################################################
#####################################################################

class File_Expt(object):
    #Create a class that can be used with sqlachemy:
    def __init__(self,diag_tree_desc):
        for tree_desc in diag_tree_desc:
            setattr(self,tree_desc,'')

def load_database(diag_tree_desc):
    #This function creates an in-memory sqlite database, for easy subselecting.
    #Uses sqlalchemy

    engine = sqlalchemy.create_engine('sqlite:///:memory:', echo=False)
    metadata = sqlalchemy.MetaData(bind=engine)

    time_db = sqlalchemy.Table('time_db',metadata,
            sqlalchemy.Column('case_id',sqlalchemy.Integer,primary_key=True),
            *(sqlalchemy.Column(diag_tree, sqlalchemy.String(255)) for diag_tree in diag_tree_desc)
            )
    metadata.create_all()
    sqlalchemy.orm.clear_mappers()
    sqlalchemy.orm.mapper(File_Expt,time_db)

    session = sqlalchemy.orm.create_session(bind=engine, autocommit=False, autoflush=True)
    return session, time_db

def create_entry(session,file_expt,keys_dict):
    #Create an entry in the database
    for key in keys_dict.keys():
        setattr(file_expt,key,keys_dict[key])
    session.add(file_expt)
    session.commit()

        
def create_database_from_tree(session,file_expt,paths_dict,top_name,propagated_values,find_function,tree_desc):
    #Recursively creates a database from tree:
    if isinstance(paths_dict,dict):
        for value in [key for key in paths_dict.keys() if key]:
            if value[0] != '_':
                file_expt_copy = copy.deepcopy(file_expt)
                setattr(file_expt_copy,paths_dict['_name'],value)
                tree_desc_copy = copy.deepcopy(tree_desc)
                if len(tree_desc)>0:
                    tree_desc_copy+=paths_dict['_name']+': '+value+', '
                propagated_values_copy= copy.deepcopy(propagated_values)
                propagated_values_copy[paths_dict['_name']]=value
                create_database_from_tree(session,file_expt_copy,paths_dict[value],value,propagated_values_copy,find_function,tree_desc_copy)

                #Controls the verbosity:
                if len(tree_desc)>0 and paths_dict['_name']=='frequency': print(tree_desc[:-2]+'.')
    else:
        for value in paths_dict:
            #if len(tree_desc)>0: print(tree_desc[:-2]+'.')
            file_expt_copy = copy.deepcopy(file_expt)
            find_function(session,file_expt_copy,value,top_name,propagated_values)
    return

                    paths_dict[level]=unique_tree(paths_dict[level],diag_desc)
    return paths_dict
