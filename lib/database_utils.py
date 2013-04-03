import sqlalchemy
import sqlalchemy.orm
import numpy as np

import copy

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

def create_tree(item,diag_tree_desc,paths_dict):
    new_paths_dict=paths_dict
    #This function recursively creates the output dictionary from one database entry:
    if len(diag_tree_desc)>1 and isinstance(diag_tree_desc,list):
        #At each call, diag_tree_desc is reduced by one element.
        #If it is a list of more than one element, we continue the recursion.

        if '_name' not in new_paths_dict.keys():
            #This branch has not been created yet, so create it:
            new_paths_dict['_name']=diag_tree_desc[0]
        dict_name=getattr(item,diag_tree_desc[0])

        if dict_name not in new_paths_dict.keys():
            #This branch name has not been created, so create it:
            if len(diag_tree_desc)==2:
                new_paths_dict[dict_name]=[]
            else:
                new_paths_dict[dict_name]={}
        #Recursively create tree:
        new_paths_dict[dict_name]==create_tree(item,diag_tree_desc[1:],new_paths_dict[dict_name])
    else:
        #The end of the recursion has been reached. This is 
        #to ensure a robust implementation.
        if isinstance(diag_tree_desc,list):
            diag_tree_desc=diag_tree_desc[0]

        if getattr(item,diag_tree_desc) not in new_paths_dict:
            new_paths_dict.append(getattr(item,diag_tree_desc))
    return new_paths_dict
        
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

def slice_data(paths_dict,options):
    #Removes branches of a tree
    new_paths_dict=paths_dict

    if isinstance(new_paths_dict,dict):
        if '_name' not in new_paths_dict.keys():
            raise IOError('Dictionnary passed to slice_data must have a _name entry at each level except the last')

        level_name=new_paths_dict['_name']

        #Delete the values that were not mentioned:
        if level_name in [opt for opt in dir(options) if getattr(options,opt)]:
            for value in [item for item in paths_dict.keys() if item[0]!='_']:
                if (value!=str(getattr(options,level_name))):
                    del new_paths_dict[value]

        list_of_remaining_values=[item for item in new_paths_dict.keys() if item[0]!='_']
        #Loop over the remaining values:
        for value in list_of_remaining_values:
            new_entries=slice_data(new_paths_dict[value],options)
            if new_entries:
                new_paths_dict[value]=new_entries
            else:
                #if the slicing returned None, delete this entry:
                del new_paths_dict[value]

        #Check the remaining values:
        list_of_remaining_values=[item for item in new_paths_dict.keys() if item[0]!='_']
        if len(list_of_remaining_values)==0:
            new_paths_dict=None
    return new_paths_dict

def replace_path(paths_dict,options,path_equivalence):
    import valid_experiments_months
    import copy
    temp_options=copy.deepcopy(options)
    new_paths_dict=paths_dict

    if isinstance(paths_dict,dict):
        if '_name' not in paths_dict.keys():
            raise IOError('Dictionnary passed to replace_path must have a _name entry at each level except the last')

        level_name=paths_dict['_name']
        for value in [item for item in paths_dict.keys() if item[0]!='_']:
            setattr(temp_options,level_name,value)
            modified_dict, temp_options=replace_path(paths_dict[value],temp_options,path_equivalence)
            new_paths_dict[getattr(temp_options,level_name)]=modified_dict
            if getattr(temp_options,level_name)!=value:
                del new_paths_dict[value]
    else:
        if paths_dict[0] in path_equivalence.keys(): 
            new_path_name=path_equivalence[paths_dict[0]]
        else:
            new_path_name=None

        if new_path_name!=None:
            #A local copy was found. One just needs to find the indices:

            if temp_options.frequency in ['fx','clim']:
                new_paths_dict=[new_path_name]
            else:
                year_axis, month_axis = valid_experiments_months.get_year_axis(new_path_name)
                if year_axis is None or month_axis is None:
                    raise IOError('replace_path netcdf file could not be opened')

                year_id = np.where(year_axis==int(temp_options.year))[0]
                if temp_options.frequency in ['yr']:
                    month_id=year_id
                else:
                    month_id = year_id[np.where(int(temp_options.month)==month_axis[year_id])[0]]
                new_paths_dict=[new_path_name+'|'+str(np.min(month_id))+'|'+str(np.max(month_id))]

            #Change the file_type to "local_file"
            setattr(temp_options,'file_type','local_file')
    return new_paths_dict, temp_options
    #return new_paths_dict

def list_level(paths_dict,options,level_to_list):
    list_of_names=[]
    if isinstance(paths_dict,dict):
        if '_name' not in paths_dict.keys():
            raise IOError('Dictionnary passed to list_level must have a _name entry at each level except the last')

        level_name=paths_dict['_name']
        if level_to_list==level_name:
            list_of_names=[item for item in paths_dict.keys() if item[0]!='_']
        else:
            if level_name in dir(options):
                slice_value=getattr(options,level_name)
            else:
                slice_value=None
                
            if slice_value==None:
                for value in [item for item in paths_dict.keys() if item[0]!='_']:
                    list_of_names.extend(list_level(paths_dict[value],options,level_to_list))
            else:
                if slice_value in [item for item in paths_dict.keys() if str(item)[0]!='_']:
                    list_of_names=list_level(paths_dict[slice_value],options,level_to_list)

    return list(sorted(set(list_of_names)))

def unique_tree(paths_dict,diag_desc):
    #Simplifies the output tree to make it unique:
    if isinstance(paths_dict,dict):
        if '_name' in paths_dict.keys():
            #Read the level name:
            level_name=paths_dict['_name']
            if level_name=='version':
                #the 'version' field is peculiar. Here, we use the most recent, or largest version number:
                version_list=[version[1:] for version in paths_dict.keys() if str(version)[0]!='_' ]

                max_version=max([int(version) for version in version_list if version.isdigit()])
                #Keep only the last version:
                for version in version_list:
                    if version != str(max_version):
                        del paths_dict['v'+version]

                paths_dict[version]=unique_tree(paths_dict['v'+str(max_version)],diag_desc)
            elif level_name+'_list' in diag_desc.keys() and isinstance(diag_desc[level_name+'_list'],list):
                #The level was not specified but an ordered list was provided in the diagnostic header.
                #Go through the list and pick the first avilable one:
                level_ordering=[level for level in diag_desc[level_name+'_list'] if level in paths_dict.keys()]

                #Keep only the first:
                if len(level_ordering)>1:
                    for level in level_ordering[1:]:
                        del paths_dict[level]

                paths_dict[level_ordering[0]]=unique_tree(paths_dict[level_ordering[0]],diag_desc)
            else:
                for level in paths_dict.keys():
                    if str(level)[0]!='_':
                        paths_dict[level]=unique_tree(paths_dict[level],diag_desc)
        else:
            for level in paths_dict.keys():
                if str(level)[0]!='_':
                    paths_dict[level]=unique_tree(paths_dict[level],diag_desc)
    return paths_dict
