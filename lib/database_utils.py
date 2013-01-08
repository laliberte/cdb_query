import sqlalchemy
import sqlalchemy.orm

import copy

class File_Expt(object):
    #Create a class that can be used with sqlachemy:
    def __init__(self,diag_tree_desc):
        for tree_desc in diag_tree_desc:
            setattr(self,tree_desc,'')

def load_database(diag_tree_desc):
    #This function creates an in-memory sqlite database, for easy subselecting.

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
    for key in keys_dict.keys():
        setattr(file_expt,key,keys_dict[key])
    session.add(file_expt)
    session.commit()

def create_tree(item,diag_tree_desc,paths_dict):
    #This function recursively creates the output dictionary from one database entry:
    if len(diag_tree_desc)>1 and isinstance(diag_tree_desc,list):
        #At each call, diag_tree_desc is reduced by one element.
        #If it is a list of more than one element, we continue the recursion.

        if '_name' not in paths_dict.keys():
            #This branch has not been created yet, so create it:
            paths_dict['_name']=diag_tree_desc[0]
        dict_name=getattr(item,diag_tree_desc[0])

        if dict_name not in paths_dict.keys():
            #This branch name has not been created it, so create it:
            if len(diag_tree_desc)==2:
                paths_dict[dict_name]=[]
            else:
                paths_dict[dict_name]={}
        #Recursively create tree:
        paths_dict[dict_name]==create_tree(item,diag_tree_desc[1:],paths_dict[dict_name])
    else:
        #The end of the recursion has been reached. This is 
        #to ensure a robust implementation.
        if isinstance(diag_tree_desc,list):
            diag_tree_desc=diag_tree_desc[0]

        paths_dict.append(getattr(item,diag_tree_desc))
    return paths_dict
        
def create_database_from_tree(session,file_expt,paths_dict,top_name,propagated_values,find_function,tree_desc):
    if isinstance(paths_dict,dict):
        for value in paths_dict.keys():
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

def slice_data(options,paths_dict):
    if isinstance(paths_dict,dict):
        if '_name' not in paths_dict.keys():
            raise IOError('Dictionnary passed to slice_data must have a _name entry at each level except the last')

        level_name=paths_dict['_name']

        #Delete the values that were not mentioned:
        if level_name in dir(options):
            for value in paths_dict.keys():
                if value[0]!='_' and (value!=getattr(options,level_name)):
                    del paths_dict[value]

        for value in paths_dict.keys():
                if value[0]!='_':
                    paths_dict[value]=slice_data(options,paths_dict[value])
    return paths_dict
    
