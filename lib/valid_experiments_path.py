import copy
import database_utils
from database_utils import File_Expt

import sqlalchemy

##### PATH #####
def find_path(pointers):
    pointers.session.add(pointers.file_expt)
    pointers.session.commit()
    return

def intersection(pointers):
    #Find the intersection of all models that have at least one file in each experiment
    pointers.create_database(find_path)
    print dir(File_Expt)

    #Step one: find all the center / model tuples with all the requested variables
    model_list=pointers.session.query(
                             File_Expt.center,
                             File_Expt.model,
                             File_Expt.rip
                            ).distinct().all()

    for experiment in paths_dict['diagnostic']['experiment_list'].keys():
        for var_name in paths_dict['diagnostic']['variable_list'].keys():
            if not paths_dict['diagnostic']['variable_list'][var_name][2][:2] in ['fx','en']:
                #Do this without fx variables:
                conditions=[
                             File_Expt.var==var_name,
                             File_Expt.frequency==paths_dict['diagnostic']['variable_list'][var_name][0],
                             File_Expt.realm==paths_dict['diagnostic']['variable_list'][var_name][1],
                             File_Expt.mip==paths_dict['diagnostic']['variable_list'][var_name][2],
                             File_Expt.experiment==experiment,
                           ]
                model_list_var=pointers.session.query(
                                         File_Expt.center,
                                         File_Expt.model,
                                         File_Expt.rip
                                        ).filter(sqlalchemy.and_(*conditions)).distinct().all()
                model_list=set(model_list).intersection(set(model_list_var))

        #Do it for fx variables:
        model_list_fx=[model[:-1] for model in model_list]
        for var_name in paths_dict['diagnostic']['variable_list'].keys():
            if paths_dict['diagnostic']['variable_list'][var_name][2][:2] in ['fx','en']:
                conditions=[
                             File_Expt.var==var_name,
                             File_Expt.frequency==paths_dict['diagnostic']['variable_list'][var_name][0],
                             File_Expt.realm==paths_dict['diagnostic']['variable_list'][var_name][1],
                             File_Expt.mip==paths_dict['diagnostic']['variable_list'][var_name][2],
                             File_Expt.experiment==experiment
                           ]
                model_list_var=pointers.session.query(
                                         File_Expt.center,
                                         File_Expt.model,
                                           ).filter(sqlalchemy.and_(*conditions)).distinct().all()
                model_list_fx=set(model_list_fx).intersection(set(model_list_var))


    #Step two: create the new paths dictionary:
    diag_tree_desc_final.append('path')
    new_paths_dict={}
    new_paths_dict['diagnostic']=paths_dict['diagnostic']
    new_paths_dict['data_pointers']={}

    variable_list_requested=[]
    for var_name in paths_dict['diagnostic']['variable_list'].keys():
        variable_list_requested.append((var_name,)+tuple(paths_dict['diagnostic']['variable_list'][var_name]))

    #Create a simulation list
    new_paths_dict['simulations_list']=[]

    for model in model_list:
        if model[:-1] in model_list_fx:
            new_paths_dict['simulations_list'].append('_'.join(model))

            runs_list=pointers.session.query(
                                      File_Expt
                                     ).filter(sqlalchemy.and_(
                                                                File_Expt.center==model[0],
                                                                File_Expt.model==model[1],
                                                             )).all()
            for item in runs_list:
                if item.var in paths_dict['diagnostic']['variable_list'].keys():
                    if [item.frequency,item.realm,item.mip]==paths_dict['diagnostic']['variable_list'][item.var]:
                        if item.frequency!='fx':
                            #Works if the variable is not fx:
                            if item.rip==model[2]:
                                new_paths_dict['data_pointers']=database_utils.create_tree(item,diag_tree_desc_final,new_paths_dict['data_pointers'])
                        else:
                            #If fx, we create the time axis for easy retrieval:
                            new_paths_dict['data_pointers']=database_utils.create_tree(item,diag_tree_desc_final,new_paths_dict['data_pointers'])

    new_paths_dict['simulations_list'].sort()
    return new_paths_dict

